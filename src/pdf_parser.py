import os
import json
import logging
import io
import time
import base64
from pypdf import PdfReader, PdfWriter
import litellm

logger = logging.getLogger(__name__)

PRIMARY_MODEL = "gemini/gemini-2.5-flash"
FALLBACK_MODEL = "anthropic/claude-sonnet-4-6"

_quota_exhausted_models: set[str] = set()

# The free-tier Gemini quota is per-minute (~15-20 requests). Space out primary
# (Gemini) calls so batch runs stay under it and don't fall back to the paid
# Claude model. Override via GAJANA_GEMINI_MIN_INTERVAL (seconds; 0 disables).
_DEFAULT_PRIMARY_INTERVAL = 4.0
_last_primary_call = 0.0


def _is_primary_model(model: str) -> bool:
    return not ("anthropic" in model or "claude" in model)


def _throttle_primary() -> None:
    """Sleep so consecutive primary-model calls are spaced out, avoiding the
    free-tier per-minute rate limit."""
    global _last_primary_call
    interval = float(
        os.environ.get("GAJANA_GEMINI_MIN_INTERVAL", _DEFAULT_PRIMARY_INTERVAL)
    )
    if interval <= 0:
        return
    wait = interval - (time.monotonic() - _last_primary_call)
    if wait > 0:
        logger.info(f"Throttling {wait:.1f}s before primary call (rate limit).")
        time.sleep(wait)
    _last_primary_call = time.monotonic()


_EXTRACTION_PROMPT = """
You are a precise data extraction assistant.
Extract all financial transactions from the provided bank statement.
Only extract actual transactions. Ignore opening/closing balances, rewards points, page headers, or other summary text.

Return a JSON object with a single key "transactions" containing an array of objects.
Each object must have exactly these fields:
- "date": The transaction date in YYYY-MM-DD format.
- "description": The exact transaction details, description, narration, or payee.
- "debit": The outgoing amount (withdrawal/debit). Use empty string if no debit occurred. No currency symbols or commas.
- "credit": The incoming amount (deposit/credit). Use empty string if no credit occurred. No currency symbols or commas.
"""


def _load_secret_key(filename: str, env_var: str) -> str:
    import re

    json_path = os.path.join(os.getcwd(), "secrets", filename)
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            if not content.startswith("{"):
                content = "{" + content + "}"
            try:
                data = json.loads(content)
                if "api_key" in data:
                    return str(data["api_key"]).strip()
            except Exception:
                match = re.search(r'"api_key"\s*:\s*"([^"]+)"', content)
                if match:
                    return match.group(1).strip()
        except Exception:
            pass
    return os.environ.get(env_var, "")


def configure_api_keys() -> None:
    """Load API keys from secrets files into env vars for LiteLLM."""
    gemini_key = _load_secret_key("gemini.json", "GEMINI_API_KEY")
    if gemini_key:
        os.environ.setdefault("GEMINI_API_KEY", gemini_key)

    anthropic_key = _load_secret_key("anthropic.json", "ANTHROPIC_API_KEY")
    if anthropic_key:
        os.environ.setdefault("ANTHROPIC_API_KEY", anthropic_key)


def has_any_api_key() -> bool:
    return bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY"))


class PDFParser:
    def __init__(
        self,
        primary_model: str = PRIMARY_MODEL,
        fallback_model: str = FALLBACK_MODEL,
    ):
        configure_api_keys()
        self.primary_model = primary_model
        self.fallback_model = fallback_model
        if not has_any_api_key():
            logger.warning("No LLM API keys configured. PDF parsing will fail.")
        else:
            logger.info(
                f"PDFParser ready. Primary: {primary_model}, Fallback: {fallback_model}"
            )

    def _parse_response(self, content: str, tool_calls=None) -> list[dict]:
        if not content and tool_calls:
            content = tool_calls[0].function.arguments
        text = content.strip()
        # Strip markdown code fences (```json ... ``` or ``` ... ```)
        if text.startswith("```"):
            lines = text.splitlines()
            lines = lines[1:]  # drop opening fence line
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()
        data = json.loads(text)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for val in data.values():
                if isinstance(val, list):
                    return val
        return []

    def _call_llm_with_pdf(self, model: str, pdf_b64: str) -> list[dict]:
        if model in _quota_exhausted_models:
            raise litellm.RateLimitError(
                f"Skipping {model}: quota known exhausted this session.",
                llm_provider="",
                model=model,
            )
        # Anthropic requires "document" content block; others use image_url
        if "anthropic" in model or "claude" in model:
            pdf_content: dict = {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": pdf_b64,
                },
            }
        else:
            pdf_content = {
                "type": "image_url",
                "image_url": {"url": f"data:application/pdf;base64,{pdf_b64}"},
            }
        if _is_primary_model(model):
            _throttle_primary()
        try:
            response = litellm.completion(
                model=model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": _EXTRACTION_PROMPT},
                            pdf_content,
                        ],
                    }
                ],
                response_format={"type": "json_object"},
            )
        except litellm.RateLimitError:
            _quota_exhausted_models.add(model)
            raise
        msg = response.choices[0].message
        return self._parse_response(msg.content or "", msg.tool_calls)

    def _call_llm_with_text(self, model: str, text: str) -> list[dict]:
        if model in _quota_exhausted_models:
            raise litellm.RateLimitError(
                f"Skipping {model}: quota known exhausted this session.",
                llm_provider="",
                model=model,
            )
        if _is_primary_model(model):
            _throttle_primary()
        try:
            response = litellm.completion(
                model=model,
                messages=[
                    {
                        "role": "user",
                        "content": f"{_EXTRACTION_PROMPT}\n\nStatement Text:\n{text}",
                    }
                ],
                response_format={"type": "json_object"},
            )
        except litellm.RateLimitError:
            _quota_exhausted_models.add(model)
            raise
        msg = response.choices[0].message
        return self._parse_response(msg.content or "", msg.tool_calls)

    def parse_pdf(self, pdf_bytes: bytes, password: str = "") -> list[dict]:
        if not has_any_api_key():
            logger.error("No LLM API keys configured. Cannot parse PDF.")
            return []

        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            if reader.is_encrypted:
                if not password:
                    logger.error("PDF is encrypted but no password provided.")
                    return []
                if reader.decrypt(password) == 0:
                    logger.error("Failed to decrypt PDF. Incorrect password.")
                    return []
                writer = PdfWriter()
                for page in reader.pages:
                    writer.add_page(page)
                out_buf = io.BytesIO()
                writer.write(out_buf)
                pdf_payload_bytes = out_buf.getvalue()
            else:
                pdf_payload_bytes = pdf_bytes

            pdf_b64 = base64.b64encode(pdf_payload_bytes).decode("utf-8")

            # 1. Primary model with PDF
            try:
                logger.info(f"Parsing PDF with primary model: {self.primary_model}")
                txns = self._call_llm_with_pdf(self.primary_model, pdf_b64)
                logger.info(
                    f"Parsed {len(txns)} transactions via {self.primary_model}."
                )
                return txns
            except Exception as e:
                logger.warning(f"Primary model failed: {e}. Trying fallback...")

            # 2. Fallback model with PDF
            try:
                logger.info(f"Parsing PDF with fallback model: {self.fallback_model}")
                txns = self._call_llm_with_pdf(self.fallback_model, pdf_b64)
                logger.info(
                    f"Parsed {len(txns)} transactions via {self.fallback_model}."
                )
                return txns
            except Exception as e:
                logger.warning(f"Fallback model failed: {e}. Trying text extraction...")

            # 3. Text extraction — try primary then fallback
            text = "".join(page.extract_text() or "" for page in reader.pages)
            if not text.strip():
                logger.error("No text extractable from PDF. Cannot parse.")
                return []

            for model in (self.primary_model, self.fallback_model):
                try:
                    logger.info(f"Parsing extracted text with: {model}")
                    txns = self._call_llm_with_text(model, text)
                    logger.info(
                        f"Parsed {len(txns)} transactions via text extraction ({model})."
                    )
                    return txns
                except Exception as e:
                    logger.warning(f"Text extraction with {model} failed: {e}")

            logger.error("All models failed on text extraction.")
            return []

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {e}")
            return []
        except Exception as e:
            logger.error(f"Failed to parse PDF: {e}")
            return []
