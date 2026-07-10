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

Copy every value EXACTLY as printed on the statement. Do NOT reformat, re-order,
compute, translate, or normalise anything. Transcribe, never interpret.

Return a JSON object with two keys: "transactions" and "summary".

"transactions": an array of objects, each with exactly these fields:
- "date": The transaction DATE copied verbatim as printed (keep the original
  format, e.g. "06/06/2026" or "06 Jun 26"). Do NOT convert it to another
  format. If a time of day is also shown, omit it — the date only.
- "description": The exact transaction details, description, narration, or
  payee, copied verbatim.
- "debit": The outgoing amount (withdrawal/debit) copied verbatim. Use an empty
  string if no debit occurred. No currency symbols or commas.
- "credit": The incoming amount (deposit/credit) copied verbatim. Use an empty
  string if no credit occurred. No currency symbols or commas.

"summary": the statement's OWN printed totals, READ from its summary/total
section — do NOT compute or sum the transactions yourself; copy the figures the
statement prints. This is an independent cross-check of the transactions above.
- "total_debit": the statement's stated total of all money OUT (total
  withdrawals / total debits / total purchases / payments made by you). No
  symbols or commas. Empty string if the statement does not print such a total.
- "total_credit": the statement's stated total of all money IN (total deposits /
  total credits / payments+credits received). Empty string if not printed.
- "opening_balance": the opening / previous balance as printed. For a bank
  statement listing several accounts, use the primary transacting account (the
  one the transactions above belong to), not a fixed deposit. Empty if not shown.
- "closing_balance": the closing balance / total amount due as printed, for that
  same account. Empty if not shown.
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

    def _parse_response(self, content: str, tool_calls=None) -> tuple[list[dict], dict]:
        """Parse the LLM JSON into (transactions, summary)."""
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
        return self._transactions_from(data), self._summary_from(data)

    @staticmethod
    def _transactions_from(data) -> list[dict]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            txns = data.get("transactions")
            if isinstance(txns, list):
                return txns
            for val in data.values():  # fall back to the first list value
                if isinstance(val, list):
                    return val
        return []

    @staticmethod
    def _summary_from(data) -> dict:
        if isinstance(data, dict) and isinstance(data.get("summary"), dict):
            return data["summary"]
        return {}

    def _call_llm_with_pdf(self, model: str, pdf_b64: str) -> tuple[list[dict], dict]:
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

    def _call_llm_with_text(self, model: str, text: str) -> tuple[list[dict], dict]:
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
        """Backward-compatible wrapper: returns only the transaction list."""
        txns, _text, _summary = self.parse_pdf_with_text(pdf_bytes, password)
        return txns

    def parse_pdf_with_text(
        self,
        pdf_bytes: bytes,
        password: str = "",
        models: "list[str] | None" = None,
    ) -> "tuple[list[dict], str, dict]":
        """Parse a statement PDF; also return its text layer and summary totals.

        The text layer (never mangled, unlike vision OCR) is the token oracle;
        the ``summary`` holds the statement's own printed totals, an independent
        cross-check of the transactions. ``models`` overrides the model order
        (e.g. fallback-first on a retry). Returns
        ``(transactions, extracted_text, summary)``; text is "" and summary {}
        when unavailable.
        """
        if not has_any_api_key():
            logger.error("No LLM API keys configured. Cannot parse PDF.")
            return [], "", {}

        model_order = models or [self.primary_model, self.fallback_model]

        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            if reader.is_encrypted:
                if not password:
                    logger.error("PDF is encrypted but no password provided.")
                    return [], "", {}
                if reader.decrypt(password) == 0:
                    logger.error("Failed to decrypt PDF. Incorrect password.")
                    return [], "", {}
                writer = PdfWriter()
                for page in reader.pages:
                    writer.add_page(page)
                out_buf = io.BytesIO()
                writer.write(out_buf)
                pdf_payload_bytes = out_buf.getvalue()
            else:
                pdf_payload_bytes = pdf_bytes

            # Always extract the text layer up front — the validation oracle.
            text = "".join(page.extract_text() or "" for page in reader.pages)

            pdf_b64 = base64.b64encode(pdf_payload_bytes).decode("utf-8")

            # 1. Vision extraction (keeps table/column structure). Try each model.
            for model in model_order:
                try:
                    logger.info(f"Parsing PDF (vision) with model: {model}")
                    txns, summary = self._call_llm_with_pdf(model, pdf_b64)
                    logger.info(f"Parsed {len(txns)} transactions via {model}.")
                    return txns, text, summary
                except Exception as e:
                    logger.warning(f"Vision parse with {model} failed: {e}.")

            # 2. Fallback to text extraction when vision fails entirely.
            if not text.strip():
                logger.error("No text extractable from PDF. Cannot parse.")
                return [], "", {}

            for model in model_order:
                try:
                    logger.info(f"Parsing extracted text with: {model}")
                    txns, summary = self._call_llm_with_text(model, text)
                    logger.info(
                        f"Parsed {len(txns)} transactions via text extraction ({model})."
                    )
                    return txns, text, summary
                except Exception as e:
                    logger.warning(f"Text extraction with {model} failed: {e}")

            logger.error("All models failed on text extraction.")
            return [], text, {}

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {e}")
            return [], "", {}
        except Exception as e:
            logger.error(f"Failed to parse PDF: {e}")
            return [], "", {}
