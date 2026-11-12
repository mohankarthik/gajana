import base64
import io
import json
import logging
from pypdf import PdfReader, PdfWriter

from src import llm_client

logger = logging.getLogger(__name__)

PRIMARY_MODEL = "gemini"
FALLBACK_MODEL = "claude"


def _model_to_family(model: str) -> str | None:
    """Map a model or family identifier to a gateway family ('gemini' or 'claude')."""
    m = (model or "").strip().lower()
    if m == "gemini" or m.startswith("gemini/") or "gemini" in m:
        return "gemini"
    if (
        m == "claude"
        or m.startswith("claude")
        or m.startswith("anthropic/")
        or "claude" in m
        or "anthropic" in m
    ):
        return "claude"
    return None


def password_candidates(password: "str | list[str] | None") -> list[str]:
    """Normalize a configured password value into an ordered candidate list.

    A ``passwords.json`` entry may be a single password or a list of them
    (current one first, older ones after) -- banks rotate statement passwords
    without warning, and previously downloaded statements keep the password they
    were issued under, so both have to stay openable.
    """
    if password is None:
        return []
    if isinstance(password, str):
        return [password] if password else []
    return [p for p in password if isinstance(p, str) and p]


def _try_decrypt(reader: PdfReader, candidates: list[str]) -> bool:
    """Try each candidate password in order; True on the first that unlocks.

    Passwords are never logged -- only their position in the list.
    """
    for idx, pw in enumerate(candidates, 1):
        try:
            if reader.decrypt(pw) != 0:
                if idx > 1:
                    logger.info(f"PDF decrypted with backup password #{idx}.")
                return True
        except Exception as e:
            logger.warning(f"Decrypt attempt #{idx} errored: {e}")
    return False


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


def configure_api_keys() -> None:
    """Backward-compatible no-op; the host LLM gateway manages credentials."""
    return None


def has_any_api_key() -> bool:
    """Return True when the host LLM gateway Unix socket is reachable."""
    return llm_client.is_available()


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
            logger.warning("LLM gateway unavailable. PDF parsing will fail.")
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

    def _call_llm_with_pdf(
        self, model: str, pdf_payload: bytes | str
    ) -> tuple[list[dict], dict]:
        raw_bytes = (
            base64.b64decode(pdf_payload)
            if isinstance(pdf_payload, str)
            else pdf_payload
        )
        resp = llm_client.complete(
            prompt=_EXTRACTION_PROMPT,
            attachments=[
                {
                    "filename": "statement.pdf",
                    "data": raw_bytes,
                    "media_type": "application/pdf",
                }
            ],
            response_format="json",
            tier="fast",
            family=_model_to_family(model),
            title="gajana_pdf",
        )
        raw_text = resp.get("text") or json.dumps(resp["data"])
        return self._parse_response(raw_text, None)

    def _call_llm_with_text(self, model: str, text: str) -> tuple[list[dict], dict]:
        resp = llm_client.complete(
            prompt=f"{_EXTRACTION_PROMPT}\n\nStatement Text:\n{text}",
            response_format="json",
            tier="fast",
            family=_model_to_family(model),
            title="gajana_pdf_text",
        )
        raw_text = resp.get("text") or json.dumps(resp["data"])
        return self._parse_response(raw_text, None)

    def parse_pdf(
        self, pdf_bytes: bytes, password: "str | list[str]" = ""
    ) -> list[dict]:
        """Backward-compatible wrapper: returns only the transaction list."""
        txns, _text, _summary = self.parse_pdf_with_text(pdf_bytes, password)
        return txns

    def parse_pdf_with_text(
        self,
        pdf_bytes: bytes,
        password: "str | list[str]" = "",
        models: "list[str] | None" = None,
    ) -> "tuple[list[dict], str, dict]":
        """Parse a statement PDF; also return its text layer and summary totals.

        The text layer (never mangled, unlike vision OCR) is the token oracle;
        the ``summary`` holds the statement's own printed totals, an independent
        cross-check of the transactions. ``password`` may be one password or an
        ordered list of them, tried until one decrypts. ``models`` overrides the
        model order (e.g. fallback-first on a retry). Returns
        ``(transactions, extracted_text, summary)``; text is "" and summary {}
        when unavailable.
        """
        if not has_any_api_key():
            logger.error("LLM gateway unavailable. Cannot parse PDF.")
            return [], "", {}

        model_order = models or [self.primary_model, self.fallback_model]

        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            if reader.is_encrypted:
                candidates = password_candidates(password)
                if not candidates:
                    logger.error("PDF is encrypted but no password provided.")
                    return [], "", {}
                if not _try_decrypt(reader, candidates):
                    logger.error(
                        "Failed to decrypt PDF. All "
                        f"{len(candidates)} configured password(s) rejected."
                    )
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

            # 1. Vision extraction (keeps table/column structure). Try each model.
            for model in model_order:
                try:
                    logger.info(f"Parsing PDF (vision) with model: {model}")
                    txns, summary = self._call_llm_with_pdf(model, pdf_payload_bytes)
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
