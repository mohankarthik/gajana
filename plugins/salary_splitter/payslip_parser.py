"""Payslip PDF → structured fields.

Bespoke to the Google payslip layout. Reuses gajana's LiteLLM key handling and
model choices (src.pdf_parser) but uses a payslip-specific prompt and returns a
single object (not a transaction list), so it does its own completion call
rather than going through PDFParser.parse_pdf.
"""

from __future__ import annotations

import base64
import io
import json
import logging
from typing import Any

import litellm
from pypdf import PdfReader

from src.pdf_parser import (
    FALLBACK_MODEL,
    PRIMARY_MODEL,
    configure_api_keys,
    has_any_api_key,
)

logger = logging.getLogger(__name__)

# Numeric fields we need off the payslip. All amounts are positive rupee values
# (the LLM strips signs/commas); the caller applies signs where needed.
NUMERIC_FIELDS = [
    "net_pay",
    "basic",
    "hra",
    "special_allowance",
    "baby_bonus",
    "medical_insurance_topup",
    "gsus_income",
    "gsus_broker_tax",
    "income_tax",
]

_PROMPT = """
You are a precise data extraction assistant. Extract fields from this Google
(Alphabet) India monthly payslip PDF.

Return a JSON object with exactly these keys. Use the CURRENT MONTH "Amount"
column, never the "YTD" column. Amounts must be plain numbers with no currency
symbol, no commas, and no sign. Use 0 if a line item is absent.

- "date_of_payment": the "Date of Payment" as YYYY-MM-DD.
- "net_pay": the "Net Pay" value.
- "basic": Earnings "Basic".
- "hra": Earnings "House Rent Allowance".
- "special_allowance": Earnings "Special Allowance".
- "baby_bonus": Earnings "Baby Bonus".
- "gsus_income": Earnings "GSUs Income".
- "medical_insurance_topup": Deductions "Medical Insurance Top Up".
- "gsus_broker_tax": Deductions "GSUs - Taxes at broker" (return its absolute value).
- "income_tax": Deductions "Income Tax".
"""


def _complete(model: str, pdf_b64: str) -> dict[str, Any]:
    if "anthropic" in model or "claude" in model:
        pdf_block: dict = {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": pdf_b64,
            },
        }
    else:
        pdf_block = {
            "type": "image_url",
            "image_url": {"url": f"data:application/pdf;base64,{pdf_b64}"},
        }
    response = litellm.completion(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [{"type": "text", "text": _PROMPT}, pdf_block],
            }
        ],
        response_format={"type": "json_object"},
    )
    content = (response.choices[0].message.content or "").strip()
    if content.startswith("```"):
        lines = content.splitlines()[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        content = "\n".join(lines).strip()
    return json.loads(content)


def _coerce(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize LLM output: numeric fields → float, keep date_of_payment str."""
    out: dict[str, Any] = {
        "date_of_payment": str(raw.get("date_of_payment", "")).strip()
    }
    for field in NUMERIC_FIELDS:
        value = raw.get(field, 0)
        try:
            out[field] = abs(
                float(str(value).replace(",", "").replace("₹", "").strip() or 0)
            )
        except (ValueError, TypeError):
            out[field] = 0.0
    return out


def parse_payslip(pdf_bytes: bytes) -> dict[str, Any]:
    """Parses a payslip PDF into the field dict. Raises on total failure."""
    configure_api_keys()
    if not has_any_api_key():
        raise RuntimeError("No LLM API keys configured; cannot parse payslip.")

    # Payslips are not password protected, but read once to fail fast on garbage.
    PdfReader(io.BytesIO(pdf_bytes))
    pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")

    last_err: Exception | None = None
    for model in (PRIMARY_MODEL, FALLBACK_MODEL):
        try:
            logger.info(f"Parsing payslip with {model}.")
            fields = _coerce(_complete(model, pdf_b64))
            logger.info(
                f"Parsed payslip: net_pay={fields['net_pay']:.0f}, "
                f"date={fields['date_of_payment']}"
            )
            return fields
        except Exception as e:  # noqa: BLE001 - fall through to next model
            logger.warning(f"Payslip parse with {model} failed: {e}")
            last_err = e
    raise RuntimeError(f"All models failed to parse payslip: {last_err}")
