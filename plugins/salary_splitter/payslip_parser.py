"""Payslip PDF → structured fields.

Bespoke to the Google payslip layout. Reuses gajana's host LLM gateway client
(src.llm_client) and model choices (src.pdf_parser) but uses a payslip-specific
prompt and returns a single object (not a transaction list), so it does its own
completion call rather than going through PDFParser.parse_pdf.
"""

from __future__ import annotations

import base64
import io
import json
import logging
from typing import Any

from pypdf import PdfReader

from src import llm_client
from src.pdf_parser import (
    FALLBACK_MODEL,
    PRIMARY_MODEL,
    _model_to_family,
    configure_api_keys,
    has_any_api_key,
)

logger = logging.getLogger(__name__)


class PayslipParseError(RuntimeError):
    """Raised when a payslip PDF cannot be parsed into required fields."""


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


def _complete(model: str, pdf_payload: bytes | str) -> dict[str, Any]:
    pdf_bytes = (
        base64.b64decode(pdf_payload) if isinstance(pdf_payload, str) else pdf_payload
    )
    data, _meta = llm_client.complete_pdf(
        _PROMPT,
        pdf_bytes,
        filename="payslip.pdf",
        response_format="json",
        tier="fast",
        family=_model_to_family(model),
        title="gajana_payslip",
    )
    if isinstance(data, str):
        content = data.strip()
        if content.startswith("```"):
            lines = content.splitlines()[1:]
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            content = "\n".join(lines).strip()
        data = json.loads(content)
    if not isinstance(data, dict):
        raise PayslipParseError("LLM payslip response is not a JSON object")
    return data


def _coerce(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize LLM output: numeric fields → float, keep date_of_payment str."""
    if not isinstance(raw, dict):
        raise PayslipParseError("Payslip response must be a JSON object.")
    date_of_payment = str(raw.get("date_of_payment", "")).strip()
    if not date_of_payment or "net_pay" not in raw:
        raise PayslipParseError(
            "Payslip response missing required fields (date_of_payment, net_pay)."
        )
    out: dict[str, Any] = {"date_of_payment": date_of_payment}
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
        raise PayslipParseError("LLM gateway unavailable; cannot parse payslip.")

    # Payslips are not password protected, but read once to fail fast on garbage.
    PdfReader(io.BytesIO(pdf_bytes))

    last_err: Exception | None = None
    for model in (PRIMARY_MODEL, FALLBACK_MODEL):
        try:
            logger.info(f"Parsing payslip with {model}.")
            fields = _coerce(_complete(model, pdf_bytes))
            logger.info(
                f"Parsed payslip: net_pay={fields['net_pay']:.0f}, "
                f"date={fields['date_of_payment']}"
            )
            return fields
        except Exception as e:  # noqa: BLE001 - fall through to next model
            logger.warning(f"Payslip parse with {model} failed: {e}")
            last_err = e
    raise PayslipParseError(f"All models failed to parse payslip: {last_err}")
