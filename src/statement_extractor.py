# gajana/statement_extractor.py
"""Deterministic statement extractor: reads a statement PDF's own text layer.

This is the trustworthy counterpart to :mod:`src.pdf_parser` (which asks an LLM
to read the statement). No model is involved here; every number comes from the
PDF's text layer, and the module refuses to certify anything it cannot prove.

Two invariants define what "prove" means:

**Sign never comes from a guessed column band.** Every bank layout in this
corpus except ``axis-new`` prints a running balance on each transaction row, so
the side of a row is ``balance - previous_balance`` -- exact, and immune to the
x-positions drifting between statement generations. ``axis-new`` prints no
per-row balance, so there (and only there) the side comes from which column the
amount sits under, and the opening/closing identity is what proves the whole
column assignment right. Credit cards print the side explicitly: Axis writes
``Dr``/``Cr`` after every amount, HDFC writes a bare ``Cr`` on credits only.

**A statement is ``verified`` only when it carries an independent printed figure
that agrees.** At least one of: opening+closing balance, printed debit/credit
totals, or printed debit/credit row counts; every printed figure present must
reconcile, and no row may have raised a warning. Zero printed figures means
``unverified`` -- never ``verified``. The audit tool this module replaces got
that backwards: it skipped each check whose printed figure was absent and then
reported the statement clean, so 19 files from which it had extracted no rows
at all self-certified as correct.

Nothing here writes anywhere. Layouts live in the registry tables
``BANK_LAYOUTS`` / ``CARD_LAYOUTS``; adding a statement generation means adding
a row to one of them.
"""

from __future__ import annotations

import datetime
import io
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from pypdf import PdfReader

logger = logging.getLogger(__name__)

DEFAULT_PASSWORDS_PATH = os.path.join("secrets", "passwords.json")

# Amount tokens always carry two decimals. No trailing-digit guard: the
# axis-compact layout glues the Init.Br code onto the balance
# ("823984.251920"), and the balance still has to be read as 823984.25.
NUM_RE = re.compile(r"(?<![\d.,])-?\d[\d,]*\.\d{2}")
DATE_RE = re.compile(r"\b(\d{2})[-/](\d{2})[-/](\d{4}|\d{2})\b")
SLASH_DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")
# Axis cards print the side after the amount; HDFC prints "Cr" or nothing.
CARD_AMOUNT_RE = re.compile(r"(?<![\d.,])(\d[\d,]*\.\d{2})\s*(Dr|Cr)?(?![\w])")
HDFC_CARD_AMOUNT_RE = re.compile(r"(?<![\d.,])(\d[\d,]*\.\d{2})\s*(Cr)?(?!\d)")

# Cheap pre-filter so the (expensive) header patterns are only tried on lines
# that could plausibly be a transaction-table header.
HEADER_HINT_RE = re.compile(r"(Tran Date|Txn Date|Date\s+Transaction|Date\s+Narration)")

# Amounts and balances are printed to the paisa; a 1-rupee band absorbs the
# statements' own rounding without hiding a real break.
RECONCILE_TOLERANCE = 1.0
# A row's own stated amount must match its balance delta to the paisa.
ROW_TOLERANCE = 0.011


@dataclass(frozen=True)
class BankLayout:
    """One bank-statement generation.

    ``header`` is matched against the whitespace-normalized header line;
    ``columns`` maps ``dr``/``cr``/``bal`` to patterns located on the *raw*
    header line, whose character offsets give the column bands.
    """

    name: str
    header: str
    columns: Mapping[str, str]
    running_balance: bool


# Registry: one row per statement generation seen in the corpus.
BANK_LAYOUTS: Tuple[BankLayout, ...] = (
    BankLayout(
        name="axis-rel",
        header=r"Date\s+Transaction\s+Details\s+Chq.*Withdrawal\s+Deposits\s+Balance",
        columns={"dr": r"Withdrawal", "cr": r"Deposits", "bal": r"Balance"},
        running_balance=True,
    ),
    BankLayout(
        name="axis-compact",
        header=r"Tran\s+Date\s+Chq\s+No\s+Particulars\s+Debit\s+Credit\s+Balance",
        columns={"dr": r"Debit", "cr": r"Credit", "bal": r"Balance"},
        running_balance=True,
    ),
    BankLayout(
        name="axis-new",
        header=(
            r"Txn\s+Date\s+Transaction\s+(?:Value\s+Date\s+)?"
            r"Withdrawals\s+Deposits\s+Balance"
        ),
        columns={"dr": r"Withdrawals", "cr": r"Deposits", "bal": r"Balance"},
        running_balance=False,
    ),
    BankLayout(
        name="hdfc-rel",
        header=r"Txn\s+Date\s+Narration\s+.*Withdrawals\s+Deposits\s+Closing\s+Balance",
        columns={
            "dr": r"Withdrawals",
            "cr": r"Deposits",
            "bal": r"Closing\s+Balance",
        },
        running_balance=True,
    ),
    BankLayout(
        name="hdfc-classic",
        header=(
            r"Date\s+Narration\s+Chq\./Ref\.No\.\s+Value\s+Dt\s+Withdrawal\s+Amt\.\s+"
            r"Deposit\s+Amt\.\s+Closing\s+Balance"
        ),
        columns={
            "dr": r"Withdrawal\s+Amt\.",
            "cr": r"Deposit\s+Amt\.",
            "bal": r"Closing\s+Balance",
        },
        running_balance=True,
    ),
)

PERIOD_PATTERNS: Tuple[str, ...] = (
    r"period\s*\(?\s*[Ff]rom\s*:?\s*(\d{2}[-/]\d{2}[-/]\d{4})"
    r".{0,8}[Tt]o\s*:?\s*(\d{2}[-/]\d{2}[-/]\d{4})",
    r"between\s+(\d{2}[-/]\d{2}[-/]\d{4})\s+to\s+(\d{2}[-/]\d{2}[-/]\d{4})",
    r"From\s*:\s*(\d{2}[-/]\d{2}[-/]\d{4})\s+To\s*:?\s*(\d{2}[-/]\d{2}[-/]\d{4})",
)


@dataclass
class StatementRow:
    """One extracted transaction. ``amount`` is signed (negative = debit)."""

    date: datetime.datetime
    description: str
    amount: float
    balance: Optional[float] = None
    stated: Optional[float] = None
    page: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date.strftime("%Y-%m-%d"),
            "desc": self.description,
            "amt": self.amount,
        }


@dataclass
class ExtractedStatement:
    """Rows plus whatever cross-check figures the statement printed itself."""

    kind: str = ""
    family: str = "bank"
    rows: List[StatementRow] = field(default_factory=list)
    opening: Optional[float] = None
    closing: Optional[float] = None
    total_debit: Optional[float] = None
    total_credit: Optional[float] = None
    n_debit: Optional[int] = None
    n_credit: Optional[int] = None
    # Credit-card summary boxes, kept raw for reporting; ``total_debit`` /
    # ``total_credit`` are derived from them so one reconcile covers both
    # families.
    payments: Optional[float] = None
    purchases: Optional[float] = None
    charges: Optional[float] = None
    period: Optional[Tuple[str, str]] = None
    warnings: List[str] = field(default_factory=list)

    @property
    def debit_sum(self) -> float:
        return round(sum(-r.amount for r in self.rows if r.amount < 0), 2)

    @property
    def credit_sum(self) -> float:
        return round(sum(r.amount for r in self.rows if r.amount > 0), 2)

    @property
    def verified(self) -> bool:
        return reconcile(self)[0]

    @property
    def problems(self) -> List[str]:
        return reconcile(self)[1]

    def summary(self) -> Dict[str, Any]:
        """Compact, JSON-safe view; the snapshot format the CLI writes."""
        verified, problems = reconcile(self)
        return {
            "kind": self.kind,
            "family": self.family,
            "verified": verified,
            "problems": problems,
            "period": list(self.period) if self.period else None,
            "n": len(self.rows),
            "dr": self.debit_sum,
            "cr": self.credit_sum,
            "opening": self.opening,
            "closing": self.closing,
            "td": self.total_debit,
            "tc": self.total_credit,
            "rows": [r.as_dict() for r in self.rows],
        }


# --------------------------------------------------------------------------
# PDF reading
# --------------------------------------------------------------------------


def statement_passwords(
    filename: str, passwords_path: str = DEFAULT_PASSWORDS_PATH
) -> List[str]:
    """Password candidates for a statement file, most specific first.

    Mirrors :class:`~src.transaction_processor.TransactionProcessor`: the
    account-specific key (``axis-secondary``) is tried before the bank-level one
    (``axis``), and either value may be a list because banks rotate statement
    passwords while already-downloaded statements keep the old one.
    """
    # Imported lazily: src.pdf_parser pulls in litellm, which this module and
    # its callers otherwise never need.
    from src.pdf_parser import password_candidates

    parts = os.path.basename(filename).split("-")
    if len(parts) < 3:
        return []
    if not os.path.exists(passwords_path):
        return []
    try:
        with open(passwords_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Could not read {passwords_path}: {e}")
        return []
    out: List[str] = []
    for key in (f"{parts[1]}-{parts[2]}".lower(), parts[1].lower()):
        for pw in password_candidates(data.get(key)):
            if pw not in out:
                out.append(pw)
    return out


def read_pdf_pages(data: bytes, passwords: Sequence[str] = ()) -> List[str]:
    """Page texts in pypdf's layout mode, which preserves column x-positions.

    Layout mode is what makes the column bands (and therefore ``axis-new``'s
    side assignment) meaningful; plain extraction collapses them.
    """
    reader = PdfReader(io.BytesIO(data))
    if reader.is_encrypted:
        for idx, pw in enumerate(passwords, 1):
            try:
                if reader.decrypt(pw) != 0:
                    break
            except Exception as e:  # pragma: no cover - pypdf internals
                logger.warning(f"Decrypt attempt #{idx} errored: {e}")
        else:
            raise ValueError("PDF is encrypted and no configured password opened it.")
    return [page.extract_text(extraction_mode="layout") or "" for page in reader.pages]


# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------


def _num(token: str) -> float:
    return float(token.replace(",", ""))


def _to_date(match: "re.Match[str]") -> datetime.datetime:
    day, month, year = match.groups()
    y = int(year)
    return datetime.datetime(y + 2000 if y < 100 else y, int(month), int(day))


def _norm(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip())


def _columns(
    line: str, spec: Mapping[str, str]
) -> Optional[Dict[str, Tuple[int, int]]]:
    """Character bands for each column label on a raw header line."""
    out: Dict[str, Tuple[int, int]] = {}
    for key, pattern in spec.items():
        m = re.search(pattern, line)
        if not m:
            return None
        out[key] = (m.start(), m.end())
    return out


def _match_period(norm: str) -> Optional[Tuple[str, str]]:
    for pattern in PERIOD_PATTERNS:
        m = re.search(pattern, norm)
        if m:
            return (m.group(1).replace("/", "-"), m.group(2).replace("/", "-"))
    return None


def _center(start: int, end: int) -> float:
    return (start + end) / 2


# --------------------------------------------------------------------------
# Bank statements
# --------------------------------------------------------------------------


def _read_printed_figures(st: ExtractedStatement, lines: Sequence[str]) -> None:
    """Pre-pass for the statement's own figures, wherever on the page they sit.

    It runs before the row pass because HDFC prints the opening balance only in
    a trailing summary block, and the opening balance is what seeds
    ``prev_balance`` for the very first row's sign.
    """
    total_columns: Optional[Dict[str, Tuple[int, int]]] = None
    for i, line in enumerate(lines):
        norm = _norm(line)
        upper = norm.upper()
        nums = NUM_RE.findall(line)
        if st.period is None:
            st.period = _match_period(norm)

        # HDFC "relationship" tail block: labels on one line, values below.
        if re.search(
            r"Opening Balance\s+Debit Amount\s+Credit Amount\s+Closing Balance",
            norm,
            re.I,
        ):
            for j in range(i + 1, min(i + 4, len(lines))):
                values = NUM_RE.findall(lines[j])
                if len(values) >= 4:
                    (
                        st.opening,
                        st.total_debit,
                        st.total_credit,
                        st.closing,
                    ) = [_num(v) for v in values[:4]]
                    break
            for j in range(i + 1, min(i + 8, len(lines))):
                if re.search(r"Debit Count\s+Credit Count", lines[j], re.I):
                    for k in range(j + 1, min(j + 4, len(lines))):
                        counts = re.findall(r"(?<![\d.,])\d{1,5}(?![\d.,])", lines[k])
                        if len(counts) >= 2:
                            st.n_debit, st.n_credit = int(counts[0]), int(counts[1])
                            break
                    break
            continue

        # HDFC "classic" tail block: one label line, values on the next.
        if re.search(
            r"Opening Balance\s+.*Dr Count\s+Cr Count\s+Debits\s+Credits", norm, re.I
        ):
            for j in range(i + 1, min(i + 4, len(lines))):
                values = NUM_RE.findall(lines[j])
                counts = re.findall(r"(?<![\d.,])\d{1,4}(?![\d.,])", lines[j])
                if len(values) >= 3:
                    st.opening = _num(values[0])
                    st.total_debit, st.total_credit = _num(values[1]), _num(values[2])
                    if len(counts) >= 2:
                        st.n_debit, st.n_credit = int(counts[0]), int(counts[1])
                    break
            continue

        if "OPENING BALANCE" in upper and nums and st.opening is None:
            m = re.search(r"Opening Balance\s*:\s*(-?[\d,]+\.\d{2})", line, re.I)
            st.opening = _num(m.group(1)) if m else _num(nums[-1])
        elif (
            "CLOSING BALANCE" in upper
            and nums
            and st.closing is None
            and st.opening is not None
            and not HEADER_HINT_RE.search(line)
        ):
            st.closing = _num(nums[-1])
        elif (
            re.match(r"^(TRANSACTION TOTAL|Total)\b", norm, re.I)
            and len(nums) >= 2
            and not DATE_RE.search(line)
            and total_columns
            and st.total_debit is None
        ):
            # Axis prints its totals under the same columns as the rows, so the
            # header's bands say which figure is which.
            got: Dict[str, float] = {}
            for m in NUM_RE.finditer(line):
                center = _center(m.start(), m.end())
                key = min(
                    total_columns,
                    key=lambda k: abs(center - _center(*total_columns[k])),  # type: ignore[index] # noqa: E501
                )
                got.setdefault(key, _num(m.group()))
            if "dr" in got and "cr" in got:
                st.total_debit, st.total_credit = got["dr"], got["cr"]
        elif HEADER_HINT_RE.search(line):
            for layout in BANK_LAYOUTS:
                if re.search(layout.header, norm):
                    columns = _columns(line, layout.columns)
                    if columns:
                        total_columns = columns
                    break


def _row_sign_from_balance(
    st: ExtractedStatement,
    values: Sequence[Tuple[float, int, int]],
    columns: Mapping[str, Tuple[int, int]],
    prev_balance: Optional[float],
    date_token: str,
    norm: str,
) -> Tuple[float, Optional[float], float]:
    """Sign a row from its printed running balance. Returns (signed, stated, balance)."""
    balance = values[-1][0]
    # Amount tokens sit at or after the debit column; a small slack absorbs
    # right-aligned figures that start just left of the label.
    before = [v for v, s, _ in values[:-1] if s >= columns["dr"][0] - 24]
    nonzero = [v for v in before if v != 0.0]
    stated = nonzero[0] if nonzero else (before[0] if before else None)
    if prev_balance is None:
        st.warnings.append(f"row before opening balance: {norm[:50]}")
        signed = -(stated or 0.0)
    else:
        signed = round(balance - prev_balance, 2)
    if stated is None:
        st.warnings.append(f"{date_token} no amount token; derived {signed}")
    elif abs(abs(signed) - stated) > ROW_TOLERANCE:
        st.warnings.append(f"{date_token} stated {stated} != balance delta {signed}")
    return signed, stated, balance


def extract_bank_statement(pages: Sequence[str]) -> ExtractedStatement:
    """Extract a bank statement from its layout-mode page texts."""
    st = ExtractedStatement(family="bank")
    page_lines = [text.split("\n") for text in pages]
    _read_printed_figures(st, [line for lines in page_lines for line in lines])

    columns: Optional[Dict[str, Tuple[int, int]]] = None
    date_x = 0
    desc_end = 0
    prev_balance = st.opening
    pending: List[str] = []

    for page_no, lines in enumerate(page_lines):
        for line in lines:
            norm = _norm(line)
            if HEADER_HINT_RE.search(line):
                for layout in BANK_LAYOUTS:
                    if re.search(layout.header, norm):
                        found = _columns(line, layout.columns)
                        if found:
                            st.kind = st.kind or layout.name
                            columns = found
                            starts = [
                                line.find(label)
                                for label in ("Tran Date", "Txn Date", "Date")
                                if line.find(label) >= 0
                            ]
                            date_x = min(starts) if starts else 0
                            ref = re.search(r"Chq\./Ref\.No\.", line)
                            desc_end = ref.start() if ref else found["dr"][0]
                        break
                pending = []
                continue
            if columns is None:
                continue

            upper = norm.upper()
            if (
                "OPENING BALANCE" in upper
                or "CLOSING BALANCE" in upper
                or re.match(r"^(TRANSACTION TOTAL|Total)\b", norm, re.I)
            ):
                pending = []
                continue

            numbers = [(m.group(), m.start(), m.end()) for m in NUM_RE.finditer(line)]
            date_match = DATE_RE.search(line)
            if not (
                date_match and date_match.start() <= max(6, date_x + 10) and numbers
            ):
                # A continuation line: narration wrapped below its own row.
                body = line[:desc_end].strip()
                if len(body) > 2 and not re.search(
                    r"Page No|Balance|Statement|Narration", body
                ):
                    pending.append(body)
                continue

            values = [(_num(t), s, e) for t, s, e in numbers]
            if st.kind == "axis-new":
                # The only layout without a running balance: the amount's
                # column decides the side, and the opening/closing identity in
                # reconcile() is what proves that assignment right.
                candidates = [v for v in values if v[1] < columns["bal"][0] - 4]
                if not candidates:
                    pending.append(norm[:60])
                    continue
                value, start, end = candidates[0]
                center = _center(start, end)
                side = (
                    "dr"
                    if abs(center - _center(*columns["dr"]))
                    <= abs(center - _center(*columns["cr"]))
                    else "cr"
                )
                signed = -value if side == "dr" else value
                stated: Optional[float] = value
                balance: Optional[float] = None
            else:
                signed, stated, balance = _row_sign_from_balance(
                    st, values, columns, prev_balance, date_match.group(), norm
                )
                prev_balance = balance

            head = DATE_RE.sub("", line[:desc_end], count=1).strip()
            description = _norm(" ".join([head] + pending))
            pending = []
            st.rows.append(
                StatementRow(
                    date=_to_date(date_match),
                    description=description,
                    amount=signed,
                    balance=balance,
                    stated=stated,
                    page=page_no,
                )
            )

    if st.period is None and st.rows:
        dates = sorted(r.date for r in st.rows)
        st.period = (
            dates[0].strftime("%d-%m-%Y"),
            dates[-1].strftime("%d-%m-%Y"),
        )
    return st


# --------------------------------------------------------------------------
# Credit-card statements
# --------------------------------------------------------------------------


def extract_axis_card(pages: Sequence[str]) -> ExtractedStatement:
    """Axis credit card: every amount is followed by an explicit Dr/Cr marker."""
    st = ExtractedStatement(kind="axis-cc", family="card")
    amount_x: Optional[int] = None
    for text in pages:
        lines = text.split("\n")
        for i, line in enumerate(lines):
            norm = _norm(line)
            if "AMOUNT (Rs.)" in line:
                amount_x = line.find("AMOUNT (Rs.)")
                continue
            if "Statement Period" in line and st.period is None:
                for j in range(i, min(i + 4, len(lines))):
                    p = re.search(
                        r"(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})", lines[j]
                    )
                    if p:
                        st.period = (p.group(1), p.group(2))
                        break
            # The payment-summary equation: previous - payments - credits +
            # purchases + debits + charges = total due. The two sides are
            # printed separately, so a debit/credit swap cannot reconcile.
            if re.search(
                r"Previous Balance\s*-\s*Payments\s*-\s*Credits\s*\+\s*Purchase",
                norm,
                re.I,
            ):
                for j in range(i + 1, min(i + 5, len(lines))):
                    found = list(CARD_AMOUNT_RE.finditer(lines[j]))
                    if len(found) >= 6:
                        st.payments = _num(found[1].group(1)) + _num(found[2].group(1))
                        st.purchases = _num(found[3].group(1)) + _num(found[4].group(1))
                        st.charges = _num(found[5].group(1))
                        break
                continue
            if amount_x is None:
                continue
            date_match = SLASH_DATE_RE.search(line)
            if not date_match or date_match.start() > 6:
                continue
            candidates = [
                m
                for m in CARD_AMOUNT_RE.finditer(line)
                if m.start() >= amount_x - 40 and m.group(2)
            ]
            if not candidates:
                st.warnings.append(f"{date_match.group()} no Dr/Cr amount: {norm[:60]}")
                continue
            amount = candidates[-1]
            value = _num(amount.group(1))
            st.rows.append(
                StatementRow(
                    date=_to_date(date_match),
                    description=_norm(line[date_match.end() : amount.start()]),
                    amount=-value if amount.group(2) == "Dr" else value,
                )
            )
    _apply_card_summary(st)
    return st


def _hdfc_card_period_from_statement_date(norm: str) -> Optional[Tuple[str, str]]:
    """HDFC cards without a printed billing period state a statement date; the
    cycle is the month ending on it."""
    m = re.search(r"Statement Date\s*:?\s*(\d{2})/(\d{2})/(\d{4})", norm)
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return (
        f"{day + 1:02d}/{month - 1 or 12:02d}/{year if month > 1 else year - 1}",
        f"{day:02d}/{month:02d}/{year}",
    )


def extract_hdfc_card(pages: Sequence[str]) -> ExtractedStatement:
    """HDFC credit card, both generations: credits carry a bare ``Cr``."""
    st = ExtractedStatement(kind="hdfc-cc", family="card")
    for text in pages:
        lines = text.split("\n")
        for i, line in enumerate(lines):
            norm = _norm(line)
            if st.period is None:
                p = re.search(
                    r"Billing Period\s+(\d{1,2} \w{3}, \d{4})\s*-\s*(\d{1,2} \w{3}, \d{4})",
                    norm,
                )
                if p:
                    st.period = (p.group(1), p.group(2))
            if re.search(r"^Account Summary$", norm, re.I):
                # Older Infinia/Regalia layout: Opening | Payment/Credits |
                # Purchase/Debits | Finance Charges | Total Dues.
                for j in range(i + 1, min(i + 6, len(lines))):
                    values = re.findall(r"(?<![\d.,])([\d,]+\.\d{2})", lines[j])
                    if len(values) >= 5:
                        st.payments = _num(values[1])
                        st.purchases = _num(values[2])
                        st.charges = _num(values[3])
                        break
                continue
            if st.period is None:
                st.period = _hdfc_card_period_from_statement_date(norm)
            if re.search(r"PREVIOUS STATEMENT DUES", norm, re.I):
                # Newer layout prints the same four boxes prefixed with the
                # rupee glyph, which extracts as a bare "C".
                for j in range(i, min(i + 6, len(lines))):
                    values = re.findall(r"C\s?([\d,]+\.\d{2})", lines[j])
                    if len(values) >= 4:
                        st.payments = _num(values[1])
                        st.purchases = _num(values[2])
                        st.charges = _num(values[3])
                        break
                continue
            date_match = re.match(
                r"\s*(\d{2})/(\d{2})/(\d{4})\s*\|?\s*(\d{2}:\d{2})?", line
            )
            if not date_match:
                continue
            tail = line[date_match.end() :]
            found = list(HDFC_CARD_AMOUNT_RE.finditer(tail))
            if not found:
                st.warnings.append(f"{line[:40]} no amount")
                continue
            amount = found[-1]
            value = _num(amount.group(1))
            st.rows.append(
                StatementRow(
                    date=datetime.datetime(
                        int(date_match.group(3)),
                        int(date_match.group(2)),
                        int(date_match.group(1)),
                    ),
                    description=_norm(tail[: amount.start()]),
                    amount=value if amount.group(2) == "Cr" else -value,
                )
            )
    _apply_card_summary(st)
    return st


def _apply_card_summary(st: ExtractedStatement) -> None:
    """Fold the card's printed summary boxes into the common total fields, so
    one reconcile covers both families. Finance charges are debits the card
    prints outside the purchases box."""
    if st.payments is not None:
        st.total_credit = st.payments
    if st.purchases is not None:
        st.total_debit = round(st.purchases + (st.charges or 0.0), 2)


@dataclass(frozen=True)
class CardLayout:
    """One credit-card issuer's statement family."""

    name: str
    matches: Callable[[str], bool]
    extract: Callable[[Sequence[str]], ExtractedStatement]


CARD_LAYOUTS: Tuple[CardLayout, ...] = (
    CardLayout(
        name="hdfc-cc",
        matches=lambda name: "-hdfc-" in name,
        extract=extract_hdfc_card,
    ),
    CardLayout(
        name="axis-cc",
        matches=lambda name: True,
        extract=extract_axis_card,
    ),
)


# --------------------------------------------------------------------------
# Verification
# --------------------------------------------------------------------------


def reconcile(st: ExtractedStatement) -> Tuple[bool, List[str]]:
    """Cross-check the extracted rows against the statement's printed figures.

    Returns ``(verified, problems)``. ``verified`` requires at least one printed
    figure to check against *and* no problem at all -- "nothing to check" is a
    problem, not a pass.
    """
    problems: List[str] = []
    checks = 0

    if st.total_debit is not None:
        checks += 1
        if abs(st.debit_sum - st.total_debit) > RECONCILE_TOLERANCE:
            problems.append(
                f"debit sum {st.debit_sum:.2f} != printed {st.total_debit:.2f}"
            )
    if st.total_credit is not None:
        checks += 1
        if abs(st.credit_sum - st.total_credit) > RECONCILE_TOLERANCE:
            problems.append(
                f"credit sum {st.credit_sum:.2f} != printed {st.total_credit:.2f}"
            )
    if st.opening is not None and st.closing is not None:
        checks += 1
        derived = round(st.opening - st.debit_sum + st.credit_sum, 2)
        if abs(derived - st.closing) > RECONCILE_TOLERANCE:
            problems.append(
                f"opening {st.opening} -{st.debit_sum:.2f} +{st.credit_sum:.2f} "
                f"= {derived} != closing {st.closing}"
            )
        last = st.rows[-1].balance if st.rows else None
        if last is not None and abs(last - st.closing) > RECONCILE_TOLERANCE:
            problems.append(f"last row balance {last} != closing {st.closing}")
    if st.n_debit is not None and st.n_credit is not None:
        checks += 1
        n_debit = sum(1 for r in st.rows if r.amount < 0)
        n_credit = sum(1 for r in st.rows if r.amount > 0)
        if (n_debit, n_credit) != (st.n_debit, st.n_credit):
            problems.append(
                f"row counts dr/cr {n_debit}/{n_credit} != printed "
                f"{st.n_debit}/{st.n_credit}"
            )

    if checks == 0:
        problems.append("no printed figure to check against")
    if st.warnings:
        problems.append(f"{len(st.warnings)} row warnings e.g. {st.warnings[0]}")
    return (checks > 0 and not problems), problems


# --------------------------------------------------------------------------
# Entry points
# --------------------------------------------------------------------------


def extract_from_pages(pages: Sequence[str], filename: str) -> ExtractedStatement:
    """Dispatch to the right family/layout for ``filename``'s page texts."""
    name = os.path.basename(filename).lower()
    if not name.startswith("cc-"):
        return extract_bank_statement(pages)
    for layout in CARD_LAYOUTS:
        if layout.matches(name):
            return layout.extract(pages)
    raise ValueError(f"No card layout matches {filename}")  # pragma: no cover


def extract_statement_bytes(
    data: bytes, filename: str, passwords: Sequence[str] = ()
) -> ExtractedStatement:
    """Extract a statement from raw PDF bytes."""
    return extract_from_pages(read_pdf_pages(data, passwords), filename)


def extract_statement(
    path: str, passwords: Optional[Sequence[str]] = None
) -> ExtractedStatement:
    """Extract a statement from a PDF on disk, decrypting it if needed."""
    if passwords is None:
        passwords = statement_passwords(path)
    with open(path, "rb") as fh:
        return extract_statement_bytes(fh.read(), path, passwords)
