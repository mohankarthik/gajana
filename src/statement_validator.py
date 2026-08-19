"""Validate LLM-extracted statement transactions against the PDF text layer.

The vision model owns *structure* (which column a number sits in); the embedded
text layer owns *tokens* (it never mangles a digit or a date). This module
crosses the two: every extracted token must be corroborated by the text, every
date must fall inside the statement window, and the per-statement totals must
reconcile. Anything that fails a hard check is flagged for human review instead
of being written to the live ledger.

All checks are deterministic — no LLM calls here.
"""

from __future__ import annotations

import datetime
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from src.utils import _DATE_TOKEN_RE, parse_mixed_datetime

logger = logging.getLogger(__name__)

# Transactions sometimes post a day or two after the statement's printed end
# date (value-date lag). Allow this slack on the upper bound so legitimate
# near-boundary rows are not quarantined, while dates months in the future (the
# hallucination we actually care about) still fail.
STATEMENT_END_GRACE_DAYS = 5
# Reconcile flags only *gross* errors (a dropped txn or a debit/credit-column
# flip). Tolerate sub-rupee rounding and statements' own paise-vs-rupee internal
# inconsistency: flag when the net is off by more than max(abs, rel * expected).
RECONCILE_ABS_TOLERANCE = 2.0
RECONCILE_REL_TOLERANCE = 0.0025
# A *gross* reconcile break is not a soft warning: it means the extracted rows
# cannot be the statement's rows (a debit/credit column flip, a dropped block).
# Booking those corrupts the ledger silently — both live cases (bank-axis-mini
# Jul-2026, cc-hdfc-infinia Jun-2026) were column flips that reconcile saw and
# soft-flagged while the wrong rows were written anyway. Past this threshold the
# whole statement is quarantined; below it the flag stays advisory, so a
# near-tolerance wobble (a paise-level summary misread) doesn't block booking.
RECONCILE_HARD_ABS_TOLERANCE = 100.0
RECONCILE_HARD_REL_TOLERANCE = 0.01
# Fraction of "significant" (>=4 char) description tokens that must appear in the
# source text before we trust the description. Below this it is likely garbled.
DESC_MIN_TOKEN_OVERLAP = 0.5


@dataclass
class Mismatch:
    """One reconcile break: which side, how it read, and by how much."""

    label: str
    message: str
    got: float
    stated: float

    @property
    def delta(self) -> float:
        return abs(self.got - self.stated)

    @property
    def gross(self) -> bool:
        """True when the break is too large to be a rounding/summary artefact.

        Gross breaks quarantine the statement; smaller ones stay advisory.
        """
        return self.delta > max(
            RECONCILE_HARD_ABS_TOLERANCE,
            RECONCILE_HARD_REL_TOLERANCE * abs(self.stated),
        )


@dataclass
class ValidationResult:
    passed: list[dict] = field(default_factory=list)
    # (raw_txn, [reasons]) for rows that failed a hard check.
    flagged: list[tuple[dict, list[str]]] = field(default_factory=list)
    # Statement-wide soft warnings (count/reconcile/description confidence).
    statement_flags: list[str] = field(default_factory=list)


def _norm(text: str) -> str:
    """Lowercase and collapse all whitespace to single spaces."""
    return re.sub(r"\s+", " ", text).strip().lower()


def _amount_num(raw: Any) -> Optional[float]:
    """Parse a printed amount token to a float, or None if empty/unparseable."""
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").replace("₹", "").replace("$", "")
    if not s:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None


def _amount_in_text(raw: Any, norm_text: str) -> bool:
    """True if the amount appears in the source (comma-insensitive)."""
    num = _amount_num(raw)
    if num is None:
        return False
    text_nc = norm_text.replace(",", "")
    # Match the number with an optional decimal, on a digit boundary.
    whole = str(int(num)) if num == int(num) else None
    patterns = [rf"(?<!\d){re.escape(f'{num:.2f}')}(?!\d)"]
    if whole:
        patterns.append(rf"(?<!\d){re.escape(whole)}(?:\.0+)?(?!\d)")
    return any(re.search(p, text_nc) for p in patterns)


def _date_in_text(date_raw: str, norm_text: str) -> bool:
    """True if the transaction date is corroborated by the source text.

    Some statements carry a date+time column (e.g. HDFC Infinia "11/04/2026
    19:51" or "20/04/2026 | 08:34"); the LLM copies the time but may render it
    differently than the text layer (dropped seconds, spacing, a "|" separator).
    Match on the date alone so the whole row isn't quarantined over the time.
    """
    d = _norm(date_raw)
    if d in norm_text:
        return True
    m = _DATE_TOKEN_RE.search(d)
    if m is None:
        return False
    token = m.group()
    return token != d and token in norm_text


def _desc_token_overlap(desc: str, norm_text: str) -> float:
    tokens = [t for t in re.findall(r"[a-z0-9]+", desc.lower()) if len(t) >= 4]
    if not tokens:
        return 1.0  # nothing significant to corroborate; don't penalise
    hits = sum(1 for t in tokens if t in norm_text)
    return hits / len(tokens)


def _statement_end(
    norm_text: str,
    config: dict[str, Any],
    stmt_end_date: Optional[datetime.datetime],
    today: datetime.datetime,
) -> datetime.datetime:
    """Return the latest date a transaction may legitimately carry.

    Prefers the printed "Statement Period" end (via config
    ``statement_period_patterns``); falls back to the filename-derived end date.
    Always capped at ``today`` — a real transaction cannot be in the future.

    Only an upper bound is enforced: future/after-period dates are the dangerous
    hallucination (see the Aug-2026 neorupay bug). A too-old date is harmless and
    is handled by the incremental ``> last_txn_date`` filter downstream, so we do
    not reject on a lower bound (which would wrongly quarantine legitimate
    month-spanning or year-granularity statements).
    """
    date_formats = config.get("date_formats", [])
    end: Optional[datetime.datetime] = None

    for pat in config.get("statement_period_patterns", []):
        m = re.search(pat, norm_text)
        if m:
            end = parse_mixed_datetime(
                logger, m.groupdict().get("end", ""), date_formats
            )
            if end:
                break

    if end is None:
        end = stmt_end_date

    if end is None:
        return today
    # Grace for value-date lag, but never allow a date beyond today.
    return min(end + datetime.timedelta(days=STATEMENT_END_GRACE_DAYS), today)


def validate_statement(
    txns: list[dict],
    text: str,
    config: dict[str, Any],
    stmt_end_date: Optional[datetime.datetime] = None,
    today: Optional[datetime.datetime] = None,
    summary: Optional[dict] = None,
) -> ValidationResult:
    """Split extracted transactions into trustworthy vs review-needed.

    Hard (per-txn -> review): unparseable date, date not in source text, date
    outside the statement window / in the future, amount not in source text.
    Soft (per-statement warning): row-count mismatch, low-confidence
    descriptions, and totals that don't reconcile against the statement's own
    printed summary (``summary`` — an LLM-read cross-check, no per-bank regex).
    """
    result = ValidationResult()
    today = today or datetime.datetime.now()

    # No oracle (e.g. scanned PDF with no text layer): we cannot corroborate
    # tokens, so quarantine everything for review rather than trust vision blind.
    norm_text = _norm(text)
    if not norm_text:
        result.statement_flags.append("no_text_layer: cannot validate; sent to review")
        result.flagged = [(t, ["no_text_layer"]) for t in txns]
        return result

    # ISO is unambiguous; keep it as a fallback so a verbatim ISO date parses via
    # strptime rather than parse_mixed_datetime's dayfirst pandas guess.
    date_formats = list(config.get("date_formats", []))
    if "%Y-%m-%d" not in date_formats:
        date_formats.append("%Y-%m-%d")
    end = _statement_end(norm_text, config, stmt_end_date, today)

    low_conf_desc = 0
    for txn in txns:
        reasons: list[str] = []

        date_raw = str(txn.get("date", "")).strip()
        dt = parse_mixed_datetime(logger, date_raw, date_formats) if date_raw else None
        if dt is None:
            reasons.append(f"date_unparseable:{date_raw!r}")
        else:
            if date_raw and not _date_in_text(date_raw, norm_text):
                reasons.append(f"date_not_in_source:{date_raw!r}")
            if dt > end:
                reasons.append(f"date_after_end:{dt:%Y-%m-%d}>{end:%Y-%m-%d}")

        has_debit = _amount_num(txn.get("debit")) is not None
        has_credit = _amount_num(txn.get("credit")) is not None
        if not has_debit and not has_credit:
            reasons.append("no_amount")
        else:
            if has_debit and not _amount_in_text(txn.get("debit"), norm_text):
                reasons.append(f"debit_not_in_source:{txn.get('debit')!r}")
            if has_credit and not _amount_in_text(txn.get("credit"), norm_text):
                reasons.append(f"credit_not_in_source:{txn.get('credit')!r}")

        desc = str(txn.get("description", ""))
        if _desc_token_overlap(desc, norm_text) < DESC_MIN_TOKEN_OVERLAP:
            low_conf_desc += 1  # soft signal only; don't over-quarantine

        if reasons:
            result.flagged.append((txn, reasons))
        else:
            result.passed.append(txn)

    _add_statement_flags(result, txns, text, norm_text, summary or {}, low_conf_desc)

    # A gross reconcile break means these rows are not the statement's rows.
    # Quarantine everything rather than book a known-corrupt parse; the caller
    # retries with the fallback model first, so a clean re-read still gets through.
    gross = [m for m in reconcile_mismatches(txns, summary or {}) if m.gross]
    if gross and result.passed:
        reason = "reconcile_gross:" + "; ".join(
            f"{m.label} off {m.delta:.2f}" for m in gross
        )
        result.flagged.extend((t, [reason]) for t in result.passed)
        result.passed = []
    return result


def _txn_like_line_count(text: str) -> int:
    """Count source lines that look like a transaction row: a date *and* an
    amount on the same line.

    Counting bare date tokens across the whole document (the previous approach)
    swept in due-date, statement-period, fee-schedule and deposit-summary lines,
    which fired count_mismatch on correctly-parsed statements (bank-axis-karti
    Jul-2026: 9 real txns vs "~28 dated lines"). Line structure only survives in
    the raw text, so this runs before normalisation.
    """
    count = 0
    for line in text.splitlines():
        low = line.lower()
        has_date = bool(
            re.search(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", low)
            or re.search(r"\b\d{1,2} [a-z]{3},? \d{2,4}\b", low)
        )
        if has_date and re.search(r"\d[\d,]*\.\d{2}\b", low):
            count += 1
    return count


def _add_statement_flags(
    result: ValidationResult,
    txns: list[dict],
    text: str,
    norm_text: str,
    summary: dict[str, Any],
    low_conf_desc: int,
) -> None:
    if low_conf_desc:
        result.statement_flags.append(
            f"low_confidence_descriptions: {low_conf_desc}/{len(txns)}"
        )

    # Count sanity: compare against date-like token *occurrences* (not distinct
    # dates — an active account has many txns sharing one date). Only a gross
    # order-of-magnitude gap signals a mis-parse; keep the band wide because
    # summary/balance lines also carry dates.
    date_occurrences = _txn_like_line_count(text)
    # Skip tiny statements: a handful of txns vs the dates in due-date/period
    # lines trips the ratio for no real signal (e.g. a 1-txn paid-in-full card).
    if (
        len(txns) >= 5
        and date_occurrences
        and not (date_occurrences / 3 <= len(txns) <= date_occurrences * 3)
    ):
        result.statement_flags.append(
            f"count_mismatch: {len(txns)} txns vs ~{date_occurrences} dated lines"
        )

    for m in reconcile_mismatches(txns, summary):
        severity = "gross" if m.gross else "minor"
        result.statement_flags.append(
            f"reconcile_mismatch ({m.label}, {severity}): {m.message}"
        )


def _within_tolerance(a: float, b: float) -> bool:
    """True if a and b agree within a rupee / relative floor (gross-error only —
    ignores paise rounding and statements' rupee-vs-paise inconsistency)."""
    return abs(a - b) <= max(RECONCILE_ABS_TOLERANCE, RECONCILE_REL_TOLERANCE * abs(b))


def reconcile_mismatches(txns: list[dict], summary: Optional[dict]) -> list["Mismatch"]:
    """Cross-check the transaction sums against the statement's OWN printed
    totals (read semantically by the LLM into ``summary``), independent of any
    per-bank text layout.

    Two signals, in order of trust:

    - **Balance net** (``opening_balance`` / ``closing_balance``): ``|closing −
      opening|`` == ``|sum_debit − sum_credit|`` (sign-agnostic, holds for banks
      and CCs). Balances are exact, so this is the ground truth. It's weaker
      (can't see a debit/credit swap that preserves the net) but never false-fails.
    - **Per-side** (``total_debit`` / ``total_credit``): catches a column swap,
      but a statement may print these as *subtotals* (e.g. Axis CC prints
      "Purchase" separately from "Other charges"), so the LLM can return a partial
      figure. We therefore trust per-side only when both totals are present and
      they reproduce the balance change (``|total_debit − total_credit|`` ==
      ``|closing − opening|``); otherwise the totals are a subtotal and we use the
      balance net instead. With no balances to cross-check, per-side is used
      best-effort.

    Returns the mismatches found; empty when everything agrees or the statement
    printed nothing to check against.
    """
    if not summary:
        return []

    td = _amount_num(summary.get("total_debit"))
    tc = _amount_num(summary.get("total_credit"))
    opening = _amount_num(summary.get("opening_balance"))
    closing = _amount_num(summary.get("closing_balance"))
    sum_debit = sum(_amount_num(t.get("debit")) or 0.0 for t in txns)
    sum_credit = sum(_amount_num(t.get("credit")) or 0.0 for t in txns)
    bal_net = (
        abs(closing - opening) if opening is not None and closing is not None else None
    )

    # Per-side totals are trustworthy grand-totals only if they can't be a silent
    # subtotal — i.e. no balances to check against, or they match the balance net.
    side_trustworthy = (
        td is not None
        and tc is not None
        and (bal_net is None or _within_tolerance(abs(td - tc), bal_net))
    )

    mismatches: list[Mismatch] = []

    def check(label: str, got: float, stated: float, what: str, src: str) -> None:
        if not _within_tolerance(got, stated):
            mismatches.append(
                Mismatch(
                    label,
                    f"txns {what} {got:.2f} vs {src} {stated:.2f}",
                    got,
                    stated,
                )
            )

    if side_trustworthy and td is not None and tc is not None:
        check("debit", sum_debit, td, "sum", "statement")
        check("credit", sum_credit, tc, "sum", "statement")
    elif bal_net is not None:
        check("net", abs(sum_debit - sum_credit), bal_net, "net", "balances")
    else:  # only a single-side total, no balances to cross-check → best effort
        if td is not None:
            check("debit", sum_debit, td, "sum", "statement")
        if tc is not None:
            check("credit", sum_credit, tc, "sum", "statement")
    return mismatches


def reconcile_summary(
    txns: list[dict], summary: Optional[dict]
) -> list[tuple[str, str]]:
    """``reconcile_mismatches`` as ``(side, message)`` pairs."""
    return [(m.label, m.message) for m in reconcile_mismatches(txns, summary)]
