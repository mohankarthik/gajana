# gajana/statement_reconciler.py
"""Reconcile statement PDFs against the live ledger. Read-only.

The ledger is the thing under test: statements are the source of truth, so
every row a *verified* statement prints must appear in the ledger exactly as
many times as the statement prints it, and no more. Unverified statements are
excluded outright -- an extraction that could not certify itself has no
standing to accuse the ledger of anything.

Three things make the comparison honest:

* **Multisets, not sets.** Three identical 10,000 ATM withdrawals on one day are
  three transactions. Counting them (rather than collapsing to a set) is what
  makes a dropped duplicate visible.
* **Salary splits folded back to their net.** The salary splitter books one bank
  credit as several categorized legs, so the ledger legitimately holds rows the
  statement never printed. Each day carrying an ``Income:Google`` row has its
  split legs collapsed back into a single net row before comparing.
* **Statement files de-duplicated by (account, period).** ``_copy1`` files and
  re-issued statements otherwise double every row in their month.

Run as a CLI:

    python -m src.statement_reconciler --pdf-dir ~/statements
    python -m src.statement_reconciler --statements-json snapshot.json --json out.json

Nothing here writes to a sheet; the data source is used for reads only.
"""

from __future__ import annotations

import argparse
import collections
import datetime
import glob
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from src.interfaces import DataSourceInterface
from src.statement_extractor import (
    ExtractedStatement,
    extract_statement,
    extract_statement_bytes,
    statement_passwords,
)

logger = logging.getLogger(__name__)

DEFAULT_SINCE = "2024-09"

# A salary day's split legs: the source credit's categories, as booked by
# plugins/salary_splitter. Folding them back to their net is what lets a
# split-out salary be compared against the single credit the bank printed.
SALARY_ANCHOR_CATEGORY = "Income:Google"
SALARY_SPLIT_CATEGORIES: Tuple[str, ...] = (
    "Income:Google",
    "Tax",
    "Investment Expense",
    "Insurance:",
)
SALARY_FOLD_DESCRIPTION = "<salary split collapsed>"

_MONTHS = {
    m: i + 1
    for i, m in enumerate(
        [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
    )
}


# --------------------------------------------------------------------------
# Records
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class StatementRecord:
    """One statement file, reduced to what the comparison needs."""

    name: str
    account: str
    period: Optional[Tuple[str, str]]
    verified: bool
    problems: Tuple[str, ...] = ()
    rows: Tuple[Tuple[str, float, str], ...] = ()  # (iso date, signed amount, desc)

    @classmethod
    def from_extracted(cls, name: str, st: ExtractedStatement) -> "StatementRecord":
        verified, problems = st.verified, st.problems
        return cls(
            name=name,
            account=account_from_filename(name),
            period=tuple(st.period) if st.period else None,  # type: ignore[arg-type]
            verified=verified,
            problems=tuple(problems),
            rows=tuple(
                (r.date.strftime("%Y-%m-%d"), round(r.amount, 2), r.description)
                for r in st.rows
            ),
        )

    @classmethod
    def from_summary(cls, name: str, summary: Mapping[str, Any]) -> "StatementRecord":
        """Rebuild from the JSON snapshot ``ExtractedStatement.summary()`` writes."""
        period = summary.get("period")
        return cls(
            name=name,
            account=account_from_filename(name),
            period=(period[0], period[1]) if period else None,
            verified=bool(summary.get("verified")),
            problems=tuple(summary.get("problems") or ()),
            rows=tuple(
                (r["date"], round(float(r["amt"]), 2), r.get("desc", ""))
                for r in summary.get("rows") or ()
            ),
        )


@dataclass(frozen=True)
class Difference:
    """One (date, amount) whose statement multiplicity != its ledger multiplicity."""

    date: str
    amount: float
    statement_count: int
    ledger_count: int
    statement_desc: str = ""
    ledger_desc: str = ""
    ledger_category: str = ""

    @property
    def missing(self) -> int:
        """Copies a verified statement printed that the ledger never booked."""
        return max(0, self.statement_count - self.ledger_count)

    @property
    def excess(self) -> int:
        """Copies the ledger booked that the statement does not account for."""
        return max(0, self.ledger_count - self.statement_count)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "amt": self.amount,
            "stmt": self.statement_count,
            "led": self.ledger_count,
            "sdesc": self.statement_desc,
            "ldesc": self.ledger_desc,
            "lcat": self.ledger_category,
        }


@dataclass(frozen=True)
class MonthReport:
    account: str
    month: str
    statement_rows: int
    ledger_rows: int
    differences: Tuple[Difference, ...]
    splits: Mapping[str, Tuple[int, float]] = field(default_factory=dict)

    @property
    def differs(self) -> bool:
        return bool(self.differences)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "stmt_n": self.statement_rows,
            "led_n": self.ledger_rows,
            "items": [d.as_dict() for d in self.differences],
            "splits": {d: list(v) for d, v in self.splits.items()},
        }


@dataclass(frozen=True)
class ReconcileReport:
    months: Tuple[MonthReport, ...]
    duplicate_files: Tuple[Tuple[str, str, Optional[Tuple[str, str]]], ...] = ()
    unverified: Tuple[Tuple[str, Tuple[str, ...]], ...] = ()

    @property
    def differing(self) -> Tuple[MonthReport, ...]:
        return tuple(m for m in self.months if m.differs)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "report": {f"{m.account}@{m.month}": m.as_dict() for m in self.months},
            "dupfiles": [list(d) for d in self.duplicate_files],
            "unverified": [[name, list(p)] for name, p in self.unverified],
        }


# --------------------------------------------------------------------------
# Statement side
# --------------------------------------------------------------------------


def previous_month(month: str) -> str:
    """``2024-09`` -> ``2024-08``.

    Statement filenames are stamped with the *fetch* month, which is the month
    after the period they cover, so the file-name cutoff for a period cutoff is
    one month earlier.
    """
    year, mon = int(month[:4]), int(month[5:7])
    return f"{year - 1}-12" if mon == 1 else f"{year}-{mon - 1:02d}"


def account_from_filename(name: str) -> str:
    """``bank-axis-karti-2026-05_copy1.pdf`` -> ``bank-axis-karti``."""
    return "-".join(os.path.basename(name).split("-")[:3])


def parse_statement_date(token: str) -> Optional[datetime.date]:
    """Parse the date forms statements print their period in."""
    m = re.match(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", token)
    if m:
        return datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    m = re.match(r"(\d{1,2}) (\w{3}),? (\d{4})", token)
    if m and m.group(2) in _MONTHS:
        return datetime.date(int(m.group(3)), _MONTHS[m.group(2)], int(m.group(1)))
    return None


def period_months(period: Optional[Sequence[str]]) -> List[str]:
    """Every ``YYYY-MM`` a statement period touches."""
    if not period:
        return []
    start, end = parse_statement_date(period[0]), parse_statement_date(period[1])
    if start is None or end is None or end < start:
        return []
    months: List[str] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append(f"{year}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def canonical_statements(
    records: Iterable[StatementRecord],
) -> Tuple[List[StatementRecord], List[Tuple[str, str, Optional[Tuple[str, str]]]]]:
    """Keep one verified statement per (account, period).

    A re-fetched or re-issued statement is byte-different but transaction-
    identical; booking both would double its whole month. Filenames sort
    deterministically, so the first one seen wins and the rest are reported.
    """
    kept: Dict[Tuple[str, Optional[Tuple[str, str]]], StatementRecord] = {}
    duplicates: List[Tuple[str, str, Optional[Tuple[str, str]]]] = []
    for rec in sorted(records, key=lambda r: r.name):
        if not rec.verified:
            continue
        key = (rec.account, rec.period)
        if key in kept:
            duplicates.append((rec.name, kept[key].name, rec.period))
            continue
        kept[key] = rec
    return list(kept.values()), duplicates


# --------------------------------------------------------------------------
# Ledger side
# --------------------------------------------------------------------------


def _iso(value: Any) -> str:
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.strftime("%Y-%m-%d")
    return str(value)[:10]


def fold_salary_splits(
    txns: Sequence[Mapping[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[int, float]]]:
    """Collapse each salary day's split legs into the single net credit.

    The splitter books one bank credit as an ``Income:Google`` leg plus tax,
    investment and insurance deductions. The bank printed one row, so the legs
    are folded back to their net before comparing; anything else on the same day
    is left alone. Returns the adjusted transactions and, per date,
    ``(leg count, net amount)``.
    """
    by_date: Dict[str, List[Mapping[str, Any]]] = collections.defaultdict(list)
    for txn in txns:
        by_date[_iso(txn.get("date"))].append(txn)

    adjusted: List[Dict[str, Any]] = []
    splits: Dict[str, Tuple[int, float]] = {}
    for date, day_txns in by_date.items():
        categories = [str(t.get("category") or "") for t in day_txns]
        if SALARY_ANCHOR_CATEGORY not in categories:
            adjusted.extend(dict(t) for t in day_txns)
            continue
        legs = [
            t
            for t, cat in zip(day_txns, categories)
            if cat.startswith(SALARY_SPLIT_CATEGORIES)
        ]
        rest = [
            t
            for t, cat in zip(day_txns, categories)
            if not cat.startswith(SALARY_SPLIT_CATEGORIES)
        ]
        net = round(sum(float(t.get("amount") or 0.0) for t in legs), 2)
        splits[date] = (len(legs), net)
        adjusted.extend(dict(t) for t in rest)
        adjusted.append(
            {
                "date": date,
                "amount": net,
                "description": SALARY_FOLD_DESCRIPTION,
                "category": SALARY_ANCHOR_CATEGORY,
                "account": day_txns[0].get("account"),
                "remarks": "",
            }
        )
    return adjusted, splits


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------


def compare_month(
    statement_rows: Sequence[Tuple[str, float, str]],
    ledger_txns: Sequence[Mapping[str, Any]],
) -> List[Difference]:
    """Multiset diff of (date, amount) between a statement month and the ledger."""
    stmt_counts = collections.Counter((d, round(a, 2)) for d, a, _ in statement_rows)
    ledger_counts = collections.Counter(
        (_iso(t.get("date")), round(float(t.get("amount") or 0.0), 2))
        for t in ledger_txns
    )
    out: List[Difference] = []
    for key in sorted(set(stmt_counts) | set(ledger_counts)):
        if stmt_counts[key] == ledger_counts[key]:
            continue
        sdesc = next((d for dt, a, d in statement_rows if (dt, round(a, 2)) == key), "")
        led = next(
            (
                t
                for t in ledger_txns
                if (_iso(t.get("date")), round(float(t.get("amount") or 0.0), 2)) == key
            ),
            None,
        )
        out.append(
            Difference(
                date=key[0],
                amount=key[1],
                statement_count=stmt_counts[key],
                ledger_count=ledger_counts[key],
                statement_desc=sdesc[:90],
                ledger_desc=str(led.get("description", ""))[:90] if led else "",
                ledger_category=str(led.get("category", "")) if led else "",
            )
        )
    return out


def reconcile(
    records: Iterable[StatementRecord],
    ledger: Sequence[Mapping[str, Any]],
    since: str = DEFAULT_SINCE,
    accounts: Optional[Sequence[str]] = None,
) -> ReconcileReport:
    """Compare every statement-covered account-month against the ledger.

    Only months a verified statement actually covers are compared: a month with
    no statement is unknown, not clean.
    """
    all_records = list(records)
    canonical, duplicates = canonical_statements(all_records)
    unverified = tuple(
        (r.name, r.problems)
        for r in sorted(all_records, key=lambda r: r.name)
        if not r.verified
    )

    stmt_rows: Dict[str, List[Tuple[str, float, str]]] = collections.defaultdict(list)
    coverage: Dict[str, set] = collections.defaultdict(set)
    for rec in canonical:
        if accounts and rec.account not in accounts:
            continue
        stmt_rows[rec.account].extend(rec.rows)
        coverage[rec.account].update(period_months(rec.period))

    ledger_by_account: Dict[str, List[Mapping[str, Any]]] = collections.defaultdict(
        list
    )
    for txn in ledger:
        ledger_by_account[str(txn.get("account") or "")].append(txn)

    months: List[MonthReport] = []
    for account in sorted(coverage):
        adjusted, splits = fold_salary_splits(ledger_by_account.get(account, []))
        by_month_stmt: Dict[str, List[Tuple[str, float, str]]] = (
            collections.defaultdict(list)
        )
        for row in stmt_rows[account]:
            by_month_stmt[row[0][:7]].append(row)
        by_month_ledger: Dict[str, List[Mapping[str, Any]]] = collections.defaultdict(
            list
        )
        for txn in adjusted:
            by_month_ledger[_iso(txn.get("date"))[:7]].append(txn)

        for month in sorted(coverage[account]):
            if month < since:
                continue
            srows = by_month_stmt.get(month, [])
            lrows = by_month_ledger.get(month, [])
            months.append(
                MonthReport(
                    account=account,
                    month=month,
                    statement_rows=len(srows),
                    ledger_rows=len(lrows),
                    differences=tuple(compare_month(srows, lrows)),
                    splits={d: v for d, v in splits.items() if d[:7] == month},
                )
            )
    return ReconcileReport(
        months=tuple(months), duplicate_files=tuple(duplicates), unverified=unverified
    )


# --------------------------------------------------------------------------
# Sources
# --------------------------------------------------------------------------


def records_from_pdf_dir(
    pdf_dir: str, prefix: str = "", since_file: str = ""
) -> Tuple[List[StatementRecord], Dict[str, Any]]:
    """Extract every statement PDF in a local directory.

    ``since_file`` filters on the statement filename's ``YYYY-MM`` suffix, which
    is the fetch month -- cheap way to skip a decade of archived statements.
    """
    records: List[StatementRecord] = []
    snapshot: Dict[str, Any] = {}
    pattern = os.path.join(pdf_dir, f"{prefix}*.pdf" if prefix else "*.pdf")
    for path in sorted(glob.glob(pattern)):
        name = os.path.basename(path)
        if since_file and name[-11:-4] < since_file:
            continue
        try:
            statement = extract_statement(path)
        except Exception as e:
            logger.warning(f"{name}: extraction failed: {e}")
            continue
        snapshot[name] = statement.summary()
        records.append(StatementRecord.from_extracted(name, statement))
    return records, snapshot


def records_from_data_source(
    data_source: DataSourceInterface, prefix: str = "", since_file: str = ""
) -> Tuple[List[StatementRecord], Dict[str, Any]]:
    """Extract every statement PDF the data source lists (downloads, no writes)."""
    records: List[StatementRecord] = []
    snapshot: Dict[str, Any] = {}
    for detail in sorted(
        data_source.list_statement_file_details(), key=lambda d: d.name
    ):
        name = detail.name
        if not name.lower().endswith(".pdf"):
            continue
        if prefix and not name.startswith(prefix):
            continue
        if since_file and name[-11:-4] < since_file:
            continue
        try:
            data = data_source.download_file(detail.id)
            statement = extract_statement_bytes(data, name, statement_passwords(name))
        except Exception as e:
            logger.warning(f"{name}: extraction failed: {e}")
            continue
        snapshot[name] = statement.summary()
        records.append(StatementRecord.from_extracted(name, statement))
    return records, snapshot


def records_from_snapshot(snapshot: Mapping[str, Any]) -> List[StatementRecord]:
    """Rebuild records from a snapshot JSON written by a previous run."""
    return [
        StatementRecord.from_summary(name, summary)
        for name, summary in sorted(snapshot.items())
    ]


def load_ledger(
    data_source: DataSourceInterface, log_type: str
) -> List[Dict[str, Any]]:
    """Read the transaction log through the data source. Read-only."""
    from src.transaction_processor import TransactionProcessor

    txns = TransactionProcessor(data_source).get_old_transactions(log_type)
    return [{str(k): v for k, v in t.items()} for t in txns]


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------


def format_report(report: ReconcileReport) -> str:
    lines: List[str] = []
    for month in report.differing:
        lines.append(
            f"===== {month.account} {month.month}  "
            f"stmt={month.statement_rows} ledger={month.ledger_rows}"
        )
        for d in month.differences:
            lines.append(
                f"  {d.date} {d.amount:14.2f} stmt={d.statement_count} "
                f"led={d.ledger_count}  S:{d.statement_desc[:52]}   "
                f"L:[{d.ledger_category}] {d.ledger_desc[:46]}"
            )
    if report.duplicate_files:
        lines.append("")
        lines.append("duplicate statement files (same account+period):")
        for name, kept, period in report.duplicate_files:
            lines.append(f"  {name}  ==  {kept}   {period}")
    if report.unverified:
        lines.append("")
        lines.append("unverified statements (excluded from the comparison):")
        for name, problems in report.unverified:
            lines.append(f"  {name}: {'; '.join(problems)[:110]}")
    lines.append("")
    lines.append(
        f"account-months compared: {len(report.months)}  "
        f"with differences: {len(report.differing)}"
    )
    return "\n".join(lines)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile statement PDFs against the ledger (read-only)."
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--pdf-dir", help="Local directory of statement PDFs to extract."
    )
    source.add_argument(
        "--statements-json",
        help="Reuse a snapshot written by --snapshot instead of re-parsing PDFs.",
    )
    parser.add_argument(
        "--csv-db-path", help="Read the ledger from local CSVs instead of Sheets."
    )
    parser.add_argument(
        "--log-type",
        default="bank",
        choices=["bank", "cc"],
        help="Which ledger to reconcile (default: bank).",
    )
    parser.add_argument(
        "--since", default=DEFAULT_SINCE, help="First month to compare (YYYY-MM)."
    )
    parser.add_argument(
        "--account", action="append", help="Limit to this account (repeatable)."
    )
    parser.add_argument("--json", help="Write the full report as JSON here.")
    parser.add_argument(
        "--snapshot", help="Write the raw extraction snapshot as JSON here."
    )
    parser.add_argument(
        "--no-ledger",
        action="store_true",
        help="Extract and verify statements only; never touch the ledger.",
    )
    return parser


def _make_data_source(csv_db_path: Optional[str]) -> DataSourceInterface:
    if csv_db_path:
        from src.csv_data_source import CSVDataSource

        return CSVDataSource(csv_db_path)
    from src.google_data_source import GoogleDataSource

    return GoogleDataSource()


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    prefix = "bank-" if args.log_type == "bank" else "cc-"
    since_file = previous_month(args.since) if args.since else ""

    data_source: Optional[DataSourceInterface] = None
    snapshot: Dict[str, Any] = {}
    if args.statements_json:
        with open(args.statements_json, "r", encoding="utf-8") as fh:
            snapshot = {
                name: summary
                for name, summary in json.load(fh).items()
                if name.startswith(prefix)
            }
        records = records_from_snapshot(snapshot)
    elif args.pdf_dir:
        records, snapshot = records_from_pdf_dir(args.pdf_dir, prefix, since_file)
    else:
        data_source = _make_data_source(args.csv_db_path)
        records, snapshot = records_from_data_source(data_source, prefix, since_file)

    verified = sum(1 for r in records if r.verified)
    print(f"statements: {verified}/{len(records)} verified")
    if args.snapshot:
        with open(args.snapshot, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh)
        print(f"snapshot written to {args.snapshot}")
    if args.no_ledger:
        for name, problems in sorted(
            (r.name, r.problems) for r in records if not r.verified
        ):
            print(f"  UNVERIFIED {name}: {'; '.join(problems)[:110]}")
        return 0

    if data_source is None:
        data_source = _make_data_source(args.csv_db_path)
    ledger = load_ledger(data_source, args.log_type)
    report = reconcile(records, ledger, since=args.since, accounts=args.account)
    print(format_report(report))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(report.as_dict(), fh, indent=1)
        print(f"report written to {args.json}")
    return 1 if report.differing else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
