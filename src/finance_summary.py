"""Finance summary writer — emit the Homepage dashboard feed as a static JSON.

Gajana already holds the Google service-account credentials, so it (not a
separate service) reads the two sheets and writes ``data/finance/summary.json``.
A credential-free nginx then serves that file. This module never starts a
server; it is a batch job run hourly from cron.

The output schema mirrors the previous interim ``finance-summary`` FastAPI
service exactly, so the Homepage customapi mappings do not change:

  * Investments (``Holding`` tab): net worth / portfolio value, invested,
    profit, profit %, XIRR.
  * Annual cash flow (``Yearly`` pivot, current-year column auto-detected):
    income, regular expenses, new investments.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

from src.google_data_source import GoogleDataSource
from src.interfaces import DataSourceInterface
from src.settings import INVESTMENTS_SHEET_ID, TRANSACTIONS_SHEET_ID

logger = logging.getLogger(__name__)

# Container cwd is /app, so this resolves to /app/data/finance/summary.json.
# The dir is mounted read-only into nginx; keep it dedicated to this feed.
OUTPUT_PATH = os.environ.get("FINANCE_SUMMARY_OUTPUT", "data/finance/summary.json")

# 'Yearly' pivot rows (col-A labels) pulled for the current-year column.
ANNUAL_ROWS = {
    "income": "Income",
    "regular_expenses": "Regular Expenses",
    "new_investments": "New Investments",
}


def _cell(grid: List[List[Any]], r: int, c: int = 0) -> Optional[Any]:
    return grid[r][c] if r < len(grid) and c < len(grid[r]) else None


def _num(s: Any) -> Optional[float]:
    """Parse a formatted sheet value (₹1,39,54,452 / 56.80% / -₹19 / #N/A)."""
    if s is None:
        return None
    s = str(s).strip()
    if not s or s.startswith("#"):
        return None
    neg = s[0] in "-−"  # ascii minus or unicode minus
    digits = re.sub(r"[^0-9.]", "", s)
    if digits in ("", "."):
        return None
    v = float(digits)
    return -v if neg else v


def _inr(n: Optional[float]) -> str:
    """Compact Indian currency: crore / lakh."""
    if n is None:
        return "—"
    a = abs(n)
    if a >= 1e7:
        return f"₹{n / 1e7:.2f} Cr"
    if a >= 1e5:
        return f"₹{n / 1e5:.2f} L"
    return f"₹{n:,.0f}"


def _pct(s: Any) -> str:
    v = _num(s)
    return f"{v:.2f}%" if v is not None else "—"


def _investments(ds: DataSourceInterface) -> Dict[str, Any]:
    # 'Holding' scalar cells: B2 invested, B3 current value; M2 XIRR, M3 profit %.
    b = ds.get_sheet_data(INVESTMENTS_SHEET_ID, "Holding", "B2:B3")
    m = ds.get_sheet_data(INVESTMENTS_SHEET_ID, "Holding", "M2:M3")
    invested = _num(_cell(b, 0))
    value = _num(_cell(b, 1))
    profit = value - invested if (value is not None and invested is not None) else None
    return {
        "value": value,
        "invested": invested,
        "profit": profit,
        "xirr": _pct(_cell(m, 0)),
        "profit_pct": _pct(_cell(m, 1)),
    }


def _annual(ds: DataSourceInterface) -> Dict[str, Any]:
    grid = ds.get_sheet_data(TRANSACTIONS_SHEET_ID, "Yearly", "A2:AB200")
    header = grid[0] if grid else []
    rows = {r[0]: r for r in grid[1:] if r}
    # Locate the current-year column by matching the header (fall back to latest).
    yearcols: Dict[int, int] = {}
    for i, h in enumerate(header):
        try:
            yearcols[int(str(h).strip())] = i
        except ValueError:
            pass
    year = datetime.date.today().year
    if year not in yearcols and yearcols:
        year = max(yearcols)
    col = yearcols.get(year)

    def val(label: str) -> Optional[float]:
        r = rows.get(label)
        if not r or col is None or col >= len(r):
            return None
        return _num(r[col])

    out: Dict[str, Any] = {"year": year}
    for key, label in ANNUAL_ROWS.items():
        out[key] = val(label)
    return out


def build_summary(ds: DataSourceInterface) -> Dict[str, Any]:
    """Compute the full summary dict from the data source."""
    inv = _investments(ds)
    ann = _annual(ds)
    return {
        # investments
        "net_worth": _inr(inv["value"]),
        "net_worth_raw": inv["value"],
        "portfolio_value": _inr(inv["value"]),
        "invested": _inr(inv["invested"]),
        "profit": _inr(inv["profit"]),
        "profit_pct": inv["profit_pct"],
        "xirr": inv["xirr"],
        # annual cash flow (current year)
        "year": ann["year"],
        "income": _inr(ann["income"]),
        "income_raw": ann["income"],
        "regular_expenses": _inr(ann["regular_expenses"]),
        "new_investments": _inr(ann["new_investments"]),
        "updated": datetime.datetime.now().isoformat(timespec="seconds"),
    }


def write_summary(data: Dict[str, Any], path: str = OUTPUT_PATH) -> None:
    """Atomically write the summary JSON (temp file in same dir + os.replace)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    if not INVESTMENTS_SHEET_ID:
        raise RuntimeError(
            "google_sheets_investments_id not set in settings.json; "
            "cannot build the finance summary."
        )
    ds = GoogleDataSource()
    data = build_summary(ds)
    write_summary(data)
    logger.info("Wrote finance summary to %s: %s", OUTPUT_PATH, data)


if __name__ == "__main__":
    main()
