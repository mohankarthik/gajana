"""Mirror bank cash movements into the shared Cash Transactions ledger.

An ATM/cash withdrawal shows up in a bank statement as a debit (money leaves the
bank), but that cash physically enters the wallet -> it should appear as a
*credit* (cash-in) in the Cash Transactions tab. A cash deposit into the bank is
the reverse: a bank credit that removes cash from the wallet -> a Cash *debit*.

Which bank categories to mirror, and in which direction, is configured in
``data/cash_mirror.json`` (falls back to the committed ``.example``):

    {"Transfer:Cash": "in", "Transfer:Cash Deposit": "out"}

``in``  -> wallet gains cash -> Cash credit (positive).
``out`` -> wallet loses cash -> Cash debit (negative).

Idempotency: every mirrored row carries a stable marker in its Remarks column
(``auto:{account}:{date}:{signed_amount}:{deschash}``). Before writing, existing
Cash rows are scanned and any transaction whose marker is already present is
skipped, so repeated daily runs never double-book.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import re
from typing import Any, Hashable

logger = logging.getLogger(__name__)

# Personal map lives in data/cash_mirror.json (gitignored); fall back to the
# committed example so a fresh clone runs without extra setup.
CASH_MIRROR_FILE_PATH = (
    "data/cash_mirror.json"
    if os.path.exists("data/cash_mirror.json")
    else "data/cash_mirror.example.json"
)

MARKER_PREFIX = "auto"


def load_cash_mirror_map(path: str = CASH_MIRROR_FILE_PATH) -> dict[str, str]:
    """Loads the {category: "in"|"out"} mirror map. Missing/broken -> {}."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load cash-mirror map from {path}: {e}")
        return {}
    if not isinstance(data, dict):
        logger.warning(f"Cash-mirror map {path} is not an object; ignoring it.")
        return {}
    out: dict[str, str] = {}
    for cat, direction in data.items():
        d = str(direction).strip().lower()
        if d not in ("in", "out"):
            logger.warning(
                f"Cash-mirror map: category {cat!r} has invalid direction "
                f"{direction!r} (want 'in'/'out'); skipping."
            )
            continue
        out[str(cat)] = d
    return out


def _normalize(s: str) -> str:
    """Lowercase alphanumerics only; robust to whitespace/punctuation drift."""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def build_marker(txn: dict[Hashable, Any]) -> str:
    """Stable per-transaction marker used to dedupe mirrored cash rows."""
    date = txn.get("date")
    date_str = (
        date.strftime("%Y-%m-%d") if isinstance(date, datetime.datetime) else str(date)
    )
    try:
        amount = float(txn.get("amount", 0.0))
    except (ValueError, TypeError):
        amount = 0.0
    account = str(txn.get("account", ""))
    desc_hash = hashlib.sha1(
        _normalize(str(txn.get("description", ""))).encode("utf-8")
    ).hexdigest()[:8]
    return f"{MARKER_PREFIX}:{account}:{date_str}:{amount:.2f}:{desc_hash}"


def _existing_markers(cash_rows: list[list[Any]]) -> set[str]:
    """Extract markers already present in the Cash tab's Remarks column (idx 5,
    for B3:G rows). Scans the whole cell so extra text around a marker is fine."""
    markers: set[str] = set()
    pat = re.compile(rf"{MARKER_PREFIX}:[^\s]+")
    for row in cash_rows:
        if len(row) < 6:
            continue
        for m in pat.findall(str(row[5])):
            markers.add(m)
    return markers


def _cash_row(txn: dict[Hashable, Any], direction: str, marker: str) -> list[Any]:
    """Build a Cash tab row (B:G) mirroring one bank cash transaction.

    ``in`` -> Credit (wallet gains cash); ``out`` -> Debit (wallet loses cash).
    Amount magnitude is taken from the bank txn regardless of its sign, so the
    direction is driven purely by config, not by a possibly-mis-signed source.
    """
    date = txn.get("date")
    date_str = (
        date.strftime("%Y-%m-%d") if isinstance(date, datetime.datetime) else str(date)
    )
    try:
        magnitude = abs(float(txn.get("amount", 0.0)))
    except (ValueError, TypeError):
        magnitude = 0.0
    debit = "" if direction == "in" else f"{magnitude:.2f}"
    credit = f"{magnitude:.2f}" if direction == "in" else ""
    return [
        date_str,
        str(txn.get("description", "")),
        debit,
        credit,
        str(txn.get("category", "")),
        marker,
    ]


def mirror_bank_cash_txns(data_source: Any, txns: list[dict[Hashable, Any]]) -> int:
    """Mirror bank cash movements in ``txns`` into the Cash Transactions tab.

    Returns the number of rows written. No-op (returns 0) when the data source
    has no cash ledger (e.g. CSVDataSource), the map is empty, or nothing
    matches. Already-mirrored transactions are skipped via their Remarks marker.
    """
    if not txns:
        return 0
    if not (
        hasattr(data_source, "get_cash_log_data")
        and hasattr(data_source, "append_cash_rows")
    ):
        logger.debug("Data source has no cash ledger; skipping cash mirror.")
        return 0

    mirror_map = load_cash_mirror_map()
    if not mirror_map:
        return 0

    candidates = [t for t in txns if str(t.get("category", "")) in mirror_map]
    if not candidates:
        return 0

    existing = _existing_markers(data_source.get_cash_log_data())

    rows: list[list[Any]] = []
    seen: set[str] = set()  # guard against dupes within this same batch
    for txn in candidates:
        marker = build_marker(txn)
        if marker in existing or marker in seen:
            continue
        seen.add(marker)
        direction = mirror_map[str(txn.get("category"))]
        rows.append(_cash_row(txn, direction, marker))

    if not rows:
        logger.info("Cash mirror: all matching bank txns already mirrored.")
        return 0

    data_source.append_cash_rows(rows)
    logger.info(f"Cash mirror: wrote {len(rows)} row(s) to the Cash Transactions tab.")
    return len(rows)
