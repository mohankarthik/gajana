"""Tests for the bank -> Cash Transactions mirror (src/cash_mirror.py)."""

from __future__ import annotations

import datetime
import json
from unittest.mock import MagicMock

import pytest

from src import cash_mirror
from src.cash_mirror import (
    build_marker,
    load_cash_mirror_map,
    mirror_bank_cash_txns,
)


class FakeCashDataSource:
    """Minimal stand-in exposing just the cash-ledger surface the mirror uses."""

    def __init__(self, existing_rows=None):
        self.existing_rows = existing_rows or []
        self.appended: list[list] = []

    def get_cash_log_data(self):
        return self.existing_rows

    def append_cash_rows(self, rows):
        self.appended.extend(rows)


class NoCashDataSource:
    """A data source without a cash ledger (e.g. CSV)."""


def _txn(desc, amount, category, account="bank-axis-primary", date=None):
    return {
        "date": date or datetime.datetime(2026, 5, 3),
        "description": desc,
        "amount": amount,
        "category": category,
        "account": account,
    }


@pytest.fixture(autouse=True)
def _map(monkeypatch, tmp_path):
    """Point the mirror at a known map regardless of the repo's data/ files."""
    p = tmp_path / "cash_mirror.json"
    p.write_text(json.dumps({"Transfer:Cash": "in", "Transfer:Cash Deposit": "out"}))
    monkeypatch.setattr(cash_mirror, "CASH_MIRROR_FILE_PATH", str(p))
    return p


def test_load_map_filters_invalid_directions(tmp_path):
    p = tmp_path / "m.json"
    p.write_text(json.dumps({"A": "in", "B": "sideways", "C": "OUT"}))
    assert load_cash_mirror_map(str(p)) == {"A": "in", "C": "out"}


def test_withdrawal_mirrors_as_credit():
    ds = FakeCashDataSource()
    n = mirror_bank_cash_txns(ds, [_txn("ATM CASH WDL", -2000, "Transfer:Cash")])
    assert n == 1
    date, desc, debit, credit, cat, remarks = ds.appended[0]
    assert debit == "" and credit == "2000.00"
    assert cat == "Transfer:Cash"
    assert remarks.startswith("auto:bank-axis-primary:2026-05-03:-2000.00:")


def test_deposit_mirrors_as_debit():
    ds = FakeCashDataSource()
    n = mirror_bank_cash_txns(ds, [_txn("CASH DEP BR", 5000, "Transfer:Cash Deposit")])
    assert n == 1
    _, _, debit, credit, _, _ = ds.appended[0]
    assert debit == "5000.00" and credit == ""


def test_non_cash_categories_ignored():
    ds = FakeCashDataSource()
    n = mirror_bank_cash_txns(ds, [_txn("ZOMATO", -300, "Expense:Dining")])
    assert n == 0
    assert ds.appended == []


def test_already_mirrored_is_skipped():
    txn = _txn("ATM CASH WDL", -2000, "Transfer:Cash")
    marker = build_marker(txn)
    existing = [["2026-05-03", "ATM CASH WDL", "", "2000.00", "Transfer:Cash", marker]]
    ds = FakeCashDataSource(existing_rows=existing)
    assert mirror_bank_cash_txns(ds, [txn]) == 0
    assert ds.appended == []


def test_repeated_withdrawal_in_batch_mirrors_each_copy():
    txn = _txn("ATM CASH WDL", -2000, "Transfer:Cash")
    ds = FakeCashDataSource()
    # Two booked rows = two real withdrawals sharing one marker; the wallet
    # gained ₹4,000, so the Cash tab needs both rows.
    assert mirror_bank_cash_txns(ds, [dict(txn), dict(txn)]) == 2


def test_same_day_amount_different_desc_not_confused():
    ds = FakeCashDataSource()
    t1 = _txn("ATM WDL HSR BRANCH", -2000, "Transfer:Cash")
    t2 = _txn("ATM WDL KORAMANGALA", -2000, "Transfer:Cash")
    assert mirror_bank_cash_txns(ds, [t1, t2]) == 2


def test_data_source_without_cash_ledger_is_noop():
    assert (
        mirror_bank_cash_txns(NoCashDataSource(), [_txn("ATM", -100, "Transfer:Cash")])
        == 0
    )


def test_empty_input():
    assert mirror_bank_cash_txns(FakeCashDataSource(), []) == 0


def test_mirror_writes_one_cash_row_per_identical_withdrawal(monkeypatch):
    """Three identical ATM withdrawals mirror three times, not once.

    They share a marker (same account/date/amount/description), so set-based
    dedup mirrored only the first and left ₹20,000 of cash unrecorded.
    """
    import src.cash_mirror as cm

    monkeypatch.setattr(
        cm, "load_cash_mirror_map", lambda *a, **k: {"Transfer:Cash": "in"}
    )
    txn = {
        "date": datetime.datetime(2026, 7, 31),
        "description": "ATM WITHDRAWAL : YBL MANIPAL HSPTL-ANGALORE",
        "amount": -10000.0,
        "account": "bank-axis-karti",
        "category": "Transfer:Cash",
    }
    ds = MagicMock()
    ds.get_cash_log_data.return_value = []
    assert cm.mirror_bank_cash_txns(ds, [dict(txn) for _ in range(3)]) == 3

    # A rerun with one already mirrored tops up only the missing two.
    written = ds.append_cash_rows.call_args[0][0]
    ds2 = MagicMock()
    ds2.get_cash_log_data.return_value = [written[0]]
    assert cm.mirror_bank_cash_txns(ds2, [dict(txn) for _ in range(3)]) == 2
