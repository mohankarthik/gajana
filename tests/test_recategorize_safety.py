"""Tests for the overwrite safety guards that prevent live-sheet data loss."""

from __future__ import annotations

import pytest

import main


@pytest.fixture(autouse=True)
def mock_log_and_exit(mocker):
    return mocker.patch("main.log_and_exit", side_effect=SystemExit)


def _txns(*accounts):
    return [
        {"account": a, "date": None, "amount": -1, "description": "x"} for a in accounts
    ]


def test_partition_routes_by_prefix():
    buckets, unknown = main.partition_by_sheet(
        _txns(
            "bank-axis-primary", "cc-axis-platinum", "cc-hdfc-og", "bank-hdfc-secondary"
        )
    )
    assert len(buckets["bank"]) == 2
    assert len(buckets["cc"]) == 2
    assert unknown == []


def test_partition_flags_unknown_prefix():
    buckets, unknown = main.partition_by_sheet(_txns("bank-x", "wallet-paytm", "cc-y"))
    assert len(unknown) == 1
    assert unknown[0]["account"] == "wallet-paytm"


def test_no_row_is_dropped():
    txns = _txns("bank-a", "cc-b", "cc-c", "bank-d")
    buckets, unknown = main.partition_by_sheet(txns)
    assert len(buckets["bank"]) + len(buckets["cc"]) + len(unknown) == len(txns)


def test_guard_aborts_on_unknown_prefix(mocker):
    mocker.patch("main.backup_baseline_counts", return_value={"bank": 0, "cc": 0})
    buckets, unknown = main.partition_by_sheet(_txns("wallet-x"))
    with pytest.raises(SystemExit):
        main.assert_safe_to_overwrite(buckets, unknown, require_baseline=False)


def test_guard_aborts_on_truncated_read(mocker):
    # Live read far below the backup baseline -> truncated -> abort.
    mocker.patch("main.backup_baseline_counts", return_value={"bank": 1000, "cc": 1000})
    buckets, unknown = main.partition_by_sheet(_txns(*(["cc-a"] * 100)))
    with pytest.raises(SystemExit):
        main.assert_safe_to_overwrite(buckets, unknown, require_baseline=True)


def test_guard_requires_baseline_when_asked(mocker):
    mocker.patch("main.backup_baseline_counts", return_value=None)
    buckets, unknown = main.partition_by_sheet(_txns("cc-a"))
    with pytest.raises(SystemExit):
        main.assert_safe_to_overwrite(buckets, unknown, require_baseline=True)


def test_guard_passes_when_counts_healthy(mocker):
    mocker.patch("main.backup_baseline_counts", return_value={"bank": 100, "cc": 100})
    buckets, unknown = main.partition_by_sheet(
        _txns(*(["bank-a"] * 100 + ["cc-b"] * 100))
    )
    # Should not raise.
    main.assert_safe_to_overwrite(buckets, unknown, require_baseline=True)


def test_guard_allows_growth_over_baseline(mocker):
    mocker.patch("main.backup_baseline_counts", return_value={"bank": 50, "cc": 50})
    buckets, unknown = main.partition_by_sheet(
        _txns(*(["bank-a"] * 200 + ["cc-b"] * 200))
    )
    main.assert_safe_to_overwrite(buckets, unknown, require_baseline=True)


# --- Recategorize writes cells, never the whole sheet ------------------------


class _ExplodingProcessor:
    """A processor whose overwrite path fails the test if it is ever reached."""

    def __init__(self, txns, log_rows):
        self.txns = txns
        self.log_rows = log_rows
        self.applied: list[tuple[str, list]] = []

    def get_all_transactions_for_recategorize(self):
        return self.txns

    def apply_category_updates(self, account_type, updates):
        self.applied.append((account_type, updates))
        return len(updates)

    def overwrite_transaction_log(self, txns, account_type):  # pragma: no cover
        raise AssertionError(
            "recategorize must not overwrite the log; it writes Category cells"
        )


class _StubCategorizer:
    llm = None

    def build_index(self, history, enable_llm=False):
        pass

    def categorize(self, txns):
        for txn in txns:
            txn["category"] = "Expense:Dining"
        return txns


def _recat_txns():
    from src.transaction_processor import ROW_KEY

    return [
        {
            "account": "cc-hdfc-og",
            "date": None,
            "amount": -1,
            "description": "a",
            "category": "Uncategorized",
            ROW_KEY: 0,
        },
        {
            "account": "cc-hdfc-og",
            "date": None,
            "amount": -2,
            "description": "b",
            "category": "Expense:Fuel",
            ROW_KEY: 1,
        },
        {
            "account": "bank-axis-primary",
            "date": None,
            "amount": -3,
            "description": "c",
            "category": "Uncategorized",
            ROW_KEY: 7,
        },
    ]


def test_recategorize_never_overwrites_the_log(mocker):
    mocker.patch("main.backup_baseline_counts", return_value={"bank": 1, "cc": 1})
    proc = _ExplodingProcessor(_recat_txns(), [])

    main.run_recategorize_mode(proc, _StubCategorizer())

    # Only the two Uncategorized rows are touched, addressed by their row index.
    assert proc.applied == [
        ("bank", [(7, "Expense:Dining")]),
        ("cc", [(0, "Expense:Dining")]),
    ]


def test_recategorize_dry_run_writes_nothing(mocker):
    mocker.patch("main.backup_baseline_counts", return_value={"bank": 1, "cc": 1})
    proc = _ExplodingProcessor(_recat_txns(), [])

    main.run_recategorize_mode(proc, _StubCategorizer(), dry_run=True)

    assert proc.applied == []


def test_recategorize_skips_truncated_read_check(mocker):
    """A short read can no longer delete rows, so it must not abort the run."""
    mocker.patch("main.backup_baseline_counts", return_value={"bank": 9999, "cc": 9999})
    proc = _ExplodingProcessor(_recat_txns(), [])

    main.run_recategorize_mode(proc, _StubCategorizer())  # must not SystemExit

    assert proc.applied
