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
