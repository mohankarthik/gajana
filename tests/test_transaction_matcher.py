# tests/test_transaction_matcher.py
from __future__ import annotations

import datetime
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from src.transaction_matcher import SOURCE_KEY, TransactionMatcher


# Fixture to provide common transaction data
@pytest.fixture
def sample_base_txn() -> Dict[str, Any]:
    return {
        "date": datetime.datetime(2023, 1, 15, 10, 30, 0),
        "account": "acc-1",
        "amount": -100.50,
        "description": "Test Transaction 1",
        "category": "Old Category",
    }


@pytest.fixture
def old_transactions(sample_base_txn: Dict[str, Any]) -> List[Dict[str, Any]]:
    txn1 = sample_base_txn.copy()
    txn2 = {
        "date": datetime.datetime(2023, 1, 16),
        "account": "acc-2",
        "amount": 250.00,
        "description": "Another Old One",
    }
    return [txn1, txn2]


@pytest.fixture
def potential_transactions(sample_base_txn: Dict[str, Any]) -> List[Dict[str, Any]]:
    txn1_old_duplicate = sample_base_txn.copy()  # Duplicate of an old one
    txn2_new = {
        "date": datetime.datetime(2023, 1, 17),
        "account": "acc-1",
        "amount": -75.00,
        "description": "A New Transaction",
    }
    txn3_also_new = {
        "date": datetime.datetime(2023, 1, 18),
        "account": "acc-3",
        "amount": 500.00,
        "description": "Completely New",
    }
    # Same transaction reaching us from a second feed: one transaction, not two.
    txn4_potential_duplicate = txn2_new.copy()
    txn2_new[SOURCE_KEY] = "feed-a.pdf"
    txn4_potential_duplicate[SOURCE_KEY] = "feed-b.pdf"
    return [txn1_old_duplicate, txn2_new, txn3_also_new, txn4_potential_duplicate]


def test_find_new_txns_no_potential_transactions(old_transactions):
    """Test when all_potential_txns is empty."""
    result = TransactionMatcher.find_new_txns(old_transactions, [])
    assert result == []


def test_find_new_txns_no_old_transactions(potential_transactions):
    """Test when old_txns is empty, all potential should be new."""
    result = TransactionMatcher.find_new_txns([], potential_transactions)
    assert len(result) == 4
    assert all(SOURCE_KEY not in txn for txn in result)


def test_find_new_txns_some_new_some_old(old_transactions, potential_transactions):
    """Test with a mix of old and new transactions."""
    result = TransactionMatcher.find_new_txns(old_transactions, potential_transactions)
    # txn2_new and txn3_also_new; txn4 is txn2 from a second feed, not a third txn.
    assert len(result) == 2
    descriptions = {txn["description"] for txn in result}
    assert "A New Transaction" in descriptions
    assert "Completely New" in descriptions
    # Check sorting (implicit if we check specific items by index after sorting expected)
    expected_new = [
        {k: v for k, v in potential_transactions[1].items() if k != SOURCE_KEY},
        potential_transactions[2],
    ]  # Based on fixture
    expected_new.sort(
        key=lambda x: (x["date"], x["account"], x["amount"], x["description"])
    )
    result.sort(key=lambda x: (x["date"], x["account"], x["amount"], x["description"]))
    assert result == expected_new


def test_find_new_txns_all_potential_are_old(old_transactions):
    """Test when all potential transactions are already in old_txns."""
    # Use copies of old_transactions as potential_transactions
    result = TransactionMatcher.find_new_txns(
        old_transactions, [t.copy() for t in old_transactions]
    )
    assert result == []


def test_find_new_txns_all_potential_are_new(old_transactions):
    """Test when all potential transactions are new."""
    new_set = [
        {
            "date": datetime.datetime(2024, 1, 1),
            "account": "new-acc-1",
            "amount": 10.0,
            "description": "New 1",
        },
        {
            "date": datetime.datetime(2024, 1, 2),
            "account": "new-acc-2",
            "amount": -20.0,
            "description": "New 2",
        },
    ]
    result = TransactionMatcher.find_new_txns(old_transactions, new_set)
    assert len(result) == 2
    # Result should be sorted, compare against sorted new_set
    new_set.sort(key=lambda x: (x["date"], x["account"], x["amount"], x["description"]))
    result.sort(key=lambda x: (x["date"], x["account"], x["amount"], x["description"]))
    assert result == new_set


def test_find_new_txns_keeps_repeats_from_one_statement(old_transactions):
    """A statement listing the same row twice means two transactions, not one.

    Three identical ATM withdrawals on one day are a real pattern (bank-axis-karti
    Jul-2026); collapsing them silently loses money from the ledger.
    """
    potential_with_duplicates = [
        {
            "date": datetime.datetime(2024, 1, 1),
            "account": "new-acc",
            "amount": 10.0,
            "description": "Unique New 1",
        },
        {
            "date": datetime.datetime(2024, 1, 1),
            "account": "new-acc",
            "amount": 10.0,
            "description": "Unique New 1",
        },  # Identical
        {
            "date": datetime.datetime(2024, 1, 2),
            "account": "new-acc",
            "amount": 20.0,
            "description": "Unique New 2",
        },
    ]
    result = TransactionMatcher.find_new_txns(
        old_transactions, potential_with_duplicates
    )
    assert len(result) == 3  # both copies of Unique New 1, plus Unique New 2


def test_find_new_txns_collapses_same_txn_from_two_feeds(old_transactions):
    """The same transaction arriving via two statements is still one transaction."""
    txn = {
        "date": datetime.datetime(2024, 1, 1),
        "account": "new-acc",
        "amount": 10.0,
        "description": "Unique New 1",
    }
    from_a = dict(txn, **{SOURCE_KEY: "bank-axis-karti-2026-07.pdf"})
    from_b = dict(txn, **{SOURCE_KEY: "gmail-bank-axis-karti-2026-07.pdf"})
    result = TransactionMatcher.find_new_txns(old_transactions, [from_a, from_b])
    assert len(result) == 1


def test_find_new_txns_books_only_the_missing_copies(old_transactions):
    """Two of three repeats already booked -> only the third is new."""
    base = {
        "date": datetime.datetime(2026, 7, 31),
        "account": "bank-axis-karti",
        "amount": -10000.0,
        "description": "ATM WITHDRAWAL : YBL MANIPAL HSPTL-ANGALORE",
    }
    old = old_transactions + [base.copy(), base.copy()]
    potential = [dict(base, **{SOURCE_KEY: "s.pdf"}) for _ in range(3)]
    result = TransactionMatcher.find_new_txns(old, potential)
    assert len(result) == 1


@patch(
    "src.transaction_matcher.logger"
)  # Mock the logger used within TransactionMatcher
def test_find_new_txns_key_error_in_old_txns(mock_logger, potential_transactions):
    """Test handling of KeyError when creating IDs for old_txns."""
    # Malformed old transaction (missing 'amount')
    malformed_old_txns = [
        {
            "date": datetime.datetime(2023, 1, 15),
            "account": "acc-1",
            "description": "Test",
        }
    ]
    # All potential transactions should be considered new as old_txn_ids set will be empty
    unique_potential = [
        potential_transactions[0],
        potential_transactions[1],
        potential_transactions[2],
    ]
    unique_potential.sort(
        key=lambda x: (x["date"], x["account"], x["amount"], x["description"])
    )

    result = TransactionMatcher.find_new_txns(
        malformed_old_txns, potential_transactions
    )

    mock_logger.fatal.assert_called_once()
    assert (
        "Missing key 'amount' in old transactions" in mock_logger.fatal.call_args[0][0]
    )

    # Result should be all unique potential transactions because old_txn_ids became empty
    result.sort(key=lambda x: (x["date"], x["account"], x["amount"], x["description"]))
    assert result == unique_potential
    assert len(result) == 3


@patch("src.transaction_matcher.logger")  # Mock the logger
def test_find_new_txns_key_error_in_potential_txns(mock_logger, old_transactions):
    """Test handling of KeyError when creating IDs for a potential_txn."""
    malformed_potential_txns = [
        old_transactions[0].copy(),  # An old one
        {
            "date": datetime.datetime(2024, 1, 1),
            "account": "new-acc",
            "description": "Bad New",
        },  # Missing 'amount'
        {
            "date": datetime.datetime(2024, 1, 2),
            "account": "new-acc-2",
            "amount": 50.0,
            "description": "Good New",
        },
    ]
    result = TransactionMatcher.find_new_txns(
        old_transactions, malformed_potential_txns
    )

    # Check that logger.fatal was called for the malformed transaction
    # It might be called multiple times if other errors occur, check specific call
    fatal_calls = [call_args[0][0] for call_args in mock_logger.fatal.call_args_list]
    assert any(
        "Missing key 'amount' in potential transaction" in call for call in fatal_calls
    )

    # Only "Good New" should be returned
    assert len(result) == 1
    assert result[0]["description"] == "Good New"


@patch("src.transaction_matcher.logger")
def test_find_new_txns_exception_creating_potential_id(mock_logger, old_transactions):
    """Test handling of general exception when creating ID for a potential_txn."""
    potential_txns_with_bad_date_type = [
        old_transactions[0].copy(),
        {
            "date": "not-a-datetime",
            "account": "new-acc",
            "amount": 10.0,
            "description": "Bad Date Type",
        },
        {
            "date": datetime.datetime(2024, 1, 2),
            "account": "new-acc-2",
            "amount": 50.0,
            "description": "Good New",
        },
    ]
    result = TransactionMatcher.find_new_txns(
        old_transactions, potential_txns_with_bad_date_type
    )

    fatal_calls = [call_args[0][0] for call_args in mock_logger.fatal.call_args_list]
    assert any(
        "Error creating ID for potential transaction" in call for call in fatal_calls
    )

    # Only "Good New" should be returned
    assert len(result) == 1
    assert result[0]["description"] == "Good New"


def test_find_new_txns_description_case_and_whitespace_are_deduped(old_transactions):
    """Case and whitespace variants of the same transaction are duplicates, not new."""
    # old_transactions[0] has description "Test Transaction 1"
    potential = [
        {
            "date": datetime.datetime(2023, 1, 15, 10, 30, 0),
            "account": "acc-1",
            "amount": -100.50,
            "description": "test transaction 1",
            SOURCE_KEY: "feed-a.pdf",
        },  # Lowercase
        {
            "date": datetime.datetime(2023, 1, 15, 10, 30, 0),
            "account": "acc-1",
            "amount": -100.50,
            "description": "Test Transaction 1 ",
            SOURCE_KEY: "feed-b.pdf",
        },  # Trailing space
    ]
    result = TransactionMatcher.find_new_txns(old_transactions, potential)
    # Both normalize to the same signature as the existing old transaction.
    assert result == []


def test_find_new_txns_dedupes_value_dt_metadata_variant(old_transactions):
    """A second feed appending ' Value Dt .../Ref ...' must not read as a new txn."""
    old = old_transactions + [
        {
            "date": datetime.datetime(2026, 2, 1),
            "account": "bank-hdfc-karti",
            "amount": -16285.0,
            "description": "CC 000437546XXXXXX4812 AUTOPAY SI-TAD",
        }
    ]
    potential = [
        {
            "date": datetime.datetime(2026, 2, 1),
            "account": "bank-hdfc-karti",
            "amount": -16285.0,
            "description": (
                "CC 000437546XXXXXX4812 Autopay SI-TAD "
                "Value Dt 01/02/2026 Ref 719206235"
            ),
        }
    ]
    assert TransactionMatcher.find_new_txns(old, potential) == []


def test_find_new_txns_dedupes_by_payment_reference(old_transactions):
    """Same 12-digit UPI reference => duplicate, despite case/space/OCR drift."""
    old = old_transactions + [
        {
            "date": datetime.datetime(2026, 2, 12),
            "account": "bank-hdfc-karti",
            "amount": -35000.0,
            "description": (
                "UPI-LAWYERSONIA1OKAXIS-LAWYERSONIA-1@OKAXIS-"
                "FDRL0001471-604379248662-SUHASINI NOTICE"
            ),
        }
    ]
    potential = [
        {
            "date": datetime.datetime(2026, 2, 12),
            "account": "bank-hdfc-karti",
            "amount": -35000.0,
            "description": (
                "UPI-lawyersonia1okaxis-lawyersonia-1@okaxis-"
                "FDRL0001471-604379248662-Suhasininotice "
                "Value Dt 12/02/2026 Ref 604379248662"
            ),
        }
    ]
    assert TransactionMatcher.find_new_txns(old, potential) == []


def test_find_new_txns_keeps_gst_split_distinct():
    """CGST and SGST postings share one reference but are separate transactions."""
    potential = [
        {
            "date": datetime.datetime(2026, 3, 5),
            "account": "bank-hdfc-karti",
            "amount": -605.51,
            "description": "0503261049900118 DPD026043436402 CGST",
        },
        {
            "date": datetime.datetime(2026, 3, 5),
            "account": "bank-hdfc-karti",
            "amount": -605.51,
            "description": "0503261049900118 DPD026043436402 SGST",
        },
    ]
    result = TransactionMatcher.find_new_txns([], potential)
    assert len(result) == 2


def test_find_new_txns_keeps_distinct_references_distinct():
    """Same-day, same-amount ATM withdrawals with distinct RRNs are not duplicates."""
    potential = [
        {
            "date": datetime.datetime(2026, 5, 5),
            "account": "bank-hdfc-karti",
            "amount": -10000.0,
            "description": (
                "NWD-406584XXXXXX8559-05376621-BANGALORE "
                f"Value Dt 05/05/2026 Ref {rrn}"
            ),
        }
        for rrn in ("612509004108", "612509007940", "612509026289")
    ]
    result = TransactionMatcher.find_new_txns([], potential)
    assert len(result) == 3
