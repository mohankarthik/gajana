# tests/test_transaction_matcher.py
from __future__ import annotations

import datetime
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from src.transaction_matcher import TransactionMatcher


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
    # Duplicate within potential list, should only be added once
    txn4_potential_duplicate = txn2_new.copy()
    return [txn1_old_duplicate, txn2_new, txn3_also_new, txn4_potential_duplicate]


def test_find_new_txns_no_potential_transactions(old_transactions):
    """Test when all_potential_txns is empty."""
    result = TransactionMatcher.find_new_txns(old_transactions, [])
    assert result == []


def test_find_new_txns_no_old_transactions(potential_transactions):
    """Test when old_txns is empty, all potential should be new."""
    # Remove internal duplicate from potential_transactions for this test's expectation
    unique_potential = [
        potential_transactions[0],
        potential_transactions[1],
        potential_transactions[2],
        potential_transactions[3],
    ]
    unique_potential.sort(
        key=lambda x: (x["date"], x["account"], x["amount"], x["description"])
    )

    result = TransactionMatcher.find_new_txns([], potential_transactions)
    # Result is sorted, so compare sorted
    result.sort(key=lambda x: (x["date"], x["account"], x["amount"], x["description"]))
    assert result == unique_potential
    assert len(result) == 4


def test_find_new_txns_some_new_some_old(old_transactions, potential_transactions):
    """Test with a mix of old and new transactions."""
    result = TransactionMatcher.find_new_txns(old_transactions, potential_transactions)
    # Expected new transactions (txn2_new and txn3_also_new from potential_transactions fixture)
    assert len(result) == 2
    descriptions = {txn["description"] for txn in result}
    assert "A New Transaction" in descriptions
    assert "Completely New" in descriptions
    # Check sorting (implicit if we check specific items by index after sorting expected)
    expected_new = [
        potential_transactions[1],
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


def test_find_new_txns_handles_internal_duplicates_in_potential(old_transactions):
    """Test that duplicates within all_potential_txns are only added once."""
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
    assert len(result) == 2  # Should only have Unique New 1 and Unique New 2


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


def test_find_new_txns_description_whitespace_and_case_sensitivity(old_transactions):
    """Test if description matching is sensitive to case and whitespace (it should be by default)."""
    # old_transactions[0] has description "Test Transaction 1"
    potential = [
        {
            "date": datetime.datetime(2023, 1, 15, 10, 30, 0),
            "account": "acc-1",
            "amount": -100.50,
            "description": "test transaction 1",
        },  # Lowercase
        {
            "date": datetime.datetime(2023, 1, 15, 10, 30, 0),
            "account": "acc-1",
            "amount": -100.50,
            "description": "Test Transaction 1 ",
        },  # Trailing space
    ]
    result = TransactionMatcher.find_new_txns(old_transactions, potential)
    # Both should be considered new because the ID creation uses description directly
    assert len(result) == 2
