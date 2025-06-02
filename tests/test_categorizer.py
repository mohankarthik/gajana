# tests/test_categorizer.py
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

import pytest

from src.categorizer import Categorizer
from src.constants import DEFAULT_CATEGORY


@pytest.fixture(autouse=True)
def mock_log_and_exit(mocker):
    """Mocks utils.log_and_exit to prevent test termination and allow assertion."""
    return mocker.patch("src.categorizer.log_and_exit", side_effect=SystemExit)


@pytest.fixture
def mock_matchers_file(tmp_path):
    """Creates a temporary matchers.json file and returns its path."""
    matchers_content = []
    file_path = tmp_path / "matchers.json"
    with open(file_path, "w") as f:
        json.dump(matchers_content, f)
    return str(file_path)  # Return as string, as open() expects


@pytest.fixture
def sample_transactions() -> List[Dict[str, Any]]:
    return [
        {
            "description": "Payment to Zomato",
            "amount": -500.00,
            "account": "cc-hdfc-infiniametal",
        },
        {
            "description": "Salary Credit",
            "amount": 50000.00,
            "account": "bank-hdfc-karti",
        },
        {
            "description": "AMAZON PAYMENTS INDIA",
            "amount": -1200.00,
            "account": "cc-icici-amazonpay",
        },
        {
            "description": "ATM WITHDRAWAL",
            "amount": -2000.00,
            "account": "bank-axis-karti",
        },
        {
            "description": "Unknown random stuff",
            "amount": -100.00,
            "account": "cc-axis-magnus",
        },
        {
            "description": "Regex Target 123",
            "amount": -50.00,
            "account": "bank-axis-mini",
        },
    ]


def write_matchers(file_path: str, matchers_data: List[Dict[str, Any]]):
    with open(file_path, "w") as f:
        json.dump(matchers_data, f)


# --- Tests for __init__ ---


def test_categorizer_init_success(mock_matchers_file):
    """Test successful initialization with a valid (empty) matchers file."""
    write_matchers(mock_matchers_file, [{"category": "Test", "description": ["test"]}])
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    assert len(categorizer.matchers) == 1
    assert categorizer.matchers[0]["category"] == "Test"


def test_categorizer_init_file_not_found(mock_log_and_exit, caplog):
    """Test __init__ when matchers file is not found."""
    with pytest.raises(SystemExit):  # Expect log_and_exit to cause SystemExit
        Categorizer(matchers_file="non_existent_file.json")
    mock_log_and_exit.assert_called_once()
    # Check if the logger passed to log_and_exit was the categorizer's logger
    # and if the message was correct.
    args, _ = mock_log_and_exit.call_args
    assert "Matchers file not found" in args[1]  # args[1] is the message
    assert isinstance(args[0], logging.Logger)  # args[0] is the logger instance


def test_categorizer_init_json_decode_error(mock_matchers_file, mock_log_and_exit):
    """Test __init__ with a malformed JSON file."""
    with open(mock_matchers_file, "w") as f:
        f.write("this is not json")
    with pytest.raises(SystemExit):
        Categorizer(matchers_file=mock_matchers_file)
    mock_log_and_exit.assert_called_once()
    args, _ = mock_log_and_exit.call_args
    assert "Error loading or validating matchers file" in args[1]


def test_categorizer_init_matchers_not_a_list(mock_matchers_file, mock_log_and_exit):
    """Test __init__ when matchers JSON root is not a list."""
    write_matchers(mock_matchers_file, {"not_a_list": "value"})  # Root is a dict
    with pytest.raises(SystemExit):
        Categorizer(matchers_file=mock_matchers_file)
    mock_log_and_exit.assert_called_once()
    args, _ = mock_log_and_exit.call_args
    assert (
        "Error loading or validating matchers file" in args[1]
    )  # Catches AssertionError


# --- Tests for categorize method ---


def test_categorize_no_matchers_loaded(
    mock_matchers_file, sample_transactions, mock_log_and_exit
):
    """Test categorize when no matchers are loaded (e.g., init failed softly or empty list)."""
    # Simulate no matchers by providing an empty list
    write_matchers(mock_matchers_file, [])
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    categorizer.matchers = []  # Explicitly empty matchers after init for this test

    with pytest.raises(SystemExit):
        categorizer.categorize(sample_transactions)
    mock_log_and_exit.assert_called_with(
        logging.getLogger(
            "src.categorizer"
        ),  # or categorizer.logger if it's an instance var
        "No valid matchers loaded. Assigning default category to all transactions.",
    )


def test_categorize_simple_description_match_debit(
    mock_matchers_file, sample_transactions
):
    matchers = [{"category": "Food", "description": ["zomato"], "debit": True}]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    result = categorizer.categorize([sample_transactions[0]])
    assert result[0]["category"] == "Food"


def test_categorize_simple_description_match_credit(
    mock_matchers_file, sample_transactions
):
    matchers = [
        {"category": "Salary", "description": ["salary credit"], "debit": False}
    ]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    result = categorizer.categorize([sample_transactions[1]])
    assert result[0]["category"] == "Salary"


def test_categorize_account_match(mock_matchers_file, sample_transactions):
    matchers = [
        {
            "category": "Online Shopping",
            "description": ["amazon"],
            "account": "cc-icici-amazonpay",
        }
    ]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    result = categorizer.categorize([sample_transactions[2]])
    assert result[0]["category"] == "Online Shopping"


def test_categorize_regex_match(mock_matchers_file, sample_transactions):
    matchers = [
        {"category": "Regex Matched", "description": [r"target \d+"], "use_regex": True}
    ]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    result = categorizer.categorize([sample_transactions[5]])
    assert result[0]["category"] == "Regex Matched"


def test_categorize_no_match(mock_matchers_file, sample_transactions):
    matchers = [{"category": "Specific", "description": ["non_existent_keyword"]}]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    result = categorizer.categorize([sample_transactions[4]])  # "Unknown random stuff"
    assert result[0]["category"] == DEFAULT_CATEGORY


def test_categorize_empty_transactions_list(mock_matchers_file):
    write_matchers(mock_matchers_file, [{"category": "Test", "description": ["test"]}])
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    result = categorizer.categorize([])
    assert result == []


def test_categorize_transaction_missing_amount(mock_matchers_file, mock_log_and_exit):
    tx_missing_amount = {"description": "test", "account": "test-acc"}
    write_matchers(mock_matchers_file, [{"category": "Test", "description": ["test"]}])
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    with pytest.raises(SystemExit):
        categorizer.categorize([tx_missing_amount])
    args, _ = mock_log_and_exit.call_args
    assert "missing keys" in args[1]


def test_categorize_transaction_unparsable_amount(
    mock_matchers_file, mock_log_and_exit
):
    tx_bad_amount = {
        "description": "test",
        "amount": "not-a-float",
        "account": "test-acc",
    }
    write_matchers(mock_matchers_file, [{"category": "Test", "description": ["test"]}])
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    with pytest.raises(SystemExit):
        categorizer.categorize([tx_bad_amount])
    args, _ = mock_log_and_exit.call_args
    assert "Could not determine debit/credit" in args[1]


def test_categorize_matcher_invalid_description_format(
    mock_matchers_file, sample_transactions, mock_log_and_exit
):
    matchers = [{"category": "Error Test", "description": "not_a_list_of_strings"}]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    with pytest.raises(SystemExit):
        categorizer.categorize([sample_transactions[0]])
    args, _ = mock_log_and_exit.call_args
    assert "Matcher has invalid 'description' format" in args[1]


def test_categorize_matcher_invalid_regex_pattern(
    mock_matchers_file, sample_transactions, mock_log_and_exit
):
    matchers = [
        {
            "category": "Regex Error",
            "description": ["*invalid_regex("],
            "use_regex": True,
        }
    ]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    with pytest.raises(SystemExit):
        categorizer.categorize([sample_transactions[0]])
    args, _ = mock_log_and_exit.call_args
    assert "Invalid regex pattern in matcher" in args[1]


def test_categorize_first_matching_rule_applies(
    mock_matchers_file, sample_transactions
):
    matchers = [
        {"category": "Food First", "description": ["zomato"], "debit": True},
        {"category": "Generic Expense", "description": ["payment"], "debit": True},
    ]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    # Transaction "Payment to Zomato"
    result = categorizer.categorize([sample_transactions[0]])
    assert result[0]["category"] == "Food First"


def test_categorize_case_insensitivity_description(
    mock_matchers_file, sample_transactions
):
    matchers = [
        {
            "category": "Food",
            "description": ["ZOMATO"],
            "debit": True,
        }  # Uppercase matcher
    ]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    # Transaction "Payment to Zomato" (lowercase in description)
    result = categorizer.categorize([sample_transactions[0]])
    assert result[0]["category"] == "Food"


def test_categorize_case_insensitivity_account(mock_matchers_file):
    matchers = [
        {
            "category": "Test Account",
            "description": ["test"],
            "account": "CC-HDFC-TEST",
        }  # Uppercase account in matcher
    ]
    write_matchers(mock_matchers_file, matchers)
    categorizer = Categorizer(matchers_file=mock_matchers_file)
    test_txn = [
        {"description": "test desc", "amount": -100, "account": "cc-hdfc-test"}
    ]  # Lowercase account in txn
    result = categorizer.categorize(test_txn)
    assert result[0]["category"] == "Test Account"
