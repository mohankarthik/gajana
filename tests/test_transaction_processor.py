# tests/test_transaction_processor.py
from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Assuming your project structure is src/gajana/...
from src.interfaces import DataSourceInterface, DataSourceFile
from src.transaction_processor import TransactionProcessor, parse_mixed_datetime
from src.constants import INTERNAL_TXN_KEYS

# --- Fixtures ---


@pytest.fixture
def mock_data_source():
    """Provides a mock of the DataSourceInterface."""
    return MagicMock(spec=DataSourceInterface)


@pytest.fixture
def transaction_processor(mock_data_source):
    """Provides an instance of TransactionProcessor with a mocked data source."""
    return TransactionProcessor(mock_data_source)


@pytest.fixture(autouse=True)
def mock_log_and_exit_fixture(mocker):
    """Mocks the log_and_exit utility to prevent tests from exiting."""
    return mocker.patch(
        "src.transaction_processor.log_and_exit", side_effect=SystemExit
    )


# --- Tests for Helper Functions ---


@pytest.mark.parametrize(
    "input_val, expected",
    [
        ("1,005.50", 1005.50),
        ("-50.25", -50.25),
        ("(50.25)", -50.25),
        ("100.00 Cr", 100.00),
        ("150.00 Dr", -150.00),
        ("₹250.75", 250.75),
        ("1.23E+2", 123.0),  # Scientific notation
        (None, 0.0),
        ("", 0.0),
        (pd.NA, 0.0),
        (0, 0.0),
    ],
)
def test_parse_amount(input_val, expected, transaction_processor):
    """Tests various amount formats."""
    assert transaction_processor._parse_amount(input_val) == expected


def test_parse_amount_invalid_exits(transaction_processor, mock_log_and_exit_fixture):
    """Tests that an unparseable amount string calls log_and_exit."""
    with pytest.raises(SystemExit):
        transaction_processor._parse_amount("invalid amount")
    mock_log_and_exit_fixture.assert_called_once()


def test_parse_mixed_datetime_pandas_failure(caplog):
    """Tests that a completely unparseable date returns None and logs a warning."""
    # This string format will cause pd.to_datetime with errors='coerce' to return NaT
    result = parse_mixed_datetime("not a real date", [])
    assert result is None
    assert "Could not parse date string" in caplog.text


def test_parse_mixed_datetime_unexpected_exception(mock_log_and_exit_fixture, mocker):
    """Tests the main exception handler in date parsing."""
    mocker.patch("pandas.to_datetime", side_effect=Exception("Unexpected error"))
    with pytest.raises(SystemExit):
        parse_mixed_datetime("any-date", [])
    mock_log_and_exit_fixture.assert_called_once()
    assert (
        "Unexpected error parsing date string"
        in mock_log_and_exit_fixture.call_args[0][1]
    )


@pytest.mark.parametrize(
    "date_str, formats, expected_date",
    [
        ("23-01-2024", ["%d-%m-%Y"], datetime.datetime(2024, 1, 23)),
        ("01/23/2024", ["%m/%d/%Y"], datetime.datetime(2024, 1, 23)),
        ("23-Jan-2024", ["%d-%b-%Y"], datetime.datetime(2024, 1, 23)),
        ("23/01/24", [], datetime.datetime(2024, 1, 23)),  # Fallback to pandas
        ("invalid-date", [], None),
        (None, [], None),
    ],
)
def test_parse_mixed_datetime(date_str, formats, expected_date):
    """Tests the standalone date parsing helper."""
    assert parse_mixed_datetime(date_str, formats) == expected_date


@pytest.mark.parametrize(
    "filename, account_list, acc_type, expected_acc, expected_date",
    [
        (
            "bank-sbi-2023.csv",
            ["sbi", "hdfc"],
            "bank",
            "sbi",
            datetime.datetime(2023, 12, 31),
        ),
        (
            "cc-hdfc-2023-05.gsheet",
            ["amex", "hdfc"],
            "cc",
            "hdfc",
            datetime.datetime(2023, 5, 31),
        ),
        ("cc-amex-unsupported.txt", ["amex"], "cc", "amex", None),
        ("other-file.csv", ["sbi"], "bank", None, None),
        # Test case where date parsing from filename fails
        ("bank-sbi-baddate.csv", ["sbi"], "bank", "sbi", None),
    ],
)
def test_get_account_and_date_from_filename(
    transaction_processor, filename, account_list, acc_type, expected_acc, expected_date
):
    """Tests filename parsing for account and date."""
    account, date = transaction_processor._get_account_and_date_from_filename(
        filename, account_list, acc_type
    )
    assert account == expected_acc
    assert date == expected_date


# --- Tests for DataFrame Processing ---


def test_parse_statement_data_with_pandas_header_detection(transaction_processor):
    """Tests that the header row is correctly detected."""
    # Mock config with a header pattern
    config = {"header_patterns": [["Date", "Transaction Details", "Amount"]]}
    raw_data = [
        ["Statement Summary"],
        ["Account: 12345"],
        [],  # Empty row
        ["Date", "Transaction Details", "Amount"],  # Header
        ["23-01-2024", "Purchase 1", "100.00"],
    ]
    df = transaction_processor._parse_statement_data_with_pandas(raw_data, config)
    assert df is not None
    assert list(df.columns) == ["Date", "Transaction Details", "Amount"]
    assert len(df) == 1
    assert df.iloc[0]["Amount"] == "100.00"


def test_parse_statement_data_with_pandas_special_handling_tilde(transaction_processor):
    """Tests the special handling for HDFC tilde-separated data."""
    config = {
        "special_handling": "hdfc_cc_tilde",
        "header_patterns": [["Date~Description~Amount"]],
    }
    raw_data = [
        ["Header1~Header2"],  # some other header
        ["Date~Description~Amount"],  # The actual header
        ["23-Jan-2024~Purchase~1000"],
    ]
    df = transaction_processor._parse_statement_data_with_pandas(raw_data, config)
    assert df is not None
    assert list(df.columns) == ["Date", "Description", "Amount"]
    assert len(df) == 1
    assert df.iloc[0]["Description"] == "Purchase"


def test_parse_statement_data_with_pandas_no_header_found(
    transaction_processor, caplog
):
    """Tests fallback to first row if no header pattern matches."""
    config = {"header_patterns": [["Non-existent", "Pattern"]]}
    raw_data = [["Col1", "Col2"], ["Val1", "Val2"]]  # Should be treated as header
    df = transaction_processor._parse_statement_data_with_pandas(raw_data, config)
    assert "Could not reliably detect header row" in caplog.text
    assert df is not None
    assert list(df.columns) == ["Col1", "Col2"]
    assert len(df) == 1


def test_parse_statement_data_pandas_creation_fails(
    transaction_processor, mock_log_and_exit_fixture, mocker
):
    """Tests that an exception during DataFrame creation is handled."""
    mocker.patch("pandas.DataFrame", side_effect=Exception("Pandas Error"))
    config = {"header_patterns": []}
    raw_data = [["Col1"], ["Val1"]]
    df = transaction_processor._parse_statement_data_with_pandas(raw_data, config)
    assert df is None
    # Note: your code logs an error but doesn't exit here, which is fine. This test confirms it returns None.


def test_standardize_parsed_df_empty_or_none_input(transaction_processor):
    """Tests that standardize_parsed_df handles None or empty DataFrame inputs."""
    assert transaction_processor._standardize_parsed_df(None, {}, "acc") is None
    assert (
        transaction_processor._standardize_parsed_df(pd.DataFrame(), {}, "acc") is None
    )


def test_standardize_parsed_df(transaction_processor):
    """Tests the full standardization logic on a parsed DataFrame."""
    # This config mimics a real entry from PARSING_CONFIG
    config = {
        "column_map": {
            "Date": "date",
            "Transaction Details": "description",
            "Debit": "debit",
            "Credit": "credit",
        },
        "date_formats": ["%d/%m/%Y"],
    }
    input_df = pd.DataFrame(
        [
            {
                "Date": "23/01/2024",
                "Transaction Details": "Payment",
                "Debit": "100.50",
                "Credit": pd.NA,
            },
            {
                "Date": "24/01/2024",
                "Transaction Details": "Refund",
                "Debit": pd.NA,
                "Credit": "50.25",
            },
            # Add a row with a bad date that should be dropped
            {
                "Date": "invalid-date",
                "Transaction Details": "Bad Date",
                "Credit": "10.0",
            },
        ]
    )

    std_df = transaction_processor._standardize_parsed_df(
        input_df, config, "test-account"
    )

    assert std_df is not None
    # The invalid date row should be dropped, so length is 2
    assert len(std_df) == 2
    assert all(col in std_df.columns for col in INTERNAL_TXN_KEYS)

    # Check first row
    assert std_df.iloc[0]["date"] == datetime.datetime(2024, 1, 23)
    assert std_df.iloc[0]["description"] == "Payment"
    assert std_df.iloc[0]["amount"] == -100.50
    assert std_df.iloc[0]["account"] == "test-account"

    # Check second row
    assert std_df.iloc[1]["amount"] == 50.25


def test_standardize_parsed_df_no_amount_cols(transaction_processor, caplog):
    """Tests standardization when no amount columns are found."""
    config = {"column_map": {"Date": "date"}, "date_formats": []}
    input_df = pd.DataFrame([{"Date": "2024-01-23"}])
    std_df = transaction_processor._standardize_parsed_df(input_df, config, "acc")
    assert std_df is None
    assert "Could not find columns to calculate amount" in caplog.text


def test_standardize_parsed_df_missing_internal_key(transaction_processor, caplog):
    """Tests that a missing internal key (e.g., description) is added with a default."""
    config = {"column_map": {"Date": "date", "Value": "amount"}, "date_formats": []}
    input_df = pd.DataFrame([{"Date": "2024-01-23", "Value": "100"}])
    std_df = transaction_processor._standardize_parsed_df(input_df, config, "acc")
    assert std_df is not None
    assert "description" in std_df.columns
    assert std_df.iloc[0]["description"] == ""  # Default value
    assert "Internal key 'description' missing" in caplog.text


# --- Tests for Orchestration Methods ---


def test_get_old_transactions(transaction_processor, mock_data_source):
    """Tests fetching and processing of old transactions from the data source."""
    # Mock the raw data returned from the data source
    mock_data_source.get_transaction_log_data.return_value = [
        ["Date", "Description", "Debit", "Credit", "Category", "Remarks", "Account"],
        ["2024-01-20", "Old Purchase", "50.00", "", "Shopping", "", "test-account"],
    ]

    old_txns = transaction_processor.get_old_transactions("bank")

    mock_data_source.get_transaction_log_data.assert_called_once_with("bank")
    assert len(old_txns) == 1
    txn = old_txns[0]
    assert txn["date"] == datetime.datetime(2024, 1, 20)
    assert txn["description"] == "Old Purchase"
    assert txn["amount"] == -50.00
    assert txn["account"] == "test-account"
    assert txn["category"] == "Shopping"


def test_get_old_transactions_empty_or_header_only(
    transaction_processor, mock_data_source
):
    """Tests get_old_transactions with no real data."""
    # Case 1: No data at all
    mock_data_source.get_transaction_log_data.return_value = []
    assert transaction_processor.get_old_transactions("bank") == []

    # Case 2: Header only
    mock_data_source.get_transaction_log_data.return_value = [["Header1", "Header2"]]
    assert transaction_processor.get_old_transactions("bank") == []


def test_get_old_transactions_processing_error(
    transaction_processor, mock_data_source, mock_log_and_exit_fixture, mocker
):
    """Tests exception handling during old transaction processing."""
    mock_data_source.get_transaction_log_data.return_value = [["Date"], ["2024-01-20"]]
    # Mock pandas to fail
    mocker.patch("pandas.DataFrame.rename", side_effect=Exception("Pandas Error"))
    with pytest.raises(SystemExit):
        transaction_processor.get_old_transactions("bank")
    mock_log_and_exit_fixture.assert_called_once()
    assert "Error processing old bank txns" in mock_log_and_exit_fixture.call_args[0][1]


def test_get_new_transactions_from_statements(
    transaction_processor, mock_data_source, mocker
):
    """Tests the end-to-end flow of finding and parsing new transactions."""
    # Mock constants to simplify the test
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["mini-sbi"])
    mocker.patch(
        "src.transaction_processor.PARSING_CONFIG",
        {
            "bank-sbi": {
                "header_patterns": [["Date", "Description", "Amount"]],
                "column_map": {
                    "Date": "date",
                    "Description": "description",
                    "Amount": "amount",
                },
                "date_formats": ["%d-%b-%y"],
            }
        },
    )

    # Mock data source calls
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="file123", name="bank-mini-sbi-2024.csv")
    ]
    mock_data_source.get_first_sheet_name_from_file.return_value = "Sheet1"
    mock_data_source.get_sheet_data.return_value = [
        ["Date", "Description", "Amount"],
        ["25-Jan-24", "New Purchase", "250.00"],
    ]

    # Assume no old transactions for this account
    latest_txn_by_account = {}

    new_txns = transaction_processor.get_new_transactions_from_statements(
        "bank", latest_txn_by_account
    )

    mock_data_source.list_statement_file_details.assert_called_once()
    mock_data_source.get_first_sheet_name_from_file.assert_called_once_with("file123")
    mock_data_source.get_sheet_data.assert_called_once_with("file123", "Sheet1", "A:Z")

    assert len(new_txns) == 1
    txn = new_txns[0]
    assert txn["date"] == datetime.datetime(2024, 1, 25)
    assert txn["description"] == "New Purchase"
    assert txn["amount"] == 250.00
    assert txn["account"] == "mini-sbi"


def test_get_new_transactions_skips_old_statement(
    transaction_processor, mock_data_source, mocker
):
    """Tests that a statement is skipped if it's older than the last known transaction."""
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["hdfc"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="file123", name="cc-hdfc-2023-04.gsheet")  # April 2023
    ]
    latest_txn_by_account = {
        "hdfc": datetime.datetime(2023, 5, 15)
    }  # Last txn in May 2023

    new_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", latest_txn_by_account
    )
    assert new_txns == []
    # Ensure we didn't try to fetch data for the old file
    mock_data_source.get_sheet_data.assert_not_called()


def test_get_new_transactions_skips_no_config(
    transaction_processor, mock_data_source, mocker, caplog
):
    """Tests that a file is skipped if no parsing config is found."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["no-config-bank"])
    mocker.patch("src.transaction_processor.PARSING_CONFIG", {})  # Empty config
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="file123", name="bank-no-config-bank-2024.csv")
    ]

    new_txns = transaction_processor.get_new_transactions_from_statements("bank", {})
    assert new_txns == []
    assert "No parsing config for 'bank-config'" in caplog.text


def test_format_txns_for_storage(transaction_processor):
    """Tests the formatting of standardized transactions into a list of lists for storage."""
    txns = [
        {
            "date": datetime.datetime(2024, 1, 25),
            "description": "Credit",
            "amount": 100.0,
            "category": "Salary",
            "remarks": "Jan",
            "account": "acc-1",
        },
        {
            "date": datetime.datetime(2024, 1, 26),
            "description": "Debit",
            "amount": -50.55,
            "category": "Groceries",
            "remarks": "",
            "account": "acc-2",
        },
        # Test with non-numeric amount that should be handled gracefully
        {
            "date": datetime.datetime(2024, 1, 27),
            "description": "Bad Amount",
            "amount": "invalid",
            "account": "acc-3",
        },
    ]

    formatted_data = transaction_processor._format_txns_for_storage(txns)

    assert formatted_data == [
        ["2024-01-25", "Credit", "", "100.00", "Salary", "Jan", "acc-1"],
        ["2024-01-26", "Debit", "50.55", "", "Groceries", "", "acc-2"],
        ["2024-01-27", "Bad Amount", "", "", "Uncategorized", "", "acc-3"],
    ]


def test_add_new_transactions_to_log(transaction_processor, mock_data_source):
    """Tests that the data source's append method is called with correctly formatted data."""
    txns = [
        {
            "date": datetime.datetime(2024, 1, 25),
            "description": "Test",
            "amount": 100.0,
            "account": "acc-1",
        }
    ]

    transaction_processor.add_new_transactions_to_log(txns, "bank")

    # Check that append_transactions_to_log was called once with the correct arguments
    mock_data_source.append_transactions_to_log.assert_called_once()
    # The first arg is 'bank', the second is the formatted data
    call_args = mock_data_source.append_transactions_to_log.call_args[0]
    assert call_args[0] == "bank"
    assert call_args[1] == [
        ["2024-01-25", "Test", "", "100.00", "Uncategorized", "", "acc-1"]
    ]


def test_add_new_transactions_to_log_no_txns(transaction_processor, mock_data_source):
    """Tests that append is not called if there are no transactions."""
    transaction_processor.add_new_transactions_to_log([], "bank")
    mock_data_source.append_transactions_to_log.assert_not_called()


def test_overwrite_transaction_log(transaction_processor, mock_data_source):
    """Tests that clear and write methods are called in order."""
    txns = [
        {
            "date": datetime.datetime(2024, 1, 25),
            "description": "Overwrite",
            "amount": -50.0,
            "account": "acc-1",
        }
    ]

    transaction_processor.overwrite_transaction_log(txns, "cc")

    mock_data_source.clear_transaction_log_range.assert_called_once_with("cc")
    mock_data_source.write_transactions_to_log.assert_called_once()
    call_args = mock_data_source.write_transactions_to_log.call_args[0]
    assert call_args[0] == "cc"
    assert call_args[1] == [
        ["2024-01-25", "Overwrite", "50.00", "", "Uncategorized", "", "acc-1"]
    ]


def test_overwrite_transaction_log_no_txns(transaction_processor, mock_data_source):
    """Tests that clear and write are not called if there are no transactions."""
    transaction_processor.overwrite_transaction_log([], "cc")
    mock_data_source.clear_transaction_log_range.assert_not_called()
    mock_data_source.write_transactions_to_log.assert_not_called()


def test_get_all_transactions_for_recategorize(transaction_processor, mock_data_source):
    """Tests the aggregation of bank and cc transactions."""
    mock_data_source.get_transaction_log_data.side_effect = [
        # Return for bank
        [
            ["Date", "Description", "Debit", "Account"],
            ["2024-01-10", "Bank Txn", "10", "bank-acc"],
        ],
        # Return for cc
        [
            ["Date", "Description", "Debit", "Account"],
            ["2024-01-15", "CC Txn", "20", "cc-acc"],
        ],
    ]

    all_txns = transaction_processor.get_all_transactions_for_recategorize()

    assert len(all_txns) == 2
    assert mock_data_source.get_transaction_log_data.call_count == 2
    # Check that they are sorted by date
    assert all_txns[0]["description"] == "Bank Txn"
    assert all_txns[1]["description"] == "CC Txn"
