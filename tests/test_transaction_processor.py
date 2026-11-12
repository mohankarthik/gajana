# tests/test_transaction_processor.py
from __future__ import annotations

import datetime
import json
from unittest.mock import MagicMock, patch

import pandas as pd

import pytest

# Assuming your project structure is src/gajana/...
from src.interfaces import DataSourceInterface, DataSourceFile
from src.transaction_processor import TransactionProcessor
from src.constants import DEFAULT_CATEGORY, INTERNAL_TXN_KEYS

# --- Fixtures ---


@pytest.fixture
def mock_data_source():
    """Provides a mock of the DataSourceInterface."""
    ds = MagicMock(spec=DataSourceInterface)
    ds.get_processed_statements.return_value = {}  # real dict for cache merge
    return ds


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
        ("50.0%", 50.0),  # Percentage
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
        # Prefix collision: longer/more-specific name must win regardless of
        # account_list order (cc-axis-neo is a substring of cc-axis-neorupay).
        (
            "cc-axis-neorupay-2026-05.pdf",
            ["cc-axis-neo", "cc-axis-neorupay"],
            "cc",
            "cc-axis-neorupay",
            datetime.datetime(2026, 5, 31),
        ),
        (
            "cc-axis-neo-2026-05.pdf",
            ["cc-axis-neo", "cc-axis-neorupay"],
            "cc",
            "cc-axis-neo",
            datetime.datetime(2026, 5, 31),
        ),
    ],
)
def test_get_account_and_date_from_filename(
    transaction_processor,
    filename,
    account_list,
    acc_type,
    expected_acc,
    expected_date,
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


def test_parse_statement_data_with_pandas_special_handling_tilde(
    transaction_processor,
):
    """Tests the special handling for HDFC tilde-separated data."""
    config = {
        "special_handling": "hdfc_cc_tilde",
        "header_patterns": [["Date~Description~Amount"]],
    }
    raw_data = [
        ["Header1~Header2"],  # some other header
        ["Date~Description~Amount"],  # The actual header
        ["23-Jan-2024~Purchase~1000"],
        ["24-Jan-2024~Refund~500~Extra"],  # Malformed row, should be skipped
        [""],  # Empty row, should be skipped
    ]
    df = transaction_processor._parse_statement_data_with_pandas(raw_data, config)
    assert df is not None
    assert list(df.columns) == ["Date", "Description", "Amount"]
    assert len(df) == 1  # Only the valid row should be parsed
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


def test_parse_statement_data_empty_input(transaction_processor):
    """Tests that empty raw data returns None."""
    assert transaction_processor._parse_statement_data_with_pandas([], {}) is None


def test_parse_statement_data_results_in_empty_df(transaction_processor):
    """Tests that data resulting in an empty DataFrame after cleaning returns None."""
    config = {"header_patterns": [["Header"]]}
    raw_data = [["Header"], [pd.NA], [""]]  # All data rows are empty
    df = transaction_processor._parse_statement_data_with_pandas(raw_data, config)
    assert df is None


def test_parse_statement_data_pandas_creation_fails(transaction_processor, mocker):
    """Tests that an exception during DataFrame creation is handled."""
    mocker.patch("pandas.DataFrame", side_effect=Exception("Pandas Error"))
    config = {"header_patterns": []}
    raw_data = [["Col1"], ["Val1"]]
    result = transaction_processor._parse_statement_data_with_pandas(raw_data, config)
    assert result is None


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


def test_standardize_parsed_df_with_amount_sign_col(transaction_processor):
    """Tests standardization using amount_sign_col for debits."""
    config = {
        "column_map": {"Date": "date", "Value": "amount", "Type": "sign"},
        "date_formats": [],
        "amount_sign_col": "sign",
        "debit_value": "dr",
    }
    input_df = pd.DataFrame(
        [
            {"Date": "2024-01-23", "Value": "100", "Type": "dr"},
            {"Date": "2024-01-24", "Value": "200", "Type": "cr"},
        ]
    )
    std_df = transaction_processor._standardize_parsed_df(input_df, config, "acc")
    assert std_df is not None
    assert len(std_df) == 2
    assert std_df.iloc[0]["amount"] == -100.0
    assert std_df.iloc[1]["amount"] == 200.0


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


def test_standardize_df_date_parsing_exception(transaction_processor, mocker):
    """Tests that an exception during date parsing within standardization is handled."""
    config = {"column_map": {"Date": "date", "Value": "amount"}, "date_formats": []}
    input_df = pd.DataFrame([{"Date": "2024-01-23", "Value": "100"}])

    mocker.patch(
        "src.transaction_processor.parse_mixed_datetime",
        side_effect=Exception("Date Parse Fail"),
    )
    std_df = None
    with pytest.raises(SystemExit):
        std_df = transaction_processor._standardize_parsed_df(input_df, config, "acc")
    assert std_df is None


# --- Tests for Orchestration Methods ---


def test_get_old_transactions_unified_pipeline(transaction_processor, mock_data_source):
    """
    Tests that get_old_transactions correctly uses the unified standardization pipeline.
    """
    # Mock the raw data returned from the data source, including multiple accounts
    mock_data_source.get_transaction_log_data.return_value = [
        ["Date", "Description", "Debit", "Credit", "Category", "Remarks", "Account"],
        ["2024-01-20", "Old Purchase 1", "50.00", "", "Shopping", "", "bank-acc-1"],
        ["2024-01-21", "Old Credit 1", "", "150.00", "Income", "Pay", "bank-acc-2"],
        ["2024-01-22", "Old Purchase 2", "25.50", "", "Groceries", "", "bank-acc-1"],
        [
            "bad-date",
            "Bad Data",
            "10",
            "",
            "Junk",
            "",
            "bank-acc-1",
        ],  # Should be dropped
    ]

    old_txns = transaction_processor.get_old_transactions("bank")

    mock_data_source.get_transaction_log_data.assert_called_once_with("bank")

    # Should have 3 valid transactions after processing
    assert len(old_txns) == 3

    # Verify transactions are sorted by date
    assert old_txns[0]["description"] == "Old Purchase 1"
    assert old_txns[1]["description"] == "Old Credit 1"
    assert old_txns[2]["description"] == "Old Purchase 2"

    # Check transaction 1 (bank-acc-1, debit)
    txn1 = old_txns[0]
    assert txn1["date"] == datetime.datetime(2024, 1, 20)
    assert txn1["amount"] == -50.00
    assert txn1["account"] == "bank-acc-1"
    assert txn1["category"] == "Shopping"

    # Check transaction 2 (bank-acc-2, credit)
    txn2 = old_txns[1]
    assert txn2["date"] == datetime.datetime(2024, 1, 21)
    assert txn2["amount"] == 150.00
    assert txn2["account"] == "bank-acc-2"
    assert txn2["category"] == "Income"
    assert txn2["remarks"] == "Pay"

    # Check transaction 3 (bank-acc-1, debit)
    txn3 = old_txns[2]
    assert txn3["date"] == datetime.datetime(2024, 1, 22)
    assert txn3["amount"] == -25.50
    assert txn3["account"] == "bank-acc-1"
    assert txn3["category"] == "Groceries"


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
    # Mock the standardization step to fail
    mocker.patch.object(
        transaction_processor,
        "_standardize_parsed_df",
        side_effect=Exception("Standardize Fail"),
    )
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
    assert txn["category"] == DEFAULT_CATEGORY  # Check default category is added


@patch("src.pdf_parser.PDFParser")
def test_get_new_transactions_from_statements_pdf(
    mock_pdf_parser_class, transaction_processor, mock_data_source, mocker
):
    """Tests that PDF statement files are correctly routed to PDFParser."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["mini-sbi"])
    mocker.patch(
        "src.transaction_processor.PARSING_CONFIG",
        {"bank-sbi": {}},
    )

    # Mock PDFParser: parse_pdf_with_text returns (txns, oracle_text, summary).
    # The oracle must corroborate every token or validation quarantines the row.
    mock_parser_instance = MagicMock()
    mock_pdf_parser_class.return_value = mock_parser_instance
    mock_parser_instance.parse_pdf_with_text.return_value = (
        [
            {
                "date": "2024-01-25",
                "description": "New PDF Purchase",
                "debit": "150.00",
                "credit": "",
            },
        ],
        "2024-01-25 New PDF Purchase 150.00",
        {},
    )

    # Mock data source calls
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="filepdf123", name="bank-mini-sbi-2024.pdf")
    ]
    mock_data_source.download_file.return_value = b"pdf binary content"

    latest_txn_by_account = {}

    new_txns = transaction_processor.get_new_transactions_from_statements(
        "bank", latest_txn_by_account
    )

    mock_data_source.list_statement_file_details.assert_called_once()
    mock_data_source.download_file.assert_called_once_with("filepdf123")
    mock_pdf_parser_class.assert_called_once()
    mock_parser_instance.parse_pdf_with_text.assert_called_once_with(
        b"pdf binary content", []
    )

    assert len(new_txns) == 1
    txn = new_txns[0]
    assert txn["date"] == datetime.datetime(2024, 1, 25)
    assert txn["description"] == "New PDF Purchase"
    assert txn["amount"] == -150.00  # Debit amount is converted to negative amount
    assert txn["account"] == "mini-sbi"


@patch("src.pdf_parser.PDFParser")
def test_pdf_password_candidates_specific_then_bank(
    mock_pdf_parser_class,
    transaction_processor,
    mock_data_source,
    mocker,
    tmp_path,
    monkeypatch,
):
    """Account-specific passwords are tried before bank-level ones, list entries
    keep their order, and duplicates collapse — so a rotated password can sit
    ahead of the old one that still opens older statements."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["bank-sbi-mini"])
    mocker.patch("src.transaction_processor.PARSING_CONFIG", {"bank-sbi": {}})

    secrets_dir = tmp_path / "secrets"
    secrets_dir.mkdir()
    (secrets_dir / "passwords.json").write_text(
        json.dumps({"sbi-mini": ["NEWPW", "OLDPW"], "sbi": ["BANKPW", "NEWPW"]})
    )
    monkeypatch.chdir(tmp_path)

    mock_parser_instance = MagicMock()
    mock_pdf_parser_class.return_value = mock_parser_instance
    mock_parser_instance.parse_pdf_with_text.return_value = ([], "", {})

    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="filepdf123", name="bank-sbi-mini-2024.pdf")
    ]
    mock_data_source.download_file.return_value = b"pdf binary content"

    transaction_processor.get_new_transactions_from_statements("bank", {})

    mock_parser_instance.parse_pdf_with_text.assert_called_once_with(
        b"pdf binary content", ["NEWPW", "OLDPW", "BANKPW"]
    )


@patch("src.pdf_parser.PDFParser")
def test_pdf_validation_quarantines_hallucinated_rows(
    mock_pdf_parser_class, transaction_processor, mock_data_source, mocker
):
    """A row whose date is absent from the PDF text layer is routed to the review
    surface and never returned as a booked transaction."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["mini-sbi"])
    mocker.patch("src.transaction_processor.PARSING_CONFIG", {"bank-sbi": {}})

    mock_parser_instance = MagicMock()
    mock_pdf_parser_class.return_value = mock_parser_instance
    # date "2099-01-01" appears nowhere in the oracle text -> hallucination.
    oracle = "25/01/2024 Real Purchase 150.00"
    mock_parser_instance.parse_pdf_with_text.return_value = (
        [
            {
                "date": "2099-01-01",
                "description": "Ghost Purchase",
                "debit": "999.00",
                "credit": "",
            }
        ],
        oracle,
        {},
    )

    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="filepdf123", name="bank-mini-sbi-2024.pdf")
    ]
    mock_data_source.download_file.return_value = b"pdf binary content"

    new_txns = transaction_processor.get_new_transactions_from_statements("bank", {})

    assert new_txns == []
    # Retried once with fallback-first, then the still-flagged row went to review.
    assert mock_parser_instance.parse_pdf_with_text.call_count == 2
    mock_data_source.write_review_rows.assert_called_once()
    review_rows = mock_data_source.write_review_rows.call_args[0][0]
    assert any("Ghost Purchase" in str(cell) for row in review_rows for cell in row)


def _mock_clean_pdf(mock_pdf_parser_class, mock_data_source, mocker):
    """Wires a mock that cleanly parses one 2024-01-25 txn from a PDF."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["mini-sbi"])
    mocker.patch("src.transaction_processor.PARSING_CONFIG", {"bank-sbi": {}})
    inst = MagicMock()
    mock_pdf_parser_class.return_value = inst
    inst.parse_pdf_with_text.return_value = (
        [{"date": "2024-01-25", "description": "P", "debit": "150.00", "credit": ""}],
        "2024-01-25 P 150.00",
        {},
    )
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="filepdf123", name="bank-mini-sbi-2024.pdf")
    ]
    mock_data_source.download_file.return_value = b"pdf"
    return inst


@patch("src.pdf_parser.PDFParser")
def test_pdf_cache_records_clean_statement(
    mock_pdf_parser_class, transaction_processor, mock_data_source, mocker
):
    """A cleanly-parsed statement is cached by its latest booked txn date."""
    _mock_clean_pdf(mock_pdf_parser_class, mock_data_source, mocker)
    transaction_processor.get_new_transactions_from_statements("bank", {})
    mock_data_source.save_processed_statements.assert_called_once()
    saved = mock_data_source.save_processed_statements.call_args[0][0]
    assert saved == {"filepdf123": "2024-01-25"}


@patch("src.pdf_parser.PDFParser")
def test_pdf_cache_skips_when_watermark_covers(
    mock_pdf_parser_class, transaction_processor, mock_data_source, mocker
):
    """A cached statement whose latest txn the watermark already covers is
    skipped without downloading or calling the LLM."""
    inst = _mock_clean_pdf(mock_pdf_parser_class, mock_data_source, mocker)
    mock_data_source.get_processed_statements.return_value = {
        "filepdf123": "2024-01-25"
    }
    watermark = {"mini-sbi": datetime.datetime(2024, 1, 25)}
    new_txns = transaction_processor.get_new_transactions_from_statements(
        "bank", watermark
    )
    assert new_txns == []
    inst.parse_pdf_with_text.assert_not_called()
    mock_data_source.download_file.assert_not_called()


@patch("src.pdf_parser.PDFParser")
def test_pdf_cache_reparses_when_watermark_drops(
    mock_pdf_parser_class, transaction_processor, mock_data_source, mocker
):
    """Self-heal: if the watermark falls below the cached date (rows deleted),
    the statement is re-parsed."""
    inst = _mock_clean_pdf(mock_pdf_parser_class, mock_data_source, mocker)
    mock_data_source.get_processed_statements.return_value = {
        "filepdf123": "2024-01-25"
    }
    watermark = {"mini-sbi": datetime.datetime(2024, 1, 1)}  # below cached
    transaction_processor.get_new_transactions_from_statements("bank", watermark)
    inst.parse_pdf_with_text.assert_called()


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


def test_get_new_transactions_file_processing_fails(
    transaction_processor, mock_data_source, mock_log_and_exit_fixture, mocker
):
    """Tests that a failure to process one sheet doesn't stop the whole process and calls log_and_exit."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["mini-sbi"])
    mocker.patch("src.transaction_processor.PARSING_CONFIG", {"bank-sbi": {}})
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="file123", name="bank-mini-sbi-2024.csv")
    ]
    # Mock a failure deep inside the processing loop
    mocker.patch.object(
        transaction_processor,
        "_parse_statement_data_with_pandas",
        side_effect=Exception("Parse Fail"),
    )

    result = transaction_processor.get_new_transactions_from_statements("bank", {})
    assert result == []


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
    """Overwrite must delegate to the data source's safe write-then-trim, and
    must NOT pre-clear (a failed write must not leave the log empty)."""
    txns = [
        {
            "date": datetime.datetime(2024, 1, 25),
            "description": "Overwrite",
            "amount": -50.0,
            "account": "acc-1",
        }
    ]

    # read-back returns header + the 1 written row -> verification passes
    mock_data_source.get_transaction_log_data.return_value = [
        ["Date", "Description", "Debit", "Credit", "Category", "Remarks", "Account"],
        ["2024-01-25", "Overwrite", "50.00", "", "Uncategorized", "", "acc-1"],
    ]

    transaction_processor.overwrite_transaction_log(txns, "cc")

    mock_data_source.clear_transaction_log_range.assert_not_called()
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


def test_overwrite_verification_fails_on_short_readback(
    transaction_processor, mock_data_source, mock_log_and_exit_fixture
):
    """If the read-back has fewer rows than were written, abort loudly."""
    txns = [
        {
            "date": datetime.datetime(2024, 1, i + 1),
            "description": f"T{i}",
            "amount": -10.0,
            "account": "cc-x",
        }
        for i in range(3)
    ]
    # wrote 3 rows, but read-back shows only 1 (truncated) -> must fail
    mock_data_source.get_transaction_log_data.return_value = [
        ["Date", "Description", "Debit", "Credit", "Category", "Remarks", "Account"],
        ["2024-01-01", "T0", "10.00", "", "Uncategorized", "", "cc-x"],
    ]
    with pytest.raises(SystemExit):
        transaction_processor.overwrite_transaction_log(txns, "cc")
    args, _ = mock_log_and_exit_fixture.call_args
    assert "Post-write verification FAILED" in args[1]


def test_overwrite_verification_passes_without_header(
    transaction_processor, mock_data_source
):
    """Read-back with no header row still counts correctly (no false abort)."""
    txns = [
        {
            "date": datetime.datetime(2024, 1, 1),
            "description": "T0",
            "amount": -10.0,
            "account": "cc-x",
        }
    ]
    mock_data_source.get_transaction_log_data.return_value = [
        ["2024-01-01", "T0", "10.00", "", "Uncategorized", "", "cc-x"],
    ]
    transaction_processor.overwrite_transaction_log(txns, "cc")  # no raise
    mock_data_source.write_transactions_to_log.assert_called_once()


def test_get_all_transactions_for_recategorize(transaction_processor, mock_data_source):
    """Tests the aggregation of bank and cc transactions."""
    mock_data_source.get_transaction_log_data.side_effect = [
        # Return for bank
        [
            ["Date", "Description", "amount", "Account"],
            ["2024-01-10", "Bank Txn", "10", "bank-acc"],
        ],
        # Return for cc
        [
            ["Date", "Description", "amount", "Account"],
            ["2024-01-15", "CC Txn", "20", "cc-acc"],
        ],
    ]

    all_txns = transaction_processor.get_all_transactions_for_recategorize()

    assert len(all_txns) == 2
    assert mock_data_source.get_transaction_log_data.call_count == 2
    # Check that they are sorted by date
    assert all_txns[0]["description"] == "Bank Txn"
    assert all_txns[1]["description"] == "CC Txn"


def test_format_txns_for_storage_sanitizes_nan(transaction_processor):
    """NaN/None cells must become '' (or defaults) so the Sheets JSON is valid."""
    import datetime as _dt

    rows = transaction_processor._format_txns_for_storage(
        [
            {
                "date": _dt.datetime(2024, 1, 1),
                "description": float("nan"),
                "amount": -100.0,
                "category": None,
                "remarks": float("nan"),
                "account": float("nan"),
            }
        ]
    )
    assert rows == [["2024-01-01", "", "100.00", "", "Uncategorized", "", "Unknown"]]


def test_apply_cycle_end_moves_month_end_to_the_real_cycle_end():
    """HDFC bills the 13th to the 12th, so a "2026-07" file ends 12 Aug.

    The watermark skip compares against this date; leaving it at 31 Jul made the
    statement look already-covered and its last week unreachable.
    """
    from src.transaction_processor import TransactionProcessor

    july = datetime.datetime(2026, 7, 31)
    assert TransactionProcessor._apply_cycle_end(
        july, {"statement_cycle_end_day": 12}
    ) == datetime.datetime(2026, 8, 12)
    # December rolls the year over.
    assert TransactionProcessor._apply_cycle_end(
        datetime.datetime(2026, 12, 31), {"statement_cycle_end_day": 12}
    ) == datetime.datetime(2027, 1, 12)
    # No cycle configured (and no date) -> unchanged.
    assert TransactionProcessor._apply_cycle_end(july, {}) == july
    assert (
        TransactionProcessor._apply_cycle_end(None, {"statement_cycle_end_day": 12})
        is None
    )


def test_statement_watermark_ignores_plugin_written_rows():
    """A payslip row must not make that month's statement look already booked.

    The salary splitter dates its rows at month end. Counting them in the
    watermark skipped the statement and dropped every row before the payslip
    date -- bank-hdfc-karti Jul-2026 kept only two 31-Jul rows, losing the rent.
    """
    from main import find_latest_transaction_by_account

    txns = [
        {
            "account": "bank-hdfc-karti",
            "date": datetime.datetime(2026, 7, 1),
            "description": "NEFT DR-RENT",
        },
        {
            "account": "bank-hdfc-karti",
            "date": datetime.datetime(2026, 7, 30),
            "description": "Google Salary Jul-26 (auto-split from payslip)",
        },
    ]
    assert find_latest_transaction_by_account(txns)[
        "bank-hdfc-karti"
    ] == datetime.datetime(2026, 7, 30)
    assert find_latest_transaction_by_account(txns, statement_only=True)[
        "bank-hdfc-karti"
    ] == datetime.datetime(2026, 7, 1)


def test_statement_ending_on_the_watermark_is_still_read(
    transaction_processor, mock_data_source, mocker
):
    """Watermark == statement end means we probably hold only its last day."""
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-infiniametal"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f1", name="cc-hdfc-infiniametal-2026-07.pdf")
    ]
    mock_data_source.get_processed_statements.return_value = {}
    mock_data_source.download_file.return_value = b"%PDF-"
    parsed = mocker.patch.object(
        transaction_processor, "_parse_validate_pdf", return_value=([], False)
    )

    # cc-hdfc cycle ends 12 Aug for a July file; a watermark on that date must
    # not skip it, but one past it still does.
    transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-infiniametal": datetime.datetime(2026, 8, 12)}
    )
    assert parsed.called

    parsed.reset_mock()
    transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-infiniametal": datetime.datetime(2026, 8, 13)}
    )
    assert not parsed.called


# --- Targeted category updates (recategorize writes cells, not sheets) -------


def _log_rows():
    return [
        ["Date", "Description", "Debit", "Credit", "Category", "Remarks", "Account"],
        ["2025-01-01", "SHOP A", "100.00", "", "Uncategorized", "", "cc-hdfc-og"],
        ["2025-01-02", "SHOP B", "200.00", "", "Expense:Dining", "", "cc-hdfc-og"],
        ["2025-01-03", "SHOP C", "300.00", "", "Uncategorized", "", "cc-hdfc-og"],
    ]


def test_apply_category_updates_writes_only_uncategorized_rows(
    transaction_processor, mock_data_source
):
    mock_data_source.get_transaction_log_data.return_value = _log_rows()
    mock_data_source.update_transaction_categories.return_value = 2

    updated = transaction_processor.apply_category_updates(
        "cc", [(0, "Expense:Clothing"), (2, "Expense:Medical")]
    )

    assert updated == 2
    mock_data_source.update_transaction_categories.assert_called_once_with(
        "cc", [(0, "Expense:Clothing"), (2, "Expense:Medical")]
    )


def test_apply_category_updates_refuses_already_labeled_row(
    transaction_processor, mock_data_source
):
    """A stale index must never overwrite a real category."""
    mock_data_source.get_transaction_log_data.return_value = _log_rows()
    mock_data_source.update_transaction_categories.return_value = 1

    transaction_processor.apply_category_updates(
        "cc", [(1, "Expense:Clothing"), (2, "Expense:Medical")]
    )

    # Row 1 already carries Expense:Dining, so it is dropped, not overwritten.
    mock_data_source.update_transaction_categories.assert_called_once_with(
        "cc", [(2, "Expense:Medical")]
    )


def test_apply_category_updates_drops_out_of_range_rows(
    transaction_processor, mock_data_source
):
    mock_data_source.get_transaction_log_data.return_value = _log_rows()
    mock_data_source.update_transaction_categories.return_value = 0

    transaction_processor.apply_category_updates("cc", [(99, "Expense:Clothing")])

    mock_data_source.update_transaction_categories.assert_called_once_with("cc", [])


def test_apply_category_updates_no_updates_skips_read(
    transaction_processor, mock_data_source
):
    assert transaction_processor.apply_category_updates("cc", []) == 0
    mock_data_source.update_transaction_categories.assert_not_called()


def test_get_old_transactions_carries_row_index(
    transaction_processor, mock_data_source
):
    """ROW_KEY must be each txn's position among the log's data rows."""
    from src.transaction_processor import ROW_KEY

    mock_data_source.get_transaction_log_data.return_value = _log_rows()
    txns = transaction_processor.get_old_transactions("cc")

    assert [t[ROW_KEY] for t in txns] == [0, 1, 2]
    assert [t["description"] for t in txns] == ["SHOP A", "SHOP B", "SHOP C"]


def _generate_synthetic_pdf(text: str) -> bytes:
    import io
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    buf = io.BytesIO()
    pdf = canvas.Canvas(buf, pagesize=A4)
    pdf.setFont("Courier", 6)
    y = A4[1] - 30
    for line in text.lstrip("\n").split("\n"):
        pdf.drawString(20, y, line)
        y -= 8
    pdf.save()
    return buf.getvalue()


_SYNTHETIC_AXIS_NEW = """
 Statement Period From : 01-06-2026 To : 30-06-2026
  Txn Date                    Transaction                 Withdrawals        Deposits           Balance
                Opening Balance                                                              200,000.00
  02-06-2026    UPI/SYNTHETIC GROCERY/1234                   2,000.00
  09-06-2026    NEFT REFUND FROM STORE                                       1,000.00
               Closing Balance                                                               199,000.00
"""


@patch("src.pdf_parser.PDFParser")
def test_deterministic_extractor_used_ahead_of_pdf_parser(
    mock_pdf_parser_class, transaction_processor, mock_data_source, mocker
):
    """When StatementExtractor verifies, PDFParser is never instantiated."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["bank-axis-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f1", name="bank-axis-test-2026-06.pdf")
    ]
    mock_data_source.download_file.return_value = _generate_synthetic_pdf(
        _SYNTHETIC_AXIS_NEW
    )
    mock_data_source.get_processed_statements.return_value = {}

    txns = transaction_processor.get_new_transactions_from_statements(
        "bank", {"bank-axis-test": datetime.datetime(2026, 5, 31)}
    )
    assert len(txns) == 2
    assert mock_pdf_parser_class.call_count == 0
    assert txns[0]["description"] == "UPI/SYNTHETIC GROCERY/1234"
    assert txns[0]["amount"] == -2000.0


@patch("src.pdf_parser.PDFParser")
def test_fallback_to_pdf_parser_when_extractor_unverified(
    mock_pdf_parser_class, transaction_processor, mock_data_source, mocker
):
    """When StatementExtractor cannot verify, falls back to PDFParser + validator."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["bank-axis-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f1", name="bank-axis-test-2026-06.pdf")
    ]
    # Remove closing balance so deterministic extractor fails verification
    tampered = _SYNTHETIC_AXIS_NEW.replace("Closing Balance", "Some Balance")
    mock_data_source.download_file.return_value = _generate_synthetic_pdf(tampered)
    mock_data_source.get_processed_statements.return_value = {}

    inst = mock_pdf_parser_class.return_value
    inst.parse_pdf_with_text.return_value = (
        [
            {
                "date": "2026-06-02",
                "description": "UPI/SYNTHETIC GROCERY/1234",
                "debit": "2000.00",
                "credit": "",
            }
        ],
        "pdf text",
        {"closing_balance": "199000.00"},
    )
    mocker.patch(
        "src.statement_validator.validate_statement",
        return_value=mocker.MagicMock(
            passed=[
                {
                    "date": "2026-06-02",
                    "description": "UPI/SYNTHETIC GROCERY/1234",
                    "debit": "2000.00",
                    "credit": "",
                }
            ],
            flagged=[],
            statement_flags=[],
        ),
    )

    txns = transaction_processor.get_new_transactions_from_statements(
        "bank", {"bank-axis-test": datetime.datetime(2026, 5, 31)}
    )
    assert mock_pdf_parser_class.call_count >= 1
    assert len(txns) == 1


def test_settlement_lag_lookback_and_boundary_dedup(
    transaction_processor, mock_data_source, mocker
):
    """Lookback preserves boundary splits (txn_date == last_txn_date) and backdated refunds
    within 45 days, drops txns older than 45 days, and TransactionMatcher deduplicates booked txns.
    """
    from src.transaction_matcher import TransactionMatcher

    last_txn = datetime.datetime(2026, 7, 10, 0, 0, 0)
    statement_txns = [
        {
            "date": datetime.datetime(2026, 5, 21),  # 50 days ago
            "description": "OLD TXN",
            "amount": -100.0,
            "account": "cc-hdfc-test",
        },
        {
            "date": datetime.datetime(2026, 6, 20),  # 20 days ago
            "description": "BACKDATED REFUND",
            "amount": 250.0,
            "account": "cc-hdfc-test",
        },
        {
            "date": datetime.datetime(2026, 7, 10),  # boundary split (same day)
            "description": "ADDON SWIPE (Ref# VT262860075035080000135)",
            "amount": -500.0,
            "account": "cc-hdfc-test",
        },
        {
            "date": datetime.datetime(2026, 7, 11),  # forward txn with spaced Ref#
            "description": "FORWARD SWIPE (Ref# VT9999)",
            "amount": -150.0,
            "account": "cc-hdfc-test",
        },
        {
            "date": datetime.datetime(2026, 7, 12),  # genuine new forward txn
            "description": "NEW SWIPE",
            "amount": -200.0,
            "account": "cc-hdfc-test",
        },
    ]

    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f1", name="cc-hdfc-test-2026-07.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": t["date"].strftime("%Y-%m-%d"),
                    "description": t["description"],
                    "debit": f"{-t['amount']:.2f}" if t["amount"] < 0 else "",
                    "credit": f"{t['amount']:.2f}" if t["amount"] >= 0 else "",
                }
                for t in statement_txns
            ],
            True,
        ),
    )

    parsed_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": last_txn}
    )

    # 1 (OLD TXN) was dropped (>45d); 2, 3, 4, 5 were kept
    assert len(parsed_txns) == 4
    descriptions = [t["description"] for t in parsed_txns]
    assert "OLD TXN" not in descriptions
    assert "BACKDATED REFUND" in descriptions
    assert "ADDON SWIPE (Ref# VT262860075035080000135)" in descriptions
    assert "FORWARD SWIPE (Ref# VT9999)" in descriptions
    assert "NEW SWIPE" in descriptions

    # Deduplicate against already-booked sheet transactions:
    # - Row 3 (ADDON SWIPE) is <= watermark: multiset matching dedupes it against old_booked_txns[0].
    # - Row 4 (FORWARD SWIPE) is > watermark: _description_signature normalizes Ref# spacing
    #   and dedupes it against old_booked_txns[1].
    old_booked_txns = [
        {
            "date": datetime.datetime(2026, 7, 10),
            "account": "cc-hdfc-test",
            "amount": -500.0,
            "description": "ADDON SWIPE (Ref#VT262860075035080000135)",
        },
        {
            "date": datetime.datetime(2026, 7, 11),
            "account": "cc-hdfc-test",
            "amount": -150.0,
            "description": "FORWARD SWIPE (Ref#VT9999)",
        },
    ]
    new_final = TransactionMatcher.find_new_txns(
        old_booked_txns, parsed_txns, watermark_by_account={"cc-hdfc-test": last_txn}
    )
    assert len(new_final) == 2
    final_descs = [t["description"] for t in new_final]
    assert "BACKDATED REFUND" in final_descs
    assert "NEW SWIPE" in final_descs
    assert "ADDON SWIPE (Ref# VT262860075035080000135)" not in final_descs
    assert "FORWARD SWIPE (Ref# VT9999)" not in final_descs


def test_first_deploy_uncached_legacy_pdf_uses_strict_filter(
    transaction_processor, mock_data_source, mocker
):
    """On first deploy, an uncached legacy PDF whose max date <= last_txn_date uses
    strict forward filter (re-booking 0 edited/deleted rows) and writes its clean cache entry.
    """
    last_txn = datetime.datetime(2026, 7, 10, 0, 0, 0)
    statement_txns = [
        {
            "date": datetime.datetime(2026, 6, 25),
            "description": "USER DELETED ROW",
            "amount": -500.0,
        },
        {
            "date": datetime.datetime(2026, 7, 10),
            "description": "LAST ROW IN STMT",
            "amount": -200.0,
        },
    ]
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_legacy", name="cc-hdfc-test-2026-07.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": t["date"].strftime("%Y-%m-%d"),
                    "description": t["description"],
                    "debit": f"{-t['amount']:.2f}",
                    "credit": "",
                }
                for t in statement_txns
            ],
            True,
        ),
    )

    parsed_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": last_txn}
    )
    # stmt_max_date (2026-07-10) <= last_txn_date (2026-07-10) -> strict > filter applied -> 0 rows
    assert len(parsed_txns) == 0
    # Clean PDF cache entry was saved
    mock_data_source.save_processed_statements.assert_called_once_with(
        {"f_legacy": "2026-07-10"}
    )


def test_daily_retry_of_partially_quarantined_pdf(
    transaction_processor, mock_data_source, mocker
):
    """On Day 2 of a partially-quarantined PDF:
    - Cached 'partial' dict preserves Day 1's base_watermark and emitted multiset.
    - Rescues rows dated on or before Day 2's watermark (which advanced after Day 1).
    - Does NOT re-book rows deleted from the ledger on Day 1 evening.
    - Overwrites cache with clean ISO max date and calls clear_review_rows only after rescue.
    """
    # On Day 2 morning, ledger watermark has advanced to Day 1's latest booked txn (2026-07-31)
    last_txn = datetime.datetime(2026, 7, 31, 0, 0, 0)
    statement_txns = [
        {
            "date": datetime.datetime(2026, 7, 11),
            "description": "DAY 1 PASSED ROW (DELETED FROM LEDGER ON DAY 1 EVENING)",
            "amount": -100.0,
        },
        {
            "date": datetime.datetime(2026, 7, 15),
            "description": "DAY 1 FLAGGED ROW (RESCUED ON DAY 2 <= WATERMARK)",
            "amount": -200.0,
        },
        {
            "date": datetime.datetime(2026, 7, 31),
            "description": "DAY 1 PASSED ROW (BOOKED ON DAY 1)",
            "amount": -300.0,
        },
    ]
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_partial", name="cc-hdfc-test-2026-07.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    # Present in processed_cache as partial dict recording Day 1 state
    mock_data_source.get_processed_statements.return_value = {
        "f_partial": {
            "status": "partial",
            "base_watermark": "2026-07-01",
            "opened_lookback": True,
            "emitted": [
                ["2026-07-11", -100.0],
                ["2026-07-31", -300.0],
            ],
        }
    }

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": t["date"].strftime("%Y-%m-%d"),
                    "description": t["description"],
                    "debit": f"{-t['amount']:.2f}",
                    "credit": "",
                }
                for t in statement_txns
            ],
            True,  # Now verified clean on Day 2 retry!
        ),
    )

    parsed_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": last_txn}
    )

    # Rescued row dated 2026-07-15 (<= Day 2 watermark 2026-07-31) was emitted!
    # Deleted 2026-07-11 row was NOT re-booked (subtracted 1-for-1 from emitted)!
    # Already-booked 2026-07-31 row was NOT re-booked!
    assert len(parsed_txns) == 1
    assert (
        parsed_txns[0]["description"]
        == "DAY 1 FLAGGED ROW (RESCUED ON DAY 2 <= WATERMARK)"
    )
    assert parsed_txns[0]["amount"] == -200.0
    assert parsed_txns[0]["date"] == datetime.datetime(2026, 7, 15)

    # clear_review_rows was called for the rescued statement
    mock_data_source.clear_review_rows.assert_called_once_with(
        "cc-hdfc-test", "cc-hdfc-test-2026-07.pdf"
    )

    # Cache overwritten with clean ISO date
    mock_data_source.save_processed_statements.assert_called_once_with(
        {"f_partial": "2026-07-31"}
    )


def test_day1_100pct_flagged_pdf_records_partial_cache(
    transaction_processor, mock_data_source, mocker
):
    """When 100% of rows in a PDF are flagged on Day 1, df is empty/None but partial cache entry is saved."""
    last_txn = datetime.datetime(2026, 7, 1, 0, 0, 0)
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_flagged", name="cc-hdfc-test-2026-07.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=([], False),  # 100% flagged
    )

    parsed_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": last_txn}
    )
    assert len(parsed_txns) == 0
    mock_data_source.clear_review_rows.assert_not_called()
    mock_data_source.save_processed_statements.assert_called_once_with(
        {
            "f_flagged": {
                "status": "partial",
                "base_watermark": "2026-07-01T00:00:00",
                "opened_lookback": False,
                "was_first_ingest": True,
                "emitted": [],
            }
        }
    )


def test_partial_retry_opened_lookback_false_and_pre_cutoff_leaves_partial_state(
    transaction_processor, mock_data_source, mocker
):
    """A partial retry with opened_lookback=False (or pre-cutoff rows) promotes cleanly
    to an ISO date and clears Review once pdf_clean=True, rather than looping forever.
    """
    last_txn = datetime.datetime(2026, 7, 15, 0, 0, 0)
    statement_txns = [
        {
            "date": datetime.datetime(2026, 7, 5),
            "description": "PRE-WATERMARK HISTORICAL ROW",
            "amount": -50.0,
        },
        {
            "date": datetime.datetime(2026, 7, 11),
            "description": "DAY 1 PASSED ROW",
            "amount": -100.0,
        },
        {
            "date": datetime.datetime(2026, 7, 15),
            "description": "DAY 2 RESCUED ROW",
            "amount": -200.0,
        },
    ]
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_selfheal", name="cc-hdfc-test-2026-07.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {
        "f_selfheal": {
            "status": "partial",
            "base_watermark": "2026-07-10T00:00:00",
            "opened_lookback": False,
            "was_first_ingest": False,
            "emitted": [["2026-07-11", -100.0]],
        }
    }
    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": t["date"].strftime("%Y-%m-%d"),
                    "description": t["description"],
                    "debit": f"{-t['amount']:.2f}",
                    "credit": "",
                }
                for t in statement_txns
            ],
            True,
        ),
    )

    parsed_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": last_txn}
    )
    assert len(parsed_txns) == 1
    assert parsed_txns[0]["description"] == "DAY 2 RESCUED ROW"
    mock_data_source.save_processed_statements.assert_called_once_with(
        {
            "f_selfheal": "2026-07-15",
        }
    )
    mock_data_source.clear_review_rows.assert_called_once_with(
        "cc-hdfc-test", "cc-hdfc-test-2026-07.pdf"
    )


def test_run_normal_mode_does_not_commit_partial_cache_when_sheet_write_fails(
    transaction_processor, mock_data_source, mocker
):
    """When add_new_transactions_to_log fails in run_normal_mode, partial emitted rows
    and Review clears are NOT persisted so the next run retries without losing rows."""
    import main as gajana_main
    from src.categorizer import Categorizer

    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", [])
    mock_data_source.get_transaction_log_data.return_value = []
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_fail", name="cc-hdfc-test-2026-07.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": "2026-07-15",
                    "description": "PASSED ROW",
                    "debit": "125.00",
                    "credit": "",
                }
            ],
            False,  # Partial (some other row flagged)
        ),
    )
    mocker.patch.object(
        transaction_processor,
        "add_new_transactions_to_log",
        side_effect=RuntimeError("Sheets 503"),
    )
    categorizer = Categorizer(matchers_file="data/matchers.example.json", llm=None)

    errors = gajana_main.run_normal_mode(transaction_processor, categorizer)
    assert errors == ["cc: Sheets 503"]

    # Sheet write failed -> save_processed_statements and clear_review_rows must NOT have run!
    mock_data_source.save_processed_statements.assert_not_called()
    mock_data_source.clear_review_rows.assert_not_called()


def test_non_pdf_sheet_statements_use_strict_forward_filter(
    transaction_processor, mock_data_source, mocker
):
    """Mutable non-PDF Sheet statements use strict forward filter (date > last_txn_date),
    never re-reading or re-offering rows on or before the watermark.
    """
    last_txn = datetime.datetime(2026, 7, 10, 0, 0, 0)
    sheet_data = [
        ["Date", "Description", "Amount"],
        ["2026-06-20", "OLD ROW IN LOOKBACK", "-100.00"],
        ["2026-07-10", "SAME DAY AS WATERMARK", "-200.00"],
        ["2026-07-11", "NEW FORWARD ROW", "-300.00"],
    ]

    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_sheet1", name="cc-hdfc-test-2026-07.gsheet")
    ]
    mock_data_source.get_first_sheet_name_from_file.return_value = "Sheet1"
    mock_data_source.get_sheet_data.return_value = sheet_data
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch(
        "src.transaction_processor.PARSING_CONFIG",
        {
            "cc-hdfc": {
                "header_patterns": [["Date", "Description", "Amount"]],
                "column_map": {
                    "Date": "date",
                    "Description": "description",
                    "Amount": "amount",
                },
                "date_formats": ["%Y-%m-%d"],
            }
        },
    )

    parsed_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": last_txn}
    )

    # For .gsheet, strict > last_txn_date applies: 2026-06-20 and 2026-07-10 are dropped
    assert len(parsed_txns) == 1
    assert parsed_txns[0]["description"] == "NEW FORWARD ROW"
    assert parsed_txns[0]["amount"] == -300.0


def test_clean_pdf_clears_review_rows(transaction_processor, mock_data_source, mocker):
    """When a PDF verifies cleanly via deterministic extractor, clear_review_rows is called."""
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["bank-axis-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f1", name="bank-axis-test-2026-06.pdf")
    ]
    mock_data_source.download_file.return_value = _generate_synthetic_pdf(
        _SYNTHETIC_AXIS_NEW
    )
    mock_data_source.get_processed_statements.return_value = {}

    transaction_processor.get_new_transactions_from_statements(
        "bank", {"bank-axis-test": datetime.datetime(2026, 5, 31)}
    )

    mock_data_source.clear_review_rows.assert_called_once_with(
        "bank-axis-test", "bank-axis-test-2026-06.pdf"
    )


def test_partial_retry_absorbs_llm_to_extractor_date_drift(
    transaction_processor, mock_data_source, mocker
):
    """When Day 1 emitted a row via LLM at its value date (Jul 16) and Day 2 re-parses
    via deterministic extractor at its transaction date (Jul 15, within ±3d), the Day 1
    row is subtracted rather than emitted twice."""
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_drift", name="cc-hdfc-test-2026-07.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {
        "f_drift": {
            "status": "partial",
            "base_watermark": "2026-07-10T00:00:00",
            "opened_lookback": True,
            "was_first_ingest": True,
            "emitted": [["2026-07-16", -500.0]],
        }
    }
    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": "2026-07-15",
                    "description": "SAME PURCHASE TXN DATE",
                    "debit": "500.00",
                    "credit": "",
                },
                {
                    "date": "2026-07-18",
                    "description": "RESCUED ROW",
                    "debit": "900.00",
                    "credit": "",
                },
            ],
            True,
        ),
    )

    res = transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": datetime.datetime(2026, 7, 16)}
    )
    assert len(res) == 1
    assert res[0]["amount"] == -900.0
    assert (
        mock_data_source.save_processed_statements.call_args[0][0]["f_drift"]
        == "2026-07-18"
    )


def test_pdf_parse_loss_records_quarantine_and_partial_cache(
    transaction_processor, mock_data_source, mocker
):
    """When a PDF drops unparseable or future-dated rows (parse_loss=True), those rows
    are written to Review, recorded in quarantined_files, and valid rows are saved in
    a partial cache entry; if write_review_rows fails, review_write_failed_files tracks it.
    """
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_loss", name="cc-hdfc-test-2026-07.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}
    future_date = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()
    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": "2026-07-15",
                    "description": "VALID ROW",
                    "debit": "250.00",
                    "credit": "",
                },
                {
                    "date": "NOT-A-DATE",
                    "description": "BAD DATE TOKEN",
                    "debit": "111.00",
                    "credit": "",
                },
                {
                    "date": future_date,
                    "description": "HALLUCINATED FUTURE ROW",
                    "debit": "999.00",
                    "credit": "",
                },
            ],
            True,
        ),
    )

    res = transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": datetime.datetime(2026, 7, 10)}
    )
    assert len(res) == 1
    assert res[0]["amount"] == -250.0
    assert transaction_processor.quarantined_files["cc-hdfc-test-2026-07.pdf"] == 2
    mock_data_source.write_review_rows.assert_called_once()
    review_written = mock_data_source.write_review_rows.call_args[0][0]
    assert len(review_written) == 2
    assert review_written[0][5] == "unparseable date"
    assert review_written[1][5] == "future date"
    saved = mock_data_source.save_processed_statements.call_args[0][0]["f_loss"]
    assert saved["status"] == "partial"
    assert saved["emitted"] == [["2026-07-15", -250.0]]

    mock_data_source.write_review_rows.side_effect = RuntimeError("Sheets timeout")
    transaction_processor.get_new_transactions_from_statements(
        "cc", {"cc-hdfc-test": datetime.datetime(2026, 7, 10)}
    )
    assert (
        transaction_processor.review_write_failed_files["cc-hdfc-test-2026-07.pdf"] == 2
    )


def test_partial_retry_through_matcher_preserves_repeated_identical_rows(
    transaction_processor, mock_data_source, mocker
):
    """End-to-end processor + matcher check for repeated identical rows:
    1) Forward case (5 Jul > base_watermark 30 Jun, Day 1 advances watermark to 31 Jul):
       Statement has two -10,000 ATM rows on 5 Jul and one -500 row on 31 Jul.
       Day 1 books one -10,000 row + the -500 row; Day 2 rescues and books the second
       -10,000 row through TransactionMatcher without double-subtracting Day 1's row.
    2) Lookback case (5 Jul <= base_watermark 10 Jul):
       Same two -10,000 rows inside Day 1's lookback window; Day 2 still rescues and
       books the second -10,000 row cleanly and strips all transient keys."""
    from src.transaction_matcher import TransactionMatcher

    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", ["bank-hdfc-test"])

    for base_wm_day in (30, 10):
        base_wm = (
            datetime.datetime(2026, 6, 30)
            if base_wm_day == 30
            else datetime.datetime(2026, 7, 10)
        )
        ledger = [
            {
                "date": base_wm,
                "account": "bank-hdfc-test",
                "amount": -100.0,
                "description": "PRIOR WATERMARK ROW",
            }
        ]
        cache: dict = {}
        mock_data_source.list_statement_file_details.return_value = [
            DataSourceFile(id="f_atm", name="bank-hdfc-test-2026-07.pdf")
        ]
        mock_data_source.download_file.return_value = b"dummy"
        mock_data_source.get_processed_statements.side_effect = lambda c=cache: dict(c)
        mock_data_source.save_processed_statements.side_effect = (
            lambda u, c=cache: c.update(u)
        )

        # Day 1: one ATM row passes, second ATM row was flagged, 31 Jul row passes
        mocker.patch.object(
            transaction_processor,
            "_parse_validate_pdf",
            return_value=(
                [
                    {
                        "date": "2026-07-05",
                        "description": "ATM CASH WDL",
                        "debit": "10000.00",
                        "credit": "",
                    },
                    {
                        "date": "2026-07-31",
                        "description": "END OF MONTH PURCHASE",
                        "debit": "500.00",
                        "credit": "",
                    },
                ],
                False,
            ),
        )
        wm_day1 = {"bank-hdfc-test": max(t["date"] for t in ledger)}
        pot_day1 = transaction_processor.get_new_transactions_from_statements(
            "bank", wm_day1
        )
        booked_day1 = TransactionMatcher.find_new_txns(
            ledger, pot_day1, watermark_by_account=wm_day1
        )
        assert [t["amount"] for t in booked_day1] == [-10000.0, -500.0]
        ledger.extend(booked_day1)

        # Day 2: watermark is now 2026-07-31; full statement parses cleanly with BOTH ATM rows
        mocker.patch.object(
            transaction_processor,
            "_parse_validate_pdf",
            return_value=(
                [
                    {
                        "date": "2026-07-05",
                        "description": "ATM CASH WDL",
                        "debit": "10000.00",
                        "credit": "",
                    },
                    {
                        "date": "2026-07-05",
                        "description": "ATM CASH WDL",
                        "debit": "10000.00",
                        "credit": "",
                    },
                    {
                        "date": "2026-07-31",
                        "description": "END OF MONTH PURCHASE",
                        "debit": "500.00",
                        "credit": "",
                    },
                ],
                True,
            ),
        )
        wm_day2 = {"bank-hdfc-test": max(t["date"] for t in ledger)}
        assert wm_day2["bank-hdfc-test"] == datetime.datetime(2026, 7, 31)
        pot_day2 = transaction_processor.get_new_transactions_from_statements(
            "bank", wm_day2
        )
        booked_day2 = TransactionMatcher.find_new_txns(
            ledger, pot_day2, watermark_by_account=wm_day2
        )
        assert len(booked_day2) == 1
        assert booked_day2[0]["amount"] == -10000.0
        assert booked_day2[0]["date"] == datetime.datetime(2026, 7, 5)
        assert "_effective_wm" not in booked_day2[0]
        assert "_pre_consumed" not in booked_day2[0]
        assert "_source" not in booked_day2[0]
        assert cache["f_atm"] == "2026-07-31"


def test_run_normal_mode_triggers_telegram_notification_on_addon_swipes(
    transaction_processor, mock_data_source, mocker
):
    """When newly added CC transactions contain add-on swipes, send_telegram_addon_notification
    is invoked with the combined history."""
    import main as gajana_main
    from src.categorizer import Categorizer

    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", [])
    mock_data_source.get_transaction_log_data.return_value = []
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_addon", name="cc-hdfc-test-2026-08.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": "2026-08-15",
                    "description": "PHARMACY",
                    "debit": "1500.00",
                    "credit": "",
                    "cardholder": "PATRICIA MORGAN",
                }
            ],
            True,
        ),
    )
    transaction_processor.addon_cards = {
        "primary_cardholders": ["ALEX MORGAN"],
        "users": [
            {
                "id": "mom",
                "label": "Mom",
                "greeting_name": "Amma",
                "cardholder_patterns": ["PATRICIA MORGAN"],
                "reimburse": True,
            }
        ],
    }

    mock_notify = mocker.patch("src.addon_tracker.send_telegram_addon_notification")
    categorizer = Categorizer(
        matchers_file="data/matchers.example.json",
        llm=None,
        addon_cards=transaction_processor.addon_cards,
    )

    errors = gajana_main.run_normal_mode(transaction_processor, categorizer)
    assert errors == []
    assert mock_notify.call_count == 1
    call_kwargs = mock_notify.call_args.kwargs
    assert len(call_kwargs["newly_added_cc"]) == 1
    assert "addon:Mom" in call_kwargs["newly_added_cc"][0]["remarks"]


def test_run_normal_mode_does_not_fail_if_telegram_notification_raises(
    transaction_processor, mock_data_source, mocker
):
    """If send_telegram_addon_notification raises (e.g. Telegram 500 or network timeout),
    run_normal_mode catches it, logs a warning, and finishes cleanly."""
    import main as gajana_main
    from src.categorizer import Categorizer

    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mocker.patch("src.transaction_processor.BANK_ACCOUNTS", [])
    mock_data_source.get_transaction_log_data.return_value = []
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_addon_err", name="cc-hdfc-test-2026-08.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": "2026-08-15",
                    "description": "PHARMACY",
                    "debit": "1500.00",
                    "credit": "",
                    "cardholder": "PATRICIA MORGAN",
                }
            ],
            True,
        ),
    )
    transaction_processor.addon_cards = {
        "primary_cardholders": ["ALEX MORGAN"],
        "users": [
            {
                "id": "mom",
                "label": "Mom",
                "cardholder_patterns": ["PATRICIA MORGAN"],
                "reimburse": True,
            }
        ],
    }

    mocker.patch(
        "src.addon_tracker.send_telegram_addon_notification",
        side_effect=RuntimeError("Telegram 500 Outage"),
    )
    categorizer = Categorizer(
        matchers_file="data/matchers.example.json",
        llm=None,
        addon_cards=transaction_processor.addon_cards,
    )

    errors = gajana_main.run_normal_mode(transaction_processor, categorizer)
    # The statement booking succeeded cleanly; telegram error was logged as warning and did not fail run_normal_mode!
    assert errors == []


def test_primary_and_addon_rows_produce_json_serializable_remarks_without_pd_na(
    transaction_processor, mock_data_source, mocker
):
    """Primary rows (empty remarks) and add-on rows produce clean str remarks,
    never pd.NA, so json.dumps succeeds and Review remarks are not suppressed."""
    import json
    from src.categorizer import Categorizer

    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.get_transaction_log_data.return_value = []
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_pdf", name="cc-hdfc-test-2026-08.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": "2026-08-10",
                    "description": "PRIMARY GROCERY ROW",
                    "debit": "1000.00",
                    "credit": "",
                    "cardholder": "ALEX MORGAN",
                },
                {
                    "date": "2026-08-12",
                    "description": "ADDON MOM ROW",
                    "debit": "2000.00",
                    "credit": "",
                    "cardholder": "PATRICIA MORGAN",
                },
                {
                    "date": "2026-08-14",
                    "description": "UNKNOWN NO MATCH MERCHANT",
                    "debit": "500.00",
                    "credit": "",
                    "cardholder": "",
                },
            ],
            True,
        ),
    )
    transaction_processor.addon_cards = {
        "primary_cardholders": ["ALEX MORGAN"],
        "users": [
            {
                "id": "mom",
                "label": "Mom",
                "cardholder_patterns": ["PATRICIA MORGAN"],
                "reimburse": True,
            }
        ],
    }

    categorizer = Categorizer(
        matchers_file="data/matchers.example.json",
        llm=None,
        addon_cards=transaction_processor.addon_cards,
    )

    new_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", latest_txn_by_account={"cc-hdfc-test": None}
    )
    assert len(new_txns) == 3

    # Primary row remarks must be "" (not pd.NA)
    assert new_txns[0]["remarks"] == ""
    # Addon row remarks must be "addon:Mom"
    assert new_txns[1]["remarks"] == "addon:Mom"
    # Unspecified cardholder remarks must be "" (not pd.NA)
    assert new_txns[2]["remarks"] == ""

    # Categorize
    cat_txns = categorizer.categorize(new_txns)
    assert cat_txns[1]["category"] == "Transfer"
    assert cat_txns[1]["remarks"] == "addon:Mom"

    # Format for storage
    storage_values = transaction_processor._format_txns_for_storage(cat_txns)
    # json.dumps MUST serialize without TypeError
    dumped = json.dumps(storage_values)
    assert dumped is not None
    assert "<NA>" not in dumped
    assert "NaN" not in dumped


def test_llm_pdf_card_bill_payment_not_tagged_addon(
    transaction_processor, mock_data_source, mocker
):
    """Card bill payments and cashbacks parsed by LLM must NOT be tagged with addon: even if cardholder matches."""
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.get_transaction_log_data.return_value = []
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_billpay", name="cc-hdfc-test-2026-08.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": "2026-08-10",
                    "description": "PAYMENT RECEIVED - AUTOPAY",
                    "debit": "",
                    "credit": "15000.00",
                    "cardholder": "PATRICIA MORGAN",
                },
                {
                    "date": "2026-08-11",
                    "description": "PHARMACY STORE",
                    "debit": "2000.00",
                    "credit": "",
                    "cardholder": "PATRICIA MORGAN",
                },
            ],
            True,
        ),
    )
    transaction_processor.addon_cards = {
        "primary_cardholders": ["ALEX MORGAN"],
        "users": [
            {
                "id": "mom",
                "label": "Mom",
                "cardholder_patterns": ["PATRICIA MORGAN"],
                "reimburse": True,
            }
        ],
    }
    new_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", latest_txn_by_account={"cc-hdfc-test": None}
    )
    assert len(new_txns) == 2
    # Bill payment row remarks must be "" (NOT "addon:Mom")
    assert new_txns[0]["remarks"] == ""
    # Purchase row remarks must be "addon:Mom"
    assert new_txns[1]["remarks"] == "addon:Mom"


def test_llm_pdf_mixed_types_credit_column_no_na_ambiguity_crash(
    transaction_processor, mock_data_source, mocker
):
    """When passed_txns mixes float debit + empty credit AND empty debit + float credit
    (matching StatementExtractor types), df.replace('', pd.NA) creates object dtype with pd.NA.
    _apply_cardholder_tag must not crash with 'TypeError: boolean value of NA is ambiguous'.
    """
    mocker.patch("src.transaction_processor.CC_ACCOUNTS", ["cc-hdfc-test"])
    mock_data_source.get_transaction_log_data.return_value = []
    mock_data_source.list_statement_file_details.return_value = [
        DataSourceFile(id="f_mixed", name="cc-hdfc-test-2026-08.pdf")
    ]
    mock_data_source.download_file.return_value = b"dummy"
    mock_data_source.get_processed_statements.return_value = {}

    mocker.patch.object(
        transaction_processor,
        "_parse_validate_pdf",
        return_value=(
            [
                {
                    "date": "2026-08-10",
                    "description": "GROCERY SPEND",
                    "debit": 500.0,  # float debit, string empty credit
                    "credit": "",
                    "cardholder": "PATRICIA MORGAN",
                },
                {
                    "date": "2026-08-12",
                    "description": "PAYMENT RECEIVED - AUTOPAY",
                    "debit": "",
                    "credit": 5000.0,  # string empty debit, float credit
                    "cardholder": "PATRICIA MORGAN",
                },
            ],
            True,
        ),
    )
    transaction_processor.addon_cards = {
        "primary_cardholders": ["ALEX MORGAN"],
        "users": [
            {
                "id": "mom",
                "label": "Mom",
                "cardholder_patterns": ["PATRICIA MORGAN"],
                "reimburse": True,
            }
        ],
    }
    new_txns = transaction_processor.get_new_transactions_from_statements(
        "cc", latest_txn_by_account={"cc-hdfc-test": None}
    )
    assert len(new_txns) == 2
    # Debit row gets tagged addon:Mom without boolean ambiguity crash
    assert new_txns[0]["remarks"] == "addon:Mom"
    # Credit bill payment gets empty remarks
    assert new_txns[1]["remarks"] == ""
