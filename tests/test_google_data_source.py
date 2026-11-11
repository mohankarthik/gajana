# tests/test_google_data_source.py
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError as GoogleHttpError

# Assuming your project structure allows these imports
from src.google_data_source import GoogleDataSource
from src.interfaces import DataSourceFile


# Mock the utils.log_and_exit function for all tests in this module
@pytest.fixture(autouse=True)
def mock_log_and_exit_fixture(mocker):
    return mocker.patch("src.google_data_source.log_and_exit", side_effect=SystemExit)


@pytest.fixture
def mock_google_services(mocker):
    """Mocks Google API services (Drive and Sheets) and their builder."""
    mock_drive_service = MagicMock()
    mock_sheets_service = MagicMock()

    # Mock the build function
    # It needs to return the correct service based on the serviceName argument
    def build_side_effect(serviceName, version, credentials, cache_discovery):
        if serviceName == "drive":
            return mock_drive_service
        elif serviceName == "sheets":
            return mock_sheets_service
        raise ValueError(f"Unexpected serviceName: {serviceName}")

    mocker.patch("src.google_data_source.build", side_effect=build_side_effect)
    return mock_drive_service, mock_sheets_service


@pytest.fixture
def mock_service_account_credentials(mocker):
    """Mocks ServiceAccountCredentials.from_json_keyfile_name."""
    mock_creds = MagicMock()
    return mocker.patch(
        "oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name",
        return_value=mock_creds,
    )


# --- Tests for Initialization and Service Getters ---
def test_google_data_source_init_success(
    mock_service_account_credentials, mock_google_services
):
    """Test successful initialization of GoogleDataSource."""
    try:
        gds = GoogleDataSource()
        assert gds.creds is not None
        assert gds.drive_service is not None
        assert gds.sheets_service is not None
        mock_service_account_credentials.assert_called_once()
        # build is called twice (once for drive, once for sheets)
        assert gds.drive_service == mock_google_services[0]
        assert gds.sheets_service == mock_google_services[1]
    except SystemExit:
        pytest.fail("GoogleDataSource initialization failed unexpectedly.")


def test_get_credential_file_not_found(mock_log_and_exit_fixture, mocker):
    """Test _get_credential when the key file is not found."""
    mocker.patch(
        "oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name",
        side_effect=FileNotFoundError("File not found"),
    )
    with pytest.raises(SystemExit):
        GoogleDataSource()
    mock_log_and_exit_fixture.assert_called_once()
    args, kwargs = mock_log_and_exit_fixture.call_args
    assert "Service account key file not found" in args[1]


def test_get_credential_other_exception(mock_log_and_exit_fixture, mocker):
    """Test _get_credential with a generic exception during credential loading."""
    mocker.patch(
        "oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name",
        side_effect=Exception("Some auth error"),
    )
    with pytest.raises(SystemExit):
        GoogleDataSource()
    mock_log_and_exit_fixture.assert_called_once()
    args, kwargs = mock_log_and_exit_fixture.call_args
    assert "Error during authentication" in args[1]


@patch("src.google_data_source.build", side_effect=Exception("Build failed"))
def test_get_drive_service_build_failure(
    mock_build, mock_service_account_credentials, mock_log_and_exit_fixture
):
    with pytest.raises(SystemExit):
        GoogleDataSource()  # Drive service build is called in __init__
    # log_and_exit should be called by _get_drive_service
    # Check that the message contains "Failed to build Google Drive service"
    # This can be tricky if _get_credential also fails, so ensure it passes first
    # For simplicity, assuming _get_credential passes due to mock_service_account_credentials
    args, kwargs = mock_log_and_exit_fixture.call_args
    assert "Failed to build Google Drive service" in args[1]


# --- Tests for list_statement_file_details ---
def test_list_statement_file_details_success(
    mock_service_account_credentials, mock_google_services
):
    gds = GoogleDataSource()
    mock_drive_service, _ = mock_google_services
    mock_files_list_execute = MagicMock(
        return_value={
            "files": [
                {"id": "file1_id", "name": "file1_name.gsheet"},
                {"id": "file2_id", "name": "file2_name.gsheet"},
            ],
            "nextPageToken": None,
        }
    )
    mock_drive_service.files.return_value.list.return_value.execute = (
        mock_files_list_execute
    )

    result = gds.list_statement_file_details()

    assert len(result) == 2
    assert isinstance(result[0], DataSourceFile)
    assert result[0].id == "file1_id"
    assert result[0].name == "file1_name.gsheet"
    mock_drive_service.files.return_value.list.assert_called_once()


def test_list_statement_file_details_pagination(
    mock_service_account_credentials, mock_google_services
):
    gds = GoogleDataSource()
    mock_drive_service, _ = mock_google_services
    # Simulate pagination: first call returns a page token, second call returns the rest
    page1_response = {
        "files": [{"id": "file1_id", "name": "file1.gsheet"}],
        "nextPageToken": "token123",
    }
    page2_response = {
        "files": [{"id": "file2_id", "name": "file2.gsheet"}],
        "nextPageToken": None,
    }
    mock_drive_service.files.return_value.list.return_value.execute.side_effect = [
        page1_response,
        page2_response,
    ]

    result = gds.list_statement_file_details()
    assert len(result) == 2
    assert result[1].id == "file2_id"
    assert mock_drive_service.files.return_value.list.call_count == 2


def test_list_statement_file_details_api_error(
    mock_service_account_credentials, mock_google_services, mock_log_and_exit_fixture
):
    gds = GoogleDataSource()
    mock_drive_service, _ = mock_google_services
    # Simulate an HttpError
    mock_response = MagicMock()
    mock_response.status = 500
    mock_drive_service.files.return_value.list.return_value.execute.side_effect = (
        GoogleHttpError(resp=mock_response, content=b"Server error")
    )

    with pytest.raises(SystemExit):
        gds.list_statement_file_details()
    mock_log_and_exit_fixture.assert_called_once()
    args, kwargs = mock_log_and_exit_fixture.call_args
    assert "Google Drive API error listing files" in args[1]


# --- Tests for get_first_sheet_name_from_file ---
def test_get_first_sheet_name_success(
    mock_service_account_credentials, mock_google_services
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    mock_sheets_service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"title": "Hidden Sheet", "hidden": True}},
            {"properties": {"title": "VisibleSheet1", "hidden": False}},
            {"properties": {"title": "VisibleSheet2", "hidden": False}},
        ]
    }
    result = gds.get_first_sheet_name_from_file("test_file_id")
    assert result == "VisibleSheet1"
    mock_sheets_service.spreadsheets.return_value.get.assert_called_once_with(
        spreadsheetId="test_file_id", fields="sheets(properties(title,hidden,sheetId))"
    )


def test_get_first_sheet_name_no_visible_sheets(
    mock_service_account_credentials, mock_google_services
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    mock_sheets_service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Hidden Sheet", "hidden": True}}]
    }
    result = gds.get_first_sheet_name_from_file("test_file_id")
    assert result is None


def test_get_first_sheet_name_api_error(
    mock_service_account_credentials, mock_google_services, caplog
):
    gds = (
        GoogleDataSource()
    )  # log_and_exit is NOT called for this specific error by default
    _, mock_sheets_service = mock_google_services
    mock_response = MagicMock()
    mock_response.status = 404
    mock_sheets_service.spreadsheets.return_value.get.return_value.execute.side_effect = GoogleHttpError(
        resp=mock_response, content=b"Not Found"
    )
    result = None
    with pytest.raises(SystemExit):
        result = gds.get_first_sheet_name_from_file("test_file_id")
    assert result is None


def test_get_sheet_data_retry_logic(
    mock_service_account_credentials,
    mock_google_services,
    mock_log_and_exit_fixture,
    mocker,
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services

    mock_response_error = MagicMock()
    mock_response_error.status = 429  # Rate limit error
    error_429 = GoogleHttpError(resp=mock_response_error, content=b"Rate limit")
    success_response = {"values": [["final_data"]]}

    # Simulate: first call fails with 429, second call succeeds
    mock_sheets_service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = [
        error_429,
        success_response,
    ]
    mocker.patch("time.sleep")  # Mock time.sleep to speed up test

    result = gds.get_sheet_data("test_id", "Sheet1", "A1:B2")
    assert result == [["final_data"]]
    assert (
        mock_sheets_service.spreadsheets.return_value.values.return_value.get.call_count
        == 2
    )
    time.sleep.assert_called_once_with(5 * (2**0))  # Check backoff


def test_get_sheet_data_final_failure(
    mock_service_account_credentials,
    mock_google_services,
    mock_log_and_exit_fixture,
    mocker,
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    mock_response_error = MagicMock()
    mock_response_error.status = 500
    error_500 = GoogleHttpError(resp=mock_response_error, content=b"Server error")
    mock_sheets_service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = (
        error_500
    )
    mocker.patch("time.sleep")

    with pytest.raises(SystemExit):
        gds.get_sheet_data("test_id", "Sheet1", "A1:B2")
    mock_log_and_exit_fixture.assert_called_once()
    args, kwargs = mock_log_and_exit_fixture.call_args
    assert "Final API error in get_sheet_data" in args[1]


# --- Tests for append_transactions_to_log ---
def test_append_transactions_to_log_success(
    mock_service_account_credentials, mock_google_services, mocker
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    mock_execute = MagicMock(return_value={"updates": {"updatedCells": 5}})
    mock_sheets_service.spreadsheets.return_value.values.return_value.append.return_value.execute = (
        mock_execute
    )
    mocker.patch("src.google_data_source.TRANSACTIONS_SHEET_ID", "dummy_txn_sheet_id")

    gds.append_transactions_to_log("bank", [["row1"], ["row2"]])
    mock_sheets_service.spreadsheets.return_value.values.return_value.append.assert_called_once_with(
        spreadsheetId="dummy_txn_sheet_id",
        range="Bank transactions",  # Uses sheet name from constants
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [["row1"], ["row2"]]},
    )


def test_append_transactions_no_data(
    mock_service_account_credentials, mock_google_services
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    gds.append_transactions_to_log("bank", [])
    mock_sheets_service.spreadsheets.return_value.values.return_value.append.assert_not_called()


# --- Tests for clear_transaction_log_range ---
def test_clear_transaction_log_range_success(
    mock_service_account_credentials, mock_google_services, mocker
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    mock_execute = MagicMock(return_value={"clearedRange": "Bank transactions!B3:H"})
    mock_sheets_service.spreadsheets.return_value.values.return_value.clear.return_value.execute = (
        mock_execute
    )
    mocker.patch("src.google_data_source.TRANSACTIONS_SHEET_ID", "dummy_txn_sheet_id")
    mocker.patch(
        "src.google_data_source.BANK_TRANSACTIONS_FULL_RANGE", "Bank transactions!B2:H"
    )  # Match constant

    gds.clear_transaction_log_range("bank")
    mock_sheets_service.spreadsheets.return_value.values.return_value.clear.assert_called_once_with(
        spreadsheetId="dummy_txn_sheet_id",
        range="Bank transactions!B3:H",  # Based on logic in clear_transaction_log_range
        body={},
    )


# --- Tests for write_transactions_to_log ---
def test_write_transactions_to_log_success(
    mock_service_account_credentials, mock_google_services, mocker
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    mock_execute = MagicMock(
        return_value={"updatedCells": 10, "updatedRange": "Bank transactions!B3:D5"}
    )
    mock_sheets_service.spreadsheets.return_value.values.return_value.update.return_value.execute = (
        mock_execute
    )
    mocker.patch("src.google_data_source.TRANSACTIONS_SHEET_ID", "dummy_txn_sheet_id")

    test_data = [["r1c1", "r1c2"], ["r2c1", "r2c2"]]
    gds.write_transactions_to_log("bank", test_data)
    mock_sheets_service.spreadsheets.return_value.values.return_value.update.assert_called_once_with(
        spreadsheetId="dummy_txn_sheet_id",
        range="Bank transactions!B3",  # Based on logic in write_transactions_to_log
        valueInputOption="USER_ENTERED",
        body={"values": test_data},
    )


def test_write_transactions_no_data(
    mock_service_account_credentials, mock_google_services
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    gds.write_transactions_to_log("bank", [])
    mock_sheets_service.spreadsheets.return_value.values.return_value.update.assert_not_called()


def test_write_transactions_then_clears_trailing(
    mock_service_account_credentials, mock_google_services, mocker
):
    """Write must happen first, then a trailing clear starting just below the
    written block (never a pre-clear that could empty the sheet)."""
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    values = mock_sheets_service.spreadsheets.return_value.values.return_value
    values.update.return_value.execute = MagicMock(
        return_value={"updatedCells": 14, "updatedRange": "Bank transactions!B3:H4"}
    )
    values.clear.return_value.execute = MagicMock(
        return_value={"clearedRange": "Bank transactions!B5:H"}
    )
    mocker.patch("src.google_data_source.TRANSACTIONS_SHEET_ID", "dummy")
    mocker.patch(
        "src.google_data_source.BANK_TRANSACTIONS_FULL_RANGE", "Bank transactions!B2:H"
    )

    gds.write_transactions_to_log("bank", [["a"] * 7, ["b"] * 7])

    # 2 rows written from B3 -> trailing clear starts at row 5.
    values.clear.assert_called_once_with(
        spreadsheetId="dummy", range="Bank transactions!B5:H", body={}
    )
    # update called before clear
    method_order = [c[0] for c in values.method_calls if c[0] in ("update", "clear")]
    assert method_order[0] == "update" and "clear" in method_order


# --- Tests for write_review_rows ---
def test_write_review_rows_appends_below_existing_rows(
    mock_service_account_credentials, mock_google_services
):
    """Flagged rows land in column A, under the last used row.

    The old append(range="A:Z") let Sheets re-anchor the table on each call, so
    successive batches stepped five columns to the right instead of stacking.
    """
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    values = mock_sheets_service.spreadsheets.return_value.values.return_value
    values.get.return_value.execute.return_value = {"values": [["a"], ["b"], ["c"]]}
    values.update.return_value.execute.return_value = {"updatedCells": 14}

    gds.write_review_rows([["acc", "2026-07-06", "desc", "", "2", "why", "f.pdf"]])

    values.append.assert_not_called()
    kwargs = values.update.call_args.kwargs
    assert kwargs["range"].endswith("!A4")
    assert kwargs["body"]["values"][0][0] == "acc"


def test_write_review_rows_noop_on_empty(
    mock_service_account_credentials, mock_google_services
):
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    gds.write_review_rows([])
    mock_sheets_service.spreadsheets.return_value.values.return_value.update.assert_not_called()


def test_write_review_rows_idempotent_delete_dimension(
    mock_service_account_credentials, mock_google_services
):
    """write_review_rows deletes prior unacknowledged rows via deleteDimension batchUpdate,
    preserves acknowledged rows, and appends remaining incoming rows via RAW.
    """
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    sheets = mock_sheets_service.spreadsheets.return_value
    values = sheets.values.return_value

    sheets.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Review", "sheetId": 999}}]
    }
    sheets.batchUpdate.return_value.execute.return_value = {}

    existing = [
        ["acc-other", "2026-07-01", "other txn", "10.00", "", "why1", "other.pdf", ""],
        [
            "acc-target",
            "2026-07-05",
            "old unack",
            "50.00",
            "",
            "why2",
            "target.pdf",
            "",
        ],
        [
            "acc-target",
            "2026-07-06",
            "old acked",
            "60.00",
            "",
            "why3",
            "target.pdf",
            "ack",
        ],
    ]
    values.get.return_value.execute.return_value = {"values": existing}
    values.update.return_value.execute.return_value = {"updatedCells": 7}

    new_rows = [
        ["acc-target", "2026-07-05", "new unack", "50.00", "", "why-new", "target.pdf"],
        [
            "acc-target",
            "2026-07-06",
            "old acked",
            "60.00",
            "",
            "why3",
            "target.pdf",
        ],  # Matches acked row
    ]
    gds.write_review_rows(new_rows)

    # Row 1 (unacknowledged target) was deleted via batchUpdate deleteDimension
    sheets.batchUpdate.assert_called_once()
    reqs = sheets.batchUpdate.call_args.kwargs["body"]["requests"]
    assert len(reqs) == 1
    del_req = reqs[0]["deleteDimension"]["range"]
    assert del_req["sheetId"] == 999
    assert del_req["startIndex"] == 1
    assert del_req["endIndex"] == 2

    # Acknowledged row 07-06 was filtered out of incoming; only 07-05 was appended at row 3 (len 3 - 1 del + 1)
    values.update.assert_called_once()
    update_kwargs = values.update.call_args.kwargs
    assert update_kwargs["range"].endswith("!A3")
    assert update_kwargs["valueInputOption"] == "RAW"
    appended = update_kwargs["body"]["values"]
    assert len(appended) == 1
    assert appended[0][2] == "new unack"


def test_clear_review_rows_deletes_matching_rows_atomically(
    mock_service_account_credentials, mock_google_services
):
    """clear_review_rows deletes matching rows atomically via deleteDimension batchUpdate."""
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    sheets = mock_sheets_service.spreadsheets.return_value
    values = sheets.values.return_value

    sheets.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Review", "sheetId": 888}}]
    }
    sheets.batchUpdate.return_value.execute.return_value = {}

    existing = [
        ["acc-other", "2026-07-01", "other txn", "10.00", "", "why1", "other.pdf"],
        ["acc-target", "2026-07-05", "flagged 1", "50.00", "", "why2", "target.pdf"],
        ["acc-target", "2026-07-06", "flagged 2", "60.00", "", "why3", "target.pdf"],
    ]
    values.get.return_value.execute.return_value = {"values": existing}

    gds.clear_review_rows("acc-target", "target.pdf")

    sheets.batchUpdate.assert_called_once()
    reqs = sheets.batchUpdate.call_args.kwargs["body"]["requests"]
    assert len(reqs) == 1
    del_range = reqs[0]["deleteDimension"]["range"]
    assert del_range["sheetId"] == 888
    # Merged contiguous rows 1 and 2 -> range startIndex=1, endIndex=3
    assert del_range["startIndex"] == 1
    assert del_range["endIndex"] == 3


def test_clear_review_rows_no_matching_makes_zero_writes(
    mock_service_account_credentials, mock_google_services
):
    """When no matching rows exist, clear_review_rows makes zero update or delete calls."""
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    sheets = mock_sheets_service.spreadsheets.return_value
    values = sheets.values.return_value

    existing = [
        ["acc-other", "2026-07-01", "other txn", "10.00", "", "why1", "other.pdf"]
    ]
    values.get.return_value.execute.return_value = {"values": existing}

    gds.clear_review_rows("acc-target", "clean.pdf")

    sheets.batchUpdate.assert_not_called()
    values.update.assert_not_called()
    values.clear.assert_not_called()


def test_get_unresolved_review_counts_filters_headers_and_ack_tokens(
    mock_service_account_credentials, mock_google_services
):
    """get_unresolved_review_counts counts unresolved rows per file, ignoring headers and acked rows."""
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    values = mock_sheets_service.spreadsheets.return_value.values.return_value

    existing = [
        [
            "Account",
            "Date",
            "Description",
            "Debit",
            "Credit",
            "Reason",
            "File",
            "Status",
        ],
        ["acc1", "2026-07-01", "txn1", "10.00", "", "why1", "file1.pdf", ""],
        ["acc1", "2026-07-02", "txn2", "20.00", "", "why2", "file1.pdf", "ack"],
        ["acc2", "2026-07-03", "txn3", "30.00", "", "why3", "file2.pdf", "DONE"],
        ["acc2", "2026-07-04", "txn4", "40.00", "", "why4", "file2.pdf", "pending"],
    ]
    values.get.return_value.execute.return_value = {"values": existing}

    counts = gds.get_unresolved_review_counts()
    assert counts == {"file1.pdf": 1, "file2.pdf": 1}

    values.update.assert_not_called()
    values.clear.assert_not_called()


def test_get_unresolved_review_counts_skips_soft_statement_rows(
    mock_service_account_credentials, mock_google_services
):
    """get_unresolved_review_counts skips soft STATEMENT rows and empty rows so health alert is not triggered."""
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    values = mock_sheets_service.spreadsheets.return_value.values.return_value

    existing = [
        [
            "Account",
            "Date",
            "Description",
            "Debit",
            "Credit",
            "Reason",
            "File",
            "Status",
        ],
        ["cc-test", "", "STATEMENT", "", "", "End date mismatch", "soft.pdf", ""],
        ["cc-test", "", "Informational note", "", "", "No amount", "info.pdf", ""],
        [
            "cc-test",
            "2026-07-15",
            "Hard Flagged",
            "100.00",
            "",
            "Quarantined",
            "hard.pdf",
            "",
        ],
        [
            "cc-test",
            "2026-07-16",
            "Acked Row",
            "200.00",
            "",
            "Quarantined",
            "hard.pdf",
            "acknowledged",
        ],
    ]
    values.get.return_value.execute.return_value = {"values": existing}

    counts = gds.get_unresolved_review_counts()
    assert counts == {"hard.pdf": 1}


def test_write_review_rows_matches_ack_with_formatted_amounts_and_dates(
    mock_service_account_credentials, mock_google_services
):
    """write_review_rows matches acknowledged rows even when date format differs
    or amount has no trailing decimals."""
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    sheets = mock_sheets_service.spreadsheets.return_value
    values = sheets.values.return_value

    sheets.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Review", "sheetId": 777}}]
    }
    sheets.batchUpdate.return_value.execute.return_value = {}

    existing = [
        [
            "cc-test",
            "15/07/2026",
            "Merchant A",
            "50",
            "",
            "Reason",
            "stmt.pdf",
            "acknowledged",
        ],
        [
            "cc-test",
            "2026-07-16",
            "Merchant B",
            "1,000.00",
            "",
            "Reason",
            "stmt.pdf",
            "done",
        ],
    ]
    values.get.return_value.execute.return_value = {"values": existing}

    # Incoming rows formatted with ISO date, 2-decimal amounts, and drifted LLM descriptions!
    new_rows = [
        [
            "cc-test",
            "2026-07-15",
            "Merchant A (LLM reworded)",
            "50.00",
            "",
            "Reason",
            "stmt.pdf",
        ],
        [
            "cc-test",
            "2026-07-16",
            "Merchant B (LLM reworded)",
            "1000.00",
            "",
            "Reason",
            "stmt.pdf",
        ],
    ]
    gds.write_review_rows(new_rows)

    # Both incoming rows match the existing acked rows; nothing new appended and nothing deleted
    values.update.assert_not_called()
    sheets.batchUpdate.assert_not_called()


def test_write_review_rows_fallback_when_sheet_id_none(
    mock_service_account_credentials, mock_google_services
):
    """When _get_sheet_id returns None, write_review_rows uses rewrite-and-clear
    fallback without stacking duplicates."""
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    sheets = mock_sheets_service.spreadsheets.return_value
    values = sheets.values.return_value

    # Metadata returns empty sheets so _get_sheet_id returns None
    sheets.get.return_value.execute.return_value = {"sheets": []}

    existing = [
        ["acc-other", "2026-07-01", "other txn", "10.00", "", "why1", "other.pdf", ""],
        [
            "acc-target",
            "2026-07-05",
            "old unack",
            "50.00",
            "",
            "why2",
            "target.pdf",
            "",
        ],
    ]
    values.get.return_value.execute.return_value = {"values": existing}
    values.update.return_value.execute.return_value = {"updatedCells": 7}
    values.clear.return_value.execute.return_value = {}

    new_rows = [
        ["acc-target", "2026-07-05", "new unack", "50.00", "", "why-new", "target.pdf"],
    ]
    gds.write_review_rows(new_rows)

    # Rewrote row 1 with [kept acc-other] + [new unack acc-target] at A1
    values.update.assert_called_once()
    assert values.update.call_args.kwargs["range"].endswith("!A1")
    written = values.update.call_args.kwargs["body"]["values"]
    assert len(written) == 2
    assert written[0][0] == "acc-other"
    assert written[1][2] == "new unack"
    sheets.batchUpdate.assert_not_called()


def test_clear_review_rows_preserves_acknowledged_rows(
    mock_service_account_credentials, mock_google_services
):
    """clear_review_rows deletes only unacknowledged rows; acknowledged rows in col H are kept."""
    gds = GoogleDataSource()
    _, mock_sheets_service = mock_google_services
    sheets = mock_sheets_service.spreadsheets.return_value
    values = sheets.values.return_value

    sheets.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Review", "sheetId": 888}}]
    }
    sheets.batchUpdate.return_value.execute.return_value = {}

    existing = [
        [
            "acc-target",
            "2026-07-05",
            "flagged unack",
            "50.00",
            "",
            "why1",
            "target.pdf",
            "",
        ],
        [
            "acc-target",
            "2026-07-06",
            "flagged acked",
            "60.00",
            "",
            "why2",
            "target.pdf",
            "resolved",
        ],
    ]
    values.get.return_value.execute.return_value = {"values": existing}

    gds.clear_review_rows("acc-target", "target.pdf")

    sheets.batchUpdate.assert_called_once()
    reqs = sheets.batchUpdate.call_args.kwargs["body"]["requests"]
    assert len(reqs) == 1
    del_range = reqs[0]["deleteDimension"]["range"]
    assert del_range["startIndex"] == 0
    assert del_range["endIndex"] == 1  # Only row 0 was deleted!


def test_write_review_rows_distinguishes_statement_flags_by_reason(
    mock_service_account_credentials, mock_google_services
):
    """Acknowledging one STATEMENT-level soft flag (empty date/amount) does not suppress
    a different STATEMENT-level reason for the same file."""
    _, mock_sheets_service = mock_google_services
    gds = GoogleDataSource()
    sheets = mock_sheets_service.spreadsheets.return_value
    sheets.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Review", "sheetId": 77}}]
    }
    values = sheets.values.return_value
    values.get.return_value.execute.return_value = {
        "values": [
            [
                "acc-1",
                "",
                "STATEMENT",
                "",
                "",
                "reconcile off by 1.50",
                "f1.pdf",
                "ack",
            ]
        ]
    }

    values.update.return_value.execute.return_value = {"updatedCells": 7}
    gds.write_review_rows(
        [
            ["acc-1", "", "STATEMENT", "", "", "reconcile off by 1.50", "f1.pdf"],
            ["acc-1", "", "STATEMENT", "", "", "missing closing balance", "f1.pdf"],
        ]
    )
    values.update.assert_called_once()
    written = values.update.call_args.kwargs["body"]["values"]
    assert len(written) == 1
    assert written[0][5] == "missing closing balance"


# --- Tests for update_transaction_categories ---
def test_update_transaction_categories_writes_column_f(
    mock_service_account_credentials, mock_google_services
):
    from src.google_data_source import GoogleDataSource

    _, mock_sheets_service = mock_google_services
    gds = GoogleDataSource()
    batch = (
        mock_sheets_service.spreadsheets.return_value.values.return_value.batchUpdate
    )
    batch.return_value.execute.return_value = {"totalUpdatedCells": 2}

    updated = gds.update_transaction_categories(
        "cc", [(0, "Expense:Fuel"), (4, "Expense:Dining")]
    )

    assert updated == 2
    body = batch.call_args.kwargs["body"]
    # Data row 0 is sheet row 3 (B2 is the header); Category is column F.
    assert [d["range"].split("!")[1] for d in body["data"]] == ["F3", "F7"]
    assert [d["values"] for d in body["data"]] == [
        [["Expense:Fuel"]],
        [["Expense:Dining"]],
    ]


def test_update_transaction_categories_no_updates_makes_no_call(
    mock_service_account_credentials, mock_google_services
):
    from src.google_data_source import GoogleDataSource

    _, mock_sheets_service = mock_google_services
    gds = GoogleDataSource()

    assert gds.update_transaction_categories("cc", []) == 0
    mock_sheets_service.spreadsheets.return_value.values.return_value.batchUpdate.assert_not_called()
