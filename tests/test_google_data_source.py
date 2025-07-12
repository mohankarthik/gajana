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
