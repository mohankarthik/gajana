"""Data source specific to GSuite."""

from __future__ import annotations

from functools import wraps
import logging
import time
from typing import Any, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError as GoogleHttpError
from oauth2client.service_account import ServiceAccountCredentials

from src.constants import (
    BANK_TRANSACTIONS_FULL_RANGE,
    BANK_TRANSACTIONS_SHEET_NAME,
    CC_TRANSACTIONS_FULL_RANGE,
    CC_TRANSACTIONS_SHEET_NAME,
    CSV_FOLDER,
    SCOPES,
    SERVICE_ACCOUNT_KEY_FILE,
    TRANSACTIONS_SHEET_ID,
)
from src.interfaces import DataSourceFile, DataSourceInterface
from src.utils import log_and_exit

logger = logging.getLogger(__name__)


def retry_on_gcp_error(max_retries=3, initial_backoff=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self_instance = args[0]
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except GoogleHttpError as e:
                    if e.resp.status in [429, 500, 503] and attempt < max_retries - 1:
                        wait_time = initial_backoff * (2**attempt)
                        logging.warning(
                            f"API error in {func.__name__}. Retrying in {wait_time}s..."
                        )
                        time.sleep(wait_time)
                    else:
                        log_and_exit(
                            self_instance.logger,
                            f"Final API error in {func.__name__}: {e}",
                            e,
                        )
                except Exception as e:
                    log_and_exit(
                        self_instance.logger,
                        f"Unexpected error in {func.__name__}: {e}",
                        e,
                    )
            log_and_exit(
                self_instance.logger,
                f"Function {func.__name__} failed after all retries.",
            )

        return wrapper

    return decorator


class GoogleDataSource(DataSourceInterface):
    """Google Sheets and Drive implementation of the DataSourceInterface.

    Handles all direct communication with Google APIs.
    """

    def __init__(self, max_retries: int = 3, initial_backoff: int = 5) -> None:
        """Constructor.

        Args:
            max_retries (int, optional): Defaults to 3.
            initial_backoff (int, optional): Defaults to 5.
        """
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.creds = self._get_credential()
        self.logger = logger
        self.drive_service = self._get_drive_service()
        self.sheets_service = self._get_sheets_service()

    def _get_credential(self) -> ServiceAccountCredentials:
        logger.info(
            f"Authenticating using service account key: {SERVICE_ACCOUNT_KEY_FILE}"
        )
        try:
            credential = ServiceAccountCredentials.from_json_keyfile_name(
                SERVICE_ACCOUNT_KEY_FILE, SCOPES
            )
            assert credential is not None, "Failed to load credentials."
            logger.info("Authentication successful.")
            return credential
        except FileNotFoundError as e:
            log_and_exit(
                logger,
                f"Service account key file not found: {SERVICE_ACCOUNT_KEY_FILE}",
                e,
            )
        except Exception as e:
            log_and_exit(logger, f"Error during authentication: {e}", e)
        return None

    def _get_drive_service(self) -> Any:
        try:
            service = build(
                "drive", "v3", credentials=self.creds, cache_discovery=False
            )
            logger.info("Google Drive service client built successfully.")
            return service
        except Exception as e:
            log_and_exit(logger, f"Failed to build Google Drive service: {e}", e)
        return None

    def _get_sheets_service(self) -> Any:
        try:
            service = build(
                "sheets", "v4", credentials=self.creds, cache_discovery=False
            )
            logger.info("Google Sheets service client built successfully.")
            return service
        except Exception as e:
            log_and_exit(logger, f"Failed to build Google Sheets service: {e}", e)
        return None

    def list_statement_file_details(self) -> List[DataSourceFile]:
        """Lists all the statement files with details.

        Returns:
            List[DataSourceFile]: _description_
        """
        files_details: List[DataSourceFile] = []
        page_token = None
        logger.info(f"Listing Google Sheets from Drive folder ID: {CSV_FOLDER}")
        query = f"parents in '{CSV_FOLDER}' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        try:
            while True:
                response = (
                    self.drive_service.files()
                    .list(
                        q=query,
                        spaces="drive",
                        fields="nextPageToken, files(id, name)",
                        pageToken=page_token,
                    )
                    .execute()
                )
                for file_item in response.get("files", []):
                    files_details.append(
                        DataSourceFile(file_item["id"], file_item["name"])
                    )
                page_token = response.get("nextPageToken", None)
                if page_token is None:
                    break
            logger.info(
                f"Found {len(files_details)} Google Sheet statement files in Drive folder."
            )
        except GoogleHttpError as e:
            log_and_exit(logger, f"Google Drive API error listing files: {e}", e)
        except Exception as e:
            log_and_exit(
                logger, f"Unexpected error listing files from Google Drive: {e}", e
            )
        return files_details

    @retry_on_gcp_error()
    def get_first_sheet_name_from_file(self, file_id: str) -> Optional[str]:
        """Gets the name (title) of the first visible sheet in a spreadsheet file."""
        spreadsheet_metadata = (
            self.sheets_service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(title,hidden,sheetId))",
            )
            .execute()
        )
        sheets = spreadsheet_metadata.get("sheets", [])
        for sheet in sheets:
            properties = sheet.get("properties", {})
            if not properties.get("hidden", False):
                title = properties.get("title")
                if title:
                    logger.debug(
                        f"Found first visible sheet name: '{title}' for file ID: {file_id}"
                    )
                    return title
        logger.warning(f"No visible sheets found for file ID: {file_id}")
        return None

    @retry_on_gcp_error()
    def get_sheet_data(
        self, source_id: str, sheet_name: Optional[str], range_spec: str
    ) -> List[List[Any]]:
        """Fetches raw data from a specific sheet or source using retry logic."""
        full_range = f"'{sheet_name}'!{range_spec}" if sheet_name else range_spec

        logger.debug(f"Getting sheet data: ID={source_id}, Range='{full_range}'")
        result = (
            self.sheets_service.spreadsheets()
            .values()
            .get(spreadsheetId=source_id, range=full_range)
            .execute()
        )
        values = result.get("values", [])
        logger.debug(f"Successfully got sheet data. Rows: {len(values)}")
        return values

    def get_transaction_log_data(self, log_type: str) -> List[List[Any]]:
        range_to_fetch = (
            BANK_TRANSACTIONS_FULL_RANGE
            if log_type == "bank"
            else CC_TRANSACTIONS_FULL_RANGE
        )
        logger.info(
            f"Getting {log_type} transaction log data from Sheet ID: {TRANSACTIONS_SHEET_ID}, Range: {range_to_fetch}"
        )
        return self.get_sheet_data(TRANSACTIONS_SHEET_ID, None, range_to_fetch)

    @retry_on_gcp_error()
    def append_transactions_to_log(
        self, log_type: str, data_values: List[List[Any]]
    ) -> None:
        if not data_values:
            logger.info(f"No data to append for {log_type} log.")
            return
        sheet_name = (
            BANK_TRANSACTIONS_SHEET_NAME
            if log_type == "bank"
            else CC_TRANSACTIONS_SHEET_NAME
        )
        logger.info(
            f"Appending {len(data_values)} rows to {log_type} log in Sheet ID: {TRANSACTIONS_SHEET_ID}, "
            f"Sheet: {sheet_name}"
        )

        body = {"values": data_values}
        result = (
            self.sheets_service.spreadsheets()
            .values()
            .append(
                spreadsheetId=TRANSACTIONS_SHEET_ID,
                range=sheet_name,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )
        updated_cells = result.get("updates", {}).get("updatedCells", 0)
        logger.info(f"Successfully appended {updated_cells} cells to {log_type} log.")
        return

    def clear_transaction_log_range(self, log_type: str) -> None:
        range_to_clear = (
            BANK_TRANSACTIONS_FULL_RANGE
            if log_type == "bank"
            else CC_TRANSACTIONS_FULL_RANGE
        )
        data_clear_range = f"{range_to_clear.split('!')[0]}!B3:H"

        logger.info(
            f"Clearing {log_type} transaction log data in Sheet ID: {TRANSACTIONS_SHEET_ID}, Range: {data_clear_range}"
        )

        result = (
            self.sheets_service.spreadsheets()
            .values()
            .clear(
                spreadsheetId=TRANSACTIONS_SHEET_ID,
                range=data_clear_range,
                body={},
            )
            .execute()
        )
        logger.info(
            f"Successfully cleared range {result.get('clearedRange', data_clear_range)} for {log_type} log."
        )
        return

    @retry_on_gcp_error()
    def write_transactions_to_log(
        self, log_type: str, data_values: List[List[Any]]
    ) -> None:
        if not data_values:
            logger.info(f"No data to write for {log_type} log.")
            return

        sheet_name = (
            BANK_TRANSACTIONS_SHEET_NAME
            if log_type == "bank"
            else CC_TRANSACTIONS_SHEET_NAME
        )
        write_range = f"{sheet_name}!B3"
        logger.info(
            f"Writing {len(data_values)} rows to {log_type} log in Sheet ID: {TRANSACTIONS_SHEET_ID}, "
            f"Range: {write_range}"
        )

        body = {"values": data_values}
        result = (
            self.sheets_service.spreadsheets()
            .values()
            .update(
                spreadsheetId=TRANSACTIONS_SHEET_ID,
                range=write_range,
                valueInputOption="USER_ENTERED",
                body=body,
            )
            .execute()
        )
        updated_cells = result.get("updatedCells", 0)
        logger.info(
            f"Successfully wrote {updated_cells} cells to {log_type} log at "
            f"{result.get('updatedRange', write_range)}."
        )
        return
