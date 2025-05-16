# gajana/google_data_source.py
from __future__ import annotations

import logging
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError as GoogleHttpError
from oauth2client.service_account import ServiceAccountCredentials

from constants import BANK_TRANSACTIONS_FULL_RANGE
from constants import BANK_TRANSACTIONS_SHEET_NAME
from constants import CC_TRANSACTIONS_FULL_RANGE
from constants import CC_TRANSACTIONS_SHEET_NAME
from constants import CSV_FOLDER
from constants import SCOPES
from constants import SERVICE_ACCOUNT_KEY_FILE
from constants import TRANSACTIONS_SHEET_ID
from interfaces import DataSourceInterface
from utils import log_and_exit

logger = logging.getLogger(__name__)


class GoogleDataSource(DataSourceInterface):
    """
    Google Sheets and Drive implementation of the DataSourceInterface.
    Handles all direct communication with Google APIs.
    """

    def __init__(self, max_retries: int = 3, initial_backoff: int = 5) -> None:
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.creds = self._get_credential()
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

    def list_statement_file_details(self) -> List[Dict[str, str]]:
        files_details: List[Dict[str, str]] = []
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
                        {"id": file_item["id"], "name": file_item["name"]}
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

    def get_first_sheet_name_from_file(self, file_id: str) -> Optional[str]:
        """Gets the name (title) of the first visible sheet in a spreadsheet file."""
        try:
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
        except GoogleHttpError as e:
            logger.error(
                f"API error getting sheet metadata for file ID {file_id}: "
                f"{e.resp.status} - {e.error_details if hasattr(e, 'error_details') else e}",
                exc_info=True,
            )
            return None
        except Exception as e:
            log_and_exit(
                logger,
                "Unexpected error getting sheet metadata for file ID {file_id}: {e}",
                e,
            )
        return None

    def get_sheet_data(
        self, source_id: str, sheet_name: Optional[str], range_spec: str
    ) -> List[List[Any]]:
        """Fetches raw data from a specific sheet or source using retry logic."""
        # If sheet_name is part of range_spec, use it directly. Otherwise, construct.
        full_range = f"'{sheet_name}'!{range_spec}" if sheet_name else range_spec

        for attempt in range(self.max_retries):
            try:
                logger.debug(
                    f"Getting sheet data: ID={source_id}, Range='{full_range}', Attempt={attempt+1}"
                )
                result = (
                    self.sheets_service.spreadsheets()
                    .values()
                    .get(spreadsheetId=source_id, range=full_range)
                    .execute()
                )
                values = result.get("values", [])
                logger.debug(f"Successfully got sheet data. Rows: {len(values)}")
                return values
            except GoogleHttpError as e:
                if e.resp.status in [429, 500, 503] and attempt < self.max_retries - 1:
                    wait_time = self.initial_backoff * (2**attempt)
                    logger.warning(
                        f"Sheets API error {e.resp.status}. Retrying get data in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    log_and_exit(
                        logger,
                        f"Non-retryable or final Sheets API error getting data: {e}",
                        e,
                    )
            except Exception as e:
                log_and_exit(logger, f"Unexpected error getting sheet data: {e}", e)
        return []  # Should not be reached

    def get_transaction_log_data(self, log_type: str) -> List[List[Any]]:
        range_to_fetch = (
            BANK_TRANSACTIONS_FULL_RANGE
            if log_type == "bank"
            else CC_TRANSACTIONS_FULL_RANGE
        )
        logger.info(
            f"Getting {log_type} transaction log data from Sheet ID: {TRANSACTIONS_SHEET_ID}, Range: {range_to_fetch}"
        )
        return self.get_sheet_data(
            TRANSACTIONS_SHEET_ID, None, range_to_fetch
        )  # sheet_name is in range_to_fetch

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

        for attempt in range(self.max_retries):
            try:
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
                logger.info(
                    f"Successfully appended {updated_cells} cells to {log_type} log."
                )
                return
            except GoogleHttpError as e:
                if e.resp.status in [429, 500, 503] and attempt < self.max_retries - 1:
                    wait_time = self.initial_backoff * (2**attempt)
                    logger.warning(
                        f"Sheets API error {e.resp.status}. Retrying append to log in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    log_and_exit(
                        logger,
                        f"Non-retryable or final Sheets API error appending to {log_type} log: {e}",
                        e,
                    )
            except Exception as e:
                log_and_exit(
                    logger, f"Unexpected error appending to {log_type} log: {e}", e
                )
        log_and_exit(
            logger,
            f"Failed to append to {log_type} log after {self.max_retries} attempts.",
        )

    def clear_transaction_log_range(self, log_type: str) -> None:
        range_to_clear = (
            BANK_TRANSACTIONS_FULL_RANGE
            if log_type == "bank"
            else CC_TRANSACTIONS_FULL_RANGE
        )
        # Adjust range for clearing if headers are not to be cleared.
        # Assuming B3:H means data starts at row 3.
        data_clear_range = (
            f"{range_to_clear.split('!')[0]}!B3:H"  # Example: "Bank transactions!B3:H"
        )

        logger.info(
            f"Clearing {log_type} transaction log data in Sheet ID: {TRANSACTIONS_SHEET_ID}, Range: {data_clear_range}"
        )
        for attempt in range(self.max_retries):
            try:
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
            except GoogleHttpError as e:
                if e.resp.status in [429, 500, 503] and attempt < self.max_retries - 1:
                    wait_time = self.initial_backoff * (2**attempt)
                    logger.warning(
                        f"Sheets API error {e.resp.status}. Retrying clear log in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    log_and_exit(
                        logger,
                        f"Non-retryable or final Sheets API error clearing {log_type} log: {e}",
                        e,
                    )
            except Exception as e:
                log_and_exit(
                    logger, f"Unexpected error clearing {log_type} log: {e}", e
                )
        log_and_exit(
            logger, f"Failed to clear {log_type} log after {self.max_retries} attempts."
        )

    def write_transactions_to_log(
        self, log_type: str, data_values: List[List[Any]]
    ) -> None:
        if not data_values:
            logger.info(f"No data to write for {log_type} log.")
            return

        # Data should be written starting from row 3 (B3)
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

        for attempt in range(self.max_retries):
            try:
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
            except GoogleHttpError as e:
                if e.resp.status in [429, 500, 503] and attempt < self.max_retries - 1:
                    wait_time = self.initial_backoff * (2**attempt)
                    logger.warning(
                        f"Sheets API error {e.resp.status}. Retrying write to log in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    log_and_exit(
                        logger,
                        f"Non-retryable or final Sheets API error writing to {log_type} log: {e}",
                        e,
                    )
            except Exception as e:
                log_and_exit(
                    logger, f"Unexpected error writing to {log_type} log: {e}", e
                )
        log_and_exit(
            logger,
            f"Failed to write to {log_type} log after {self.max_retries} attempts.",
        )
