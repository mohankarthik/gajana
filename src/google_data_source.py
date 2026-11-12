"""Data source specific to GSuite."""

from __future__ import annotations

from functools import wraps
from collections import Counter, defaultdict
import datetime
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

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
from src.settings import CASH_TRANSACTIONS_SHEET_NAME, REVIEW_TRANSACTIONS_SHEET_NAME
from src.interfaces import DataSourceFile, DataSourceInterface
from src.utils import log_and_exit

logger = logging.getLogger(__name__)

_ACK_TOKENS = {"ack", "acknowledged", "resolved", "done", "ignore", "ignored"}


def _normalize_date_str(date_str: str) -> str:
    s = date_str.strip()
    if not s:
        return ""
    try:
        return datetime.date.fromisoformat(s).isoformat()
    except ValueError:
        pass
    for fmt in (
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%m/%d/%Y",
        "%d %b %Y",
        "%d-%b-%Y",
        "%d %b %y",
        "%d-%b-%y",
    ):
        try:
            return datetime.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    return s


def _normalize_amount_str(val: Any) -> str:
    s = str(val).strip().replace(",", "")
    if not s:
        return ""
    try:
        return f"{float(s):.2f}"
    except ValueError:
        return s


def _review_row_key(r: Sequence[Any]) -> tuple[str, str, str, str, str, str]:
    account = str(r[0]).strip() if len(r) > 0 else ""
    date_str = _normalize_date_str(str(r[1])) if len(r) > 1 else ""
    debit = _normalize_amount_str(r[3]) if len(r) > 3 else ""
    credit = _normalize_amount_str(r[4]) if len(r) > 4 else ""
    reason_disc = (
        re.sub(r"[-+]?\d[\d,.]*", "#", str(r[5]).strip().lower())
        if (not date_str and not debit and not credit and len(r) > 5)
        else ""
    )
    file_name = str(r[6]).strip() if len(r) > 6 else ""
    return (account, date_str, debit, credit, reason_disc, file_name)


def retry_on_gcp_error(max_retries=3, initial_backoff=5):
    """Decorator that retries the method on GCP error."""

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

    def __init__(self) -> None:
        """Constructor."""
        self.creds = self._get_credential()
        self.logger = logger
        self.drive_service = self._get_drive_service()
        self.sheets_service = self._get_sheets_service()
        self._sheet_id_cache: dict[str, int] = {}

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
            List[DataSourceFile]: List of statement files.
        """
        files_details: List[DataSourceFile] = []
        page_token = None
        logger.info(f"Listing Google Sheets from Drive folder ID: {CSV_FOLDER}")
        query = (
            f"parents in '{CSV_FOLDER}' and "
            "(mimeType='application/vnd.google-apps.spreadsheet' or "
            "mimeType='application/pdf') and trashed=false"
        )
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
    def update_transaction_categories(
        self, log_type: str, updates: List[Tuple[int, str]]
    ) -> int:
        """Write only column F (Category) for the given rows, in one batch."""
        if not updates:
            logger.info(f"No category updates for {log_type} log.")
            return 0
        sheet_name = (
            BANK_TRANSACTIONS_SHEET_NAME
            if log_type == "bank"
            else CC_TRANSACTIONS_SHEET_NAME
        )
        # get_transaction_log_data starts at B2 (the header), so data row 0 is
        # sheet row 3. Category is column F.
        data = [
            {
                "range": f"{sheet_name}!F{row_index + 3}",
                "values": [[category]],
            }
            for row_index, category in updates
        ]
        logger.info(
            f"Updating {len(data)} category cells in {log_type} log in Sheet ID: "
            f"{TRANSACTIONS_SHEET_ID}, Sheet: {sheet_name}"
        )
        result = (
            self.sheets_service.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=TRANSACTIONS_SHEET_ID,
                body={"valueInputOption": "RAW", "data": data},
            )
            .execute()
        )
        updated = int(result.get("totalUpdatedCells", 0))
        logger.info(f"Successfully updated {updated} category cells in {log_type} log.")
        return updated

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
        return None

    @retry_on_gcp_error()
    def download_file(self, file_id: str) -> bytes:
        """Downloads a file (e.g., PDF) from Google Drive."""
        from googleapiclient.http import MediaIoBaseDownload
        import io

        logger.info(f"Downloading file ID: {file_id}")
        request = self.drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        logger.info(f"Successfully downloaded file ID: {file_id}")
        return fh.getvalue()

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

    # --- Cash ledger (shared with plugins/telegram_bot) ----------------------
    @retry_on_gcp_error()
    def get_cash_log_data(self) -> List[List[Any]]:
        """Raw data rows (B3:G) from the Cash Transactions tab:
        Date | Description | Debit | Credit | Category | Remarks."""
        return self.get_sheet_data(
            TRANSACTIONS_SHEET_ID, CASH_TRANSACTIONS_SHEET_NAME, "B3:G"
        )

    @retry_on_gcp_error()
    def append_cash_rows(self, data_values: List[List[Any]]) -> None:
        """Appends rows to the Cash Transactions tab (columns B:G)."""
        if not data_values:
            return
        result = (
            self.sheets_service.spreadsheets()
            .values()
            .append(
                spreadsheetId=TRANSACTIONS_SHEET_ID,
                range=f"'{CASH_TRANSACTIONS_SHEET_NAME}'!B:G",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": data_values},
            )
            .execute()
        )
        updated = result.get("updates", {}).get("updatedCells", 0)
        logger.info(f"Appended {updated} cells ({len(data_values)} rows) to cash log.")

    def _get_sheet_id(self, sheet_name: str) -> Optional[int]:
        """Gets numeric sheetId for a sheet tab title, caching across calls."""
        if not hasattr(self, "_sheet_id_cache"):
            self._sheet_id_cache = {}
        if sheet_name in self._sheet_id_cache:
            return self._sheet_id_cache[sheet_name]
        try:
            metadata = (
                self.sheets_service.spreadsheets()
                .get(
                    spreadsheetId=TRANSACTIONS_SHEET_ID,
                    fields="sheets.properties(sheetId,title)",
                )
                .execute()
            )
            for s in metadata.get("sheets", []):
                props = s.get("properties", {})
                title = props.get("title")
                sid = props.get("sheetId")
                if title and sid is not None:
                    self._sheet_id_cache[title] = int(sid)
            return self._sheet_id_cache.get(sheet_name)
        except Exception as e:
            logger.warning(f"Could not retrieve sheetId for '{sheet_name}': {e}")
            return None

    def _delete_sheet_rows(
        self, sheet_id: int, zero_based_row_indices: List[int]
    ) -> None:
        """Deletes rows in a sheet atomically via deleteDimension batchUpdate,
        merging contiguous indices and deleting descending to preserve row offsets."""
        if not zero_based_row_indices:
            return
        sorted_indices = sorted(set(zero_based_row_indices))
        ranges: list[tuple[int, int]] = []
        for idx in sorted_indices:
            if ranges and idx == ranges[-1][1]:
                ranges[-1] = (ranges[-1][0], idx + 1)
            else:
                ranges.append((idx, idx + 1))
        # Sort descending by start index so deletions never shift earlier row offsets
        ranges.sort(key=lambda r: r[0], reverse=True)
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_idx,
                        "endIndex": end_idx,
                    }
                }
            }
            for start_idx, end_idx in ranges
        ]
        self.sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=TRANSACTIONS_SHEET_ID,
            body={"requests": requests},
        ).execute()

    @retry_on_gcp_error()
    def write_review_rows(self, data_values: List[List[Any]]) -> None:
        """Appends validation-flagged rows to the Review tab for manual triage.

        Idempotent across daily retries:
        - Keeps acknowledged rows in col H (e.g. ack, resolved, done, ignore) untouched.
        - Deletes unacknowledged prior rows for the incoming (account, file_name) via deleteDimension.
        - Appends incoming unacknowledged rows at the tail via valueInputOption='RAW'.
        """
        if not data_values:
            return
        existing: List[List[Any]] = (
            self.sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=TRANSACTIONS_SHEET_ID,
                range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A:H",
            )
            .execute()
            .get("values", [])
        )

        incoming_keys = {
            (str(row[0]).strip(), str(row[6]).strip())
            for row in data_values
            if len(row) >= 7
        }

        # Count acknowledged rows matching incoming keys so multiplicity is preserved
        ack_counts = Counter(
            _review_row_key(r)
            for r in existing
            if len(r) >= 7
            and (str(r[0]).strip(), str(r[6]).strip()) in incoming_keys
            and (len(r) >= 8 and str(r[7]).strip().lower() in _ACK_TOKENS)
        )

        remaining_values: List[List[Any]] = []
        for v in data_values:
            if len(v) >= 7:
                key = _review_row_key(v)
                if ack_counts[key] > 0:
                    ack_counts[key] -= 1
                    continue
            remaining_values.append(v)

        # Find 0-based indices of UNACKNOWLEDGED rows matching incoming_keys to delete
        unack_indices = [
            i
            for i, r in enumerate(existing)
            if len(r) >= 7
            and (str(r[0]).strip(), str(r[6]).strip()) in incoming_keys
            and not (len(r) >= 8 and str(r[7]).strip().lower() in _ACK_TOKENS)
        ]

        deleted_count = 0
        if unack_indices:
            if len(unack_indices) == len(existing):
                self.sheets_service.spreadsheets().values().clear(
                    spreadsheetId=TRANSACTIONS_SHEET_ID,
                    range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A:Z",
                ).execute()
                deleted_count = len(existing)
                logger.info(
                    f"Cleared all {deleted_count} prior unacknowledged review row(s)."
                )
            else:
                sheet_id = self._get_sheet_id(REVIEW_TRANSACTIONS_SHEET_NAME)
                if sheet_id is not None:
                    self._delete_sheet_rows(sheet_id, unack_indices)
                    deleted_count = len(unack_indices)
                    logger.info(
                        f"Deleted {deleted_count} prior unacknowledged review row(s) via deleteDimension."
                    )
                else:
                    kept = [r for i, r in enumerate(existing) if i not in unack_indices]
                    all_to_write = kept + remaining_values
                    if all_to_write:
                        self.sheets_service.spreadsheets().values().update(
                            spreadsheetId=TRANSACTIONS_SHEET_ID,
                            range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A1",
                            valueInputOption="RAW",
                            body={"values": all_to_write},
                        ).execute()
                    if len(all_to_write) < len(existing):
                        self.sheets_service.spreadsheets().values().clear(
                            spreadsheetId=TRANSACTIONS_SHEET_ID,
                            range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A{len(all_to_write) + 1}:H",
                        ).execute()
                    logger.info(
                        f"Rewrote Review tab without {len(unack_indices)} unack row(s) "
                        f"and appended {len(remaining_values)} new row(s)."
                    )
                    return

        if remaining_values:
            start = (len(existing) - deleted_count) + 1
            result = (
                self.sheets_service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=TRANSACTIONS_SHEET_ID,
                    range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A{start}",
                    valueInputOption="RAW",
                    body={"values": remaining_values},
                )
                .execute()
            )
            updated = result.get("updatedCells", 0)
            logger.info(
                f"Wrote {updated} cells ({len(remaining_values)} rows) to Review tab from row {start}."
            )

    def clear_review_rows(self, account: str, file_name: str) -> None:
        """Removes review rows for (account, file_name) when a statement later parses cleanly.
        Uses atomic deleteDimension; makes zero write calls if no matching rows exist.
        """
        try:
            existing: List[List[Any]] = (
                self.sheets_service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=TRANSACTIONS_SHEET_ID,
                    range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A:H",
                )
                .execute()
                .get("values", [])
            )
        except Exception as e:
            logger.warning(
                f"Failed to read Review tab to clear rows for '{file_name}': {e}"
            )
            return

        indices = [
            i
            for i, r in enumerate(existing)
            if len(r) >= 7
            and str(r[0]).strip() == account.strip()
            and str(r[6]).strip() == file_name.strip()
            and not (len(r) >= 8 and str(r[7]).strip().lower() in _ACK_TOKENS)
        ]
        if not indices:
            return

        try:
            if len(indices) == len(existing):
                self.sheets_service.spreadsheets().values().clear(
                    spreadsheetId=TRANSACTIONS_SHEET_ID,
                    range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A:Z",
                ).execute()
                logger.info(
                    f"Cleared all {len(indices)} review row(s) for '{file_name}' ({account})."
                )
                return
            sheet_id = self._get_sheet_id(REVIEW_TRANSACTIONS_SHEET_NAME)
            if sheet_id is not None:
                self._delete_sheet_rows(sheet_id, indices)
                logger.info(
                    f"Deleted {len(indices)} review row(s) for '{file_name}' ({account}) via deleteDimension."
                )
            else:
                kept = [r for i, r in enumerate(existing) if i not in indices]
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=TRANSACTIONS_SHEET_ID,
                    range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A1",
                    valueInputOption="RAW",
                    body={"values": kept},
                ).execute()
                self.sheets_service.spreadsheets().values().clear(
                    spreadsheetId=TRANSACTIONS_SHEET_ID,
                    range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A{len(kept) + 1}:H",
                ).execute()
        except Exception as e:
            logger.warning(f"Failed to delete review rows for '{file_name}': {e}")

    def get_unresolved_review_counts(self) -> Optional[Dict[str, int]]:
        """Reads Review tab and returns counts of unacknowledged rows grouped by file_name.
        Returns None on read failure."""
        try:
            existing: List[List[Any]] = (
                self.sheets_service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=TRANSACTIONS_SHEET_ID,
                    range=f"'{REVIEW_TRANSACTIONS_SHEET_NAME}'!A:H",
                )
                .execute()
                .get("values", [])
            )
        except Exception as e:
            logger.warning(f"Failed to read Review tab for unresolved counts: {e}")
            return None

        counts: dict[str, int] = defaultdict(int)
        for r in existing:
            if not r or len(r) < 7:
                continue
            if (len(r) > 0 and str(r[0]).strip().lower() == "account") or (
                len(r) > 1 and str(r[1]).strip().lower() == "date"
            ):
                continue
            if str(r[2]).strip().upper() == "STATEMENT" and not str(r[1]).strip():
                continue
            debit = str(r[3]).strip() if len(r) > 3 else ""
            credit = str(r[4]).strip() if len(r) > 4 else ""
            if not str(r[1]).strip() and not debit and not credit:
                continue

            col_h = str(r[7]).strip().lower() if len(r) >= 8 else ""
            if col_h not in _ACK_TOKENS:
                file_name = str(r[6]).strip()
                if file_name:
                    counts[file_name] += 1
        return dict(counts)

    # --- Processed-statements cache (local state; avoids re-parsing/paying the
    # LLM for immutable statements the account watermark already covers) --------
    _PROCESSED_STATEMENTS_PATH = os.path.join(
        "data", "state", "processed_statements.json"
    )

    def get_processed_statements(self) -> Dict[str, Any]:
        path = self._PROCESSED_STATEMENTS_PATH
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not read processed-statements cache: {e}")
            return {}

    def save_processed_statements(self, cache: Dict[str, Any]) -> None:
        path = self._PROCESSED_STATEMENTS_PATH
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2, sort_keys=True)
        except IOError as e:
            logger.warning(f"Could not write processed-statements cache: {e}")

    @retry_on_gcp_error()
    def clear_transaction_log_range(self, log_type: str, start_row: int = 3) -> None:
        range_to_clear = (
            BANK_TRANSACTIONS_FULL_RANGE
            if log_type == "bank"
            else CC_TRANSACTIONS_FULL_RANGE
        )
        data_clear_range = f"{range_to_clear.split('!')[0]}!B{start_row}:H"

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

        # Write the new data FIRST (overwriting rows in place), THEN clear only
        # the stale rows below it. Never pre-clear: a failed write must not leave
        # the sheet empty (that is how a full-overwrite previously wiped a sheet).
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
        # Trailing cleanup: remove any old rows beyond the freshly written block.
        self.clear_transaction_log_range(log_type, start_row=3 + len(data_values))
        return
