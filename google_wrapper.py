# gajana/google_wrapper.py
from __future__ import annotations

import datetime
import logging
import time
from operator import itemgetter
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError as GoogleHttpError
from oauth2client.service_account import ServiceAccountCredentials

from constants import BANK_ACCOUNTS
from constants import BANK_TRANSACTIONS_FULL_RANGE
from constants import BANK_TRANSACTIONS_SHEET_NAME
from constants import CC_ACCOUNTS
from constants import CC_TRANSACTIONS_FULL_RANGE
from constants import CC_TRANSACTIONS_SHEET_NAME
from constants import CSV_FOLDER
from constants import DEFAULT_CATEGORY
from constants import INTERNAL_TXN_KEYS
from constants import PARSING_CONFIG
from constants import SCOPES
from constants import SERVICE_ACCOUNT_KEY_FILE
from constants import TRANSACTIONS_SHEET_ID

# Import constants

# Configure logger for this module
logger = logging.getLogger(__name__)


class GoogleWrapper:
    """
    Handles interactions with Google Drive and Google Sheets APIs
    for fetching statement files (as Sheets), reading/writing transaction data using pandas.
    """

    def __init__(self, max_retries: int = 3, initial_backoff: int = 5) -> None:
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.creds = self._get_credential()
        self.drive_service = self._get_drive_service()
        self.sheets_service = self._get_sheets_service()
        self.statement_files = self._get_statement_files()

    # --- Public Methods ---

    def get_old_transactions(self, account_type: str) -> list[dict]:
        """Fetches and parses previously processed transactions from the main sheet."""
        range_to_fetch = (
            BANK_TRANSACTIONS_FULL_RANGE
            if account_type == "bank"
            else CC_TRANSACTIONS_FULL_RANGE
        )
        sheet_id = TRANSACTIONS_SHEET_ID
        logger.info(
            f"Getting old {account_type} transactions from sheet: {sheet_id}, range: {range_to_fetch}"
        )

        values = self._get_sheet_data(sheet_id, range_to_fetch)
        if not values or len(values) <= 1:
            logger.warning(
                f"No old {account_type} transactions data found in the sheet range {range_to_fetch}."
            )
            return []

        try:
            header = values[0]
            if not all(col in header for col in ["Date", "Debit", "Credit", "Account"]):
                logger.warning(
                    f"Header in {account_type} sheet ({range_to_fetch}) seems incorrect: {header}. "
                    f"Attempting parse anyway."
                )

            df = pd.DataFrame(values[1:], columns=header)

            # --- Data Cleaning and Transformation ---
            df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce")

            if "Credit" in df.columns and "Debit" in df.columns:
                df["amount"] = df.apply(
                    lambda row: self._parse_amount(row["Credit"])
                    - self._parse_amount(row["Debit"]),
                    axis=1,
                )
            else:
                logger.error(
                    "Missing 'Credit' or 'Debit' column in old transactions sheet. Cannot calculate amount."
                )
                df["amount"] = 0.0

            rename_map = {
                "Date": "date",
                "Description": "description",
                "Category": "category",
                "Remarks": "remarks",
                "Account": "account",
            }
            df = df.rename(
                columns={k: v for k, v in rename_map.items() if k in df.columns}
            )

            # Select only required internal keys + date
            required_keys = [
                "date",
                "description",
                "amount",
                "category",
                "remarks",
                "account",
            ]
            # Add missing columns with default values if they don't exist after rename
            for key in required_keys:
                if key not in df.columns:
                    if key == "amount":
                        df[key] = 0.0
                    elif key == "date":
                        df[key] = pd.NaT
                    else:
                        df[key] = None

            df = df[required_keys]
            df = df.dropna(subset=["date"])

            txns = df.to_dict("records")
            txns.sort(key=itemgetter("date", "account", "amount", "description"))

            if txns:
                latest_date = txns[-1]["date"].strftime("%Y-%m-%d") if txns else "N/A"
                logger.info(
                    f"Found {len(txns)} old {account_type} transactions. Latest date: {latest_date}"
                )
            return txns

        except Exception as e:
            logger.fatal(
                f"Error processing old {account_type} transactions from sheet: {e}",
                exc_info=True,
            )
            return []

    def get_all_transactions_from_statements(
        self,
        account_type: str,
        latest_txn_by_account: dict[str, datetime.datetime],
    ) -> list[dict]:
        """
        Fetches data from relevant statement Google Sheets, parses, and combines transactions.
        """
        all_parsed_txns = []
        account_list = BANK_ACCOUNTS if account_type == "bank" else CC_ACCOUNTS
        logger.info(
            f"Scanning {len(self.statement_files)} statement files (Sheets) for new {account_type} transactions."
        )

        for file_info in self.statement_files:
            file_name = file_info.get("name", "")
            file_id = file_info.get("id")

            if not file_id or account_type not in file_name.lower():
                continue

            matched_account, statement_end_date = (
                self._get_account_and_date_from_filename(
                    file_name, account_list, account_type
                )
            )

            if not matched_account:
                continue

            # Check if statement period is potentially relevant
            last_txn_date = latest_txn_by_account.get(matched_account)
            if (
                last_txn_date
                and statement_end_date
                and last_txn_date.date() >= statement_end_date.date()
            ):
                continue

            logger.info(
                f"Processing relevant statement Sheet: '{file_name}' (ID: {file_id}) for account '{matched_account}'"
            )
            config_key = f"{account_type}-{matched_account.split('-')[1]}"

            if config_key not in PARSING_CONFIG:
                logger.warning(
                    f"No parsing configuration found for '{config_key}'. Skipping file '{file_name}'."
                )
                continue

            config = PARSING_CONFIG[config_key]

            # --- Fetch data from the statement Sheet ---
            # Determine range - fetch all data from the first visible sheet
            first_sheet_name = self._get_first_sheet_name(file_id)
            if not first_sheet_name:
                logger.warning(
                    f"Could not determine first sheet name for Sheet ID {file_id}. Skipping."
                )
                continue
            fetch_range = (
                f"'{first_sheet_name}'!A:Z"  # Fetch all columns in the first sheet
            )
            logger.debug(f"Fetching data from range: {fetch_range}")
            statement_data = self._get_sheet_data(
                file_id, fetch_range
            )  # Get list of lists

            if not statement_data:
                logger.warning(
                    f"No data returned from statement Sheet '{file_name}' (ID: {file_id}). Skipping."
                )
                continue

            # --- Parse fetched data using pandas ---
            try:
                df = self._parse_statement_data_with_pandas(
                    statement_data, config
                )  # Pass list of lists
                if df is not None and not df.empty:
                    standardized_df = self._standardize_parsed_df(
                        df, config, matched_account
                    )
                    file_txns = standardized_df.to_dict("records")

                    # Filter transactions older than the last known date
                    if last_txn_date:
                        original_count = len(file_txns)
                        file_txns = [
                            txn for txn in file_txns if txn["date"] > last_txn_date
                        ]
                        logger.debug(
                            f"Filtered {original_count - len(file_txns)} transactions from '{file_name}' "
                            f"older than {last_txn_date:%Y-%m-%d}."
                        )

                    if file_txns:
                        logger.info(
                            f"Successfully parsed {len(file_txns)} new transactions from '{file_name}'."
                        )
                        all_parsed_txns.extend(file_txns)
                    else:
                        logger.info(
                            f"No new transactions found in '{file_name}' after filtering."
                        )
                else:
                    logger.info(
                        f"Parsed 0 transactions or empty DataFrame from '{file_name}'."
                    )

            except Exception as e:
                logger.error(
                    f"Failed to parse or standardize statement Sheet '{file_name}' (ID: {file_id}): {e}",
                    exc_info=True,
                )

        # --- Final Sorting ---
        if all_parsed_txns:
            all_parsed_txns.sort(
                key=itemgetter("date", "account", "amount", "description")
            )
            latest_date_str = all_parsed_txns[-1]["date"].strftime("%Y-%m-%d")
            logger.info(
                f"Found total of {len(all_parsed_txns)} new {account_type} transactions in statement Sheets. "
                f"Latest date: {latest_date_str}"
            )
        else:
            logger.info(
                f"Found no new {account_type} transactions in statement Sheets."
            )
        return all_parsed_txns

    def add_new_transactions(self, txns: list[dict], account_type: str) -> None:
        """Formats and appends new transactions to the appropriate sheet."""
        if not txns:
            logger.info(f"No new {account_type} transactions to add.")
            return

        range_to_update = (
            BANK_TRANSACTIONS_FULL_RANGE
            if account_type == "bank"
            else CC_TRANSACTIONS_FULL_RANGE
        )
        sheet_id = TRANSACTIONS_SHEET_ID
        logger.info(
            f"Preparing to add {len(txns)} new {account_type} transactions to sheet {sheet_id}, "
            f"range {range_to_update}."
        )

        values_to_append = self._format_txns_for_sheet(txns)
        self._append_sheet_data(sheet_id, range_to_update, values_to_append)

    def get_all_transactions_for_recategorize(self) -> list[dict]:
        """Fetches ALL transactions from both Bank and CC sheets for recategorization."""
        logger.info("Fetching all existing transactions for recategorization.")
        bank_txns = self.get_old_transactions("bank")
        cc_txns = self.get_old_transactions("cc")
        all_txns = bank_txns + cc_txns
        all_txns.sort(key=itemgetter("date", "account", "amount", "description"))
        logger.info(
            f"Fetched a total of {len(all_txns)} transactions for recategorization."
        )
        return all_txns

    def overwrite_transactions(self, txns: list[dict], account_type: str) -> None:
        """Clears the specified sheet range and writes all provided transactions."""
        if not txns:
            logger.warning(
                f"No transactions provided to overwrite for {account_type}. Skipping overwrite."
            )
            return

        sheet_name = (
            BANK_TRANSACTIONS_SHEET_NAME
            if account_type == "bank"
            else CC_TRANSACTIONS_SHEET_NAME
        )
        # Define the exact range to clear (e.g., B3:H end) and write (B3 start)
        clear_range = (
            f"{sheet_name}!B3:H"  # Clear data area, leaving headers (if any in row 1/2)
        )
        write_start_range = f"{sheet_name}!B3"  # Write data starting from B3
        sheet_id = TRANSACTIONS_SHEET_ID
        logger.info(
            f"Preparing to overwrite {len(txns)} {account_type} transactions in sheet {sheet_id}, range {clear_range}."
        )

        values_to_write = self._format_txns_for_sheet(txns)

        logger.info(f"Clearing existing data in range: {clear_range}")
        self._clear_sheet_range(sheet_id, clear_range)

        logger.info(f"Writing new data starting at range: {write_start_range}")
        self._write_sheet_data(sheet_id, write_start_range, values_to_write)
        logger.info(f"Successfully overwrote {account_type} transactions.")

    # --- Private Helper Methods ---

    def _format_txns_for_sheet(self, txns: list[dict]) -> list[list[str]]:
        """Formats transaction dictionaries into lists suitable for sheet writing."""
        values = []
        for txn in txns:
            # Ensure date is datetime object before formatting
            date_str = (
                txn["date"].strftime("%Y-%m-%d")
                if isinstance(txn.get("date"), datetime.datetime)
                else ""
            )
            amount = txn.get("amount", 0.0)
            debit = f"{-amount:.2f}" if amount < 0 else ""
            credit = f"{amount:.2f}" if amount >= 0 else ""
            values.append(
                [
                    date_str,
                    txn.get("description", ""),
                    debit,
                    credit,
                    txn.get("category", DEFAULT_CATEGORY),
                    txn.get("remarks", ""),
                    txn.get("account", "Unknown"),
                ]
            )
        return values

    def _get_account_and_date_from_filename(
        self, filename: str, account_list: list[str], account_type: str
    ) -> Tuple[Optional[str], Optional[datetime.datetime]]:
        """Attempts to extract account name and statement end date from filename."""
        matched_account = None
        filename_lower = filename.lower()
        for acc in account_list:
            if acc.lower() in filename_lower:  # Case-insensitive match
                matched_account = acc
                break
        if not matched_account:
            return None, None

        # Date extraction logic (adjust based on your consistent naming convention)
        try:
            # Remove extensions like .csv, .gsheet etc. before splitting
            base_name = filename_lower.split(".")[0]
            parts = base_name.split("-")

            # Try parsing common date patterns found in statement names
            if account_type == "bank":
                date_part = parts[-1].strip()  # Get last part
                if len(date_part) == 4 and date_part.isdigit():  # YYYY
                    year = int(date_part)
                    return matched_account, datetime.datetime(year, 12, 31)
                # Add other bank date patterns if needed
            else:  # cc
                year_part = parts[-2].strip()
                month_part = parts[-1].strip()  # Get last part
                if (
                    len(year_part) == 4
                    and year_part.isdigit()
                    and len(month_part) == 2
                    and month_part.isdigit()
                ):
                    year = int(year_part)
                    month = int(month_part)
                    next_month_start = datetime.datetime(
                        year, month, 1
                    ) + pd.DateOffset(months=1)
                    return (
                        matched_account,
                        next_month_start - datetime.timedelta(days=1),
                    )

            logger.warning(
                f"Could not parse standard date pattern from date part '{date_part}' in filename '{filename}'"
            )
            return (
                matched_account,
                None,
            )  # Return account but no date if pattern unknown
        except (IndexError, ValueError, TypeError) as e:
            logger.warning(f"Error parsing date from filename '{filename}': {e}")
            return matched_account, None

    def _parse_statement_data_with_pandas(
        self, statement_data: list[list[str]], config: dict
    ) -> Optional[pd.DataFrame]:
        """Parses statement data (list of lists) using pandas, attempting dynamic header detection."""
        if not statement_data:
            logger.warning("No statement data provided to parse.")
            return None

        header_patterns = config.get("header_patterns", [])
        best_header_row_index = None

        # Attempt to find the header row index within the fetched data
        try:
            max_check_rows = min(30, len(statement_data))  # Check first 30 rows
            for i in range(max_check_rows):
                row_content_lower = " ".join(
                    map(str, statement_data[i])
                ).lower()  # Join row elements for searching
                for pattern in header_patterns:
                    # Check if most keywords from a pattern exist in the row
                    matches = sum(
                        1
                        for header_word in pattern
                        if header_word.lower() in row_content_lower
                    )
                    # Adjust threshold as needed (e.g., > 75% of words match)
                    if matches >= len(pattern) * 0.75:
                        best_header_row_index = i
                        logger.debug(
                            f"Detected header at row index {i}: {statement_data[i]}"
                        )
                        break
                if best_header_row_index is not None:
                    break
            if best_header_row_index is None:
                logger.warning(
                    f"Could not reliably detect header row based on patterns: {header_patterns}. "
                    f"Attempting parse assuming header is first row."
                )
                best_header_row_index = 0  # Default to first row if detection fails

        except Exception as e:
            logger.error(
                f"Error during header detection: {e}. Defaulting to first row.",
                exc_info=True,
            )
            best_header_row_index = 0

        # Create DataFrame using detected header row
        try:
            header = statement_data[best_header_row_index]
            data = statement_data[best_header_row_index + 1 :]
            if (
                "special_handling" in config
                and config["special_handling"] == "hdfc_cc_tilde"
            ):
                new_data = []
                header = statement_data[best_header_row_index][0].split("~")
                for i in range(best_header_row_index + 1, len(statement_data)):
                    if statement_data[i]:
                        temp = statement_data[i][0].split("~")
                        if len(temp) == len(header):
                            new_data.append(temp)
                data = new_data

            df = pd.DataFrame(data, columns=header)

            # --- Basic Data Cleaning ---
            # Replace empty strings with NaN for better handling
            df.replace("", pd.NA, inplace=True)
            # Drop rows where all values are NaN (completely empty rows)
            df.dropna(how="all", inplace=True)
            # Remove potential unnamed columns pandas might add if header had issues
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            # Remove columns that are entirely empty/NaN
            df.dropna(axis=1, how="all", inplace=True)

            logger.debug(
                f"Pandas DataFrame created. Shape before standardization: {df.shape}"
            )
            if df.empty:
                logger.warning("DataFrame is empty after initial creation/cleaning.")
                return None
            return df
        except Exception as e:
            logger.error(
                f"Pandas DataFrame creation/cleaning failed: {e}",
                exc_info=True,
            )
            return None  # Return None on parsing failure

    def _standardize_parsed_df(
        self, df: pd.DataFrame, config: dict, account_name: str
    ) -> Optional[pd.DataFrame]:
        """Standardizes column names, parses dates, and calculates amount."""
        if df is None or df.empty:
            return None

        # Make a copy to avoid SettingWithCopyWarning on the original df passed in
        df = df.copy()

        column_map = config["column_map"]
        date_formats = config.get("date_formats", [])  # Get configured date formats
        amount_sign_col = config.get("amount_sign_col")
        debit_value = str(config.get("debit_value", "")).lower()

        # --- Rename columns ---
        rename_dict = {}
        current_columns_map = {
            str(col).strip().lower(): str(col).strip() for col in df.columns
        }
        for source_key, target_key in column_map.items():
            source_key_clean = str(source_key).strip().lower()
            if source_key_clean in current_columns_map:
                original_col_name = current_columns_map[source_key_clean]
                rename_dict[original_col_name] = target_key
            else:
                logger.warning(
                    f"Expected column '{source_key}' not found for config {config}. Skipping."
                )
        df.rename(columns=rename_dict, inplace=True)  # Modify in place on the copy
        logger.debug(f"Columns after rename: {df.columns.tolist()}")

        # --- Date Parsing ---
        if "date" not in df.columns:
            logger.error("Standardized 'date' column not found. Cannot proceed.")
            return None
        try:
            # Apply the robust parsing function using the configured formats
            df.loc[:, "date"] = df["date"].apply(
                lambda x: self._parse_mixed_datetime(x, date_formats)
            )
            df.dropna(subset=["date"], inplace=True)  # Modify in place
            if df.empty:
                logger.warning(
                    "DataFrame empty after date parsing/dropping failed dates."
                )
                return None
        except Exception as e:
            logger.error(f"Error applying custom date parsing: {e}", exc_info=True)
            return None

        # --- Amount Calculation ---
        if "debit" in df.columns and "credit" in df.columns:
            df.loc[:, "amount"] = df.apply(
                lambda row: self._parse_amount(row.get("credit"))
                - self._parse_amount(row.get("debit")),
                axis=1,
            )
        elif (
            "amount" in df.columns and amount_sign_col and amount_sign_col in df.columns
        ):
            # Ensure the 'amount' column is numeric before applying sign
            df.loc[:, "amount"] = df["amount"].apply(self._parse_amount)
            df.loc[:, "amount"] = df.apply(
                lambda r: (
                    -r["amount"]
                    if (not debit_value and pd.isna(r.get(amount_sign_col)))
                    or (str(r.get(amount_sign_col, "")).strip().lower() == debit_value)
                    else r["amount"]
                ),
                axis=1,
            )
        elif "amount" in df.columns:
            df.loc[:, "amount"] = df["amount"].apply(self._parse_amount)
        else:
            logger.error("Could not find columns to calculate amount.")
            return None

        df.loc[:, "account"] = account_name  # Use .loc for this assignment too

        # --- Select and Order Final Columns ---
        final_cols_data = {}
        for key in INTERNAL_TXN_KEYS:
            if key in df.columns:
                final_cols_data[key] = df[key]
            else:
                default_value = None
                if key == "category":
                    default_value = None
                elif key == "remarks":
                    default_value = None
                elif key == "description":
                    default_value = ""
                elif key == "amount":
                    default_value = 0.0
                elif key == "account":
                    default_value = account_name
                final_cols_data[key] = (
                    default_value  # This will be a scalar, pandas will broadcast
                )
                if key not in ["category", "remarks"]:
                    logger.warning(
                        f"Internal key '{key}' missing. Added default: {default_value}"
                    )

        final_df = pd.DataFrame(final_cols_data)
        # Ensure correct column order and select only these columns
        final_df = final_df[INTERNAL_TXN_KEYS]

        logger.debug(f"Standardized DataFrame shape: {final_df.shape}")
        return final_df if not final_df.empty else None

    def _parse_mixed_datetime(
        self, date_str: Any, formats: List[str]
    ) -> Optional[datetime.datetime]:
        """
        Attempts to parse a date string using a list of provided formats.
        Falls back to pandas inference if specific formats fail.
        """
        if pd.isna(date_str) or date_str == "":
            return None
        try:
            cleaned_str = str(date_str).strip("' ")
            parsed_date = None

            # Try explicit formats first
            if formats:
                for fmt in formats:
                    try:
                        # Use Python's datetime.strptime for explicit format matching
                        parsed_date = datetime.datetime.strptime(cleaned_str, fmt)
                        logger.debug(f"Parsed '{cleaned_str}' using format '{fmt}'")
                        break
                    except ValueError:
                        continue
            else:
                logger.warning(
                    "No specific date formats provided, attempting inference only."
                )
                pass

            # If specific formats fail or weren't provided, try pandas inference
            if not parsed_date:
                # logger.debug(f"Specific formats failed for '{cleaned_str}', trying pandas inference...")
                # Use dayfirst=True for common Indian formats like dd/mm
                dt_obj = pd.to_datetime(cleaned_str, dayfirst=True, errors="coerce")
                if pd.isna(dt_obj):
                    logger.warning(
                        f"Could not parse date string with any provided format or inference: '{cleaned_str}'"
                    )
                    return pd.NaT
                logger.debug(f"Parsed '{cleaned_str}' using pandas inference.")
                return dt_obj.to_pydatetime()
            else:
                return parsed_date
        except Exception as e:
            # Log this error if needed
            logger.error(
                f"Unexpected error parsing date string '{date_str}': {e}",
                exc_info=True,
            )
            return None

    # --- Google API Interaction Helpers ---

    def _get_credential(self) -> ServiceAccountCredentials:
        """Creates and returns Google API Credentials."""
        logger.info(
            f"Authenticating using service account key: {SERVICE_ACCOUNT_KEY_FILE}"
        )
        try:
            credential = ServiceAccountCredentials.from_json_keyfile_name(
                SERVICE_ACCOUNT_KEY_FILE, SCOPES
            )
            assert credential is not None, "Failed to load credentials."
            # Removed credential.invalid check
            logger.info("Authentication successful (credentials loaded).")
            return credential
        except FileNotFoundError:
            logger.critical(
                f"Service account key file not found: {SERVICE_ACCOUNT_KEY_FILE}"
            )
            assert (
                False
            ), f"Service account key file not found: {SERVICE_ACCOUNT_KEY_FILE}"
        except Exception as e:
            logger.critical(f"Error during authentication: {e}", exc_info=True)
            assert False, f"Unexpected error during authentication: {e}"

    def _get_drive_service(self) -> Any:
        """Builds and returns the Google Drive API service client."""
        try:
            service = build(
                "drive", "v3", credentials=self.creds, cache_discovery=False
            )  # Disable cache
            logger.info("Google Drive service client built successfully.")
            return service
        except Exception as e:
            logger.critical(f"Failed to build Google Drive service: {e}", exc_info=True)
            assert False, f"Failed to build Google Drive service: {e}"

    def _get_sheets_service(self) -> Any:
        """Builds and returns the Google Sheets API service client."""
        try:
            service = build(
                "sheets", "v4", credentials=self.creds, cache_discovery=False
            )  # Disable cache
            logger.info("Google Sheets service client built successfully.")
            return service
        except Exception as e:
            logger.critical(
                f"Failed to build Google Sheets service: {e}", exc_info=True
            )
            assert False, f"Failed to build Google Sheets service: {e}"

    def _get_statement_files(self) -> list:
        """Lists Google Sheet files from the configured folder in Google Drive."""
        files = []
        page_token = None
        logger.info(f"Listing Google Sheets from Drive folder ID: {CSV_FOLDER}")
        # Query specifically for Google Sheets in the folder
        query = f"parents in '{CSV_FOLDER}' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        try:
            while True:
                response = (
                    self.drive_service.files()
                    .list(
                        q=query,
                        spaces="drive",
                        fields="nextPageToken, files(id, name, mimeType)",
                        pageToken=page_token,
                    )
                    .execute()
                )
                found_files = response.get("files", [])
                files.extend(found_files)
                page_token = response.get("nextPageToken", None)
                if page_token is None:
                    break
            logger.info(
                f"Found {len(files)} Google Sheet statement files in Drive folder."
            )
        except GoogleHttpError as e:
            logger.error(f"Google Drive API error listing files: {e}", exc_info=True)
            assert False, f"Google Drive API error listing files: {e}"
        except Exception as e:
            logger.error(
                f"Unexpected error listing files from Google Drive: {e}",
                exc_info=True,
            )
            assert False, f"Unexpected error listing files: {e}"
        return files

    def _get_first_sheet_name(self, spreadsheet_id: str) -> Optional[str]:
        """Gets the name (title) of the first visible sheet in a spreadsheet."""
        try:
            spreadsheet_metadata = (
                self.sheets_service.spreadsheets()
                .get(
                    spreadsheetId=spreadsheet_id,
                    fields="sheets(properties(title,hidden))",
                )
                .execute()
            )
            sheets = spreadsheet_metadata.get("sheets", [])
            for sheet in sheets:
                properties = sheet.get("properties", {})
                if not properties.get("hidden", False):  # Find first non-hidden sheet
                    title = properties.get("title")
                    if title:
                        logger.debug(
                            f"Found first visible sheet name: '{title}' for ID: {spreadsheet_id}"
                        )
                        return title
            logger.warning(
                f"No visible sheets found for spreadsheet ID: {spreadsheet_id}"
            )
            return None
        except GoogleHttpError as e:
            logger.error(
                f"API error getting sheet metadata for ID {spreadsheet_id}: {e}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error getting sheet metadata for ID {spreadsheet_id}: {e}",
                exc_info=True,
            )
            return None

    def _get_sheet_data(self, sheet_id: str, range_name: str) -> list:
        """Fetches data from Google Sheets with retry logic."""
        for attempt in range(self.max_retries):
            try:
                logger.debug(
                    f"Getting sheet data: ID={sheet_id}, Range={range_name}, Attempt={attempt+1}"
                )
                result = (
                    self.sheets_service.spreadsheets()
                    .values()
                    .get(spreadsheetId=sheet_id, range=range_name)
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
                    logger.error(
                        f"Non-retryable or final Sheets API error getting data: {e}",
                        exc_info=True,
                    )
                    assert False, f"Failed to get sheet data: {e}"  # Fail fast
            except Exception as e:
                logger.error(f"Unexpected error getting sheet data: {e}", exc_info=True)
                assert False, f"Unexpected error getting sheet data: {e}"  # Fail fast
        return []  # Should not be reached

    def _append_sheet_data(
        self, spreadsheet_id: str, range_name: str, values: list
    ) -> None:
        """Appends data to Google Sheets with retry logic."""
        if not values:
            return
        # range_name for append usually specifies the sheet, e.g., "Sheet1" or "Sheet1!A1"
        # The API appends after the last row with data in this range.
        effective_range = range_name.split("!")[
            0
        ]  # Use just the sheet name for append range usually
        for attempt in range(self.max_retries):
            try:
                body = {"values": values}
                logger.debug(
                    f"Appending {len(values)} rows: ID={spreadsheet_id}, Range={effective_range}, Attempt={attempt+1}"
                )
                result = (
                    self.sheets_service.spreadsheets()
                    .values()
                    .append(
                        spreadsheetId=spreadsheet_id,
                        range=effective_range,
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body=body,
                    )
                    .execute()
                )
                updated_cells = result.get("updates", {}).get("updatedCells", 0)
                logger.info(f"Successfully appended {updated_cells} cells.")
                return  # Success
            except GoogleHttpError as e:
                if e.resp.status in [429, 500, 503] and attempt < self.max_retries - 1:
                    wait_time = self.initial_backoff * (2**attempt)
                    logger.warning(
                        f"Sheets API error {e.resp.status}. Retrying append in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Non-retryable or final Sheets API error appending data: {e}",
                        exc_info=True,
                    )
                    assert False, f"Failed to append sheet data: {e}"  # Fail fast
            except Exception as e:
                logger.error(
                    f"Unexpected error appending sheet data: {e}",
                    exc_info=True,
                )
                assert False, f"Unexpected error appending sheet data: {e}"  # Fail fast
        assert (
            False
        ), f"Failed to append sheet data after {self.max_retries} attempts."  # Fail fast

    def _clear_sheet_range(self, spreadsheet_id: str, range_name: str) -> None:
        """Clears a specified range in Google Sheets."""
        for attempt in range(self.max_retries):
            try:
                logger.debug(
                    f"Clearing sheet range: ID={spreadsheet_id}, Range={range_name}, Attempt={attempt+1}"
                )
                result = (
                    self.sheets_service.spreadsheets()
                    .values()
                    .clear(spreadsheetId=spreadsheet_id, range=range_name, body={})
                    .execute()
                )
                logger.info(
                    f"Successfully cleared range {result.get('clearedRange', range_name)}."
                )
                return  # Success
            except GoogleHttpError as e:
                if e.resp.status in [429, 500, 503] and attempt < self.max_retries - 1:
                    wait_time = self.initial_backoff * (2**attempt)
                    logger.warning(
                        f"Sheets API error {e.resp.status}. Retrying clear in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Non-retryable or final Sheets API error clearing range: {e}",
                        exc_info=True,
                    )
                    assert False, f"Failed to clear sheet range: {e}"  # Fail fast
            except Exception as e:
                logger.error(
                    f"Unexpected error clearing sheet range: {e}",
                    exc_info=True,
                )
                assert False, f"Unexpected error clearing sheet range: {e}"  # Fail fast
        assert (
            False
        ), f"Failed to clear sheet range after {self.max_retries} attempts."  # Fail fast

    def _write_sheet_data(
        self, spreadsheet_id: str, range_name: str, values: list
    ) -> None:
        """Writes data to a specific range in Google Sheets (overwrites)."""
        if not values:
            return
        for attempt in range(self.max_retries):
            try:
                body = {"values": values}
                logger.debug(
                    f"Writing {len(values)} rows: ID={spreadsheet_id}, Range={range_name}, Attempt={attempt+1}"
                )
                result = (
                    self.sheets_service.spreadsheets()
                    .values()
                    .update(
                        spreadsheetId=spreadsheet_id,
                        range=range_name,
                        valueInputOption="USER_ENTERED",
                        body=body,
                    )
                    .execute()
                )
                updated_cells = result.get("updatedCells", 0)
                logger.info(
                    f"Successfully wrote {updated_cells} cells to range {result.get('updatedRange', range_name)}."
                )
                return  # Success
            except GoogleHttpError as e:
                if e.resp.status in [429, 500, 503] and attempt < self.max_retries - 1:
                    wait_time = self.initial_backoff * (2**attempt)
                    logger.warning(
                        f"Sheets API error {e.resp.status}. Retrying write in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Non-retryable or final Sheets API error writing data: {e}",
                        exc_info=True,
                    )
                    assert False, f"Failed to write sheet data: {e}"  # Fail fast
            except Exception as e:
                logger.error(f"Unexpected error writing sheet data: {e}", exc_info=True)
                assert False, f"Unexpected error writing sheet data: {e}"  # Fail fast
        assert (
            False
        ), f"Failed to write sheet data after {self.max_retries} attempts."  # Fail fast

    @staticmethod
    def _parse_amount(value: Any) -> float:
        """Safely parses various amount formats into a float."""
        if value is None or pd.isna(value) or value == "":
            return 0.0
        try:
            str_value = str(value).strip()
            # Remove common currency symbols and commas
            str_value = str_value.replace(",", "").replace("₹", "").replace("$", "")
            # Handle parentheses for negatives (common in accounting)
            if str_value.startswith("(") and str_value.endswith(")"):
                str_value = "-" + str_value[1:-1]
            # Handle trailing CR/DR if present (though usually handled by sign column)
            if str_value.endswith(" Cr") or str_value.endswith(" CR"):
                str_value = str_value[:-3].strip()
            elif str_value.endswith(" Dr") or str_value.endswith(" DR"):
                str_value = "-" + str_value[:-3].strip()

            # Handle potential scientific notation from Sheets
            if "E+" in str_value or "e+" in str_value:
                return float(pd.to_numeric(str_value))

            return float(str_value)
        except (ValueError, TypeError) as e:
            # Log only if value wasn't obviously zero/empty
            if value not in [0, 0.0, "0", "0.0", "", None]:
                logger.debug(
                    f"Could not parse amount value: '{value}'. Error: {e}. Returning 0.0"
                )
            return 0.0
