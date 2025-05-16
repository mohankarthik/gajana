# gajana/transaction_processor.py
from __future__ import annotations

import datetime
import logging
from operator import itemgetter
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd

from constants import BANK_ACCOUNTS
from constants import CC_ACCOUNTS
from constants import DEFAULT_CATEGORY
from constants import INTERNAL_TXN_KEYS
from constants import PARSING_CONFIG
from interfaces import DataSourceInterface
from utils import log_and_exit

logger = logging.getLogger(__name__)


# Helper function for robust date parsing with multiple formats (moved here or keep in a utils.py)
def parse_mixed_datetime(
    date_str: Any, formats: List[str]
) -> Optional[datetime.datetime]:
    if pd.isna(date_str) or date_str == "":
        return None
    try:
        cleaned_str = str(date_str).strip("' ")
        parsed_date = None
        if formats:
            for fmt in formats:
                try:
                    parsed_date = datetime.datetime.strptime(cleaned_str, fmt)
                    break
                except ValueError:
                    continue
        if not parsed_date:
            dt_obj = pd.to_datetime(cleaned_str, dayfirst=True, errors="coerce")
            if pd.isna(dt_obj):
                logger.warning(
                    f"Could not parse date string with any format/inference: '{cleaned_str}'"
                )
                return None
            return dt_obj.to_pydatetime()
        return parsed_date
    except Exception as e:
        log_and_exit(
            logger, f"Unexpected error parsing date string '{date_str}': {e}", e
        )
        return None


class TransactionProcessor:
    """
    Handles processing of raw statement data into standardized transactions,
    interpreting filenames, and preparing data for storage.
    """

    def __init__(self, data_source: DataSourceInterface):
        self.data_source = data_source

    @staticmethod
    def _parse_amount(value: Any) -> float:
        if value is None or pd.isna(value) or value == "":
            return 0.0
        try:
            s_val = (
                str(value).strip().replace(",", "").replace("₹", "").replace("$", "")
            )
            if s_val.startswith("(") and s_val.endswith(")"):
                s_val = "-" + s_val[1:-1]
            if s_val.endswith(" Cr") or s_val.endswith(" CR"):
                s_val = s_val[:-3].strip()
            elif s_val.endswith(" Dr") or s_val.endswith(" DR"):
                s_val = "-" + s_val[:-3].strip()
            if "E+" in s_val.upper():
                return float(pd.to_numeric(s_val))
            if s_val.endswith("%"):
                s_val = s_val[:-1]
            return float(s_val)
        except (ValueError, TypeError):
            if value not in [0, 0.0, "0", "0.0", "", None]:
                log_and_exit(logger, f"Could not parse amount: '{value}'")
            return 0.0

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

    def get_old_transactions(self, account_type: str) -> list[dict]:
        logger.info(f"Fetching old {account_type} transactions for processing.")
        raw_data = self.data_source.get_transaction_log_data(account_type)
        if not raw_data or len(raw_data) <= 1:
            logger.warning(f"No old {account_type} data from data source.")
            return []
        try:
            header = raw_data[0]
            df = pd.DataFrame(raw_data[1:], columns=header)
            df.loc[:, "Date"] = pd.to_datetime(
                df["Date"], format="%Y-%m-%d", errors="coerce"
            )
            if "Credit" in df.columns and "Debit" in df.columns:
                df.loc[:, "amount"] = df.apply(
                    lambda r: self._parse_amount(r["Credit"])
                    - self._parse_amount(r["Debit"]),
                    axis=1,
                )
            else:
                df.loc[:, "amount"] = 0.0
            rename_map = {
                "Date": "date",
                "Description": "description",
                "Category": "category",
                "Remarks": "remarks",
                "Account": "account",
            }
            df.rename(
                columns={k: v for k, v in rename_map.items() if k in df.columns},
                inplace=True,
            )
            for key in INTERNAL_TXN_KEYS:
                if key not in df.columns:
                    df[key] = None
            df = df[INTERNAL_TXN_KEYS].dropna(subset=["date"])
            txns = df.to_dict("records")
            for txn in txns:
                if isinstance(txn["date"], pd.Timestamp):
                    txn["date"] = txn["date"].to_pydatetime()
            txns.sort(key=itemgetter("date", "account", "amount", "description"))
            if txns:
                logger.info(
                    f"Processed {len(txns)} old {account_type} txns. Latest: {txns[-1]['date']:%Y-%m-%d}"
                )
            return txns
        except Exception as e:
            log_and_exit(
                logger,
                f"Error processing old {account_type} txns from raw data: {e}",
                e,
            )
            return []

    def get_new_transactions_from_statements(
        self, account_type: str, latest_txn_by_account: dict[str, datetime.datetime]
    ) -> list[dict]:
        all_parsed_txns = []
        account_list = BANK_ACCOUNTS if account_type == "bank" else CC_ACCOUNTS
        statement_files = self.data_source.list_statement_file_details()
        logger.info(
            f"Scanning {len(statement_files)} statement files for {account_type} transactions."
        )

        for file_info in statement_files:
            file_name, file_id = file_info.get("name", ""), file_info.get("id")
            if not file_id or account_type not in file_name.lower():
                continue

            matched_account, stmt_end_date = self._get_account_and_date_from_filename(
                file_name, account_list, account_type
            )
            if not matched_account:
                continue

            last_txn_date = latest_txn_by_account.get(matched_account)
            if (
                last_txn_date
                and stmt_end_date
                and last_txn_date.date() >= stmt_end_date.date()
            ):
                continue

            logger.info(
                f"Processing statement Sheet: '{file_name}' (ID: {file_id}) for '{matched_account}'"
            )
            config_key = f"{account_type}-{matched_account.split('-')[1]}"
            if config_key not in PARSING_CONFIG:
                logger.warning(
                    f"No parsing config for '{config_key}'. Skipping '{file_name}'."
                )
                continue
            config = PARSING_CONFIG[config_key]

            # Use file_id (which is spreadsheet_id) and get first sheet name for data fetching
            first_sheet_name = self.data_source.get_first_sheet_name_from_file(
                file_id
            )  # Method from GoogleDataSource
            if not first_sheet_name:
                logger.warning(f"Cannot get sheet name for {file_id}. Skipping.")
                continue

            raw_statement_data = self.data_source.get_sheet_data(
                file_id, first_sheet_name, "A:Z"
            )  # Fetch all
            if not raw_statement_data:
                logger.warning(f"No data from statement Sheet '{file_name}'. Skipping.")
                continue

            try:
                df = self._parse_statement_data_with_pandas(raw_statement_data, config)
                if df is not None and not df.empty:
                    std_df = self._standardize_parsed_df(df, config, matched_account)
                    if std_df is None or std_df.empty:
                        logger.warning(f"Standardization empty for '{file_name}'.")
                        continue
                    file_txns = std_df.to_dict("records")
                    for txn in file_txns:  # Ensure Python datetime
                        if isinstance(txn["date"], pd.Timestamp):
                            txn["date"] = txn["date"].to_pydatetime()
                        if not isinstance(txn["date"], datetime.datetime):
                            txn["date"] = None
                    file_txns = [
                        txn
                        for txn in file_txns
                        if isinstance(txn["date"], datetime.datetime)
                    ]

                    if last_txn_date:
                        original_count = len(file_txns)
                        file_txns = [
                            txn for txn in file_txns if txn["date"] > last_txn_date
                        ]
                        logger.debug(
                            f"Filtered {original_count - len(file_txns)} old txns from '{file_name}'."
                        )
                    if file_txns:
                        logger.info(
                            f"Parsed {len(file_txns)} new txns from '{file_name}'."
                        )
                        all_parsed_txns.extend(file_txns)
                    else:
                        logger.info(f"No new txns in '{file_name}' after filter.")
                else:
                    logger.info(f"0 txns or empty DataFrame from '{file_name}'.")
            except Exception as e:
                log_and_exit(logger, f"Failed to process sheet '{file_name}': {e}", e)

        if all_parsed_txns:
            all_parsed_txns.sort(
                key=itemgetter("date", "account", "amount", "description")
            )
            logger.info(
                f"Total {len(all_parsed_txns)} new {account_type} txns from statements. "
                f"Latest: {all_parsed_txns[-1]['date']:%Y-%m-%d}"
            )
        else:
            logger.info(f"No new {account_type} txns from statements.")
        return all_parsed_txns

    def _format_txns_for_storage(self, txns: list[dict]) -> List[List[Any]]:
        values = []
        for txn in txns:
            date_str = (
                txn["date"].strftime("%Y-%m-%d")
                if isinstance(txn.get("date"), datetime.datetime)
                else ""
            )
            amount = txn.get("amount", 0.0)
            try:
                amount = float(amount)
                debit = f"{-amount:.2f}" if amount < 0 else ""
                credit = f"{amount:.2f}" if amount >= 0 else ""
            except (ValueError, TypeError):
                debit, credit = "", ""
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

    def add_new_transactions_to_log(self, txns: list[dict], account_type: str) -> None:
        if not txns:
            logger.info(f"No new {account_type} txns to add.")
            return
        logger.info(f"Preparing to add {len(txns)} new {account_type} txns to log.")
        data_values = self._format_txns_for_storage(txns)
        self.data_source.append_transactions_to_log(account_type, data_values)

    def overwrite_transaction_log(self, txns: list[dict], account_type: str) -> None:
        if not txns:
            logger.warning(f"No txns to overwrite for {account_type}.")
            return
        logger.info(f"Preparing to overwrite {len(txns)} {account_type} txns in log.")
        data_values = self._format_txns_for_storage(txns)
        self.data_source.clear_transaction_log_range(account_type)
        self.data_source.write_transactions_to_log(account_type, data_values)

    def get_all_transactions_for_recategorize(self) -> list[dict]:
        logger.info(
            "Fetching all existing transactions for recategorization via TransactionProcessor."
        )
        bank_txns = self.get_old_transactions("bank")
        cc_txns = self.get_old_transactions("cc")
        all_txns = bank_txns + cc_txns
        all_txns.sort(key=itemgetter("date", "account", "amount", "description"))
        logger.info(f"Processor fetched {len(all_txns)} total txns for recategorize.")
        return all_txns
