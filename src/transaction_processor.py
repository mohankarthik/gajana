# gajana/transaction_processor.py
from __future__ import annotations

import datetime
import logging
import math
from operator import itemgetter
from typing import Any, Hashable, List, Optional, Tuple

import pandas as pd

from src.constants import (
    BANK_ACCOUNTS,
    CC_ACCOUNTS,
    DEFAULT_CATEGORY,
    INTERNAL_TXN_KEYS,
    PARSING_CONFIG,
)
from src.interfaces import DataSourceInterface
from src.utils import log_and_exit, parse_mixed_datetime

logger = logging.getLogger(__name__)


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
        # Match the most specific (longest) account name first so that a name
        # which is a prefix of another (e.g. cc-axis-neo vs cc-axis-neorupay)
        # does not shadow the longer one.
        for acc in sorted(account_list, key=len, reverse=True):
            if acc.lower() in filename_lower:
                matched_account = acc
                break
        if not matched_account:
            return None, None

        try:
            import re as _re

            base_name = filename_lower.split(".")[0]
            base_name = _re.sub(r"_copy\d+$", "", base_name)
            parts = base_name.split("-")

            year_part = parts[-2].strip()
            month_part = parts[-1].strip()
            if (
                len(year_part) == 4
                and year_part.isdigit()
                and len(month_part) == 2
                and month_part.isdigit()
            ):
                year = int(year_part)
                month = int(month_part)
                next_month_start = datetime.datetime(year, month, 1) + pd.DateOffset(
                    months=1
                )
                return (
                    matched_account,
                    next_month_start - datetime.timedelta(days=1),
                )
            if account_type == "bank":
                # Legacy year-only format: bank-account-YYYY.pdf
                date_part = parts[-1].strip()
                if len(date_part) == 4 and date_part.isdigit():
                    year = int(date_part)
                    return matched_account, datetime.datetime(year, 12, 31)

            logger.warning(
                f"Could not parse standard date pattern from date part in filename '{filename}'"
            )
            return (
                matched_account,
                None,
            )
        except (IndexError, ValueError, TypeError) as e:
            logger.warning(f"Error parsing date from filename '{filename}': {e}")
            return matched_account, None

    def _parse_validate_pdf(
        self,
        pdf_bytes: bytes,
        password: str,
        account_config: dict[str, Any],
        matched_account: str,
        stmt_end_date: Optional[datetime.datetime],
        file_name: str,
    ) -> Tuple[list[dict], bool]:
        """Vision-parse a statement PDF, validate the extracted tokens against
        the PDF text layer, retry once with the fallback model if anything looks
        wrong, and route rows that still fail to the review surface. Returns
        ``(passed_transactions, clean)`` where ``clean`` is True only if nothing
        was flagged for review — the caller caches only fully-clean statements so
        flagged ones keep re-parsing until a retry rescues them."""
        from src.pdf_parser import PDFParser
        from src.statement_validator import validate_statement

        parser = PDFParser()
        txns, text, summary = parser.parse_pdf_with_text(pdf_bytes, password)
        if not txns:
            return [], False

        result = validate_statement(
            txns, text, account_config, stmt_end_date, summary=summary
        )

        # Retry with the fallback model first — non-determinism / a stronger
        # model often fixes the flagged rows. Keep whichever parse validates
        # cleaner.
        if result.flagged:
            logger.warning(
                f"{len(result.flagged)} txn(s) flagged in '{file_name}'; "
                "retrying with fallback model."
            )
            retry_txns, retry_text, retry_summary = parser.parse_pdf_with_text(
                pdf_bytes,
                password,
                models=[parser.fallback_model, parser.primary_model],
            )
            if retry_txns:
                retry_result = validate_statement(
                    retry_txns,
                    retry_text or text,
                    account_config,
                    stmt_end_date,
                    summary=retry_summary,
                )
                if len(retry_result.flagged) < len(result.flagged):
                    logger.info(
                        f"Retry improved '{file_name}': "
                        f"{len(retry_result.flagged)} flagged "
                        f"(was {len(result.flagged)})."
                    )
                    result = retry_result

        if result.flagged or result.statement_flags:
            self._write_review_rows(result, matched_account, file_name)

        logger.info(
            f"'{file_name}': {len(result.passed)} txn(s) passed validation, "
            f"{len(result.flagged)} flagged for review."
        )
        return result.passed, not result.flagged

    def _write_review_rows(self, result: Any, account: str, file_name: str) -> None:
        """Append flagged rows and statement-level warnings to the review surface
        for manual triage. Never raises — a review-write failure must not abort
        the run."""
        rows: list[list[Any]] = []
        for txn, reasons in result.flagged:
            rows.append(
                [
                    account,
                    str(txn.get("date", "")),
                    str(txn.get("description", "")),
                    str(txn.get("debit", "")),
                    str(txn.get("credit", "")),
                    "; ".join(reasons),
                    file_name,
                ]
            )
        for flag in result.statement_flags:
            rows.append([account, "", "STATEMENT", "", "", flag, file_name])
        if not rows:
            return
        try:
            self.data_source.write_review_rows(rows)
        except Exception as e:
            logger.error(f"Failed to write review rows for '{file_name}': {e}")

    def _parse_statement_data_with_pandas(
        self, statement_data: list[list[str]], config: dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """Parses statement data (list of lists) using pandas, attempting dynamic header detection."""
        if not statement_data:
            logger.warning("No statement data provided to parse.")
            return None

        header_patterns = config.get("header_patterns", [])
        best_header_row_index = None

        # Attempt to find the header row index within the fetched data
        try:
            # Check first 30 rows
            max_check_rows = min(30, len(statement_data))
            for i in range(max_check_rows):
                row_content_lower = " ".join(map(str, statement_data[i])).lower()
                for pattern in header_patterns:
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
                # Default to first row if detection fails
                best_header_row_index = 0

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
            logger.error(f"Pandas DataFrame creation/cleaning failed: {e}", exc_info=e)
            return None

    def _standardize_parsed_df(
        self,
        df: pd.DataFrame,
        config: dict[str, Any],
        account_name: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """Standardizes column names, parses dates, and calculates amount."""
        if df is None or df.empty:
            return None

        # Make a copy
        df = df.copy()

        column_map = config["column_map"]
        date_formats = config.get("date_formats", [])
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
        df.rename(columns=rename_dict, inplace=True)
        logger.debug(f"Columns after rename: {df.columns.tolist()}")

        # --- Date Parsing ---
        if "date" not in df.columns:
            logger.error("Standardized 'date' column not found. Cannot proceed.")
            return None
        try:
            df["date"] = df["date"].astype(object)
            df.loc[:, "date"] = df["date"].apply(
                lambda x: parse_mixed_datetime(logger, x, date_formats)
            )
            df.dropna(subset=["date"], inplace=True)
            if df.empty:
                logger.warning(
                    "DataFrame empty after date parsing/dropping failed dates."
                )
                return None
        except Exception as e:
            log_and_exit(logger, f"Error applying custom date parsing: {e}", e)
            return None

        # --- Amount Calculation ---
        if "debit" in df.columns and "credit" in df.columns:
            df["amount"] = df.apply(
                lambda row: self._parse_amount(row.get("credit"))
                - self._parse_amount(row.get("debit")),
                axis=1,
            )
        elif (
            "amount" in df.columns and amount_sign_col and amount_sign_col in df.columns
        ):
            # Ensure the 'amount' column is numeric before applying sign
            df["amount"] = df["amount"].astype(object)
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
            df["amount"] = df["amount"].astype(object)
            df.loc[:, "amount"] = df["amount"].apply(self._parse_amount)
        else:
            logger.error("Could not find columns to calculate amount.")
            return None

        # If an account name is passed, set it for all rows. Otherwise, assume it exists.
        if account_name:
            df["account"] = account_name

        # --- Select and Order Final Columns ---
        final_cols_data = {}
        for key in INTERNAL_TXN_KEYS:
            if key in df.columns:
                final_cols_data[key] = df[key]
            else:
                default_value: Any = None
                if key == "category":
                    default_value = DEFAULT_CATEGORY
                elif key == "remarks":
                    default_value = ""
                elif key == "description":
                    default_value = ""
                elif key == "amount":
                    default_value = 0.0
                elif key == "account":
                    default_value = account_name if account_name else "Unknown"
                final_cols_data[key] = default_value
                if key not in ["category", "remarks"]:
                    logger.warning(
                        f"Internal key '{key}' missing. Added default: {default_value}"
                    )

        final_df = pd.DataFrame(final_cols_data)
        # Ensure correct column order and select only these columns
        final_df = final_df[INTERNAL_TXN_KEYS]

        logger.debug(f"Standardized DataFrame shape: {final_df.shape}")
        return final_df if not final_df.empty else None

    def get_old_transactions(self, account_type: str) -> list[dict[Hashable, Any]]:
        """
        Fetches and processes transactions from the main log sheet, using the
        same robust standardization pipeline as new transactions.
        """
        logger.info(f"Fetching old {account_type} transactions for processing.")
        raw_data = self.data_source.get_transaction_log_data(account_type)
        if not raw_data or len(raw_data) <= 1:
            logger.warning(f"No old {account_type} data found in data source.")
            return []

        try:
            header = raw_data[0]
            df = pd.DataFrame(raw_data[1:], columns=header)
            df.replace("", pd.NA, inplace=True)
            df.dropna(how="all", inplace=True)

            if df.empty:
                return []

            # A generic config that maps the log sheet columns to internal keys
            log_config = {
                "column_map": {
                    "Date": "date",
                    "Description": "description",
                    "Category": "category",
                    "Remarks": "remarks",
                    "Account": "account",
                    "Debit": "debit",
                    "Credit": "credit",
                },
                "date_formats": ["%Y-%m-%d"],
            }

            standardized_df = self._standardize_parsed_df(df, log_config)

            if standardized_df is None or standardized_df.empty:
                logger.warning(
                    f"Standardization of old {account_type} txns resulted in empty data."
                )
                return []

            txns = standardized_df.to_dict("records")
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
    ) -> list[dict[Hashable, Any]]:
        all_parsed_txns = []
        account_list = BANK_ACCOUNTS if account_type == "bank" else CC_ACCOUNTS
        statement_files = self.data_source.list_statement_file_details()
        logger.info(
            f"Scanning {len(statement_files)} statement files for {account_type} transactions."
        )

        # Cache of statement file IDs already fully parsed (file ID -> latest
        # booked txn date). A statement is immutable, so once the account
        # watermark covers its latest txn there is nothing new to gain from
        # re-parsing (and re-paying the LLM). Self-healing: if rows are later
        # deleted the watermark drops below the cached date and it re-parses.
        processed_cache = self.data_source.get_processed_statements()
        newly_processed: dict[str, str] = {}

        seen_account_dates: set[tuple[str, Any]] = set()
        for file_info in statement_files:
            file_name, file_id = file_info.name, file_info.id
            if not file_id or account_type not in file_name.lower():
                continue

            matched_account, stmt_end_date = self._get_account_and_date_from_filename(
                file_name, account_list, account_type
            )
            if not matched_account:
                continue

            dedup_key = (matched_account, stmt_end_date)
            if stmt_end_date and dedup_key in seen_account_dates:
                logger.info(
                    f"Skipping duplicate statement '{file_name}' (already processing this period)."
                )
                continue
            if stmt_end_date:
                seen_account_dates.add(dedup_key)

            last_txn_date = latest_txn_by_account.get(matched_account)
            if (
                last_txn_date
                and stmt_end_date
                and last_txn_date.date() >= stmt_end_date.date()
            ):
                continue

            # Skip statements we've already fully parsed whose latest txn the
            # account watermark still covers — no re-download, no LLM call.
            cached_max = processed_cache.get(file_id)
            if (
                cached_max
                and last_txn_date
                and last_txn_date.date() >= datetime.date.fromisoformat(cached_max)
            ):
                logger.info(
                    f"Skipping already-processed statement '{file_name}' "
                    "(cached; watermark covers it)."
                )
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

            # Only immutable PDF statements are cached; Google Sheet statements
            # are mutable, so never cache them (and re-reading a sheet is cheap).
            pdf_clean = False

            try:
                if file_name.lower().endswith(".pdf"):
                    logger.info(f"Processing statement PDF: '{file_name}'")
                    pdf_bytes = self.data_source.download_file(file_id)
                    password = ""
                    try:
                        import json
                        import os

                        pw_path = os.path.join("secrets", "passwords.json")
                        if os.path.exists(pw_path):
                            with open(pw_path, "r") as f:
                                pw_data = json.load(f)
                                # Try account-specific key first (e.g. "axis-secondary"),
                                # fall back to bank-level key (e.g. "axis")
                                parts = matched_account.split("-")
                                specific_key = "-".join(parts[1:]).lower()
                                bank_key = (
                                    parts[1].lower()
                                    if len(parts) > 1
                                    else matched_account.lower()
                                )
                                password = pw_data.get(
                                    specific_key, pw_data.get(bank_key, "")
                                )
                    except Exception as e:
                        logger.warning(
                            f"Failed to load password for {matched_account}: {e}"
                        )

                    passed_txns, pdf_clean = self._parse_validate_pdf(
                        pdf_bytes,
                        password,
                        config,
                        matched_account,
                        stmt_end_date,
                        file_name,
                    )
                    if passed_txns:
                        df = pd.DataFrame(passed_txns)
                        df.replace("", pd.NA, inplace=True)
                        # Parse the verbatim date tokens deterministically with
                        # the account's own formats (LLM no longer reformats).
                        # Always allow ISO as a fallback: it is unambiguous, so
                        # strptime matches it exactly and we never fall through to
                        # parse_mixed_datetime's dayfirst pandas guess, which
                        # silently garbles ISO dates (2026-05-02 -> 2026-02-05).
                        pdf_date_formats = list(config.get("date_formats", []))
                        if "%Y-%m-%d" not in pdf_date_formats:
                            pdf_date_formats.append("%Y-%m-%d")
                        config = {
                            "column_map": {
                                "date": "date",
                                "description": "description",
                                "debit": "debit",
                                "credit": "credit",
                            },
                            "date_formats": pdf_date_formats,
                        }
                    else:
                        df = None
                else:
                    first_sheet_name = self.data_source.get_first_sheet_name_from_file(
                        file_id
                    )
                    if not first_sheet_name:
                        logger.warning(
                            f"Cannot get sheet name for {file_id}. Skipping."
                        )
                        continue

                    raw_statement_data = self.data_source.get_sheet_data(
                        file_id, first_sheet_name, "A:Z"
                    )
                    if not raw_statement_data:
                        logger.warning(
                            f"No data from statement Sheet '{file_name}'. Skipping."
                        )
                        continue

                    df = self._parse_statement_data_with_pandas(
                        raw_statement_data, config
                    )

                if df is not None and not df.empty:
                    std_df = self._standardize_parsed_df(df, config, matched_account)
                    if std_df is None or std_df.empty:
                        logger.warning(f"Standardization empty for '{file_name}'.")
                        continue
                    file_txns = std_df.to_dict("records")
                    for txn in file_txns:
                        if isinstance(txn["date"], pd.Timestamp):
                            txn["date"] = txn["date"].to_pydatetime()
                        if not isinstance(txn["date"], datetime.datetime):
                            txn["date"] = None
                    file_txns = [
                        txn
                        for txn in file_txns
                        if isinstance(txn["date"], datetime.datetime)
                    ]

                    # Universal future-date guard: a real transaction cannot be
                    # dated after today. Catches parser hallucinations on every
                    # path (PDF or Sheet) regardless of the incremental filter
                    # below, which — being forward-only — would otherwise let a
                    # future-dated row sail straight through.
                    now = datetime.datetime.now()
                    future = [t for t in file_txns if t["date"] > now]
                    if future:
                        logger.warning(
                            f"Dropped {len(future)} future-dated txn(s) from "
                            f"'{file_name}' (latest {max(t['date'] for t in future):%Y-%m-%d})."
                        )
                        file_txns = [t for t in file_txns if t["date"] <= now]

                    # Cache a clean PDF by its latest booked txn date (computed
                    # before the incremental filter, so it reflects the whole
                    # statement). Only when nothing was flagged — a statement with
                    # rows in review must keep re-parsing so a retry can rescue
                    # them.
                    if pdf_clean and file_txns:
                        newly_processed[file_id] = (
                            max(t["date"] for t in file_txns).date().isoformat()
                        )

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
                logger.error(f"Failed to process sheet '{file_name}': {e}", exc_info=e)

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

        if newly_processed:
            self.data_source.save_processed_statements(
                {**processed_cache, **newly_processed}
            )
            logger.info(
                f"Cached {len(newly_processed)} fully-parsed {account_type} "
                "statement(s) to skip next run."
            )
        return all_parsed_txns

    @staticmethod
    def _clean_cell(value: Any, default: Any = "") -> Any:
        """Coerce None / NaN to a JSON-safe default.

        Empty sheet cells come back from pandas as float('nan'); left as-is they
        serialize to a literal ``NaN`` token and the Sheets API rejects the
        whole payload ("Invalid JSON payload ... Unexpected token").
        """
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return default
        return value

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
                    self._clean_cell(txn.get("description", "")),
                    debit,
                    credit,
                    self._clean_cell(txn.get("category"), DEFAULT_CATEGORY),
                    self._clean_cell(txn.get("remarks", "")),
                    self._clean_cell(txn.get("account"), "Unknown"),
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
        # write_transactions_to_log is a safe overwrite (write-then-trim); do NOT
        # pre-clear here or a failed write would leave the log empty.
        self.data_source.write_transactions_to_log(account_type, data_values)
        self._verify_overwrite(account_type, len(data_values))

    def _verify_overwrite(self, account_type: str, expected: int) -> None:
        """Read the log back and confirm it holds exactly `expected` data rows.

        A silent partial write or a truncated read-back would otherwise leave the
        sheet short without anyone noticing; fail loudly so the run stops and the
        user can restore from backup (--restore-db).
        """
        readback = self.data_source.get_transaction_log_data(account_type)
        rows = [r for r in readback if any(str(c).strip() for c in r)]
        # Drop a leading header row if present (Sheets range starts at the header).
        if rows and str(rows[0][0]).strip().lower() == "date":
            rows = rows[1:]
        if len(rows) != expected:
            log_and_exit(
                logger,
                f"Post-write verification FAILED for {account_type}: wrote {expected} "
                f"rows but read back {len(rows)}. The sheet may be truncated/corrupted "
                f"- restore from backup with `python main.py --restore-db`.",
            )
        logger.info(
            f"Verified {account_type} log: {len(rows)} rows written and read back."
        )

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
