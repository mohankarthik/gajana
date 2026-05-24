"""Local CSV implementation of the DataSourceInterface for testing and local use."""

from __future__ import annotations

import csv
import logging
import os
from typing import Any, List, Optional

from src.interfaces import DataSourceFile, DataSourceInterface
from src.constants import (
    BANK_TRANSACTIONS_SHEET_NAME,
    CC_TRANSACTIONS_SHEET_NAME,
    EXPECTED_SHEET_COLUMNS,
)

logger = logging.getLogger(__name__)


class CSVDataSource(DataSourceInterface):
    """CSV implementation of the DataSourceInterface.

    Handles reading and writing transaction data using local CSV files.
    Useful for end-to-end testing without external API dependencies.
    """

    def __init__(self, root_path: str) -> None:
        """Constructor.

        Args:
            root_path: The base directory where CSV files are stored.
                       It should contain a 'statements' directory and 
                       the master log files.
        """
        self.root_path = root_path
        self.statements_path = os.path.join(root_path, "statements")
        self.logger = logger
        
        # Ensure directories exist
        os.makedirs(self.statements_path, exist_ok=True)
        
        # Initialize master log files if they don't exist
        self._init_log_file(BANK_TRANSACTIONS_SHEET_NAME)
        self._init_log_file(CC_TRANSACTIONS_SHEET_NAME)

    def _get_log_path(self, log_type: str) -> str:
        """Helper to get the full path for a transaction log file."""
        sheet_name = (
            BANK_TRANSACTIONS_SHEET_NAME
            if log_type == "bank"
            else CC_TRANSACTIONS_SHEET_NAME
        )
        return os.path.join(self.root_path, f"{sheet_name}.csv")

    def _init_log_file(self, sheet_name: str) -> None:
        """Creates the CSV file with headers if it doesn't exist."""
        path = os.path.join(self.root_path, f"{sheet_name}.csv")
        if not os.path.exists(path):
            self.logger.info(f"Initializing master log: {path}")
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                # First column is usually empty in the sheet or contains an ID, 
                # but based on range B2:H, we map to EXPECTED_SHEET_COLUMNS directly.
                # However, the Google Sheets range is B2:H, and the interface returns List[List[Any]].
                # To match Google Sheet behavior exactly, we should store data 
                # consistent with how it's retrieved.
                writer.writerow(EXPECTED_SHEET_COLUMNS)

    def list_statement_file_details(self) -> List[DataSourceFile]:
        """Lists details of CSV files in the statements directory."""
        files_details: List[DataSourceFile] = []
        self.logger.info(f"Listing CSV statements from: {self.statements_path}")
        
        if not os.path.exists(self.statements_path):
            return files_details

        for filename in os.listdir(self.statements_path):
            if filename.endswith(".csv") or filename.endswith(".gsheet.csv"):
                # We use the filename as the ID for local files
                files_details.append(DataSourceFile(filename, filename))
        
        self.logger.info(f"Found {len(files_details)} CSV statement files.")
        return files_details

    def get_sheet_data(
        self, source_id: str, sheet_name: Optional[str], range_spec: str
    ) -> List[List[Any]]:
        """Fetches raw data from a specific local CSV file.
        
        For statements, source_id is the filename.
        range_spec is currently ignored for simplicity or could be implemented if needed.
        """
        file_path = os.path.join(self.statements_path, source_id)
        # If not in statements, check if it's one of the master logs
        if not os.path.exists(file_path):
            file_path = os.path.join(self.root_path, source_id if source_id.endswith(".csv") else f"{source_id}.csv")

        if not os.path.exists(file_path):
            self.logger.warning(f"File not found: {file_path}")
            return []

        self.logger.debug(f"Reading CSV data from: {file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                return list(reader)
        except Exception as e:
            self.logger.error(f"Error reading CSV {file_path}: {e}")
            return []

    def get_transaction_log_data(self, log_type: str) -> List[List[Any]]:
        """Fetches data from the local master CSV log."""
        path = self._get_log_path(log_type)
        self.logger.info(f"Reading {log_type} log from: {path}")
        
        if not os.path.exists(path):
            return []
            
        try:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                data = list(reader)
                # Google Sheets range B2:H seems to include the header as the first row.
                # So we must return the entire data including the header to match GoogleDataSource.
                return data
        except Exception as e:
            self.logger.error(f"Error reading path {path}: {e}")
            return []

    def append_transactions_to_log(
        self, log_type: str, data_values: List[List[Any]]
    ) -> None:
        """Appends rows to the local master CSV log."""
        if not data_values:
            return
            
        path = self._get_log_path(log_type)
        self.logger.info(f"Appending {len(data_values)} rows to {path}")
        
        try:
            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(data_values)
        except Exception as e:
            self.logger.error(f"Error appending to {path}: {e}")

    def clear_transaction_log_range(self, log_type: str) -> None:
        """Clears data from the local master CSV log (keeps header)."""
        path = self._get_log_path(log_type)
        self.logger.info(f"Clearing log: {path}")
        
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(EXPECTED_SHEET_COLUMNS)
        except Exception as e:
            self.logger.error(f"Error clearing {path}: {e}")

    def write_transactions_to_log(
        self, log_type: str, data_values: List[List[Any]]
    ) -> None:
        """Overwrites data in the local master CSV log (after header)."""
        self.clear_transaction_log_range(log_type)
        self.append_transactions_to_log(log_type, data_values)

    def get_first_sheet_name_from_file(self, file_id: str) -> Optional[str]:
        """For CSV, we just return the filename or a dummy value."""
        return "Sheet1"
