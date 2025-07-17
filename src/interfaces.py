# gajana/interfaces.py
from __future__ import annotations

import abc
from typing import Any, List, Optional, Dict


class DataSourceFile:
    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name


class DataSourceInterface(abc.ABC):
    """
    Abstract Base Class defining the interface for data source operations.
    Implementations of this interface will handle the specifics of interacting
    with a particular data storage backend (e.g., Google Sheets, a database).
    """

    @abc.abstractmethod
    def list_statement_file_details(self) -> List[DataSourceFile]:
        """
        Lists details of statement files (e.g., Sheets in Google Drive).

        Returns:
            A list of dictionaries, where each dictionary contains details
            like 'id' and 'name' of a statement file.
        """
        pass

    @abc.abstractmethod
    def get_sheet_data(
        self, source_id: str, sheet_name: Optional[str], range_spec: str
    ) -> List[List[Any]]:
        """
        Fetches raw data from a specific sheet or source.

        Args:
            source_id: The identifier of the data source (e.g., Google Sheet ID).
            sheet_name: The specific sheet/table name within the source.
            If None, implementation might use a default or the first.
            range_spec: The range within the sheet to fetch (e.g., "A:Z", "A1:H100").

        Returns:
            A list of lists representing the raw row data.
        """
        pass

    @abc.abstractmethod
    def get_transaction_log_data(self, log_type: str) -> List[List[Any]]:
        """
        Fetches raw data from the main transaction log (e.g., "Bank transactions" sheet).

        Args:
            log_type: Identifier for the log (e.g., "bank", "cc").

        Returns:
            A list of lists representing the raw row data from the log.
        """
        pass

    @abc.abstractmethod
    def append_transactions_to_log(
        self, log_type: str, data_values: List[List[Any]]
    ) -> None:
        """
        Appends new rows of data to the specified transaction log.

        Args:
            log_type: Identifier for the log (e.g., "bank", "cc").
            data_values: A list of lists, where each inner list is a row to append.
        """
        pass

    @abc.abstractmethod
    def clear_transaction_log_range(self, log_type: str) -> None:
        """
        Clears the data range in the specified transaction log.

        Args:
            log_type: Identifier for the log (e.g., "bank", "cc").
        """
        pass

    @abc.abstractmethod
    def write_transactions_to_log(
        self, log_type: str, data_values: List[List[Any]]
    ) -> None:
        """
        Writes data to the specified transaction log, typically overwriting existing data in a defined range.

        Args:
            log_type: Identifier for the log (e.g., "bank", "cc").
            data_values: A list of lists, where each inner list is a row to write.
        """
        pass

    @abc.abstractmethod
    def get_first_sheet_name_from_file(self, file_id: str) -> Optional[str]:
        """Gets the name (title) of the first visible sheet in a spreadsheet file."""
        pass


class BackupInterface(abc.ABC):
    """
    Abstract Base Class defining the interface for backup operations.
    """

    @abc.abstractmethod
    def backup(self, transactions: List[Dict[str, Any]]) -> None:
        """
        Backs up a list of transactions to a persistent store.

        Args:
            transactions: A list of standardized transaction dictionaries.
        """
        pass

    @abc.abstractmethod
    def restore(self) -> List[Dict[str, Any]]:
        """
        Restores all transactions from the persistent store.

        Returns:
            A list of all standardized transaction dictionaries from the backup.
        """
        pass
