# Design Doc: Local CSV Data Source

**Link to Issue:** [#6](https://github.com/mohankarthik/gajana/issues/6)

## 1. Motivation

To allow users to run the application without a dependency on Google Cloud APIs. This involves reading and writing all transaction data from/to local CSV files, making the tool more portable, accessible, and suitable for users who prefer to keep their financial data on their local machine.

## 2. High-Level Design

We will introduce a new data source type, `local`, which will be configured in the existing `src/constants.py` file. When `local` is chosen, the application will use a new `LocalDataSource` class that reads from and writes to user-configured local CSV file paths. The `DataSourceInterface` will be refactored to be more generic and less specific to the Google Sheets implementation.

## 3. Detailed Design

### A. Configuration (`src/constants.py`)

Configuration will be managed by adding new variables to the existing `src/constants.py` file. This avoids creating a new settings file and keeps configuration centralized until a broader settings refactor is complete.

```python
# src/constants.py
# ... existing constants ...
import os

# --- Core Settings ---
# Specify the data source to use: "google" or "local"
DATA_SOURCE_TYPE = "google"

# --- Local Data Source Settings ---
# These settings are used only if DATA_SOURCE_TYPE is "local"
LOCAL_DATA_PATH = os.path.expanduser("~/Documents/gajana_data")

LOCAL_MASTER_BANK_TRANSACTIONS_FILE = os.path.join(LOCAL_DATA_PATH, "master", "bank_transactions.csv")
LOCAL_MASTER_CC_TRANSACTIONS_FILE = os.path.join(LOCAL_DATA_PATH, "master", "cc_transactions.csv")
LOCAL_STATEMENTS_PATH = os.path.join(LOCAL_DATA_PATH, "statements")

# ... other existing constants ...
```

The `main.py` file will import these constants to determine which data source to instantiate.

### B. Refactored `DataSourceInterface`

The `DataSourceInterface` in `src/interfaces.py` will be refactored to be more abstract and less tied to the concept of a "sheet".

**Proposed Refactored Interface:**
```python
# src/interfaces.py

# ... (DataSourceFile remains the same)

class DataSourceInterface(abc.ABC):
    """
    Abstract Base Class defining the interface for data source operations.
    """

    @abc.abstractmethod
    def list_statement_files(self) -> List[DataSourceFile]:
        """Lists available statement files."""
        pass

    @abc.abstractmethod
    def get_statement_data(self, statement_file: DataSourceFile) -> List[List[Any]]:
        """Fetches raw data from a specific statement file."""
        pass

    @abc.abstractmethod
    def get_transaction_log_data(self, log_type: str) -> List[List[Any]]:
        """Fetches raw data from the main transaction log ('bank' or 'cc')."""
        pass

    @abc.abstractmethod
    def append_transactions_to_log(self, log_type: str, data_values: List[List[Any]]) -> None:
        """Appends new rows to the specified transaction log."""
        pass

    @abc.abstractmethod
    def clear_transaction_log(self, log_type: str) -> None:
        """Clears all data (but not headers) from the specified transaction log."""
        pass

    @abc.abstractmethod
    def write_transactions_to_log(self, log_type: str, data_values: List[List[Any]]) -> None:
        """Overwrites the specified transaction log with new data."""
        pass
```
*Note: `get_first_sheet_name_from_file` and `get_sheet_data` are removed/refactored as their logic is now encapsulated within the specific data source implementations.*

### C. New Class: `src/local_data_source.py`

A new `LocalDataSource` class will be created to implement the refactored `DataSourceInterface`.

```python
# src/local_data_source.py
import csv
import os
from typing import List, Any

from src.interfaces import DataSourceInterface, DataSourceFile
from src.constants import (
    LOCAL_STATEMENTS_PATH,
    LOCAL_MASTER_BANK_TRANSACTIONS_FILE,
    LOCAL_MASTER_CC_TRANSACTIONS_FILE
)

class LocalDataSource(DataSourceInterface):

    def list_statement_files(self) -> List[DataSourceFile]:
        # Scans the LOCAL_STATEMENTS_PATH directory for .csv files.
        # Returns a list of DataSourceFile objects.
        # The file's path will be its ID, and its basename will be its name.

    def get_statement_data(self, statement_file: DataSourceFile) -> List[List[Any]]:
        # Reads the CSV file from the path specified in statement_file.id

    def get_transaction_log_data(self, log_type: str) -> List[List[Any]]:
        # Reads from the appropriate master transaction CSV file based on log_type.

    def append_transactions_to_log(self, log_type: str, data_values: List[List[Any]]) -> None:
        # Appends rows to the appropriate master transaction CSV file.

    def clear_transaction_log(self, log_type: str) -> None:
        # Overwrites the relevant master CSV file with only its header row.

    def write_transactions_to_log(self, log_type: str, data_values: List[List[Any]]) -> None:
        # Overwrites the relevant master CSV file with a header and the new data.
```

### D. `GoogleDataSource` Updates

The `GoogleDataSource` will be updated to implement the new `DataSourceInterface`. The logic for finding the first visible sheet within a Google Sheet file will be moved inside the `get_statement_data` method, making it an internal detail of the `GoogleDataSource`.

## 4. Implementation Plan

1.  **Update `src/constants.py`:** Add new constants for `DATA_SOURCE_TYPE` and local paths.
2.  **Refactor `src/interfaces.py`:** Update the `DataSourceInterface` as described above.
3.  **Create `src/local_data_source.py`:** Implement the `LocalDataSource` class.
4.  **Update `src/google_data_source.py`:** Adapt `GoogleDataSource` to the new interface.
5.  **Update `main.py`:** Import from `constants.py` and instantiate the correct data source based on `DATA_SOURCE_TYPE`.
6.  **Add Unit Tests:** Create `tests/test_local_data_source.py` and add tests for the new class. Update tests for `GoogleDataSource` if needed.
7.  **Documentation:** Update `README.md` to explain the new configuration options.

## 5. Risks and Mitigations

*   **Risk:** Breaking changes to the interface could affect other parts of the application.
*   **Mitigation:** A thorough review of where `DataSourceInterface` methods are called (`TransactionProcessor`) will be conducted during implementation. The test suite will be crucial for catching regressions.
*   **Risk:** CSV parsing can be fragile if formats are inconsistent.
*   **Mitigation:** The implementation will be defensive, with clear error handling for malformed CSV files. The initial implementation will assume a standard CSV format consistent with the existing Google Sheets structure.