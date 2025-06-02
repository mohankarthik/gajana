# tests/test_interfaces.py
from __future__ import annotations

import abc
import pytest
from typing import Any, List, Optional

# Assuming interfaces.py is in gajana package (src/gajana/interfaces.py)
from src.interfaces import DataSourceFile, DataSourceInterface

# --- Tests for DataSourceFile ---


def test_data_source_file_creation():
    """Test the creation of a DataSourceFile object and attribute assignment."""
    file_id = "test_id_123"
    file_name = "test_statement_file.csv"
    ds_file = DataSourceFile(id=file_id, name=file_name)

    assert ds_file.id == file_id
    assert ds_file.name == file_name


def test_data_source_file_attributes():
    """Test that attributes can be accessed."""
    ds_file = DataSourceFile(id="another_id", name="another_name.gsheet")
    assert hasattr(ds_file, "id")
    assert hasattr(ds_file, "name")
    assert ds_file.id == "another_id"


# --- Tests for DataSourceInterface (ABC) ---


def test_data_source_interface_is_abc():
    """Test that DataSourceInterface is an Abstract Base Class."""
    assert hasattr(DataSourceInterface, "__abstractmethods__")
    assert issubclass(DataSourceInterface, abc.ABC)


def test_data_source_interface_cannot_be_instantiated():
    """Test that DataSourceInterface itself cannot be instantiated."""
    with pytest.raises(TypeError) as excinfo:
        DataSourceInterface()  # type: ignore
    assert "Can't instantiate abstract class DataSourceInterface" in str(excinfo.value)


def test_data_source_interface_requires_all_methods_implemented():
    """
    Test that a concrete subclass must implement all abstract methods.
    This is more of a demonstration of the ABC contract.
    """

    # Define a minimal concrete class that *doesn't* implement all methods
    class IncompleteDataSource(DataSourceInterface):
        def list_statement_file_details(self) -> List[DataSourceFile]:
            return []  # Implemented

        # get_sheet_data is missing
        def get_transaction_log_data(self, log_type: str) -> List[List[Any]]:
            return []  # Implemented

        def append_transactions_to_log(
            self, log_type: str, data_values: List[List[Any]]
        ) -> None:
            pass  # Implemented

        def clear_transaction_log_range(self, log_type: str) -> None:
            pass  # Implemented

        def write_transactions_to_log(
            self, log_type: str, data_values: List[List[Any]]
        ) -> None:
            pass  # Implemented

        def get_first_sheet_name_from_file(self, file_id: str) -> Optional[str]:
            return None  # Implemented

    with pytest.raises(TypeError) as excinfo:
        IncompleteDataSource()  # type: ignore
    # The error message will list the missing abstract methods
    assert "Can't instantiate abstract class IncompleteDataSource" in str(excinfo.value)
    assert "get_sheet_data" in str(excinfo.value)  # Check one of the missing ones

    # Define a complete concrete class
    class CompleteDataSource(DataSourceInterface):
        def list_statement_file_details(self) -> List[DataSourceFile]:
            return [DataSourceFile(id="dummy_id", name="dummy_name")]

        def get_sheet_data(
            self, source_id: str, sheet_name: Optional[str], range_spec: str
        ) -> List[List[Any]]:
            return [["dummy_data"]]

        def get_transaction_log_data(self, log_type: str) -> List[List[Any]]:
            return [["log_data"]]

        def append_transactions_to_log(
            self, log_type: str, data_values: List[List[Any]]
        ) -> None:
            pass

        def clear_transaction_log_range(self, log_type: str) -> None:
            pass

        def write_transactions_to_log(
            self, log_type: str, data_values: List[List[Any]]
        ) -> None:
            pass

        def get_first_sheet_name_from_file(self, file_id: str) -> Optional[str]:
            return "Sheet1"

    # This should not raise an error
    try:
        complete_ds = CompleteDataSource()
        assert complete_ds is not None
        # Optionally, call methods to ensure they are callable (though they are minimal here)
        assert complete_ds.list_statement_file_details()[0].name == "dummy_name"
        assert complete_ds.get_sheet_data("id", "name", "A1") == [["dummy_data"]]
    except TypeError:
        pytest.fail("CompleteDataSource should be instantiable.")
