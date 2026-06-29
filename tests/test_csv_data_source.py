# tests/test_csv_data_source.py
from __future__ import annotations

import csv
import os
from unittest.mock import patch
import pytest
from src.csv_data_source import CSVDataSource
from src.constants import (
    BANK_TRANSACTIONS_SHEET_NAME,
    CC_TRANSACTIONS_SHEET_NAME,
    EXPECTED_SHEET_COLUMNS,
)


@pytest.fixture
def temp_csv_dir(tmp_path) -> str:
    """Fixture returning a temporary root path for CSV data source."""
    return str(tmp_path)


def test_csv_data_source_init(temp_csv_dir):
    """Tests constructor, directories creation, and master log initialization."""
    ds = CSVDataSource(temp_csv_dir)
    assert ds.root_path == temp_csv_dir
    assert ds.statements_path == os.path.join(temp_csv_dir, "statements")
    assert os.path.isdir(ds.statements_path)

    bank_log = os.path.join(temp_csv_dir, f"{BANK_TRANSACTIONS_SHEET_NAME}.csv")
    cc_log = os.path.join(temp_csv_dir, f"{CC_TRANSACTIONS_SHEET_NAME}.csv")
    assert os.path.isfile(bank_log)
    assert os.path.isfile(cc_log)

    # Verify headers are written
    with open(bank_log, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == EXPECTED_SHEET_COLUMNS


def test_list_statement_file_details(temp_csv_dir):
    """Tests listing statement files in statements directory (including PDFs)."""
    ds = CSVDataSource(temp_csv_dir)

    # Initially should be empty
    assert ds.list_statement_file_details() == []

    # Add some files
    statements_dir = os.path.join(temp_csv_dir, "statements")
    with open(os.path.join(statements_dir, "stat1.csv"), "w") as f:
        f.write("a,b,c")
    with open(os.path.join(statements_dir, "stat2.gsheet.csv"), "w") as f:
        f.write("x,y,z")
    with open(os.path.join(statements_dir, "stat3.pdf"), "w") as f:
        f.write("pdf")
    with open(os.path.join(statements_dir, "not_a_csv.txt"), "w") as f:
        f.write("hello")

    details = ds.list_statement_file_details()
    assert len(details) == 3
    ids = {d.id for d in details}
    names = {d.name for d in details}
    assert ids == {"stat1.csv", "stat2.gsheet.csv", "stat3.pdf"}
    assert names == {"stat1.csv", "stat2.gsheet.csv", "stat3.pdf"}


def test_list_statement_file_details_nonexistent_dir(temp_csv_dir):
    """Tests listing statement files when statements directory does not exist."""
    ds = CSVDataSource(temp_csv_dir)
    os.rmdir(ds.statements_path)
    assert ds.list_statement_file_details() == []


def test_get_sheet_data_statements(temp_csv_dir):
    """Tests fetching sheet data for a statement file."""
    ds = CSVDataSource(temp_csv_dir)
    statements_dir = os.path.join(temp_csv_dir, "statements")
    stmt_file = "stmt.csv"

    # Write test data
    with open(
        os.path.join(statements_dir, stmt_file), "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.writer(f)
        writer.writerow(["Col1", "Col2"])
        writer.writerow(["val1", "val2"])

    data = ds.get_sheet_data(stmt_file, None, "A1:B2")
    assert data == [["Col1", "Col2"], ["val1", "val2"]]


def test_get_sheet_data_master_logs(temp_csv_dir):
    """Tests fetching sheet data for a master log when not found in statements."""
    ds = CSVDataSource(temp_csv_dir)
    data = ds.get_sheet_data(BANK_TRANSACTIONS_SHEET_NAME, None, "A1:G2")
    assert data == [EXPECTED_SHEET_COLUMNS]


def test_get_sheet_data_not_found(temp_csv_dir):
    """Tests fetching sheet data for a non-existent file."""
    ds = CSVDataSource(temp_csv_dir)
    data = ds.get_sheet_data("nonexistent.csv", None, "A1:B2")
    assert data == []


def test_get_sheet_data_exception(temp_csv_dir):
    """Tests exception handling during reading sheet data."""
    ds = CSVDataSource(temp_csv_dir)
    with patch("builtins.open", side_effect=Exception("Read error")):
        data = ds.get_sheet_data(f"{BANK_TRANSACTIONS_SHEET_NAME}.csv", None, "A1")
        assert data == []


def test_get_transaction_log_data(temp_csv_dir):
    """Tests reading master transaction logs."""
    ds = CSVDataSource(temp_csv_dir)

    # Verify initial data is just the header
    data = ds.get_transaction_log_data("bank")
    assert data == [EXPECTED_SHEET_COLUMNS]

    # Write extra rows
    bank_path = ds._get_log_path("bank")
    with open(bank_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["2026-05-24", "Desc", "10.0", "bank-acc", "category", "remarks", "10.0"]
        )

    data = ds.get_transaction_log_data("bank")
    assert len(data) == 2
    assert data[0] == EXPECTED_SHEET_COLUMNS
    assert data[1] == [
        "2026-05-24",
        "Desc",
        "10.0",
        "bank-acc",
        "category",
        "remarks",
        "10.0",
    ]


def test_get_transaction_log_data_not_found(temp_csv_dir):
    """Tests reading log when the path does not exist."""
    ds = CSVDataSource(temp_csv_dir)
    bank_path = ds._get_log_path("bank")
    if os.path.exists(bank_path):
        os.remove(bank_path)

    data = ds.get_transaction_log_data("bank")
    assert data == []


def test_get_transaction_log_data_exception(temp_csv_dir):
    """Tests exception handling when reading master logs."""
    ds = CSVDataSource(temp_csv_dir)
    with patch("builtins.open", side_effect=Exception("Read error")):
        data = ds.get_transaction_log_data("bank")
        assert data == []


def test_append_transactions_to_log(temp_csv_dir):
    """Tests appending transactions to the master logs."""
    ds = CSVDataSource(temp_csv_dir)

    # Empty data should not write/fail
    ds.append_transactions_to_log("bank", [])

    rows_to_append = [
        ["2026-05-24", "Desc1", "10.0", "acc1", "cat1", "rem1", "10.0"],
        ["2026-05-25", "Desc2", "20.0", "acc2", "cat2", "rem2", "20.0"],
    ]

    ds.append_transactions_to_log("bank", rows_to_append)

    data = ds.get_transaction_log_data("bank")
    assert len(data) == 3
    assert data[1] == rows_to_append[0]
    assert data[2] == rows_to_append[1]


def test_append_transactions_to_log_exception(temp_csv_dir):
    """Tests exception handling when appending transactions."""
    ds = CSVDataSource(temp_csv_dir)
    with patch("builtins.open", side_effect=Exception("Append error")):
        ds.append_transactions_to_log("bank", [["row"]])


def test_clear_transaction_log_range(temp_csv_dir):
    """Tests clearing transaction log while keeping header."""
    ds = CSVDataSource(temp_csv_dir)

    # Append first
    ds.append_transactions_to_log("bank", [["2026-05-24", "Desc1"]])
    assert len(ds.get_transaction_log_data("bank")) == 2

    ds.clear_transaction_log_range("bank")
    data = ds.get_transaction_log_data("bank")
    assert len(data) == 1
    assert data == [EXPECTED_SHEET_COLUMNS]


def test_clear_transaction_log_range_exception(temp_csv_dir):
    """Tests exception handling when clearing log range."""
    ds = CSVDataSource(temp_csv_dir)
    with patch("builtins.open", side_effect=Exception("Clear error")):
        ds.clear_transaction_log_range("bank")


def test_write_transactions_to_log(temp_csv_dir):
    """Tests overwriting master transaction log with new data."""
    ds = CSVDataSource(temp_csv_dir)

    # Pre-populate
    ds.append_transactions_to_log("cc", [["old_row"]])

    new_rows = [["2026-05-24", "NewCC", "15.0", "cc1", "cat1", "rem1", "15.0"]]

    ds.write_transactions_to_log("cc", new_rows)

    data = ds.get_transaction_log_data("cc")
    assert len(data) == 2
    assert data[0] == EXPECTED_SHEET_COLUMNS
    assert data[1] == new_rows[0]


def test_get_first_sheet_name_from_file(temp_csv_dir):
    """Tests getting first sheet name."""
    ds = CSVDataSource(temp_csv_dir)
    assert ds.get_first_sheet_name_from_file("file.csv") == "Sheet1"


def test_csv_data_source_download_file(temp_csv_dir):
    """Tests downloading/reading local file bytes."""
    ds = CSVDataSource(temp_csv_dir)
    file_path = os.path.join(ds.statements_path, "statement.pdf")

    with open(file_path, "wb") as f:
        f.write(b"dummy pdf content")

    content = ds.download_file("statement.pdf")
    assert content == b"dummy pdf content"
