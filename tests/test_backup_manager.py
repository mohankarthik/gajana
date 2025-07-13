# tests/test_backup_manager.py
from __future__ import annotations

import datetime
import os
import sqlite3
import tempfile
from typing import Any, Dict, Generator, Hashable, List

import pytest

from src.backup_manager import SQLiteBackupManager

# --- Fixtures ---


@pytest.fixture(autouse=True)
def mock_log_and_exit_fixture(mocker):
    """Mocks the log_and_exit utility to prevent tests from exiting."""
    return mocker.patch("src.backup_manager.log_and_exit", side_effect=SystemExit)


@pytest.fixture
def sample_transactions() -> List[Dict[Hashable, Any]]:
    """Provides a list of sample transaction dictionaries for testing."""
    return [
        {
            "date": datetime.datetime(2024, 7, 10, 10, 0, 0),
            "description": "Coffee Shop",
            "amount": -4.50,
            "category": "Food & Drink",
            "remarks": "Morning coffee",
            "account": "cc-hdfc-infiniametal",
        },
        {
            "date": datetime.datetime(2024, 7, 11, 15, 30, 0),
            "description": "Salary Deposit",
            "amount": 2500.00,
            "category": "Income",
            "remarks": "July Salary",
            "account": "bank-hdfc-karti",
        },
        {
            "date": datetime.datetime(2024, 7, 12, 12, 0, 0),
            "description": "Online Shopping",
            "amount": -75.99,
            "category": "Shopping",
            "remarks": "",
            "account": "cc-icici-amazonpay",
        },
    ]


@pytest.fixture
def file_db_manager() -> Generator[Any, Any, Any]:
    """
    Creates an instance of SQLiteBackupManager that uses a temporary
    file-based database, which is cleaned up after the test. This is necessary
    because the class opens and closes the connection on each operation.
    """
    # Create a temporary file and get its path
    db_fd, db_path = tempfile.mkstemp(suffix=".db")

    # Instantiate the manager with the path to the temporary file.
    # The __init__ method will create the file and the table.
    manager = SQLiteBackupManager(db_path=db_path)

    yield manager

    # Teardown: close the file descriptor and remove the temp file
    os.close(db_fd)
    os.unlink(db_path)


# --- Test Cases ---


def test_create_table(file_db_manager: SQLiteBackupManager):
    """Tests that the transactions table is created upon initialization."""
    # Connect to the temporary database file to inspect it
    conn = sqlite3.connect(file_db_manager.db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='transactions';"
        )
        table = cursor.fetchone()
        assert table is not None, "The 'transactions' table should exist."
        assert table[0] == "transactions"
    finally:
        conn.close()


def test_backup_and_restore_cycle(
    file_db_manager: SQLiteBackupManager, sample_transactions: List[Dict]
):
    """Tests a full backup and restore cycle to ensure data integrity."""
    # 1. Backup the data
    file_db_manager.backup(sample_transactions)

    # 2. Restore the data
    restored_txns = file_db_manager.restore()

    # 3. Assertions
    assert len(restored_txns) == len(sample_transactions)

    # Sort both lists to ensure consistent order for comparison
    sample_transactions.sort(key=lambda x: x["date"])
    restored_txns.sort(key=lambda x: x["date"])

    for original, restored in zip(sample_transactions, restored_txns):
        assert original["date"] == restored["date"]
        assert original["description"] == restored["description"]
        assert original["account"] == restored["account"]
        # Compare amounts as floats
        assert pytest.approx(original["amount"]) == pytest.approx(restored["amount"])


def test_backup_upsert_logic(
    file_db_manager: SQLiteBackupManager, sample_transactions: List[Dict]
):
    """Tests that the backup function correctly updates existing records (upsert)."""
    # 1. Initial backup
    file_db_manager.backup(sample_transactions)
    assert len(file_db_manager.restore()) == 3

    # 2. Prepare updated and new data
    # Modify an existing transaction
    modified_txn = sample_transactions[0].copy()
    modified_txn["category"] = "Updated Category"

    # Add a new transaction
    new_txn = {
        "date": datetime.datetime(2024, 7, 13, 8, 0, 0),
        "description": "New Expense",
        "amount": -15.00,
        "category": "Misc",
        "remarks": "",
        "account": "cc-hdfc-infiniametal",
    }

    # 3. Perform a second backup with the mixed list
    file_db_manager.backup([modified_txn, new_txn])

    # 4. Restore and assert
    restored_txns = file_db_manager.restore()

    # The total count should be 4 (3 original + 1 new)
    assert len(restored_txns) == 4

    # Find the modified transaction and check its category
    found_modified = False
    for txn in restored_txns:
        if txn["description"] == "Coffee Shop":
            assert txn["category"] == "Updated Category"
            found_modified = True
            break
    assert found_modified, "The modified transaction was not found or not updated."


def test_backup_with_no_transactions(file_db_manager: SQLiteBackupManager, caplog):
    """Tests that calling backup with an empty list does not error out."""
    file_db_manager.backup([])
    assert "No transactions provided to back up." in caplog.text

    # Ensure the database is still empty
    restored = file_db_manager.restore()
    assert len(restored) == 0


def test_restore_from_empty_db(file_db_manager: SQLiteBackupManager):
    """Tests that restoring from an empty database returns an empty list."""
    restored_txns = file_db_manager.restore()
    assert restored_txns == []


def test_generate_txn_id_is_deterministic(sample_transactions: List[Dict]):
    """Ensures that the ID generation is consistent for the same transaction."""
    txn = sample_transactions[0]
    id1 = SQLiteBackupManager._generate_txn_id(txn)
    id2 = SQLiteBackupManager._generate_txn_id(txn)
    assert id1 == id2

    # Check that a small change results in a different ID
    modified_txn = txn.copy()
    modified_txn["amount"] = -4.51
    id3 = SQLiteBackupManager._generate_txn_id(modified_txn)
    assert id1 != id3
