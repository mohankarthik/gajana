# gajana/backup_manager.py
from __future__ import annotations

import datetime
import hashlib
import logging
import os
import sqlite3
from typing import Any, Dict, List

from src import config_manager
from src.constants import INTERNAL_TXN_KEYS
from src.interfaces import BackupInterface
from src.utils import log_and_exit

logger = logging.getLogger(__name__)


class SQLiteBackupManager(BackupInterface):
    """
    Manages backing up and restoring transaction data to a local SQLite database.
    """

    def __init__(
        self,
        db_path: str = config_manager.get_settings().get_setting(
            "database", "db_file_path"
        ),
    ):
        self.db_path = db_path
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._create_table()

    def _get_connection(self) -> sqlite3.Connection:
        """Establishes a connection to the SQLite database."""
        try:
            return sqlite3.connect(self.db_path, check_same_thread=False)
        except sqlite3.Error as e:
            log_and_exit(
                logger,
                f"Failed to connect to SQLite database at {self.db_path}: {e}",
                e,
            )
            raise

    def _create_table(self) -> None:
        """Creates the transactions table if it doesn't already exist."""
        conn = self._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS transactions (
                        id TEXT PRIMARY KEY,
                        date TEXT NOT NULL,
                        description TEXT,
                        amount REAL NOT NULL,
                        category TEXT,
                        remarks TEXT,
                        account TEXT NOT NULL
                    )
                    """
                )
            logger.info(f"Ensured 'transactions' table exists in {self.db_path}")
        except sqlite3.Error as e:
            log_and_exit(logger, f"Failed to create 'transactions' table: {e}", e)
        finally:
            conn.close()

    @staticmethod
    def _generate_txn_id(txn: Dict[str, Any]) -> str:
        """Generates a unique, deterministic ID for a transaction."""
        date_str = (
            txn["date"].strftime("%Y-%m-%d")
            if isinstance(txn["date"], datetime.datetime)
            else str(txn["date"])
        )

        # Create a stable string representation
        id_string = f"{date_str}-{txn.get('account', '')}-{txn.get('amount', 0.0):.2f}-{txn.get('description', '')}"

        # Return the SHA-256 hash of the string
        return hashlib.sha256(id_string.encode("utf-8")).hexdigest()

    def backup(self, transactions: List[Dict[str, Any]]) -> None:
        """
        Backs up a list of transactions to the SQLite database using an 'upsert' operation.
        If a transaction with the same unique ID exists, it's updated; otherwise, it's inserted.
        """
        if not transactions:
            logger.warning("No transactions provided to back up.")
            return

        conn = self._get_connection()
        upserted_count = 0
        try:
            with conn:
                for txn in transactions:
                    txn_id = self._generate_txn_id(txn)

                    # Ensure all keys are present before creating tuple
                    values_to_insert = [txn.get(key) for key in INTERNAL_TXN_KEYS]

                    # Convert datetime to ISO 8601 string format for storage
                    if isinstance(values_to_insert[0], datetime.datetime):
                        values_to_insert[0] = values_to_insert[0].isoformat()

                    # Prepend the generated ID
                    sql_tuple = (txn_id,) + tuple(values_to_insert)

                    conn.execute(
                        """
                        INSERT OR REPLACE INTO transactions (id, date, description, amount, category, remarks, account)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        sql_tuple,
                    )
                    upserted_count += 1
            logger.info(
                f"Successfully backed up (upserted) {upserted_count} transactions to {self.db_path}"
            )
        except sqlite3.Error as e:
            log_and_exit(logger, f"Failed during database backup operation: {e}", e)
        finally:
            conn.close()

    def restore(self) -> List[Dict[str, Any]]:
        """
        Restores all transactions from the SQLite database.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT date, description, amount, category, remarks, account FROM transactions"
            )
            rows = cursor.fetchall()

            restored_txns = []
            for row in rows:
                txn = dict(zip(INTERNAL_TXN_KEYS, row))
                # Convert date string back to datetime object
                try:
                    txn["date"] = datetime.datetime.fromisoformat(txn["date"])
                except (ValueError, TypeError):
                    logger.warning(
                        f"Could not parse date from DB, leaving as string: {txn['date']}"
                    )
                restored_txns.append(txn)

            logger.info(
                f"Successfully restored {len(restored_txns)} transactions from {self.db_path}"
            )
            return restored_txns
        except sqlite3.Error as e:
            log_and_exit(logger, f"Failed during database restore operation: {e}", e)
            raise
        finally:
            conn.close()
