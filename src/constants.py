# gajana/constants.py
from __future__ import annotations

import json
import logging
import os

from src.settings import (  # re-exported for backward compat
    BANK_ACCOUNTS,
    BANK_TRANSACTIONS_FULL_RANGE,
    BANK_TRANSACTIONS_SHEET_NAME,
    CC_ACCOUNTS,
    CC_TRANSACTIONS_FULL_RANGE,
    CC_TRANSACTIONS_SHEET_NAME,
    CSV_FOLDER,
    TRANSACTIONS_SHEET_ID,
)
from src.utils import log_and_exit

__all__ = [
    "BANK_ACCOUNTS",
    "BANK_TRANSACTIONS_FULL_RANGE",
    "BANK_TRANSACTIONS_SHEET_NAME",
    "CC_ACCOUNTS",
    "CC_TRANSACTIONS_FULL_RANGE",
    "CC_TRANSACTIONS_SHEET_NAME",
    "CSV_FOLDER",
    "TRANSACTIONS_SHEET_ID",
]

logger = logging.getLogger(__name__)

CONFIG_DIR = "data/configs"


def load_parsing_config(config_path: str = CONFIG_DIR) -> dict:
    loaded_config = {}
    logger.info(f"Loading parsing configs from: {config_path}")
    if not os.path.exists(config_path):
        log_and_exit(logger, f"Configuration directory not found: {config_path}")

    for filename in os.listdir(config_path):
        if filename.endswith(".json"):
            config_key = filename[:-5]
            file_path = os.path.join(config_path, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    loaded_config[config_key] = json.load(f)
                logger.debug(f"Successfully loaded config: {config_key}")
            except (json.JSONDecodeError, IOError) as e:
                log_and_exit(
                    logger, f"Failed to load or parse config file {filename}: {e}", e
                )

    logger.info(f"Loaded {len(loaded_config)} parsing configurations.")
    return loaded_config


# --- Google API Configuration ---
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
SERVICE_ACCOUNT_KEY_FILE = "secrets/google.json"

# --- Database Configuration ---
DB_FILE_PATH = "backups/gajana.db"

# --- Sheet Data Ranges ---
BANK_TRANSACTIONS_DATA_RANGE = "B2:H"
CC_TRANSACTIONS_DATA_RANGE = "B2:H"

# Standardized column names used internally after parsing
EXPECTED_SHEET_COLUMNS = [
    "Date",
    "Description",
    "Debit",
    "Credit",
    "Category",
    "Remarks",
    "Account",
]
INTERNAL_TXN_KEYS = ["date", "description", "amount", "category", "remarks", "account"]

# --- Categorization ---
# Personal rules live in data/matchers.json (gitignored). Fall back to the
# committed example so a fresh clone runs without extra setup.
MATCHERS_FILE_PATH = (
    "data/matchers.json"
    if os.path.exists("data/matchers.json")
    else "data/matchers.example.json"
)
DEFAULT_CATEGORY = "Uncategorized"

# --- Parsing Configuration ---
PARSING_CONFIG = load_parsing_config()
