# gajana/constants.py
from __future__ import annotations

import json
import logging
import os

from src.utils import log_and_exit

logger = logging.getLogger(__name__)

CONFIG_DIR = "data/configs"


def load_parsing_config(config_path: str = CONFIG_DIR) -> dict:
    """Loads all .json parsing configurations from the specified directory."""
    loaded_config = {}
    logger.info(f"Loading parsing configs from: {config_path}")
    if not os.path.exists(config_path):
        log_and_exit(logger, f"Configuration directory not found: {config_path}")

    for filename in os.listdir(config_path):
        if filename.endswith(".json"):
            config_key = filename[:-5]  # Remove .json extension
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

# --- Sheet Ranges for Consolidated Data ---
BANK_TRANSACTIONS_SHEET_NAME = "Bank transactions"
BANK_TRANSACTIONS_DATA_RANGE = "B2:H"
BANK_TRANSACTIONS_FULL_RANGE = (
    f"{BANK_TRANSACTIONS_SHEET_NAME}!{BANK_TRANSACTIONS_DATA_RANGE}"
)

CC_TRANSACTIONS_SHEET_NAME = "CC Transactions"
CC_TRANSACTIONS_DATA_RANGE = "B2:H"
CC_TRANSACTIONS_FULL_RANGE = (
    f"{CC_TRANSACTIONS_SHEET_NAME}!{CC_TRANSACTIONS_DATA_RANGE}"
)

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
DEFAULT_CATEGORY = "Uncategorized"

# --- Parsing Configuration ---
PARSING_CONFIG = load_parsing_config()
