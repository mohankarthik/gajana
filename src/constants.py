# gajana/constants.py
from __future__ import annotations

import json
import logging
import os
import re

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

# --- Ignore rules ---
# Descriptions that the normal pipeline must never book (e.g. the Google salary
# NEFT, which plugins/salary_splitter re-books as categorized split rows).
# Personal list lives in data/ignore.json (gitignored); fall back to the
# committed example so a fresh clone runs without extra setup.
IGNORE_FILE_PATH = (
    "data/ignore.json"
    if os.path.exists("data/ignore.json")
    else "data/ignore.example.json"
)


def load_ignore_rules(path: str = IGNORE_FILE_PATH) -> list[dict]:
    """Loads the description ignore-list. Missing/broken file → no rules."""
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            rules = json.load(f)
        if not isinstance(rules, list):
            logger.warning(f"Ignore file {path} is not a list; ignoring it.")
            return []
        logger.info(f"Loaded {len(rules)} ignore rule(s) from {path}.")
        return rules
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load ignore rules from {path}: {e}")
        return []


def _normalize_for_ignore(s: str) -> str:
    """Strip to lowercase alphanumerics so matching is robust to whitespace and
    punctuation. LLM PDF parsing sometimes injects stray spaces mid-word (e.g.
    "GOOGLE IT SERVICES I NDIA"), which would defeat a plain substring match."""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def txn_matches_ignore_rule(txn: dict, rules: list[dict]) -> bool:
    """True if a transaction matches any ignore rule. A rule matches when its
    ``description_contains`` substring is present (comparing on normalized
    alphanumerics, so case/whitespace/punctuation are ignored) and, if the rule
    specifies an ``account``, that account matches too."""
    desc = _normalize_for_ignore(str(txn.get("description", "")))
    acct = str(txn.get("account", ""))
    for rule in rules:
        rule_acct = rule.get("account")
        if rule_acct and rule_acct != acct:
            continue
        sub = _normalize_for_ignore(str(rule.get("description_contains", "")))
        if sub and sub in desc:
            return True
    return False


# --- Parsing Configuration ---
PARSING_CONFIG = load_parsing_config()
