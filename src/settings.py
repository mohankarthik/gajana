from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

_SETTINGS_FILE = "settings.json"
_BANK_DATA_RANGE = "B2:H"
_CC_DATA_RANGE = "B2:H"


def _load() -> dict:
    if not os.path.exists(_SETTINGS_FILE):
        raise FileNotFoundError(
            f"{_SETTINGS_FILE} not found. Copy settings.example.json to settings.json and fill in your values."
        )
    with open(_SETTINGS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


_s = _load()

CSV_FOLDER: str = _s["google_drive_csv_folder_id"]
TRANSACTIONS_SHEET_ID: str = _s["google_sheets_transactions_id"]
BANK_TRANSACTIONS_SHEET_NAME: str = _s["bank_transactions_sheet_name"]
CC_TRANSACTIONS_SHEET_NAME: str = _s["cc_transactions_sheet_name"]
CC_ACCOUNTS: list[str] = _s["cc_accounts"]
BANK_ACCOUNTS: list[str] = _s["bank_accounts"]

BANK_TRANSACTIONS_FULL_RANGE = f"{BANK_TRANSACTIONS_SHEET_NAME}!{_BANK_DATA_RANGE}"
CC_TRANSACTIONS_FULL_RANGE = f"{CC_TRANSACTIONS_SHEET_NAME}!{_CC_DATA_RANGE}"
