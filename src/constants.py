# gajana/constants.py

# --- Location of the parsing configs ---
CONFIG_DIR = "data/configs"
SETTINGS_FILE = "settings.ini"

# --- Google API Configuration ---
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
SERVICE_ACCOUNT_KEY_FILE = "secrets/google.json"

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
MATCHERS_FILE_PATH = "data/matchers.json"
DEFAULT_CATEGORY = "Uncategorized"
