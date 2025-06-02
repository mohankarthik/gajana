# gajana/constants.py
from __future__ import annotations

# --- Google API Configuration ---
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Path to the service account key file for Google API authentication
SERVICE_ACCOUNT_KEY_FILE = "secrets/google.json"

# --- Google Drive Configuration ---
# ID of the Google Drive folder where bank/CC statement CSVs are uploaded
CSV_FOLDER = "1DwJGCYydYikP7eWxMWD6mA84Mj7fO7-3"  # Replace with your actual folder ID

# --- Google Sheets Configuration ---
# ID of the main Google Sheet storing consolidated transactions
TRANSACTIONS_SHEET_ID = (
    "1I1NkOf2L5hVB6_yV896x9H-s1CIsRYWTR2T0ioBZDZU"  # Replace with your actual Sheet ID
)

# --- Sheet Ranges for Consolidated Data ---
BANK_TRANSACTIONS_SHEET_NAME = "Bank transactions"
BANK_TRANSACTIONS_DATA_RANGE = "B2:H"  # Data starts from row 3, columns B to H
BANK_TRANSACTIONS_FULL_RANGE = (
    f"{BANK_TRANSACTIONS_SHEET_NAME}!{BANK_TRANSACTIONS_DATA_RANGE}"
)

CC_TRANSACTIONS_SHEET_NAME = "CC Transactions"
CC_TRANSACTIONS_DATA_RANGE = "B2:H"  # Data starts from row 3, columns B to H
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
INTERNAL_TXN_KEYS = [
    "date",
    "description",
    "amount",
    "category",
    "remarks",
    "account",
]

# --- Account Identifiers ---
CC_ACCOUNTS = [
    # "cc-amex-adi",
    "cc-axis-magnus",
    # "cc-axis-platinum",
    # "cc-axis-select",
    # "cc-hdfc-mb",
    # "cc-hdfc-mb+",
    # "cc-hdfc-og",
    # "cc-hdfc-regaliagold",
    "cc-icici-amazonpay",
    "cc-hdfc-infiniametal",
]

# List of bank account names/IDs used internally
BANK_ACCOUNTS = [
    "bank-axis-karti",
    "bank-axis-mini",
    "bank-hdfc-karti",
    "bank-hdfc-mini",
    # "bank-kotak-mini",
    # "bank-sbi-mini-pallikarnai",
]

# --- Categorization ---
MATCHERS_FILE_PATH = "data/matchers.json"
DEFAULT_CATEGORY = "Uncategorized"

# --- Parsing Configuration ---
PARSING_CONFIG = {
    "bank-axis": {
        "header_patterns": [
            ["Tran Date", "CHQNO", "PARTICULARS", "DR", "CR", "BAL", "SOL"]
        ],
        "column_map": {
            "Tran Date": "date",
            "PARTICULARS": "description",
            "DR": "debit",
            "CR": "credit",
        },
        "date_formats": ["%d-%m-%Y"],
    },
    "bank-hdfc": {
        "header_patterns": [["Date", "Narration", "Withdrawal Amt.", "Deposit Amt."]],
        "column_map": {
            "Date": "date",
            "Narration": "description",
            "Withdrawal Amt.": "debit",
            "Deposit Amt.": "credit",
        },
        "date_formats": ["%d/%m/%y"],
    },
    "cc-axis": {
        "header_patterns": [
            ["Date", "Transaction Details", "Amount (INR)", "Debit/Credit"]
        ],
        "column_map": {
            "Date": "date",
            "Transaction Details": "description",
            "Amount (INR)": "amount",
            "Debit/Credit": "type",
        },
        "date_formats": ["%d %b %y"],
        "amount_sign_col": "type",
        "debit_value": "Debit",
    },
    "cc-hdfc": {
        "header_patterns": [
            [
                "Transaction type",
                "Primary / Addon Customer Name",
                "DATE",
                "Description",
                "Feature Reward Points",
                "AMT",
                "Debit / Credit",
            ]
        ],
        "column_map": {
            "DATE": "date",
            "Description": "description",
            "AMT": "amount",
            "Debit / Credit": "type",
        },
        "amount_sign_col": "type",
        "debit_value": "",
        "date_formats": ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y"],
        "special_handling": "hdfc_cc_tilde",
    },
    "cc-icici": {
        "header_patterns": [
            [
                "Date",
                "Sr.No.",
                "Transaction Details",
                "Reward Point Header",
                "Intl.Amount",
                "Amount(in Rs)",
                "BillingAmountSign",
            ]
        ],
        "column_map": {
            "Date": "date",
            "Transaction Details": "description",
            "Amount(in Rs)": "amount",
            "BillingAmountSign": "type",
        },
        "date_formats": ["%d/%m/%Y"],
        "amount_sign_col": "type",
        "debit_value": "",
    },
}
