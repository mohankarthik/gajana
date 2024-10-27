from __future__ import annotations

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
SERVICE_ACCOUNT_KEY_FILE = "secrets/google.json"
CSV_FOLDER = "1DwJGCYydYikP7eWxMWD6mA84Mj7fO7-3"
TRANSACTIONS_SHEET_ID = "1I1NkOf2L5hVB6_yV896x9H-s1CIsRYWTR2T0ioBZDZU"
BANK_TRANSACTIONS_RANGE = "Bank transactions!B3:H"
CC_TRANSACTIONS_RANGE = "CC Transactions!B3:H"
AXIS_BANK_STATEMENT_RANGE = "A19:G"
AXIS_CC_STATEMENT_RANGE = "A8:F"
HDFC_BANK_STATEMENT_RANGE = "A4:G"
HDFC_CC_STATEMENT_RANGE = "A24:G"
ICICI_CC_STATEMENT_RANGE = "A9:G"

CC_ACCOUNTS = [
    "cc-amex-adi",
    "cc-axis-magnus",
    "cc-axis-platinum",
    "cc-axis-select",
    "cc-hdfc-mb",
    "cc-hdfc-mb+",
    "cc-hdfc-og",
    "cc-hdfc-regaliagold",
    "cc-icici-amazonpay",
    "cc-hdfc-infiniametal",
]

BANK_ACCOUNTS = [
    "bank-axis-karti",
    "bank-axis-mini",
    "bank-hdfc-karti",
    "bank-hdfc-mini",
    "bank-kotak-mini",
    "bank-sbi-mini-pallikarnai",
]
