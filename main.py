from __future__ import annotations

import datetime
import json
import logging
import sys
import time
from difflib import SequenceMatcher
from operator import itemgetter
from urllib.error import HTTPError

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# If modifying these scopes, delete the file token.json.
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


class GoogleWrapper:
    def __init__(self) -> None:
        self.creds = self._get_credential()
        self.drive_service = self._get_drive_service()
        self.sheets_service = self._get_sheets_service()
        self.statement_files = self._get_statement_files()

    def get_old_bank_txns(self) -> list[dict]:
        values = self._get_sheet_data(TRANSACTIONS_SHEET_ID, BANK_TRANSACTIONS_RANGE)

        txns = []
        for row in values:
            txns.append(
                {
                    "date": datetime.datetime.strptime(row[0], "%Y-%m-%d"),
                    "description": row[1],
                    "amount": self._parse_amount(row[3]) - self._parse_amount(row[2]),
                    "category": row[4],
                    "remarks": row[5],
                    "account": row[6],
                }
            )
        txns = sorted(txns, key=itemgetter("date", "account", "amount", "description"))
        logging.info(
            f"Found total of {len(txns)} bank transactions already processed with latest date of {txns[-1]['date']}"
        )
        return txns

    def get_all_bank_txns(
        self, latest_txn_by_account: dict[str, datetime.datetime]
    ) -> list[dict]:
        txns = []
        for file in self.statement_files:
            if "bank" not in file["name"]:
                continue
            statement_year = int(file["name"][-4:])

            for bank_account in BANK_ACCOUNTS:
                if bank_account not in file["name"]:
                    continue

                if latest_txn_by_account[bank_account].year <= statement_year:
                    if "axis" in bank_account:
                        txns += self._get_axis_bank_txns(file["id"], bank_account)
                    elif "hdfc" in bank_account:
                        txns += self._get_hdfc_bank_txns(file["id"], bank_account)
                    else:
                        logging.fatal(f"Unknown statement: {file['name']}")

        if txns:
            txns = sorted(
                txns,
                key=itemgetter("date", "account", "amount", "description"),
            )
            logging.info(
                f"Found total of {len(txns)} bank transactions in CSV statements with latest date of {txns[-1]['date']}"
            )
        else:
            logging.info("Found no new bank txns")
        return txns

    def get_old_cc_txns(self) -> list[dict]:
        values = self._get_sheet_data(TRANSACTIONS_SHEET_ID, CC_TRANSACTIONS_RANGE)

        txns = []
        for row in values:
            txns.append(
                {
                    "date": datetime.datetime.strptime(row[0], "%Y-%m-%d"),
                    "description": row[1],
                    "amount": self._parse_amount(row[3]) - self._parse_amount(row[2]),
                    "category": row[4],
                    "remarks": row[5],
                    "account": row[6],
                }
            )
        txns = sorted(txns, key=itemgetter("date", "account", "amount", "description"))
        logging.info(
            f"Found total of {len(txns)} cc transactions already processed with latest date of {txns[-1]['date']}"
        )
        return txns

    def get_all_cc_txns(
        self, latest_txn_by_account: dict[str, datetime.datetime]
    ) -> list[dict]:
        txns = []
        for file in self.statement_files:
            if "cc" not in file["name"]:
                continue
            statement_month = int(file["name"][-2:])
            statement_year = int(file["name"][-7:-3])
            for cc_account in CC_ACCOUNTS:
                if cc_account not in file["name"]:
                    continue

                if (
                    latest_txn_by_account[cc_account].year <= statement_year
                    and latest_txn_by_account[cc_account].month <= statement_month
                ):
                    if "axis" in cc_account:
                        txns += self._get_axis_cc_txns(file["id"], cc_account)
                    elif "hdfc" in cc_account:
                        txns += self._get_hdfc_cc_txns(file["id"], cc_account)
                    elif "icici" in cc_account:
                        txns += self._get_icici_cc_txns(file["id"], cc_account)
                    else:
                        logging.fatal(f"Found unknown CC statement: {file['name']}")
                break

        if txns:
            txns = sorted(
                txns,
                key=itemgetter("date", "account", "amount", "description"),
            )
            logging.info(
                f"Found total of {len(txns)} cc transactions in CSV statements with latest date of {txns[-1]['date']}"
            )
        else:
            logging.info("No new cc transactions in CSV statements found")
        return txns

    def add_new_bank_txns(self, txns: list[dict]) -> None:
        values = []
        for txn in txns:
            debit = ""
            credit = ""
            if txn["amount"] < 0:
                debit = str(-txn["amount"])
            else:
                credit = str(txn["amount"])
            values.append(
                [
                    txn["date"].strftime("%Y-%m-%d"),
                    txn["description"],
                    debit,
                    credit,
                    txn["category"],
                    "",
                    txn["account"],
                ]
            )

        self._update_sheet_data(TRANSACTIONS_SHEET_ID, BANK_TRANSACTIONS_RANGE, values)

    def add_new_cc_txns(self, txns: list[dict]) -> None:
        values = []
        for txn in txns:
            debit = ""
            credit = ""
            if txn["amount"] < 0:
                debit = str(-txn["amount"])
            else:
                credit = str(txn["amount"])
            values.append(
                [
                    txn["date"].strftime("%Y-%m-%d"),
                    txn["description"],
                    debit,
                    credit,
                    txn["category"],
                    "",
                    txn["account"],
                ]
            )

        self._update_sheet_data(TRANSACTIONS_SHEET_ID, CC_TRANSACTIONS_RANGE, values)

    def _get_axis_bank_txns(self, sheet_id: str, account_name: str) -> list[dict]:
        txns = []
        values = self._get_sheet_data(sheet_id, AXIS_BANK_STATEMENT_RANGE, True)
        for row in values:
            if len(row) != 7 or row[1] == "CHQNO":
                continue
            txns.append(
                {
                    "date": datetime.datetime.strptime(row[0], "%d-%m-%Y"),
                    "description": row[2],
                    "amount": self._parse_amount(row[4]) - self._parse_amount(row[3]),
                    "category": None,
                    "remarks": None,
                    "account": account_name,
                }
            )
        return txns

    def _get_axis_cc_txns(self, sheet_id: str, account_name: str) -> list[dict]:
        txns = []
        values = self._get_sheet_data(sheet_id, AXIS_CC_STATEMENT_RANGE, True)
        for row in values:
            if len(row) != 5:
                continue
            txns.append(
                {
                    "date": datetime.datetime.strptime(
                        row[0].replace("'", ""), "%d %b %y"
                    ),
                    "description": row[1],
                    "amount": self._parse_amount(row[3]),
                    "category": None,
                    "remarks": None,
                    "account": account_name,
                }
            )
            if row[4] == "Debit":
                txns[-1]["amount"] = -txns[-1]["amount"]

        return txns

    def _get_hdfc_bank_txns(self, sheet_id: str, account_name: str) -> list[dict]:
        txns = []
        values = self._get_sheet_data(sheet_id, HDFC_BANK_STATEMENT_RANGE, True)
        for row in values:
            if len(row) != 7:
                continue
            txns.append(
                {
                    "date": datetime.datetime.strptime(row[0], "%d/%m/%y"),
                    "description": row[1],
                    "amount": self._parse_amount(row[4]) - self._parse_amount(row[3]),
                    "category": None,
                    "remarks": None,
                    "account": account_name,
                }
            )
        return txns

    def _get_hdfc_cc_txns(self, sheet_id: str, account_name: str) -> list[dict]:
        txns = []
        values = self._get_sheet_data(sheet_id, HDFC_CC_STATEMENT_RANGE, True)
        for row in values:
            val = "".join(row)
            entries = val.split("~")
            if len(entries) != 6:
                continue
            txns.append(
                {
                    "date": datetime.datetime.strptime(entries[2], "%d/%m/%Y"),
                    "description": entries[3],
                    "amount": self._parse_amount(entries[4]),
                    "category": None,
                    "remarks": None,
                    "account": account_name,
                }
            )
            if entries[5] != "Cr":
                txns[-1]["amount"] = -txns[-1]["amount"]

        return txns

    def _get_icici_cc_txns(self, sheet_id: str, account_name: str) -> list[dict]:
        txns = []
        values = self._get_sheet_data(sheet_id, ICICI_CC_STATEMENT_RANGE, True)
        for row in values:
            if len(row) != 6 and len(row) != 7:
                continue
            txns.append(
                {
                    "date": datetime.datetime.strptime(row[0], "%d/%m/%Y"),
                    "description": row[2],
                    "amount": self._parse_amount(row[5]),
                    "category": None,
                    "remarks": None,
                    "account": account_name,
                }
            )
            if len(row) == 6 or row[6] != "CR":
                txns[-1]["amount"] = -txns[-1]["amount"]

        return txns

    def _get_credential(self) -> ServiceAccountCredentials:
        """Creates a Credential object with the correct OAuth2 authorization.

        Uses the service account key stored in SERVICE_ACCOUNT_KEY_FILE.

        Returns:
          Credentials, the user's credential.
        """
        credential = ServiceAccountCredentials.from_json_keyfile_name(
            SERVICE_ACCOUNT_KEY_FILE, SCOPES
        )

        if not credential or credential.invalid:
            print("Unable to authenticate using service account key.")
            sys.exit()
        return credential

    def _get_drive_service(self) -> any:
        return build("drive", "v3", credentials=self.creds)

    def _get_sheets_service(self) -> any:
        return build("sheets", "v4", credentials=self.creds)

    def _get_statement_files(self) -> list:
        files = []
        page_token = None
        while True:
            response = (
                self.drive_service.files()
                .list(
                    q=f"parents in '{CSV_FOLDER}'",
                    spaces="drive",
                    fields="nextPageToken, files(id, name)",
                    pageToken=page_token,
                )
                .execute()
            )
            for file in response.get("files", []):
                logging.debug(f'Found file: {file.get("name")}, {file.get("id")}')
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
        logging.info(f"Found a total of {len(files)} statement files")
        return files

    def _get_sheet_data(
        self,
        sheet_id: str,
        range: str,
        get_first_sheet: bool = False,
        retry_count: int = 0,
    ) -> list:
        if retry_count > 3:
            raise SystemError(f"Unable to update sheet after {retry_count} attempts")

        sheet_service = self.sheets_service.spreadsheets()

        if get_first_sheet:
            spreadsheet = (
                self.sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            )
            sheets = spreadsheet.get("sheets", "")
            title = sheets[0].get("properties", {}).get("title", "Sheet1")
            range = title + "!" + range

        try:
            result = (
                sheet_service.values()
                .get(spreadsheetId=sheet_id, range=range)
                .execute()
            )
            values = result.get("values", [])
            assert values
            return values
        except Exception as e:
            logging.warning(
                f"Hit API resource limits, waiting for 1 minute and retrying, ${repr(e)}"
            )
            time.sleep(60)
            return self._get_sheet_data(
                sheet_id, range, get_first_sheet, retry_count + 1
            )

    def _update_sheet_data(
        self,
        spreadsheet_id: str,
        range_name: str,
        values: list,
        value_input_option: str = "USER_ENTERED",
        retry_count: int = 0,
    ) -> None:
        if retry_count > 3:
            raise SystemError(f"Unable to update sheet after {retry_count} attempts")
        try:
            body = {"values": values}
            result = (
                self.sheets_service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption=value_input_option,
                    body=body,
                )
                .execute()
            )
            logging.info(
                f"{(result.get('updates').get('updatedCells'))} cells appended."
            )
            return result
        except HTTPError as err:
            logging.warning(f"Failed to update sheet with {err}, retrying")
            time.sleep(60)
            return self._update_sheet_data(
                spreadsheet_id,
                range_name,
                values,
                value_input_option,
                retry_count + 1,
            )

    @staticmethod
    def _parse_amount(value: str) -> float:
        if value == "":
            return 0

        return float(value.replace(",", "").replace("₹", ""))


class TransactionMatcher:
    @staticmethod
    def _is_txn_same(txn_a: dict, txn_b: dict) -> bool:
        return (
            (txn_a["date"] == txn_b["date"])
            and (txn_a["account"] == txn_b["account"])
            and (txn_a["amount"] == txn_b["amount"])
            and SequenceMatcher(
                None,
                txn_a["description"].lower(),
                txn_b["description"].lower(),
            ).ratio()
            > 0.5
        )

    @staticmethod
    def _is_ignored_txn(txn: dict) -> bool:
        if "ANALOG DE" in txn["description"] and txn["category"] != "Reversal":
            return True
        if "GOOGLE IT" in txn["description"] and txn["category"] != "Reversal":
            return True
        return False

    @staticmethod
    def find_new_txns(old_txns: list[dict], all_txns: list[dict]) -> list[dict]:
        missing_txns = []
        old_idx = 0
        all_idx = 0
        while old_idx < len(old_txns) and all_idx < len(all_txns):
            if all_txns[all_idx]["date"] > old_txns[old_idx]["date"]:
                # Skip past old transactions that no longer exist in the CSVs
                old_idx += 1
                continue
            if TransactionMatcher._is_txn_same(old_txns[old_idx], all_txns[all_idx]):
                old_idx += 1
                all_idx += 1
                continue
            if TransactionMatcher._is_ignored_txn(old_txns[old_idx]):
                old_idx += 1
                continue
            if TransactionMatcher._is_ignored_txn(all_txns[all_idx]):
                all_idx += 1
                continue
            missing_txns.append(all_txns[all_idx])
            all_idx += 1

        if missing_txns:
            logging.warning(f"Found total of {len(missing_txns)} missing transactions")
            for txn in missing_txns:
                print(txn)

        new_txns = all_txns[all_idx:]
        logging.info(f"Found total of {len(new_txns)} new transactions")
        return missing_txns, new_txns


class Categorizer:
    def __init__(self) -> None:
        self.matchers = None
        with open("data/matchers.json", "r") as f:
            self.matchers = json.load(f)

    def categorize(self, txns: list[dict]) -> list[dict]:
        total_uncategorized = 0
        for txn in txns:
            found = False
            is_debit = txn["amount"] < 0
            txn_description = txn["description"].lower()
            for matcher in self.matchers:
                if found:
                    break
                if ("debit" in matcher) and (
                    (matcher["debit"] and not is_debit)
                    or (not matcher["debit"] and is_debit)
                ):
                    continue
                if ("account" in matcher) and (
                    matcher["account"] not in txn["account"]
                ):
                    continue
                for matcher_description in matcher["description"]:
                    if matcher_description.lower() in txn_description:
                        found = True
                        txn["category"] = matcher["category"]
                        logging.debug(f"Found matcher {matcher} for transaction {txn}")
                        break
            if not found:
                total_uncategorized += 1
                logging.debug(f"Could not categorize transaction {txn}")

        if total_uncategorized:
            logging.warning(
                f"Out of {len(txns)}, {total_uncategorized} could not be categorized."
            )
        return txns


def find_latest_transaction_by_account(txns: list[dict]) -> dict:
    latest = {}
    for txn in txns:
        if txn["account"] in latest:
            latest[txn["account"]] = max(latest[txn["account"]], txn["date"])
        else:
            latest[txn["account"]] = txn["date"]
    return latest


def main():
    logging.basicConfig(level=logging.INFO)
    google_stub = GoogleWrapper()
    categorizer = Categorizer()

    old_bank_txns = google_stub.get_old_bank_txns()
    latest_bank_by_account = find_latest_transaction_by_account(old_bank_txns)
    all_bank_txns = google_stub.get_all_bank_txns(latest_bank_by_account)
    missing_bank_txns, new_bank_txns = TransactionMatcher.find_new_txns(
        old_bank_txns, all_bank_txns
    )
    new_bank_txns = categorizer.categorize(new_bank_txns)
    google_stub.add_new_bank_txns(new_bank_txns)

    old_cc_txns = google_stub.get_old_cc_txns()
    latest_cc_by_account = find_latest_transaction_by_account(old_cc_txns)
    all_cc_txns = google_stub.get_all_cc_txns(latest_cc_by_account)
    missing_cc_txns, new_cc_txns = TransactionMatcher.find_new_txns(
        old_cc_txns, all_cc_txns
    )
    new_cc_txns = categorizer.categorize(new_cc_txns)
    google_stub.add_new_cc_txns(new_cc_txns)


if __name__ == "__main__":
    main()
