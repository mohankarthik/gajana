from __future__ import annotations

import datetime
import logging
import sys
import time
from operator import itemgetter
from urllib.error import HTTPError

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from constants import AXIS_BANK_STATEMENT_RANGE
from constants import AXIS_CC_STATEMENT_RANGE
from constants import BANK_ACCOUNTS
from constants import BANK_TRANSACTIONS_RANGE
from constants import CC_ACCOUNTS
from constants import CC_TRANSACTIONS_RANGE
from constants import CSV_FOLDER
from constants import HDFC_BANK_STATEMENT_RANGE
from constants import HDFC_CC_STATEMENT_RANGE
from constants import ICICI_CC_STATEMENT_RANGE
from constants import SCOPES
from constants import SERVICE_ACCOUNT_KEY_FILE
from constants import TRANSACTIONS_SHEET_ID


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
