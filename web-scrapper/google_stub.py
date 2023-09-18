import os

from apiclient import discovery
from google.oauth2 import service_account
from dotenv import load_dotenv
from mf_transaction import MutualFundTransaction
from transaction import Transaction
import logging
from datetime import datetime
from utils import sanitize_currency
import io
from googleapiclient.http import MediaIoBaseDownload


class Google:
    def __init__(self) -> None:
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        secret_file = os.path.join(os.getcwd(), "google_secret.json")
        if not os.path.exists(secret_file):
            logging.error("Secret file missing")
            raise RuntimeError()

        self.credentials = service_account.Credentials.from_service_account_file(
            secret_file, scopes=scopes
        )


class Drive(Google):
    def __init__(self) -> None:
        super(Drive, self).__init__()
        self._service = discovery.build("drive", "v3", credentials=self.credentials)

    def get_files(self) -> list:
        page_token = None
        files = []
        while True:
            response = (
                self._service.files()
                .list(
                    q="mimeType='application/pdf'",
                    spaces="drive",
                    fields="nextPageToken, " "files(id, name, parents)",
                    pageToken=page_token,
                )
                .execute()
            )

            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

        return files

    def download_file(self, id: str, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path):
            os.remove(path)

        request = self._service.files().get_media(fileId=id)
        file_io = io.BytesIO()
        downloader = MediaIoBaseDownload(file_io, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}.")

        with open(path, "wb") as f:
            f.write(file_io.getvalue())


class Sheets(Google):
    def __init__(self, mf_sheet_id: str, bank_sheet_id: str) -> None:
        super(Sheets, self).__init__()
        self._service = discovery.build("sheets", "v4", credentials=self.credentials)
        self._mf_sheet_id = mf_sheet_id
        self._bank_sheet_id = bank_sheet_id

    def get_mf_transactions(self) -> list[MutualFundTransaction]:
        result: list[MutualFundTransaction] = []
        for row in (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._mf_sheet_id, range="MF Transactions!B2:M")
            .execute()
            .get("values", [])
        ):
            result.append(
                MutualFundTransaction(
                    fund_house=row[0].strip(),
                    scheme=row[1].strip(),
                    date=datetime.strptime(row[2].strip(), "%Y-%m-%d"),
                    nav=sanitize_currency(row[3]),
                    units=sanitize_currency(row[4]),
                    value=sanitize_currency(row[5]),
                    stamp_duty=sanitize_currency(row[6]),
                    type=row[7].strip(),
                    folio=row[8].strip(),
                    account=row[9].strip(),
                    sanitize_name=False,
                )
            )
        return result

    def update_mf_transactions(self, new_txns: list[MutualFundTransaction]) -> None:
        if not new_txns:
            return
        self._service.spreadsheets().values().append(
            spreadsheetId=self._mf_sheet_id,
            range="MF Transactions!B2:M",
            body={
                "values": [
                    [
                        txn.fund_house,
                        txn.scheme,
                        txn.date.strftime("%Y-%m-%d"),
                        txn.nav,
                        txn.units,
                        txn.value,
                        txn.stamp_duty,
                        txn.type,
                        txn.folio,
                        txn.account,
                        txn.units
                        if (txn.type == "Purchase" or txn.type == "Switch In")
                        else -txn.units,
                        -txn.value
                        if (txn.type == "Purchase" or txn.type == "Switch In")
                        else txn.value,
                    ]
                    for txn in new_txns
                ]
            },
            valueInputOption="USER_ENTERED",
        ).execute()

    def get_bank_transactions(self) -> list[Transaction]:
        result: list[Transaction] = []
        for row in (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._bank_sheet_id, range="Bank Transactions!B3:J")
            .execute()
            .get("values", [])
        ):
            if row[0] != "":
                result.append(
                    Transaction(
                        date=datetime.strptime(row[0].strip(), "%Y-%m-%d"),
                        description=row[3].strip(),
                        debit=float(row[4].strip().replace(",", ""))
                        if row[4] != ""
                        else "",
                        credit=float(row[5].strip().replace(",", ""))
                        if row[5] != ""
                        else "",
                        category=row[6].strip(),
                        account=row[8].strip(),
                    )
                )
        return result


if __name__ == "__main__":
    load_dotenv()
    stub = Sheets(os.getenv("SHEETS_MF_ID"), os.getenv("SHEETS_BANK_ID"))
    stub.get_mf_transactions()
    stub.update_mf_transactions(
        [
            MutualFundTransaction(
                "abc",
                datetime.now(),
                "def",
                "1234",
                "Purchase",
                1234.23,
                254.43,
                143.43,
                0.12,
            )
        ]
    )
