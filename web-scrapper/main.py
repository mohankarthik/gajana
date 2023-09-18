import datetime
from axis_bank import AxisBank
from mfutility import MFUtility
from google_stub import Sheets
from google_stub import Drive
from dotenv import load_dotenv
import os
import logging
import argparse
from PyPDF2 import PdfReader
import re


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Fetch latest transactions and update database"
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="skips actually getting data or updating data",
    )
    parser.add_argument(
        "--disable_headless",
        action="store_true",
        help="Disables web scrapper in headless mode, allowing the user to see the steps",
    )
    parser.add_argument(
        "--skip_download",
        action="store_true",
        help="Disables the download of new statements",
    )

    args = parser.parse_args()
    return args


def update_bank_and_cc(
    sheet_stub: Sheets, dry_run=False, disable_headless=False
) -> None:
    num_axis_accounts = int(os.getenv("AXIS_ACCOUNTS"))
    logging.info("Got a total of {} axis bank accounts".format(num_axis_accounts))
    for idx in range(num_axis_accounts):
        axis_stub = AxisBank(
            account=os.getenv(f"AXIS_ACCOUNT_NAME{idx+1}"),
            account_num=os.getenv(f"AXIS_ACCOUNT_NO{idx+1}"),
            username=os.getenv(f"AXIS_ACCOUNT_USERNAME{idx+1}"),
            password=os.getenv(f"AXIS_ACCOUNT_PASSWORD{idx+1}"),
            disable_headless=disable_headless,
        )

        logging.info("Fetching data for axis account: {}".format(axis_stub._account))
        axis_stub.update()
        logging.info("Got a total of {} transactions".format(len(axis_stub._data)))


def update_mf(
    sheet_stub: Sheets, dry_run=False, disable_headless=False, skip_download=False
) -> None:
    # Get existing transactions
    existing_mf_transactions = sheet_stub.get_mf_transactions()
    logging.info(
        "Got a total of {} existing MF transactions".format(
            len(existing_mf_transactions)
        )
    )

    # Get updated data from MF Utility
    mf_stub = MFUtility(
        os.getenv("MFU_USERNAME"),
        os.getenv("MFU_PASSWORD"),
        os.getenv("MFU_TXN_PASSWORD"),
        [
            {
                "id": os.getenv(f"MFU_CAN_ID{idx+1}"),
                "name": os.getenv(f"MFU_CAN_NAME{idx+1}"),
            }
            for idx in range(int(os.getenv("MFU_CAN_COUNT")))
        ],
        disable_headless=disable_headless,
    )
    logging.info("Starting web session to fetch latest MF transactions and holding")
    if skip_download:
        mf_stub._parse_orders()
    else:
        mf_stub.update()
    logging.info("Got a total of {} MF transactions".format(len(mf_stub._orders)))

    # Get new MF transactions and push it to sheets
    new_mf_txns = [
        txn for txn in mf_stub._orders if not txn.is_present(existing_mf_transactions)
    ]
    logging.info("Only {} MF transactions are new".format(len(new_mf_txns)))

    if not dry_run:
        sheet_stub.update_mf_transactions(new_mf_txns)
    logging.info("Updated Sheet with latest transactions")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    load_dotenv()

    # Instantiate link to sheets
    sheet_stub = Sheets(
        os.getenv("SHEETS_MF_ID"),
        os.getenv("SHEETS_BANK_ID"),
    )

    # update_mf(sheet_stub, args.dry_run, args.disable_headless, args.skip_download)

    folder_id_map = {
        "1dlVP4CJ_8OgI0vPqFRm5FfEuUFakdYoC": "cc-karti-hdfc-mb+",
        "1w0WbL6lEzejnLb-uicDRv-5EEwSIYL11": "bank-axis-karti",
        "1rVwHwG6pLXdT1hTAGFO_oFBcajeZ82-g": "cc-karti-axis-platinum",
        "1D9XCktTrjdJNX-mbjPBly0QGV1OX87fL": "cc-karti-axis-select",
    }

    password_map = {
        "cc-karti-hdfc-mb+": "MOHA1403",
        "bank-axis-karti": "MOHA1403",
        "cc-karti-axis-platinum": "MOHA1403",
        "cc-karti-axis-select": "MOHA1403",
    }

    bank_transactions = sheet_stub.get_bank_transactions()
    bank_accounts = set()
    last_transaction_by_account: dict[str, datetime.datetime] = dict()
    for txn in bank_transactions:
        bank_accounts.add(txn.account)
        if last_transaction_by_account.get(txn.account):
            last_transaction_by_account[txn.account] = max(
                last_transaction_by_account.get(txn.account), txn.date
            )
        else:
            last_transaction_by_account[txn.account] = txn.date
    drive_stub = Drive()
    files = drive_stub.get_files()
    for file in files:
        file_name = os.path.join("/tmp", file.get("name")) + ".pdf"
        parent = folder_id_map.get(file.get("parents")[0])
        if parent != "bank-axis-karti" or last_transaction_by_account[parent].strftime(
            "%Y-%m"
        ) > file.get("name"):
            continue
        password = password_map.get(parent)
        logging.info(
            f"Downloading file {file.get('id')}\twith parent: {parent} to {file_name}"
        )
        drive_stub.download_file(file.get("id"), file_name)

        with open(file_name, "rb") as input_file:
            reader = PdfReader(input_file)
            reader.decrypt(password)

            for page in reader.pages:
                contents = page.extract_text()
                print(contents)

        # exit(0)
