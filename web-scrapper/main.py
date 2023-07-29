from axis_bank import AxisBank
from mfutility import MFUtility
from sheets import Sheets
from dotenv import load_dotenv
import os
import logging
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Fetch latest transactions and update database"
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="skips actually getting data or updating data",
    )

    args = parser.parse_args()
    return args


def update_bank_and_cc():
    pass


def update_mf(sheet_stub: Sheets, dry_run=False) -> None:
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
    )
    logging.info("Starting web session to fetch latest MF transactions and holding")
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

    update_mf(sheet_stub, args.dry_run)
