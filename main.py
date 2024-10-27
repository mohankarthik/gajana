from __future__ import annotations

import logging

from categorizer import Categorizer
from google_wrapper import GoogleWrapper
from transaction_matcher import TransactionMatcher


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
