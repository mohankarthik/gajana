from __future__ import annotations

import logging
from difflib import SequenceMatcher


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
