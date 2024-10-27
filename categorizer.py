from __future__ import annotations

import json
import logging


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
                logging.info(f"Could not categorize transaction {txn}")

        if total_uncategorized:
            logging.warning(
                f"Out of {len(txns)}, {total_uncategorized} could not be categorized."
            )
        return txns
