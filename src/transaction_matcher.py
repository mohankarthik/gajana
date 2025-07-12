# gajana/transaction_matcher.py
from __future__ import annotations

import logging
from operator import itemgetter
from typing import Any, Hashable

logger = logging.getLogger(__name__)


class TransactionMatcher:
    """
    Compares lists of transactions to find new ones.
    Note: This is a basic implementation assuming unique transactions
          can be identified by a combination of fields after sorting.
          More robust duplicate handling might be needed depending on data.
    """

    @staticmethod
    def find_new_txns(
        old_txns: list[dict[Hashable, Any]],
        all_potential_txns: list[dict[Hashable, Any]],
    ) -> list[dict[Hashable, Any]]:
        """
        Identifies transactions present in all_potential_txns but not in old_txns.

        Args:
            old_txns: List of transactions already processed (from the sheet).
            all_potential_txns: List of transactions parsed from statements.

        Returns:
            A tuple containing:
            - missing_txns: Placeholder, currently empty (logic not defined).
            - new_txns: List of transactions deemed new.
        """
        if not all_potential_txns:
            logger.info("No potential new transactions provided to match.")
            return []
        if not old_txns:
            logger.info(
                "No old transactions provided, considering all potential transactions as new."
            )
            all_potential_txns.sort(
                key=itemgetter("date", "account", "amount", "description")
            )
            return all_potential_txns

        try:
            old_txn_ids = set(
                (
                    txn["date"].strftime("%Y-%m-%d"),
                    txn["account"],
                    f"{txn['amount']:.2f}",
                    txn["description"],
                )
                for txn in old_txns
            )
        except KeyError as e:
            logger.fatal(
                f"Missing key {e} in old transactions during ID creation. Matching may be inaccurate."
            )
            old_txn_ids = set()

        new_txns = []
        processed_potential_ids = set()

        for txn in all_potential_txns:
            try:
                potential_id = (
                    txn["date"].strftime("%Y-%m-%d"),
                    txn["account"],
                    f"{txn['amount']:.2f}",
                    txn["description"],
                )

                if (
                    potential_id not in old_txn_ids
                    and potential_id not in processed_potential_ids
                ):
                    new_txns.append(txn)
                    processed_potential_ids.add(potential_id)
                else:
                    logger.debug(f"Skipping duplicate/old transaction: {potential_id}")

            except KeyError as e:
                logger.fatal(
                    f"Missing key {e} in potential transaction. Skipping matching for: {txn}"
                )
            except Exception as e:
                logger.fatal(
                    f"Error creating ID for potential transaction: {e}. Skipping matching for: {txn}",
                    exc_info=True,
                )

        new_txns.sort(key=itemgetter("date", "account", "amount", "description"))
        logger.info(
            f"Transaction matching complete. Identified {len(new_txns)} new transactions."
        )
        return new_txns
