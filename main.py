# gajana/main.py
from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
from collections import Counter
from collections import defaultdict
from operator import itemgetter

from categorizer import Categorizer
from constants import BANK_ACCOUNTS
from constants import CC_ACCOUNTS
from constants import DEFAULT_CATEGORY
from google_data_source import GoogleDataSource
from transaction_matcher import TransactionMatcher
from transaction_processor import TransactionProcessor
from utils import log_and_exit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def find_latest_transaction_by_account(
    txns: list[dict],
) -> dict[str, datetime.datetime]:
    latest: dict[str, datetime.datetime] = {}
    if not txns:
        return latest
    for txn in txns:
        try:
            account, current_date = txn["account"], txn["date"]
            assert isinstance(
                current_date, datetime.datetime
            ), f"Date not datetime: {current_date}"
            latest[account] = max(
                latest.get(account, datetime.datetime.min), current_date
            )
        except (KeyError, AssertionError, TypeError) as e:
            log_and_exit(logger, f"Error finding latest date: {e}. Txn: {txn}")
            assert False, f"Error finding latest date: {e}"
    logger.debug(f"Latest transaction dates: {latest}")
    return latest


def run_normal_mode(processor: TransactionProcessor, categorizer: Categorizer):
    logger.info("Running in NORMAL mode.")
    all_new_txns_added_count = 0

    for acc_type in ["bank", "cc"]:
        logger.info(f"--- Processing {acc_type.upper()} Transactions ---")
        try:
            old_txns = processor.get_old_transactions(acc_type)
            latest_by_account = find_latest_transaction_by_account(old_txns)
            potential_new_txns = processor.get_new_transactions_from_statements(
                acc_type, latest_by_account
            )

            # TransactionMatcher.find_new_txns now returns only new_txns
            new_txns_to_add = TransactionMatcher.find_new_txns(
                old_txns, potential_new_txns
            )

            if new_txns_to_add:
                logger.info(
                    f"Categorizing {len(new_txns_to_add)} new {acc_type} transactions..."
                )
                categorized_txns = categorizer.categorize(new_txns_to_add)
                processor.add_new_transactions_to_log(categorized_txns, acc_type)
                all_new_txns_added_count += len(categorized_txns)
            else:
                logger.info(f"No new {acc_type} transactions found to add.")
        except Exception as e:  # Catch exceptions per account type processing
            logger.error(
                f"Error processing {acc_type} transactions in normal mode: {e}", e
            )
            # Decide if you want to continue to the next account type or assert False here

    logger.info(
        f"Normal mode finished. Added {all_new_txns_added_count} new transactions."
    )


def run_recategorize_mode(processor: TransactionProcessor, categorizer: Categorizer):
    logger.info("Running in RECATEGORIZE mode.")
    all_existing_txns = processor.get_all_transactions_for_recategorize()
    if not all_existing_txns:
        logger.warning("No existing transactions for recategorization.")
        return

    txns_to_categorize = [
        txn
        for txn in all_existing_txns
        if txn.get("category", DEFAULT_CATEGORY) == DEFAULT_CATEGORY
        or not txn.get("category")
    ]
    if not txns_to_categorize:
        logger.info("No transactions found needing recategorization.")
        return

    logger.info(
        f"Recategorizing {len(txns_to_categorize)} '{DEFAULT_CATEGORY}' transactions..."
    )
    categorizer.categorize(txns_to_categorize)  # Modifies in-place

    bank_txns = [t for t in all_existing_txns if t.get("account") in BANK_ACCOUNTS]
    cc_txns = [t for t in all_existing_txns if t.get("account") in CC_ACCOUNTS]
    bank_txns.sort(key=itemgetter("date", "account", "amount", "description"))
    cc_txns.sort(key=itemgetter("date", "account", "amount", "description"))

    logger.info("--- Overwriting Bank Transactions Sheet ---")
    processor.overwrite_transaction_log(bank_txns, "bank")
    logger.info("--- Overwriting CC Transactions Sheet ---")
    processor.overwrite_transaction_log(cc_txns, "cc")
    logger.info("Recategorize mode finished.")


def run_learn_mode(processor: TransactionProcessor):
    logger.info("Running in LEARN CATEGORIES mode.")
    all_existing_txns = processor.get_all_transactions_for_recategorize()
    if not all_existing_txns:
        logger.warning("No existing transactions to learn from.")
        return

    categorized_txns = [
        txn
        for txn in all_existing_txns
        if txn.get("category") and txn.get("category") != DEFAULT_CATEGORY
    ]
    if not categorized_txns:
        logger.warning(
            f"No transactions with categories other than '{DEFAULT_CATEGORY}'."
        )
        return

    logger.info(f"Analyzing {len(categorized_txns)} categorized transactions...")
    category_patterns = defaultdict(lambda: defaultdict(Counter))
    for txn in categorized_txns:
        try:
            cat, desc, amt = (
                txn["category"],
                str(txn.get("description", "")).lower(),
                float(txn["amount"]),
            )
            _, dc_key = amt < 0, "debit" if amt < 0 else "credit"
            words = re.findall(r"\b[a-z0-9]{3,}\b", desc)  # Words 3+ chars
            for word in words:
                if not word.isdigit():
                    category_patterns[cat][dc_key][word] += 1
        except Exception as e:
            logger.warning(f"Skipping txn during learning: {e}. Txn: {txn}")

    logger.info("--- Suggested New Categorization Rules ---")
    print(
        "\n--- Suggested New Categorization Rules ---\n(Review and manually add to matchers.json)\n"
    )
    suggestions_found = 0
    for cat, dc_data in category_patterns.items():
        for dc_key, word_counter in dc_data.items():
            suggested_keywords = [
                w for w, c in word_counter.most_common(10) if c >= 3
            ]  # Min count 3
            if suggested_keywords:
                suggestions_found += 1
                rule = {
                    "category": cat,
                    "description": sorted(suggested_keywords),
                    "debit": (dc_key == "debit"),
                }
                print(
                    f"# Suggestion for Category: {cat} ({dc_key.upper()})\n{json.dumps(rule, indent=2)}\n{'-'*20}"
                )
    if not suggestions_found:
        print("No significant patterns found.")
    logger.info("Learn categories mode finished.")


def main():
    parser = argparse.ArgumentParser(
        description="Gajana - Personal Finance Transaction Processor"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--recategorize-only",
        action="store_true",
        help="Recategorize 'Uncategorized' txns.",
    )
    mode_group.add_argument(
        "--learn-categories",
        action="store_true",
        help="Suggest new categorization rules.",
    )
    args = parser.parse_args()

    logger.info("Gajana script started.")
    start_time = datetime.datetime.now()
    try:
        data_source = GoogleDataSource()  # Instantiate the concrete data source
        processor = TransactionProcessor(data_source)  # Pass it to the processor

        if args.learn_categories:
            run_learn_mode(processor)
        else:
            categorizer = Categorizer()  # Needed for normal and recategorize
            if args.recategorize_only:
                run_recategorize_mode(processor, categorizer)
            else:
                run_normal_mode(processor, categorizer)
    except AssertionError as e:
        log_and_exit(logger, f"Critical assertion failed: {e}", e)
    except Exception as e:
        log_and_exit(logger, f"Unexpected error in main workflow: {e}", e)
    duration = datetime.datetime.now() - start_time
    logger.info(f"Gajana script finished. Total execution time: {duration}")


if __name__ == "__main__":
    main()
