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
from google_wrapper import GoogleWrapper
from transaction_matcher import TransactionMatcher

# Import local modules

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def find_latest_transaction_by_account(
    txns: list[dict],
) -> dict[str, datetime.datetime]:
    """Finds the latest transaction date for each account."""
    latest: dict[str, datetime.datetime] = {}
    if not txns:
        return latest
    for txn in txns:
        try:
            account = txn["account"]

            # Ensure date is a datetime object
            current_date = txn["date"]
            assert isinstance(
                current_date, datetime.datetime
            ), f"Date is not a datetime object: {current_date}"

            if account in latest:
                latest[account] = max(latest[account], current_date)
            else:
                latest[account] = current_date
        except (KeyError, AssertionError, TypeError) as e:
            logger.fatal(
                f"Skipping transaction in latest date calculation due to error: {e}. Txn: {txn}"
            )
    logger.debug(f"Latest transaction dates by account: {latest}")
    return latest


def run_normal_mode(google_stub: GoogleWrapper, categorizer: Categorizer):
    """Fetches new transactions, categorizes, and appends them."""
    logger.info("Running in NORMAL mode (fetch, categorize, append new).")
    all_new_txns = []

    # --- Process Bank Transactions ---
    logger.info("--- Processing Bank Transactions ---")
    old_bank_txns = google_stub.get_old_transactions("bank")
    latest_bank_by_account = find_latest_transaction_by_account(old_bank_txns)
    potential_new_bank_txns = google_stub.get_all_transactions_from_statements(
        "bank", latest_bank_by_account
    )
    new_bank_txns = TransactionMatcher.find_new_txns(
        old_bank_txns, potential_new_bank_txns
    )
    if new_bank_txns:
        logger.info(f"Categorizing {len(new_bank_txns)} new bank transactions...")
        categorized_bank_txns = categorizer.categorize(new_bank_txns)
        all_new_txns.extend(categorized_bank_txns)
        google_stub.add_new_transactions(categorized_bank_txns, "bank")
    else:
        logger.info("No new bank transactions found to add.")

    # --- Process Credit Card Transactions ---
    logger.info("--- Processing Credit Card Transactions ---")
    old_cc_txns = google_stub.get_old_transactions("cc")
    latest_cc_by_account = find_latest_transaction_by_account(old_cc_txns)
    potential_new_cc_txns = google_stub.get_all_transactions_from_statements(
        "cc", latest_cc_by_account
    )
    new_cc_txns = TransactionMatcher.find_new_txns(old_cc_txns, potential_new_cc_txns)
    if new_cc_txns:
        logger.info(f"Categorizing {len(new_cc_txns)} new CC transactions...")
        categorized_cc_txns = categorizer.categorize(new_cc_txns)
        all_new_txns.extend(categorized_cc_txns)
        google_stub.add_new_transactions(categorized_cc_txns, "cc")
    else:
        logger.info("No new CC transactions found to add.")

    logger.info(
        f"Normal mode finished. Added a total of {len(all_new_txns)} new transactions."
    )


def run_recategorize_mode(google_stub: GoogleWrapper, categorizer: Categorizer):
    """Fetches ALL existing transactions, attempts to categorize only the 'Uncategorized' ones."""
    logger.info(
        "Running in RECATEGORIZE mode (fetch all, categorize uncategorized, overwrite)."
    )

    # 1. Fetch all existing transactions
    all_existing_txns = google_stub.get_all_transactions_for_recategorize()
    if not all_existing_txns:
        logger.warning("No existing transactions found to recategorize.")
        return

    # 2. Identify transactions needing categorization
    txns_to_categorize = [
        txn
        for txn in all_existing_txns
        if txn.get("category", DEFAULT_CATEGORY) == DEFAULT_CATEGORY
        or not txn.get("category")
    ]

    if not txns_to_categorize:
        logger.info("No transactions found needing categorization.")
        return

    logger.info(
        f"Attempting to categorize {len(txns_to_categorize)} currently '{DEFAULT_CATEGORY}' transactions..."
    )

    # 3. Categorize only the selected transactions
    categorizer.categorize(txns_to_categorize)

    # 4. Merge results back (no explicit merge needed as we modified items in the original list)
    # The `all_existing_txns` list now contains the updated categories for the ones processed.

    # 5. Separate back into bank and CC transactions (using the fully updated list)
    recategorized_bank_txns = [
        t for t in all_existing_txns if t.get("account") in BANK_ACCOUNTS
    ]
    recategorized_cc_txns = [
        t for t in all_existing_txns if t.get("account") in CC_ACCOUNTS
    ]
    recategorized_bank_txns.sort(
        key=itemgetter("date", "account", "amount", "description")
    )
    recategorized_cc_txns.sort(
        key=itemgetter("date", "account", "amount", "description")
    )

    # 6. Overwrite sheets with the updated full lists
    logger.info("--- Overwriting Bank Transactions Sheet ---")
    google_stub.overwrite_transactions(recategorized_bank_txns, "bank")

    logger.info("--- Overwriting CC Transactions Sheet ---")
    google_stub.overwrite_transactions(recategorized_cc_txns, "cc")

    logger.info("Recategorize mode finished.")


def run_learn_mode(google_stub: GoogleWrapper):
    """Analyzes already categorized transactions to suggest new rules."""
    logger.info("Running in LEARN CATEGORIES mode.")

    # 1. Fetch all existing transactions
    all_existing_txns = google_stub.get_all_transactions_for_recategorize()
    if not all_existing_txns:
        logger.warning("No existing transactions found to learn from.")
        return

    # 2. Filter for categorized transactions
    categorized_txns = [
        txn
        for txn in all_existing_txns
        if txn.get("category") and txn.get("category") != DEFAULT_CATEGORY
    ]

    if not categorized_txns:
        logger.warning(
            f"No transactions found with categories other than '{DEFAULT_CATEGORY}'. Cannot learn."
        )
        return

    logger.info(
        f"Analyzing {len(categorized_txns)} categorized transactions to find patterns..."
    )

    # 3. Analyze patterns (Simple Keyword Frequency Example)
    category_patterns = defaultdict(lambda: defaultdict(Counter))

    for txn in categorized_txns:
        try:
            category = txn["category"]
            desc = str(txn.get("description", "")).lower()
            amount = float(txn["amount"])
            is_debit = amount < 0
            debit_credit_key = "debit" if is_debit else "credit"
            words = re.findall(r"\b[a-z0-9]+\b", desc)

            for word in words:
                if len(word) > 2 and not word.isdigit():
                    category_patterns[category][debit_credit_key][word] += 1
        except Exception as e:
            logger.warning(
                f"Skipping transaction during learning due to error: {e}. Txn: {txn}"
            )

    # 4. Generate and Print Suggestions
    logger.info("--- Suggested New Categorization Rules ---")
    print("\n--- Suggested New Categorization Rules ---")
    print(
        "(Review these suggestions and manually add relevant ones to matchers.json)\n"
    )

    suggestions_found = 0
    for category, debit_credit_data in category_patterns.items():
        for debit_credit_key, word_counter in debit_credit_data.items():
            is_debit_rule = debit_credit_key == "debit"
            min_count_threshold = 3
            suggested_keywords = [
                word
                for word, count in word_counter.most_common(10)
                if count >= min_count_threshold
            ]

            if suggested_keywords:
                suggestions_found += 1
                # Format as a potential JSON rule object
                suggested_rule = {
                    "category": category,
                    "description": sorted(suggested_keywords),
                    "debit": is_debit_rule,
                }
                print(
                    f"# Suggestion for Category: {category} ({debit_credit_key.upper()})"
                )
                print(json.dumps(suggested_rule, indent=2))
                print("-" * 20)

    if suggestions_found == 0:
        logger.info(
            "No significant patterns found to suggest new rules based on current criteria."
        )
        print(
            "No significant patterns found to suggest new rules based on current criteria."
        )

    logger.info("Learn categories mode finished.")


def main():
    """
    Main entry point for the Gajana script.
    Parses arguments and runs in the specified mode.
    """
    parser = argparse.ArgumentParser(
        description="Gajana - Personal Finance Transaction Processor"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--recategorize-only",
        action="store_true",
        help="Recategorize existing 'Uncategorized' transactions and overwrite sheets.",
    )
    mode_group.add_argument(
        "--learn-categories",
        action="store_true",
        help="Analyze categorized transactions and suggest new rules (does not modify sheets).",
    )

    args = parser.parse_args()

    logger.info("Gajana script started.")
    start_time = datetime.datetime.now()

    try:
        google_stub = GoogleWrapper()
        if args.learn_categories:
            run_learn_mode(google_stub)
        else:
            categorizer = Categorizer()
            if args.recategorize_only:
                run_recategorize_mode(google_stub, categorizer)
            else:
                run_normal_mode(google_stub, categorizer)

    except AssertionError as e:
        logger.fatal(f"Critical assertion failed: {e}", exc_info=True)
    except Exception as e:
        logger.fatal(
            f"An unexpected error occurred in the main workflow: {e}",
            exc_info=True,
        )

    end_time = datetime.datetime.now()
    duration = end_time - start_time
    logger.info(f"Gajana script finished. Total execution time: {duration}")


if __name__ == "__main__":
    main()
