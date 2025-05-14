# gajana/categorizer.py
from __future__ import annotations

import json
import logging
import re

from constants import DEFAULT_CATEGORY
from constants import MATCHERS_FILE_PATH

# Import constants

# Configure logger for this module
logger = logging.getLogger(__name__)


class Categorizer:
    """
    Categorizes transactions based on rules loaded from a JSON file.
    Assumes matchers.json contains a list of rule objects.
    Each rule object should have:
        - "category": string (the category to assign)
        - "description": list of strings/regex patterns to match against transaction description
    Optional keys in rule objects:
        - "debit": boolean (true = match only debits, false = match only credits)
        - "account": string (substring to match against transaction account name)
        - "use_regex": boolean (true if description strings are regex patterns)
    """

    def __init__(self, matchers_file: str = MATCHERS_FILE_PATH) -> None:
        self.matchers: list[dict] = []
        try:
            logger.info(f"Loading categorization matchers from: {matchers_file}")
            with open(matchers_file, "r", encoding="utf-8") as f:
                self.matchers = json.load(f)
            assert isinstance(self.matchers, list), "Matchers JSON root must be a list."
            logger.info(f"Successfully loaded {len(self.matchers)} matchers.")
            # Optional: Add more detailed validation of matcher structure here if needed
        except FileNotFoundError:
            logger.fatal(
                f"Matchers file not found at '{matchers_file}'. Categorization will default."
            )
        except (json.JSONDecodeError, AssertionError) as e:
            logger.fatal(
                f"Error loading or validating matchers file '{matchers_file}': {e}"
            )
        except Exception as e:
            logger.fatal(
                f"Unexpected error loading matchers from '{matchers_file}': {e}",
                exc_info=True,
            )

    def categorize(self, txns: list[dict]) -> list[dict]:
        """
        Categorizes a list of transactions based on loaded rules.

        Args:
            txns: A list of transaction dictionaries. Expected keys: 'amount', 'description', 'account'.

        Returns:
            The list of transactions with the 'category' key added/updated.
        """
        if not self.matchers:
            logger.fatal(
                "No valid matchers loaded. Assigning default category to all transactions."
            )

        total_uncategorized = 0
        processed_count = 0
        logger.info(f"Starting categorization for {len(txns)} transactions...")

        for i, txn in enumerate(txns):
            if not all(k in txn for k in ["amount", "description", "account"]):
                logger.warning(
                    f"Skipping transaction index {i} due to missing keys. Txn: {txn}"
                )
                txn["category"] = DEFAULT_CATEGORY
                continue

            txn["category"] = DEFAULT_CATEGORY
            found_match = False

            # Ensure amount is float for comparison
            try:
                is_debit = float(txn["amount"]) < 0
            except (TypeError, ValueError):
                logger.fatal(f"Could not determine debit/credit for txn index {i}.")
                is_debit = True

            txn_description = str(txn.get("description", "")).lower()
            txn_account = str(txn.get("account", "")).lower()

            for matcher in self.matchers:
                # --- Matcher Condition Checks ---
                # Check Debit/Credit
                if "debit" in matcher and isinstance(matcher["debit"], bool):
                    if matcher["debit"] != is_debit:
                        continue

                # Check Account Substring
                if "account" in matcher and isinstance(matcher["account"], str):
                    if matcher["account"].lower() not in txn_account:
                        continue

                # Check Description Keywords/Regex
                matcher_descriptions = matcher.get("description", [])
                use_regex = matcher.get("use_regex", False)

                if isinstance(matcher_descriptions, list):
                    for pattern in matcher_descriptions:
                        pattern_str = str(pattern)
                        match_found = False
                        try:
                            if use_regex and re.search(
                                pattern_str, txn_description, re.IGNORECASE
                            ):
                                match_found = True
                            elif pattern_str.lower() in txn_description:
                                match_found = True
                        except re.error as e:
                            logger.fatal(
                                f"Invalid regex pattern in matcher: '{pattern_str}'. Error: {e}."
                                f"Matcher: {matcher}"
                            )

                        if match_found:
                            category_to_set = matcher.get("category", DEFAULT_CATEGORY)
                            assert isinstance(
                                category_to_set, str
                            ), f"Invalid category type in matcher: {matcher}"
                            txn["category"] = category_to_set
                            found_match = True
                            logger.debug(
                                f"Matched txn desc '{txn['description']}' to category '{category_to_set}' "
                                f"using pattern '{pattern_str}'"
                            )
                            break
                else:
                    logger.fatal(
                        f"Matcher has invalid 'description' format (expected list): {matcher}"
                    )

                if found_match:
                    break

            # --- End of Matcher Loop ---
            if not found_match:
                total_uncategorized += 1
                logger.debug(
                    f"Uncategorized transaction: Date={txn.get('date')}, Desc={txn.get('description')}, "
                    f"Amount={txn.get('amount')}, Acc={txn.get('account')}"
                )

            processed_count += 1
        # --- End of Transaction Loop ---

        logger.info(
            f"Categorization complete. Processed: {processed_count} transactions."
        )
        if total_uncategorized > 0:
            logger.warning(
                f"{total_uncategorized} out of {len(txns)} transactions remain '{DEFAULT_CATEGORY}'."
            )
        else:
            logger.info("All processed transactions were categorized.")

        return txns
