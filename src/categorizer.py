"""Provides methods to categorize transactions.

Layered categorization (each layer only runs if configured):

1. Exact lookup   -- (norm-desc, sign) majority vote from labeled history.
2. Rule matchers  -- hand-written rules in ``data/matchers.json``.
3. Consensus fuzzy-- IDF nearest-neighbour, strict unanimous gate.
4. LLM fallback   -- Haiku for novel merchants, cached + fed back to layer 1.
5. Default        -- ``Uncategorized`` + a REVIEW remark for human triage.

With no index built and the LLM disabled (the default), only layer 2 runs, so
behaviour is identical to the original rule-only categorizer.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Hashable, Optional

from src.category_index import CategoryIndex
from src.constants import DEFAULT_CATEGORY, MATCHERS_FILE_PATH
from src.llm_categorizer import LLMCategorizer
from src.utils import log_and_exit

logger = logging.getLogger(__name__)

REVIEW_REMARK = "REVIEW: auto-categorization uncertain"


class Categorizer:
    """Categorizes transactions via lookup + rules + fuzzy + optional LLM.

    Each rule object should have:
        - "category": string (the category to assign)
        - "description": list of strings/regex patterns to match against description
    Optional keys in rule objects:
        - "debit": boolean (true = match only debits, false = match only credits)
        - "account": string (substring to match against transaction account name)
        - "use_regex": boolean (true if description strings are regex patterns)
    """

    def __init__(
        self,
        matchers_file: str = MATCHERS_FILE_PATH,
        index: Optional[CategoryIndex] = None,
        llm: Optional[LLMCategorizer] = None,
    ) -> None:
        """Constructor.

        Args:
            matchers_file: Path to the matchers JSON. Defaults to MATCHERS_FILE_PATH.
            index: Pre-built CategoryIndex (lookup + fuzzy). None disables those layers.
            llm: LLMCategorizer for novel-merchant fallback. None disables that layer.
        """
        self.matchers: list[dict[str, Any]] = []
        self.index = index
        self.llm = llm
        self.allowed_categories: list[str] = []
        self.examples: dict[str, list[str]] = {}
        try:
            logger.info(f"Loading categorization matchers from: {matchers_file}")
            with open(matchers_file, "r", encoding="utf-8") as f:
                self.matchers = json.load(f)
            assert isinstance(self.matchers, list), "Matchers JSON root must be a list."
            logger.info(f"Successfully loaded {len(self.matchers)} matchers.")
        except FileNotFoundError:
            log_and_exit(
                logger,
                f"Matchers file not found at '{matchers_file}'. Categorization will default.",
            )
        except (json.JSONDecodeError, AssertionError):
            log_and_exit(
                logger, f"Error loading or validating matchers file '{matchers_file}'"
            )
        except Exception as e:
            log_and_exit(
                logger,
                f"Unexpected error loading matchers from '{matchers_file}': {e}",
                e,
            )

    def build_index(
        self,
        historical_txns: list[dict[Hashable, Any]],
        enable_llm: bool = False,
    ) -> None:
        """Build the lookup index (and optional LLM) from labeled history.

        Derives the allowed-category vocabulary from history + matcher rules and
        precomputes few-shot examples so the LLM layer is constrained to known
        categories.
        """
        self.index = CategoryIndex().build(historical_txns)

        cats: set[str] = set()
        for txn in historical_txns:
            c = txn.get("category")
            if c and c != DEFAULT_CATEGORY:
                cats.add(str(c))
        for m in self.matchers:
            c = m.get("category")
            if c and c != DEFAULT_CATEGORY:
                cats.add(str(c))
        self.allowed_categories = sorted(cats)
        self.examples = LLMCategorizer.build_examples(
            historical_txns, self.allowed_categories
        )

        if enable_llm and self.llm is None:
            self.llm = LLMCategorizer()
        logger.info(
            f"Categorizer index ready: {len(self.allowed_categories)} categories, "
            f"LLM={'on' if self.llm else 'off'}."
        )

    def _match_rules(
        self, txn: dict[Hashable, Any], is_debit: bool, i: int
    ) -> Optional[str]:
        """Return the category from the first matching rule, or None."""
        txn_description = str(txn.get("description", "")).lower()
        txn_account = str(txn.get("account", "")).lower()

        for matcher in self.matchers:
            if (
                "debit" in matcher
                and isinstance(matcher["debit"], bool)
                and matcher["debit"] != is_debit
            ):
                continue
            if (
                "account" in matcher
                and isinstance(matcher["account"], str)
                and matcher["account"].lower() not in txn_account
            ):
                continue

            matcher_descriptions = matcher.get("description", [])
            use_regex = matcher.get("use_regex", False)
            if not isinstance(matcher_descriptions, list):
                log_and_exit(
                    logger,
                    f"Matcher has invalid 'description' format (expected list): {matcher}",
                )

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
                    log_and_exit(
                        logger,
                        f"Invalid regex pattern in matcher: '{pattern_str}'. Error: {e}."
                        f"Matcher: {matcher}",
                        e,
                    )

                if match_found:
                    category_to_set = matcher.get("category", DEFAULT_CATEGORY)
                    assert isinstance(
                        category_to_set, str
                    ), f"Invalid category type in matcher: {matcher}"
                    logger.debug(
                        f"Matched txn desc '{txn.get('description')}' to '{category_to_set}' "
                        f"using pattern '{pattern_str}'"
                    )
                    return category_to_set
        return None

    def categorize(self, txns: list[dict[Hashable, Any]]) -> list[dict[Hashable, Any]]:
        """Categorizes a list of transactions in-place.

        Args:
            txns: Transaction dicts. Expected keys: 'amount', 'description', 'account'.

        Returns:
            The same list with the 'category' key added/updated.
        """
        if not self.matchers:
            log_and_exit(
                logger,
                "No valid matchers loaded. Assigning default category to all transactions.",
            )

        logger.info(f"Starting categorization for {len(txns)} transactions...")

        sources: dict[str, int] = {}
        needs_llm: list[int] = []
        fuzzy_pending: dict[int, str] = {}

        for i, txn in enumerate(txns):
            if not all(k in txn for k in ["amount", "description", "account"]):
                log_and_exit(
                    logger,
                    f"Skipping transaction index {i} due to missing keys. Txn: {txn}",
                )

            txn["category"] = DEFAULT_CATEGORY
            try:
                is_debit = float(txn["amount"]) < 0
            except (TypeError, ValueError):
                log_and_exit(
                    logger, f"Could not determine debit/credit for txn index {i}."
                )

            # Layer 1+3: lookup (exact + consensus fuzzy) computed together.
            lk_cat, _conf, lk_src = (None, 0.0, None)
            if self.index is not None:
                lk_cat, _conf, lk_src = self.index.lookup(
                    txn.get("description"), txn["amount"]
                )

            # Layer 1: exact lookup wins outright.
            if lk_src == "exact" and lk_cat:
                txn["category"] = lk_cat
                sources["exact"] = sources.get("exact", 0) + 1
                continue

            # Layer 2: hand-written rule matchers.
            rule_cat = self._match_rules(txn, is_debit, i)
            if rule_cat is not None:
                txn["category"] = rule_cat
                sources["rule"] = sources.get("rule", 0) + 1
                continue

            # Layer 3: consensus fuzzy (already computed above).
            if lk_src == "fuzzy" and lk_cat:
                fuzzy_pending[i] = lk_cat
                continue

            # Layer 4: defer to LLM.
            needs_llm.append(i)

        # Apply fuzzy results.
        for i, cat in fuzzy_pending.items():
            txns[i]["category"] = cat
            sources["fuzzy"] = sources.get("fuzzy", 0) + 1

        # Layer 4: LLM fallback for whatever remains.
        if needs_llm and self.llm is not None:
            batch = [txns[i] for i in needs_llm]
            preds = self.llm.classify(batch, self.allowed_categories, self.examples)
            for local, gi in enumerate(needs_llm):
                if local in preds:
                    cat, src = preds[local]
                    txns[gi]["category"] = cat
                    sources[src] = sources.get(src, 0) + 1
                    # Cache-back: future exact hit.
                    if self.index is not None:
                        self.index.add(
                            txns[gi].get("description"), txns[gi]["amount"], cat
                        )

        # Layer 5: anything still default -> flag for human review.
        uncategorized = 0
        for txn in txns:
            if txn["category"] == DEFAULT_CATEGORY:
                uncategorized += 1
                if not str(txn.get("remarks", "")).strip():
                    txn["remarks"] = REVIEW_REMARK

        logger.info(
            f"Categorization complete. Processed: {len(txns)}. "
            f"By source: {sources or 'none'}."
        )
        if uncategorized:
            logger.warning(
                f"{uncategorized}/{len(txns)} remain '{DEFAULT_CATEGORY}' (flagged for review)."
            )
        else:
            logger.info("All processed transactions were categorized.")

        return txns
