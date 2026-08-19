# gajana/transaction_matcher.py
from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from operator import itemgetter
from typing import Any, Hashable

logger = logging.getLogger(__name__)

# The same transaction can arrive from different statement feeds in different
# description formats: flipped case, injected whitespace/OCR noise, and a
# trailing " Value Dt .../Ref ..." metadata suffix. These regexes normalize
# those variations away when building the dedup signature.
_META_SUFFIX_RE = re.compile(r"\s+(?:value dt|ref)\b.*$", re.IGNORECASE)
_REF_RE = re.compile(r"(?<!\d)\d{12}(?!\d)")  # 12-digit UPI/IMPS/NEFT RRN
_GST_RE = re.compile(r"\b([csi]gst)\b", re.IGNORECASE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]")

# Transient key set by TransactionProcessor naming the statement a txn came from.
# Used only to count a statement's internal repeats; stripped before storage.
SOURCE_KEY = "_source"


class TransactionMatcher:
    """
    Compares lists of transactions to find new ones.
    Note: This is a basic implementation assuming unique transactions
          can be identified by a combination of fields after sorting.
          More robust duplicate handling might be needed depending on data.
    """

    @staticmethod
    def _description_signature(description: str) -> tuple[str, Any, str]:
        """
        Collapse the many textual forms the same transaction can take into a
        single stable signature, so duplicate imports of one transaction match
        even when their descriptions differ in case, whitespace, or trailing
        bank metadata.

        - If the description carries 12-digit payment reference numbers
          (UPI/IMPS/NEFT RRNs), key on those: they uniquely identify the
          transaction. A GST tag is appended so a CGST/SGST split that shares a
          single reference stays distinct. Reference-less repeats (several
          same-amount ATM withdrawals on one day) collapse to one signature by
          design; ``find_new_txns`` counts them instead of de-duplicating them.
        - Otherwise fall back to normalized text: drop the trailing
          " Value Dt .../Ref ..." metadata, lowercase, and strip
          non-alphanumerics to absorb case and whitespace/OCR artifacts.
        """
        desc = description or ""
        refs = tuple(sorted(set(_REF_RE.findall(desc))))
        if refs:
            gst = _GST_RE.search(desc)
            return ("ref", refs, gst.group(1).lower() if gst else "")
        stripped = _META_SUFFIX_RE.sub("", desc)
        return ("txt", _NON_ALNUM_RE.sub("", stripped.lower()), "")

    @staticmethod
    def _txn_id(txn: dict[Hashable, Any]) -> tuple:
        """Build the duplicate-detection key for a single transaction."""
        return (
            txn["date"].strftime("%Y-%m-%d"),
            txn["account"],
            f"{txn['amount']:.2f}",
            TransactionMatcher._description_signature(txn["description"]),
        )

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
            for txn in all_potential_txns:
                txn.pop(SOURCE_KEY, None)
            return all_potential_txns

        # Counted, not set-based: a statement can legitimately repeat the same
        # date/amount/description (three ₹10,000 ATM withdrawals at one machine
        # on one day — bank-axis-karti Jul-2026, where set semantics booked one
        # and silently dropped ₹20,000).
        #
        # The count that matters is the *per-statement* one, taken as the max
        # across sources rather than the sum: the same transaction reaching us
        # from two feeds (Gmail PDF + Drive sheet) is still one transaction, so
        # summing would resurrect the double-import bug while the max keeps a
        # statement's genuine repeats. Re-imports stay idempotent either way —
        # once the ledger holds as many copies as the statement shows, the quota
        # is met and nothing new is emitted.
        try:
            old_txn_ids = Counter(TransactionMatcher._txn_id(txn) for txn in old_txns)
        except KeyError as e:
            logger.fatal(
                f"Missing key {e} in old transactions during ID creation. Matching may be inaccurate."
            )
            old_txn_ids = Counter()

        per_source: dict[Any, Counter] = defaultdict(Counter)
        for txn in all_potential_txns:
            try:
                per_source[txn.get(SOURCE_KEY, "")][
                    TransactionMatcher._txn_id(txn)
                ] += 1
            except Exception:
                continue  # malformed rows are reported in the emit loop below
        quota: Counter = Counter()
        for counts in per_source.values():
            for txn_id, n in counts.items():
                quota[txn_id] = max(quota[txn_id], n)

        new_txns = []
        emitted: Counter = Counter()

        for txn in all_potential_txns:
            try:
                potential_id = TransactionMatcher._txn_id(txn)
                allowance = max(quota[potential_id] - old_txn_ids[potential_id], 0)

                if emitted[potential_id] < allowance:
                    new_txns.append(txn)
                    emitted[potential_id] += 1
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
        for txn in new_txns:
            txn.pop(SOURCE_KEY, None)  # transient: never reaches storage
        logger.info(
            f"Transaction matching complete. Identified {len(new_txns)} new transactions."
        )
        return new_txns
