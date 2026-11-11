# gajana/transaction_matcher.py
from __future__ import annotations

import datetime
import logging
import re
from collections import Counter, defaultdict
from typing import Any, Hashable, Mapping, Optional

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

LOOKBACK_DATE_TOLERANCE_DAYS = 3


def _clean_desc_str(raw_desc: Any) -> str:
    if raw_desc is None or (isinstance(raw_desc, float) and raw_desc != raw_desc):
        return ""
    return str(raw_desc)


def _txn_sort_key(txn: Mapping[Hashable, Any]) -> tuple[Any, str, float, str]:
    return (
        txn["date"],
        str(txn.get("account") or ""),
        float(txn.get("amount") or 0.0),
        _clean_desc_str(txn.get("description")),
    )


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
          Spacing around "Ref#" is normalized so "(Ref# VT...)" and
          "(Ref#VT...)" match while keeping the merchant/fee prefix distinct.
        """
        desc = description or ""
        rrn_refs = _REF_RE.findall(desc)
        refs = tuple(sorted(set(rrn_refs)))
        if refs:
            gst = _GST_RE.search(desc)
            return ("ref", refs, gst.group(1).lower() if gst else "")
        desc_norm = re.sub(r"ref#\s*", "ref#", desc, flags=re.IGNORECASE)
        stripped = _META_SUFFIX_RE.sub("", desc_norm)
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
        watermark_by_account: Optional[Mapping[str, datetime.datetime]] = None,
    ) -> list[dict[Hashable, Any]]:
        """
        Identifies transactions present in all_potential_txns but not in old_txns.

        Args:
            old_txns: List of transactions already processed (from the sheet).
            all_potential_txns: List of transactions parsed from statements.
            watermark_by_account: Optional mapping of account name to latest
                transaction datetime. When provided, transactions with
                txn["date"] <= watermark are deduplicated using multiset
                (date, account, amount) matching (ignoring descriptions),
                preventing description drift between parser versions from
                duplicating rows in the lookback window.

        Returns:
            List of transactions deemed new.
        """
        if not all_potential_txns:
            logger.info("No potential new transactions provided to match.")
            return []
        if not old_txns:
            logger.info(
                "No old transactions provided, considering all potential transactions as new."
            )
            all_potential_txns.sort(key=_txn_sort_key)
            for txn in all_potential_txns:
                txn.pop(SOURCE_KEY, None)
            return all_potential_txns

        def _in_lookback_window(txn_item: dict[Hashable, Any]) -> bool:
            if not watermark_by_account:
                return False
            acc_name = str(txn_item.get("account", ""))
            wm = watermark_by_account.get(acc_name)
            if wm is None:
                acc_norm = acc_name.strip().lower()
                wm = watermark_by_account.get(acc_norm)
                if wm is None:
                    for k, v in watermark_by_account.items():
                        if str(k).strip().lower() == acc_norm:
                            wm = v
                            break
            if wm is None:
                return False
            t_d = txn_item.get("date")
            if t_d is None:
                return False
            d_val = t_d.date() if hasattr(t_d, "date") else t_d
            wm_val = wm.date() if hasattr(wm, "date") else wm
            return bool(d_val <= wm_val)

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

        # Multiset counts by (date, account, amount) for transactions inside the
        # lookback window (<= watermark), ignoring descriptions. Exclude
        # synthetic plugin rows (e.g. payslip split) from old counts.
        available_old: Counter[tuple[datetime.date, str, float]] = Counter()
        for t in old_txns:
            if "auto-split from payslip" in str(t.get("description", "")).lower():
                continue
            try:
                t_date = t["date"]
                if hasattr(t_date, "date"):
                    dt_val = t_date.date()
                elif isinstance(t_date, datetime.date):
                    dt_val = t_date
                else:
                    dt_val = datetime.date.fromisoformat(str(t_date)[:10])
                acc_clean = str(t.get("account", "")).strip().lower()
                amt_val = round(float(t.get("amount", 0.0)), 2)
                available_old[(dt_val, acc_clean, amt_val)] += 1
            except Exception:
                continue

        per_source: dict[Any, Counter] = defaultdict(Counter)
        for txn in all_potential_txns:
            try:
                if not _in_lookback_window(txn):
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
        source_da_counts: dict[Any, Counter[tuple[datetime.date, str, float]]] = (
            defaultdict(Counter)
        )
        seen_window_keys: set[tuple[tuple[datetime.date, str, float], int]] = set()
        lookback_candidates: list[dict[Hashable, Any]] = []

        for txn in all_potential_txns:
            try:
                if _in_lookback_window(txn):
                    t_date = txn["date"]
                    if not isinstance(t_date, (datetime.datetime, datetime.date)):
                        raise TypeError(
                            f"Date is not datetime/date: {type(t_date)} for {txn}"
                        )
                    c_date = t_date.date() if hasattr(t_date, "date") else t_date
                    acc_clean = str(txn["account"]).strip().lower()
                    amt_val = round(float(txn["amount"]), 2)
                    da_key = (c_date, acc_clean, amt_val)
                    source = txn.get(SOURCE_KEY, "")
                    source_da_counts[source][da_key] += 1
                    occ = source_da_counts[source][da_key]
                    window_key = (da_key, occ)
                    if window_key not in seen_window_keys:
                        seen_window_keys.add(window_key)
                        lookback_candidates.append(txn)
                    else:
                        logger.debug(
                            f"Skipping duplicate feed lookback txn: {da_key} (occ={occ})"
                        )
                else:
                    potential_id = TransactionMatcher._txn_id(txn)
                    allowance = max(quota[potential_id] - old_txn_ids[potential_id], 0)

                    if emitted[potential_id] < allowance:
                        new_txns.append(txn)
                        emitted[potential_id] += 1
                    else:
                        logger.debug(
                            f"Skipping duplicate/old transaction: {potential_id}"
                        )

            except KeyError as e:
                logger.fatal(
                    f"Missing key {e} in potential transaction. Skipping matching for: {txn}"
                )
            except Exception as e:
                logger.fatal(
                    f"Error creating ID for potential transaction: {e}. Skipping matching for: {txn}",
                    exc_info=True,
                )

        # Deterministic 2-pass greedy matching for lookback candidates:
        matched = [False] * len(lookback_candidates)

        # Pass 1 (Exact date, delta = 0):
        for i, cand in enumerate(lookback_candidates):
            c_date = (
                cand["date"].date() if hasattr(cand["date"], "date") else cand["date"]
            )
            acc_clean = str(cand["account"]).strip().lower()
            amt_val = round(float(cand["amount"]), 2)
            key = (c_date, acc_clean, amt_val)
            if available_old[key] > 0:
                available_old[key] -= 1
                matched[i] = True
                logger.debug(f"Exact match in lookback window: {key}")

        # Pass 2 (Value-date drift, delta = 1 .. LOOKBACK_DATE_TOLERANCE_DAYS):
        for i, cand in enumerate(lookback_candidates):
            if matched[i]:
                continue
            c_date = (
                cand["date"].date() if hasattr(cand["date"], "date") else cand["date"]
            )
            acc_clean = str(cand["account"]).strip().lower()
            amt_val = round(float(cand["amount"]), 2)
            for delta in range(1, LOOKBACK_DATE_TOLERANCE_DAYS + 1):
                found_neighbor = False
                for neighbor_date in (
                    c_date - datetime.timedelta(days=delta),
                    c_date + datetime.timedelta(days=delta),
                ):
                    n_key = (neighbor_date, acc_clean, amt_val)
                    if available_old[n_key] > 0:
                        available_old[n_key] -= 1
                        matched[i] = True
                        logger.info(
                            f"Drift match in lookback window (delta={delta}d): "
                            f"{c_date} -> {neighbor_date} for {acc_clean} {amt_val}"
                        )
                        found_neighbor = True
                        break
                if found_neighbor:
                    break

        for i, cand in enumerate(lookback_candidates):
            if not matched[i]:
                c_date = (
                    cand["date"].date()
                    if hasattr(cand["date"], "date")
                    else cand["date"]
                )
                acc_clean = str(cand["account"]).strip().lower()
                amt_val = round(float(cand["amount"]), 2)
                desc_snip = _clean_desc_str(cand.get("description"))[:60]
                logger.info(
                    f"Booking unmatched lookback txn (<= watermark): "
                    f"{c_date} {acc_clean} {amt_val} {desc_snip}"
                )
                new_txns.append(cand)

        new_txns.sort(key=_txn_sort_key)
        for txn in new_txns:
            txn.pop(SOURCE_KEY, None)  # transient: never reaches storage
        logger.info(
            f"Transaction matching complete. Identified {len(new_txns)} new transactions."
        )
        return new_txns
