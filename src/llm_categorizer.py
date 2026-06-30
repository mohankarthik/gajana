"""LLM fallback categorizer for novel descriptions the lookup index misses.

Used only as the last automated layer (after exact lookup, matchers and
consensus fuzzy). Classifications are constrained to the known category
vocabulary and persisted to ``data/llm_category_cache.json`` keyed by
(normalized-description, sign) so a given merchant is paid for once.

Reuses the pipeline's existing LiteLLM wiring (``configure_api_keys`` /
throttle) from ``src.pdf_parser``. Default model: Haiku 4.5.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import defaultdict
from typing import Any, Hashable, Optional

from src.category_index import norm
from src.pdf_parser import configure_api_keys, has_any_api_key

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "anthropic/claude-haiku-4-5"
CACHE_FILE_PATH = "data/llm_category_cache.json"
BATCH_SIZE = 25
MAX_FEWSHOT_PER_CAT = 3


def _sign_str(amount: Any) -> str:
    try:
        return "DEBIT" if float(amount) < 0 else "CREDIT"
    except (TypeError, ValueError):
        return "CREDIT"


def _cache_key(desc: Any, amount: Any) -> str:
    return f"{norm(desc)}|{_sign_str(amount)}"


class LLMCategorizer:
    """Classifies uncovered transactions via an LLM, with a persistent cache."""

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        cache_file: str = CACHE_FILE_PATH,
        enabled: bool = True,
    ) -> None:
        self.model = model
        self.cache_file = cache_file
        self.enabled = enabled
        self.cache: dict[str, str] = self._load_cache()
        self._client_ready = False
        if self.enabled:
            configure_api_keys()
            self._client_ready = has_any_api_key()
            if not self._client_ready:
                logger.warning(
                    "LLMCategorizer enabled but no API key configured; "
                    "falling back to cache-only."
                )

    # --- cache persistence ---

    def _load_cache(self) -> dict[str, str]:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return {str(k): str(v) for k, v in data.items()}
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not read LLM cache {self.cache_file}: {e}")
        return {}

    def _save_cache(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.cache_file) or ".", exist_ok=True)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2, sort_keys=True, ensure_ascii=False)
        except IOError as e:
            logger.warning(f"Could not write LLM cache {self.cache_file}: {e}")

    # --- prompt construction ---

    def _build_system_prompt(
        self,
        allowed_categories: list[str],
        examples_by_cat: dict[str, list[str]],
    ) -> str:
        examples = "\n".join(
            f"{cat}:\n  " + "\n  ".join(examples_by_cat[cat])
            for cat in allowed_categories
            if examples_by_cat.get(cat)
        )
        cat_list = "\n".join("- " + c for c in allowed_categories)
        return (
            "You categorize Indian personal bank/credit-card transactions.\n"
            "Assign EXACTLY ONE category from this fixed list. Output the "
            "category string verbatim, never invent new ones.\n\n"
            f"CATEGORIES:\n{cat_list}\n\n"
            f"Reference examples (description [DEBIT/CREDIT] -> category):\n"
            f"{examples}\n\n"
            "Rules: DEBIT = money out, CREDIT = money in. Use the sign + "
            "merchant text + account to decide.\n"
            'Return ONLY a JSON array: [{"i": <int>, "category": '
            '"<exact category>"}].'
        )

    def _call_llm(self, system: str, user: str) -> dict[int, str]:
        import litellm

        litellm.suppress_debug_info = True
        r = litellm.completion(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=2000,
        )
        txt = r.choices[0].message.content
        m = re.search(r"\[.*\]", txt, re.DOTALL)
        if not m:
            return {}
        out: dict[int, str] = {}
        for o in json.loads(m.group(0)):
            try:
                out[int(o["i"])] = str(o["category"])
            except (KeyError, ValueError, TypeError):
                continue
        return out

    # --- public API ---

    def classify(
        self,
        txns: list[dict[Hashable, Any]],
        allowed_categories: list[str],
        examples_by_cat: Optional[dict[str, list[str]]] = None,
    ) -> dict[int, tuple[str, str]]:
        """Classify transactions by their index in ``txns``.

        Returns ``{txn_index: (category, source)}`` where ``source`` is
        ``"llm-cache"`` or ``"llm"``. Only confident, on-vocabulary results are
        returned; anything else is omitted so the caller leaves it
        Uncategorized for review. New LLM results are written to the cache.
        """
        results: dict[int, tuple[str, str]] = {}
        if not txns:
            return results

        allowed = set(allowed_categories)
        examples_by_cat = examples_by_cat or {}

        # 1. serve from cache
        uncached: list[int] = []
        for i, txn in enumerate(txns):
            hit = self.cache.get(_cache_key(txn.get("description"), txn.get("amount")))
            if hit and hit in allowed:
                results[i] = (hit, "llm-cache")
            else:
                uncached.append(i)

        if not uncached:
            return results
        if not (self.enabled and self._client_ready):
            logger.info(
                f"LLM disabled/unavailable; {len(uncached)} txns left for review."
            )
            return results

        system = self._build_system_prompt(allowed_categories, examples_by_cat)
        new_results = 0
        for b in range(0, len(uncached), BATCH_SIZE):
            batch_idx = uncached[b : b + BATCH_SIZE]
            lines = []
            for local, gi in enumerate(batch_idx):
                txn = txns[gi]
                desc = str(txn.get("description", ""))[:80]
                amt = txn.get("amount", 0)
                try:
                    amt_abs = abs(float(amt))
                except (TypeError, ValueError):
                    amt_abs = 0.0
                lines.append(
                    f'{local}: "{desc}" {_sign_str(amt)} '
                    f"amt={amt_abs:.0f} acct={txn.get('account')}"
                )
            user = "Classify:\n" + "\n".join(lines)
            try:
                preds = self._call_llm(system, user)
            except Exception as e:
                logger.warning(f"LLM categorize batch failed: {e}")
                continue
            for local, gi in enumerate(batch_idx):
                pred = preds.get(local)
                if pred and pred in allowed:
                    results[gi] = (pred, "llm")
                    txn = txns[gi]
                    self.cache[
                        _cache_key(txn.get("description"), txn.get("amount"))
                    ] = pred
                    new_results += 1

        if new_results:
            self._save_cache()
            logger.info(f"LLM categorized {new_results} novel txns (cached).")
        return results

    @staticmethod
    def build_examples(
        historical_txns: list[dict[Hashable, Any]],
        allowed_categories: list[str],
    ) -> dict[str, list[str]]:
        """Few-shot examples: up to N descriptions per category from history."""
        ex: dict[str, list[str]] = defaultdict(list)
        seen: dict[str, set[str]] = defaultdict(set)
        allowed = set(allowed_categories)
        for txn in historical_txns:
            cat = txn.get("category")
            if cat not in allowed:
                continue
            desc = str(txn.get("description", ""))
            n = norm(desc)
            if not n or n in seen[cat] or len(ex[cat]) >= MAX_FEWSHOT_PER_CAT:
                continue
            seen[cat].add(n)
            ex[cat].append(f'"{desc[:60]}" [{_sign_str(txn.get("amount"))}]')
        return dict(ex)
