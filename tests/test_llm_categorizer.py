"""Unit tests for LLMCategorizer cache + vocabulary handling (no live calls)."""

from __future__ import annotations

import json

from src.llm_categorizer import LLMCategorizer, _cache_key


def _llm(tmp_path, enabled=False):
    return LLMCategorizer(cache_file=str(tmp_path / "cache.json"), enabled=enabled)


def test_cache_key_normalizes_desc_and_sign():
    assert _cache_key("Zomato 123", -50) == _cache_key("ZOMATO 999", -7)
    assert _cache_key("Zomato 123", -50) != _cache_key("Zomato 123", 50)


def test_classify_serves_from_cache(tmp_path):
    cache_path = tmp_path / "cache.json"
    cache_path.write_text(json.dumps({"zomato dinner #|DEBIT": "Expense:Dining"}))
    llm = LLMCategorizer(cache_file=str(cache_path), enabled=False)
    txns = [{"description": "ZOMATO DINNER 88", "amount": -100, "account": "cc-x"}]
    res = llm.classify(txns, ["Expense:Dining"])
    assert res == {0: ("Expense:Dining", "llm-cache")}


def test_classify_rejects_off_vocabulary_cache(tmp_path):
    cache_path = tmp_path / "cache.json"
    cache_path.write_text(json.dumps({"zomato dinner|DEBIT": "Not:AllowedAnymore"}))
    llm = LLMCategorizer(cache_file=str(cache_path), enabled=False)
    txns = [{"description": "zomato dinner", "amount": -100, "account": "cc-x"}]
    res = llm.classify(txns, ["Expense:Dining"])
    assert res == {}


def test_classify_disabled_leaves_uncovered(tmp_path):
    llm = _llm(tmp_path, enabled=False)
    txns = [{"description": "novel", "amount": -1, "account": "a"}]
    assert llm.classify(txns, ["X"]) == {}


def test_classify_empty_list(tmp_path):
    assert _llm(tmp_path).classify([], ["X"]) == {}


def test_build_examples_caps_and_filters_vocab():
    hist = [
        {"description": f"food vendor {i}", "amount": -1, "category": "Expense:Dining"}
        for i in range(10)
    ] + [{"description": "x", "amount": -1, "category": "NotAllowed"}]
    ex = LLMCategorizer.build_examples(hist, ["Expense:Dining"])
    assert "NotAllowed" not in ex
    assert len(ex["Expense:Dining"]) <= 3


def test_corrupt_cache_file_is_ignored(tmp_path):
    cache_path = tmp_path / "cache.json"
    cache_path.write_text("not json{{")
    llm = LLMCategorizer(cache_file=str(cache_path), enabled=False)
    assert llm.cache == {}
