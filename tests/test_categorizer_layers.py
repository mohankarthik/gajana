"""Integration tests for the layered Categorizer (index + rules + LLM stub)."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from src.categorizer import LOWCONF_REMARK, REVIEW_REMARK, Categorizer
from src.constants import DEFAULT_CATEGORY


@pytest.fixture
def matchers_file(tmp_path):
    p = tmp_path / "matchers.json"
    p.write_text(
        json.dumps(
            [{"category": "Expense:Dining", "description": ["zomato"], "debit": True}]
        )
    )
    return str(p)


def _history():
    return [
        {
            "description": "NETFLIX SUBSCRIPTION 4499",
            "amount": -649,
            "category": "Bills:Subscriptions",
        },
        {
            "description": "ACME GLOBAL CONSULTING RETAINER",
            "amount": 75000,
            "category": "Income:Consulting",
        },
        {
            "description": "ACME GLOBAL CONSULTING RETAINER",
            "amount": 75000,
            "category": "Income:Consulting",
        },
        {
            "description": "ACME GLOBAL CONSULTING RETAINER",
            "amount": 75000,
            "category": "Income:Consulting",
        },
    ]


class _StubLLM:
    """Returns a fixed category for every uncovered txn; records calls."""

    def __init__(self, category="Expense:Shopping"):
        self.category = category
        self.calls = 0

    def classify(self, txns, allowed_categories, examples=None):
        self.calls += 1
        return {i: (self.category, "llm") for i in range(len(txns))}


def test_exact_layer_wins(matchers_file):
    c = Categorizer(matchers_file=matchers_file)
    c.build_index(_history())
    out = c.categorize(
        [{"description": "NETFLIX SUBSCRIPTION 9", "amount": -649, "account": "b"}]
    )
    assert out[0]["category"] == "Bills:Subscriptions"


def test_rule_layer_when_no_exact(matchers_file):
    c = Categorizer(matchers_file=matchers_file)
    c.build_index(_history())
    out = c.categorize(
        [{"description": "zomato order", "amount": -300, "account": "cc"}]
    )
    assert out[0]["category"] == "Expense:Dining"


def test_fuzzy_layer(matchers_file):
    c = Categorizer(matchers_file=matchers_file)
    c.build_index(_history())
    out = c.categorize(
        [
            {
                "description": "ACME GLOBAL CONSULTING RETAINER PAYOUT",
                "amount": 75000,
                "account": "b",
            }
        ]
    )
    assert out[0]["category"] == "Income:Consulting"
    assert out[0]["remarks"] == LOWCONF_REMARK


def test_llm_fallback_and_cache_back(matchers_file):
    stub = _StubLLM("Expense:Shopping")
    c = Categorizer(matchers_file=matchers_file, llm=stub)
    c.build_index(_history())
    txn = {"description": "weird new merchant", "amount": -42, "account": "b"}
    out = c.categorize([txn])
    assert out[0]["category"] == "Expense:Shopping"
    assert out[0]["remarks"] == LOWCONF_REMARK
    assert stub.calls == 1
    # cache-back: the same merchant now resolves via exact lookup (no LLM call).
    cat, _conf, src = c.index.lookup("WEIRD NEW MERCHANT", -42)
    assert (cat, src) == ("Expense:Shopping", "exact")


def test_review_flag_when_unresolved(matchers_file):
    c = Categorizer(matchers_file=matchers_file)  # no index, no llm
    c.build_index(_history())
    txn = {"description": "nothing matches here", "amount": -1, "account": "b"}
    out = c.categorize([txn])
    assert out[0]["category"] == DEFAULT_CATEGORY
    assert out[0]["remarks"] == REVIEW_REMARK


def test_lowconf_flag_set_over_nan_remark(matchers_file):
    """A NaN/None remark (empty sheet cell) must not suppress the review tag."""
    c = Categorizer(matchers_file=matchers_file)
    c.build_index(_history())
    txn = {
        "description": "ACME GLOBAL CONSULTING RETAINER PAYOUT",
        "amount": 75000,
        "account": "b",
        "remarks": float("nan"),
    }
    out = c.categorize([txn])
    assert out[0]["category"] == "Income:Consulting"
    assert out[0]["remarks"] == LOWCONF_REMARK


def test_review_flag_preserves_existing_remark(matchers_file):
    c = Categorizer(matchers_file=matchers_file)
    c.build_index(_history())
    txn = {
        "description": "nothing matches",
        "amount": -1,
        "account": "b",
        "remarks": "keep me",
    }
    out = c.categorize([txn])
    assert out[0]["remarks"] == "keep me"


def test_llm_live_path_mocked(tmp_path, mocker):
    """Exercises LLMCategorizer._call_llm + cache persistence with a mock."""
    from src.llm_categorizer import LLMCategorizer

    fake = SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content='[{"i": 0, "category": "Expense:Dining"}]'
                )
            )
        ]
    )
    mocker.patch("litellm.completion", return_value=fake)
    cache = tmp_path / "cache.json"
    llm = LLMCategorizer(cache_file=str(cache), enabled=True)
    llm._client_ready = True  # bypass real API-key requirement
    txns = [{"description": "some cafe bill", "amount": -250, "account": "cc"}]
    res = llm.classify(txns, ["Expense:Dining"], {"Expense:Dining": ['"x" [DEBIT]']})
    assert res == {0: ("Expense:Dining", "llm")}
    # persisted to cache
    saved = json.loads(cache.read_text())
    assert saved == {"some cafe bill|DEBIT": "Expense:Dining"}


def test_llm_off_vocab_prediction_dropped(tmp_path, mocker):
    from src.llm_categorizer import LLMCategorizer

    fake = SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content='[{"i": 0, "category": "Bogus"}]')
            )
        ]
    )
    mocker.patch("litellm.completion", return_value=fake)
    llm = LLMCategorizer(cache_file=str(tmp_path / "c.json"), enabled=True)
    llm._client_ready = True
    txns = [{"description": "x", "amount": -1, "account": "a"}]
    assert llm.classify(txns, ["Expense:Dining"]) == {}


def test_creditcard_payment_not_automobile():
    """Regression: 'car' matcher must be word-bounded so it never matches
    'creditcard'. Uses the real data/matchers.json."""
    c = Categorizer()  # real matchers, no index
    debit = [
        {
            "description": "CREDITCARD PAYMENT XX 5742 REF#NSUU3",
            "amount": -5742,
            "account": "bank-axis-karti",
        }
    ]
    c.categorize(debit)
    assert debit[0]["category"] == "Transfer:Credit Card"
    # genuine automobile keywords still match
    for desc in ("CAR WASH DOWNTOWN", "HP PETROL PUMP", "INDIAN OIL FUEL"):
        t = [{"description": desc, "amount": -900, "account": "bank-axis-karti"}]
        c.categorize(t)
        assert t[0]["category"] == "Expense:Automobile", desc
