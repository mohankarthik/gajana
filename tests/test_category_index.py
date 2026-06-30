"""Unit tests for the retrieval CategoryIndex (exact + consensus fuzzy)."""

from __future__ import annotations

from src.category_index import CategoryIndex, norm, toks


def _hist():
    return [
        {
            "description": "NETFLIX SUBSCRIPTION 4499",
            "amount": -649,
            "category": "Bills:Subscriptions",
        },
        {
            "description": "NETFLIX SUBSCRIPTION 1102",
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
        {
            "description": "uber ride airport",
            "amount": -300,
            "category": "Expense:Local Travel",
        },
        {
            "description": "random kirana store",
            "amount": -120,
            "category": "Bills:Groceries",
        },
    ]


def test_norm_collapses_digits_and_punct():
    assert norm("NETFLIX  Sub 4499!!") == "netflix sub #"
    assert norm("UPI/123/abc") == "upi/#/abc"
    assert norm(None) == ""


def test_toks_drops_short_tokens():
    assert "ab" not in toks("ab cde fghi")
    assert set(toks("acme global x")) == {"acme", "global"}


def test_exact_match_majority_vote():
    idx = CategoryIndex().build(_hist())
    cat, conf, src = idx.lookup("NETFLIX SUBSCRIPTION 9999", -649)
    assert (cat, src) == ("Bills:Subscriptions", "exact")
    assert conf == 1.0


def test_exact_respects_sign():
    # Same normalized desc but credit (positive) has no training row -> no exact.
    idx = CategoryIndex().build(_hist())
    cat, _conf, src = idx.lookup("NETFLIX SUBSCRIPTION 9999", 649)
    assert src != "exact"


def test_fuzzy_consensus_hit():
    idx = CategoryIndex().build(_hist())
    cat, conf, src = idx.lookup("ACME GLOBAL CONSULTING RETAINER PAYOUT", 75000)
    assert (cat, src) == ("Income:Consulting", "fuzzy")
    assert conf >= 0.80


def test_unknown_returns_none():
    idx = CategoryIndex().build(_hist())
    assert idx.lookup("totally novel merchant xyz", -50) == (None, 0.0, None)


def test_skips_uncategorized_rows():
    hist = [
        {"description": "blah", "amount": -1, "category": "Uncategorized"},
        {"description": "blah2", "amount": -1, "category": ""},
        {"description": "blah3", "amount": -1, "category": None},
    ]
    idx = CategoryIndex().build(hist)
    assert idx.size == 0


def test_cache_back_creates_exact_hit():
    idx = CategoryIndex().build(_hist())
    assert idx.lookup("brand new merchant", -10)[2] is None
    idx.add("brand new merchant", -10, "Expense:Shopping")
    cat, conf, src = idx.lookup("BRAND NEW MERCHANT", -10)
    assert (cat, src) == ("Expense:Shopping", "exact")


def test_cache_back_ignores_default():
    idx = CategoryIndex().build(_hist())
    idx.add("x merchant", -10, "Uncategorized")
    assert idx.lookup("x merchant", -10)[2] is None


def test_empty_index_is_inert():
    idx = CategoryIndex()
    assert idx.lookup("anything", -1) == (None, 0.0, None)
    assert idx.size == 0
