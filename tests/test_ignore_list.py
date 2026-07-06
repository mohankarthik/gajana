from __future__ import annotations

import json

import main
from src.constants import load_ignore_rules, txn_matches_ignore_rule

RULES = [{"account": "bank-hdfc-karti", "description_contains": "GOOGLE IT SERVICES"}]


def test_matches_account_and_substring():
    txn = {
        "account": "bank-hdfc-karti",
        "description": "NEFT Cr-GOOGLE IT SERVICES INDIA PVT LTD-Salary",
    }
    assert txn_matches_ignore_rule(txn, RULES) is True


def test_case_insensitive():
    txn = {"account": "bank-hdfc-karti", "description": "google it services payroll"}
    assert txn_matches_ignore_rule(txn, RULES) is True


def test_matches_despite_injected_whitespace():
    # LLM PDF parsing can split a word: "INDIA" -> "I NDIA". Normalized matching
    # must still catch it (this exact miss let a raw salary NEFT into the ledger).
    rules = [
        {
            "account": "bank-hdfc-karti",
            "description_contains": "GOOGLE IT SERVICES INDIA",
        }
    ]
    txn = {
        "account": "bank-hdfc-karti",
        "description": "NEFT CR-CITI0000006-GOOGLE IT SERVICES I NDIA PVT LTD-M",
    }
    assert txn_matches_ignore_rule(txn, rules) is True


def test_account_must_match_when_specified():
    txn = {"account": "bank-axis-karti", "description": "GOOGLE IT SERVICES INDIA"}
    assert txn_matches_ignore_rule(txn, RULES) is False


def test_no_substring_no_match():
    txn = {"account": "bank-hdfc-karti", "description": "SWIGGY ORDER"}
    assert txn_matches_ignore_rule(txn, RULES) is False


def test_rule_without_account_matches_any_account():
    rules = [{"description_contains": "PAYROLL"}]
    txn = {"account": "whatever", "description": "monthly PAYROLL credit"}
    assert txn_matches_ignore_rule(txn, rules) is True


def test_load_missing_file_returns_empty(tmp_path):
    assert load_ignore_rules(str(tmp_path / "nope.json")) == []


def test_load_non_list_returns_empty(tmp_path):
    p = tmp_path / "ignore.json"
    p.write_text(json.dumps({"not": "a list"}))
    assert load_ignore_rules(str(p)) == []


def test_load_valid_file(tmp_path):
    p = tmp_path / "ignore.json"
    p.write_text(json.dumps(RULES))
    assert load_ignore_rules(str(p)) == RULES


def test_apply_ignore_rules_filters(monkeypatch):
    monkeypatch.setattr(main, "IGNORE_RULES", RULES)
    txns = [
        {"account": "bank-hdfc-karti", "description": "GOOGLE IT SERVICES INDIA sal"},
        {"account": "bank-hdfc-karti", "description": "AMAZON PURCHASE"},
    ]
    kept = main.apply_ignore_rules(txns)
    assert len(kept) == 1
    assert kept[0]["description"] == "AMAZON PURCHASE"


def test_apply_ignore_rules_noop_when_empty(monkeypatch):
    monkeypatch.setattr(main, "IGNORE_RULES", [])
    txns = [{"account": "x", "description": "GOOGLE IT SERVICES INDIA"}]
    assert main.apply_ignore_rules(txns) == txns
