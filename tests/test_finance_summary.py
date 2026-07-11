"""Tests for the Homepage finance-summary writer (src/finance_summary.py)."""

from __future__ import annotations

import json

from src import finance_summary
from src.finance_summary import _inr, _num, _pct, build_summary, write_summary


class FakeSheetsDataSource:
    """Returns canned grids keyed by (sheet_name, range_spec)."""

    def __init__(self, responses):
        self.responses = responses

    def get_sheet_data(self, source_id, sheet_name, range_spec):
        return self.responses.get((sheet_name, range_spec), [])


def _ds():
    # Values as the Sheets API returns them: FORMATTED_VALUE strings.
    return FakeSheetsDataSource(
        {
            ("Holding", "B2:B3"): [["₹7,56,00,000"], ["₹11,85,00,000"]],
            ("Holding", "M2:M3"): [["14.99%"], ["56.80%"]],
            ("Yearly", "A2:AB200"): [
                ["", "2025", "2026"],
                ["Income", "₹1,00,00,000", "₹1,40,00,000"],
                ["Regular Expenses", "₹20,00,000", "₹23,23,000"],
                ["New Investments", "₹30,00,000", "₹42,51,000"],
            ],
        }
    )


class TestParsing:
    def test_num_currency(self):
        assert _num("₹1,39,54,452") == 13954452.0

    def test_num_percent(self):
        assert _num("56.80%") == 56.80

    def test_num_unicode_minus(self):
        assert _num("−₹19") == -19.0

    def test_num_na_and_blank(self):
        assert _num("#N/A") is None
        assert _num("") is None
        assert _num(None) is None

    def test_inr_crore_lakh(self):
        assert _inr(118500000) == "₹11.85 Cr"
        assert _inr(2323000) == "₹23.23 L"
        assert _inr(500) == "₹500"
        assert _inr(None) == "—"

    def test_pct(self):
        assert _pct("14.99%") == "14.99%"
        assert _pct("#N/A") == "—"


class TestBuildSummary:
    def test_full_schema(self):
        out = build_summary(_ds())
        # Investments
        assert out["net_worth"] == "₹11.85 Cr"
        assert out["net_worth_raw"] == 118500000.0
        assert out["portfolio_value"] == "₹11.85 Cr"
        assert out["invested"] == "₹7.56 Cr"
        assert out["profit"] == "₹4.29 Cr"
        assert out["profit_pct"] == "56.80%"
        assert out["xirr"] == "14.99%"
        # Annual (current-year column auto-detected)
        assert out["year"] == 2026
        assert out["income"] == "₹1.40 Cr"
        assert out["income_raw"] == 14000000.0
        assert out["regular_expenses"] == "₹23.23 L"
        assert out["new_investments"] == "₹42.51 L"
        # updated timestamp present
        assert "updated" in out

    def test_schema_keys_match_interim_contract(self):
        # Homepage mappings depend on these exact keys.
        out = build_summary(_ds())
        expected = {
            "net_worth",
            "net_worth_raw",
            "portfolio_value",
            "invested",
            "profit",
            "profit_pct",
            "xirr",
            "year",
            "income",
            "income_raw",
            "regular_expenses",
            "new_investments",
            "updated",
        }
        assert set(out.keys()) == expected

    def test_missing_year_column_falls_back_to_latest(self, monkeypatch):
        import datetime as _dt

        class FrozenDate(_dt.date):
            @classmethod
            def today(cls):
                return cls(2099, 1, 1)

        monkeypatch.setattr(finance_summary.datetime, "date", FrozenDate)
        out = build_summary(_ds())
        assert out["year"] == 2026  # latest available header year
        assert out["income"] == "₹1.40 Cr"


class TestWriteSummary:
    def test_atomic_write(self, tmp_path):
        path = tmp_path / "finance" / "summary.json"
        data = {"net_worth": "₹11.85 Cr", "year": 2026}
        write_summary(data, str(path))
        assert json.loads(path.read_text(encoding="utf-8")) == data
        # temp file cleaned up by os.replace
        assert not (tmp_path / "finance" / "summary.json.tmp").exists()
