from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest

from plugins.salary_splitter import splitter as splitter_mod
from plugins.salary_splitter.splitter import (
    SalarySplitError,
    SalarySplitter,
    _col_to_num,
    _num_to_col,
)

SETTINGS = {
    "payslip_folder_id": "FOLDER",
    "salary_sheet_id": "SHEET",
    "month_col_start": "B",
    "input_row_map": {
        "Credited to A/C": "net_pay",
        "Basic": "basic",
        "GSU Value": "gsus_income",
        "Credited GSU Value": "gsus_income - gsus_broker_tax",
    },
    "salary_account": "bank-test",
    "split_map": {
        "Total Salary": {"category": "Income:X", "sign": "credit"},
        "Total Tax": {"category": "Tax", "sign": "debit"},
        "Equity": {"category": "Inv:Eq", "sign": "debit"},
    },
}

# Column A = label; column B = Jan (month 1). Split rows reconcile to net_pay:
# Total Salary - Total Tax - Equity = 800000 - 300000 - 26192 = 473808 == net_pay.
TAB_ROWS = [
    [],
    ["Credited to A/C", 473808],
    ["Basic", 312467],
    ["GSU Value", 3278546],
    ["Credited GSU Value", 2102204],
    ["Total Salary", 800000],
    ["Total Tax", 300000],
    ["Equity", 26192],
    ["Total In Hand", 473808],
]

FIELDS = {
    "date_of_payment": "2026-01-29",
    "net_pay": 473808.0,
    "basic": 312467.0,
    "gsus_income": 3278546.0,
    "gsus_broker_tax": 1176342.0,
}


class _Resp:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return self._data


class FakeSheetsService:
    def __init__(self, tab_rows):
        self.tab_rows = tab_rows
        self.batch_bodies: list[dict] = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, spreadsheetId=None, range=None, valueRenderOption=None):
        if range and range.endswith("A1:A80"):
            return _Resp({"values": [[r[0]] if r else [] for r in self.tab_rows]})
        return _Resp({"values": self.tab_rows})

    def batchUpdate(self, spreadsheetId=None, body=None):
        self.batch_bodies.append(body)
        return _Resp({})


class FakeDataSource:
    def __init__(self, tab_rows, ledger_rows=None):
        self.sheets_service = FakeSheetsService(tab_rows)
        self.drive_service = MagicMock()
        self._ledger = ledger_rows or [["Date", "Description", "Debit"]]
        self.appended: list = []

    def get_transaction_log_data(self, log_type):
        return self._ledger

    def append_transactions_to_log(self, log_type, values):
        self.appended.append((log_type, values))

    def download_file(self, file_id):
        return b"%PDF-fake"


def make_splitter(tab_rows=TAB_ROWS, ledger_rows=None):
    ds = FakeDataSource(tab_rows, ledger_rows)
    return SalarySplitter(ds, SETTINGS), ds


def test_col_helpers_roundtrip():
    for letter in ("A", "B", "M", "Z", "AA", "AB"):
        assert _num_to_col(_col_to_num(letter)) == letter


def test_month_col():
    sp, _ = make_splitter()
    assert sp._month_col(1) == "B"  # Jan
    assert sp._month_col(6) == "G"  # Jun
    assert sp._month_col(12) == "M"  # Dec


def test_fill_salary_sheet_writes_expected_cells():
    sp, ds = make_splitter()
    sp.fill_salary_sheet("2026-01", FIELDS)
    data = ds.sheets_service.batch_bodies[0]["data"]
    cells = {d["range"]: d["values"][0][0] for d in data}
    assert cells["2026!B2"] == 473808  # Credited to A/C row 2
    assert cells["2026!B3"] == 312467  # Basic row 3
    assert cells["2026!B4"] == 3278546  # GSU Value row 4
    assert cells["2026!B5"] == 2102204  # Credited GSU Value = income - broker tax


def test_fill_salary_sheet_unknown_label_raises():
    sp, _ = make_splitter(tab_rows=[["Basic", 1]])
    with pytest.raises(SalarySplitError):
        sp.fill_salary_sheet("2026-01", FIELDS)


def test_read_bottom_block_picks_month_column():
    sp, _ = make_splitter()
    block = sp.read_bottom_block("2026-01")
    assert block["Total Salary"] == 800000
    assert block["Total In Hand"] == 473808


def test_build_split_txns_signs_and_zero_skip():
    sp, _ = make_splitter()
    block = {"Total Salary": 800000, "Total Tax": 300000, "Equity": 0}
    pay_date = datetime.datetime(2026, 1, 29)
    txns = sp.build_split_txns("2026-01", block, pay_date)
    by_cat = {t["category"]: t["amount"] for t in txns}
    assert by_cat["Income:X"] == 800000  # credit positive
    assert by_cat["Tax"] == -300000  # debit negative
    assert "Inv:Eq" not in by_cat  # zero-value component skipped
    assert all(
        t["description"] == "Google Salary Jan-26 (auto-split from payslip)"
        for t in txns
    )
    assert all(t["account"] == "bank-test" for t in txns)


def test_build_split_txns_missing_row_raises():
    sp, _ = make_splitter()
    with pytest.raises(SalarySplitError):
        sp.build_split_txns(
            "2026-01", {"Total Salary": 1}, datetime.datetime(2026, 1, 1)
        )


def test_already_split_detects_existing_rows():
    desc = "Google Salary Jan-26 (auto-split from payslip)"
    ledger = [["Date", "Description"], ["2026-01-29", desc]]
    sp, _ = make_splitter(ledger_rows=ledger)
    assert sp.already_split("2026-01") is True
    assert sp.already_split("2026-02") is False


def test_run_happy_path_appends_split_rows(monkeypatch):
    sp, ds = make_splitter()
    monkeypatch.setattr(splitter_mod, "parse_payslip", lambda b: FIELDS)
    monkeypatch.setattr(sp, "find_payslip", lambda ym: ("fid", f"{ym}.pdf"))
    sp.run("2026-01")
    assert len(ds.appended) == 1
    log_type, values = ds.appended[0]
    assert log_type == "bank"
    assert len(values) == 3  # Income, Tax, Equity


def test_run_guard_blocks_on_net_pay_mismatch(monkeypatch):
    bad_tab = [r[:] for r in TAB_ROWS]
    bad_tab[7] = ["Equity", 200000]  # split rows no longer sum to net_pay
    sp, ds = make_splitter(tab_rows=bad_tab)
    monkeypatch.setattr(splitter_mod, "parse_payslip", lambda b: FIELDS)
    monkeypatch.setattr(sp, "find_payslip", lambda ym: ("fid", f"{ym}.pdf"))
    with pytest.raises(SalarySplitError):
        sp.run("2026-01")
    assert ds.appended == []  # nothing written


def test_run_guard_tolerates_gsu_rounding(monkeypatch):
    # Equity off by 231 (GSU rounding): split sum 473577 vs net 473808, within
    # the 0.1% tolerance (~474) -> should still write.
    tab = [r[:] for r in TAB_ROWS]
    tab[7] = ["Equity", 26192 + 231]
    sp, ds = make_splitter(tab_rows=tab)
    monkeypatch.setattr(splitter_mod, "parse_payslip", lambda b: FIELDS)
    monkeypatch.setattr(sp, "find_payslip", lambda ym: ("fid", f"{ym}.pdf"))
    sp.run("2026-01")
    assert len(ds.appended) == 1  # guard passed, rows written


SETTINGS_GSU = {
    **SETTINGS,
    "gsu_rounding": {
        "gross": "GSU Value",
        "credited": "Credited GSU Value",
        "taxed": "Taxed GSU Value",
    },
}

# Mirrors 2026-05: split rows overshoot net pay by the GSU rounding residual
# (2461 = 2987664 - 1913306 - 1071897), but cash reconciles exactly:
# split_sum (3702667 - 1291446 - 1913306 = 497915) - 2461 == net_pay 495454.
GSU_BLOCK = {
    "Total Salary": 3702667,
    "Total Tax": 1291446,
    "Equity": 1913306,
    "GSU Value": 2987664,
    "Credited GSU Value": 1913306,
    "Taxed GSU Value": 1071897,
}


def _gsu_splitter(monkeypatch, block):
    sp, ds = make_splitter()
    sp.s = SETTINGS_GSU
    monkeypatch.setattr(
        splitter_mod,
        "parse_payslip",
        lambda b: {"date_of_payment": "2026-05-28", "net_pay": 495454.0},
    )
    monkeypatch.setattr(sp, "find_payslip", lambda ym: ("fid", f"{ym}.pdf"))
    monkeypatch.setattr(sp, "fill_salary_sheet", lambda ym, fields: None)
    monkeypatch.setattr(sp, "read_bottom_block", lambda ym: block)
    return sp, ds


def test_gsu_rounding_residual_zero_without_config():
    sp, _ = make_splitter()  # base SETTINGS: no gsu_rounding
    assert sp._gsu_rounding_residual(GSU_BLOCK) == 0.0


def test_gsu_rounding_residual_computed_with_config(monkeypatch):
    sp, _ = _gsu_splitter(monkeypatch, GSU_BLOCK)
    assert sp._gsu_rounding_residual(GSU_BLOCK) == 2461


def test_run_guard_reconciles_gsu_residual(monkeypatch):
    # Split sum overshoots net pay by 2461, but that is exactly the GSU residual,
    # so cash reconciles and the write proceeds.
    sp, ds = _gsu_splitter(monkeypatch, GSU_BLOCK)
    sp.run("2026-05")
    assert len(ds.appended) == 1


def test_run_guard_gsu_still_catches_real_error(monkeypatch):
    # Same GSU residual, but Total Tax is 5000 short -> cash no longer reconciles.
    bad = {**GSU_BLOCK, "Total Tax": 1291446 - 5000}
    sp, ds = _gsu_splitter(monkeypatch, bad)
    with pytest.raises(SalarySplitError):
        sp.run("2026-05")
    assert ds.appended == []


def test_run_dry_run_skips_ledger_write(monkeypatch):
    sp, ds = make_splitter()
    monkeypatch.setattr(splitter_mod, "parse_payslip", lambda b: FIELDS)
    monkeypatch.setattr(sp, "find_payslip", lambda ym: ("fid", f"{ym}.pdf"))
    sp.run("2026-01", dry_run=True)
    assert ds.appended == []  # dry run: sheet filled, ledger untouched
    assert ds.sheets_service.batch_bodies  # sheet was filled


def test_run_idempotent_skip(monkeypatch):
    desc = "Google Salary Jan-26 (auto-split from payslip)"
    ledger = [["Date", "Description"], ["2026-01-29", desc]]
    sp, ds = make_splitter(ledger_rows=ledger)
    monkeypatch.setattr(splitter_mod, "parse_payslip", lambda b: FIELDS)
    monkeypatch.setattr(sp, "find_payslip", lambda ym: ("fid", f"{ym}.pdf"))
    sp.run("2026-01")
    assert ds.appended == []  # already split → no-op
