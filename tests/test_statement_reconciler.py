"""Statement-vs-ledger reconciliation: multisets, salary folds, duplicate files."""

import datetime
import json

import pytest

from src.interfaces import DataSourceFile, DataSourceInterface
from src.statement_reconciler import (
    Difference,
    StatementRecord,
    account_from_filename,
    canonical_statements,
    compare_month,
    compare_to_baseline,
    format_delta,
    fold_salary_splits,
    format_report,
    load_ledger,
    main,
    parse_statement_date,
    period_months,
    previous_month,
    reconcile,
    records_from_data_source,
    records_from_pdf_dir,
    records_from_snapshot,
)
from tests.test_statement_extractor import AXIS_REL, _pdf_bytes


def _record(name, rows, period=("01-07-2024", "31-07-2024"), verified=True):
    return StatementRecord(
        name=name,
        account=account_from_filename(name),
        period=period,
        verified=verified,
        rows=tuple(rows),
    )


def _txn(date, amount, account="bank-axis-karti", category="", description="x"):
    return {
        "date": datetime.datetime.strptime(date, "%Y-%m-%d"),
        "amount": amount,
        "account": account,
        "category": category,
        "description": description,
        "remarks": "",
    }


# --------------------------------------------------------------------------
# Small pieces
# --------------------------------------------------------------------------


def test_account_and_month_helpers():
    assert (
        account_from_filename("bank-axis-karti-2026-05_copy1.pdf") == "bank-axis-karti"
    )
    assert (
        account_from_filename("/tmp/cc-hdfc-infiniametal-2026-07.pdf")
        == "cc-hdfc-infiniametal"
    )
    assert previous_month("2024-09") == "2024-08"
    assert previous_month("2025-01") == "2024-12"


def test_period_dates_cover_every_form_statements_print():
    assert parse_statement_date("01-07-2024") == datetime.date(2024, 7, 1)
    assert parse_statement_date("20/07/2024") == datetime.date(2024, 7, 20)
    assert parse_statement_date("13 Jun, 2026") == datetime.date(2026, 6, 13)
    assert parse_statement_date("whenever") is None


def test_period_months_spans_a_cycle_that_crosses_a_month_boundary():
    assert period_months(("01-07-2024", "31-07-2024")) == ["2024-07"]
    assert period_months(("13 Jun, 2026", "12 Jul, 2026")) == ["2026-06", "2026-07"]
    assert period_months(("01-12-2025", "05-01-2026")) == ["2025-12", "2026-01"]
    assert period_months(None) == []
    assert period_months(("31-07-2024", "01-07-2024")) == []
    assert period_months(("garbage", "01-07-2024")) == []


def test_canonical_statements_drops_reissues_and_unverified_files():
    july = ("01-07-2025", "31-07-2025")
    original = _record(
        "bank-hdfc-karti-2025-07.pdf", [("2025-07-01", -10.0, "a")], july
    )
    copy = _record(
        "bank-hdfc-karti-2025-07_copy1.pdf", [("2025-07-01", -10.0, "a")], july
    )
    unverified = _record(
        "bank-hdfc-karti-2025-08.pdf",
        [("2025-08-01", -10.0, "a")],
        period=("01-08-2025", "31-08-2025"),
        verified=False,
    )
    kept, duplicates = canonical_statements([copy, original, unverified])
    assert [r.name for r in kept] == ["bank-hdfc-karti-2025-07.pdf"]
    assert duplicates == [
        (
            "bank-hdfc-karti-2025-07_copy1.pdf",
            "bank-hdfc-karti-2025-07.pdf",
            ("01-07-2025", "31-07-2025"),
        )
    ]


# --------------------------------------------------------------------------
# Multiset comparison
# --------------------------------------------------------------------------


def test_repeated_rows_are_counted_not_collapsed():
    # Three identical ATM withdrawals on one day are three transactions; a
    # ledger holding two of them is short one.
    statement = [("2026-05-05", -10000.0, "ATM WDL")] * 3
    ledger = [_txn("2026-05-05", -10000.0), _txn("2026-05-05", -10000.0)]
    (diff,) = compare_month(statement, ledger)
    assert (diff.statement_count, diff.ledger_count) == (3, 2)
    assert (diff.missing, diff.excess) == (1, 0)


def test_double_booked_row_shows_as_excess():
    statement = [("2026-05-05", -500.0, "COFFEE")]
    ledger = [_txn("2026-05-05", -500.0), _txn("2026-05-05", -500.0)]
    (diff,) = compare_month(statement, ledger)
    assert (diff.missing, diff.excess) == (0, 1)
    assert diff.ledger_desc == "x"


def test_matching_months_produce_no_differences():
    statement = [("2026-05-05", -500.0, "COFFEE"), ("2026-05-06", 100.0, "REFUND")]
    ledger = [_txn("2026-05-05", -500.0), _txn("2026-05-06", 100.0)]
    assert compare_month(statement, ledger) == []


def test_sign_flip_is_two_differences_not_zero():
    # A credit booked as a debit nets out in a total but not in a multiset.
    statement = [("2026-06-10", 5000.0, "REFUND")]
    ledger = [_txn("2026-06-10", -5000.0)]
    diffs = compare_month(statement, ledger)
    assert [(d.amount, d.statement_count, d.ledger_count) for d in diffs] == [
        (-5000.0, 0, 1),
        (5000.0, 1, 0),
    ]


# --------------------------------------------------------------------------
# Salary splits
# --------------------------------------------------------------------------


def test_salary_split_legs_fold_back_to_their_net():
    ledger = [
        _txn("2026-05-28", 800000.0, category="Income:Google"),
        _txn("2026-05-28", -250000.0, category="Tax:Income Tax"),
        _txn("2026-05-28", -52085.28, category="Investment Expense:PF"),
        _txn("2026-05-28", -1000.0, category="Insurance:Health"),
        _txn("2026-05-28", -500.0, category="Food:Coffee"),
    ]
    adjusted, splits = fold_salary_splits(ledger)
    assert splits == {"2026-05-28": (4, 496914.72)}
    amounts = sorted(t["amount"] for t in adjusted)
    assert amounts == [-500.0, 496914.72]
    # The folded net now compares 1:1 against the credit the bank printed.
    assert (
        compare_month(
            [
                ("2026-05-28", 496914.72, "NEFT Cr-GOOGLE"),
                ("2026-05-28", -500.0, "COFFEE"),
            ],
            adjusted,
        )
        == []
    )


def test_days_without_a_salary_row_are_untouched():
    ledger = [_txn("2026-05-02", -500.0, category="Tax:Income Tax")]
    adjusted, splits = fold_salary_splits(ledger)
    assert splits == {}
    assert [t["amount"] for t in adjusted] == [-500.0]


# --------------------------------------------------------------------------
# Whole-report reconcile
# --------------------------------------------------------------------------


def test_reconcile_compares_only_statement_covered_months():
    records = [
        _record(
            "bank-axis-karti-2024-08.pdf",
            [("2024-07-01", 50000.0, "SALARY"), ("2024-07-02", -1500.0, "COFFEE")],
        ),
        _record(
            "bank-axis-karti-2024-10.pdf",
            [("2024-09-04", -900.0, "FUEL")],
            period=("01-09-2024", "30-09-2024"),
        ),
    ]
    ledger = [
        _txn("2024-07-01", 50000.0),
        _txn("2024-07-02", -1500.0),
        # September is short the fuel row and carries a row of its own.
        _txn("2024-09-09", -20.0),
    ]
    report = reconcile(records, ledger, since="2024-07")
    assert [(m.account, m.month) for m in report.months] == [
        ("bank-axis-karti", "2024-07"),
        ("bank-axis-karti", "2024-09"),
    ]
    assert [m.month for m in report.differing] == ["2024-09"]
    september = report.differing[0]
    assert september.statement_rows == 1 and september.ledger_rows == 1
    assert {(d.amount, d.missing, d.excess) for d in september.differences} == {
        (-900.0, 1, 0),
        (-20.0, 0, 1),
    }


def test_reconcile_honours_since_and_account_filters():
    records = [
        _record("bank-axis-karti-2024-08.pdf", [("2024-07-01", 1.0, "a")]),
        _record(
            "bank-axis-mini-2024-10.pdf",
            [("2024-09-01", 2.0, "b")],
            period=("01-09-2024", "30-09-2024"),
        ),
    ]
    report = reconcile(records, [], since="2024-09")
    assert [m.account for m in report.months] == ["bank-axis-mini"]
    scoped = reconcile(records, [], since="2024-01", accounts=["bank-axis-karti"])
    assert [m.account for m in scoped.months] == ["bank-axis-karti"]


def test_unverified_statements_are_excluded_but_reported():
    records = [
        StatementRecord(
            name="bank-axis-karti-2024-08.pdf",
            account="bank-axis-karti",
            period=("01-07-2024", "31-07-2024"),
            verified=False,
            problems=("no printed figure to check against",),
            rows=(("2024-07-01", 50000.0, "SALARY"),),
        )
    ]
    report = reconcile(records, [_txn("2024-07-01", 50000.0)], since="2024-01")
    assert report.months == ()
    assert report.unverified == (
        ("bank-axis-karti-2024-08.pdf", ("no printed figure to check against",)),
    )


def test_report_serialises_and_formats():
    records = [_record("bank-axis-karti-2024-08.pdf", [("2024-07-01", 50000.0, "SAL")])]
    report = reconcile(records, [], since="2024-01")
    text = format_report(report)
    assert "account-months compared: 1  with differences: 1" in text
    assert "2024-07-01" in text
    payload = json.loads(json.dumps(report.as_dict()))
    assert payload["report"]["bank-axis-karti@2024-07"]["items"][0]["amt"] == 50000.0


def test_format_report_lists_duplicates_and_unverified():
    records = [
        _record(
            "bank-hdfc-karti-2025-07.pdf",
            [("2025-07-01", -10.0, "a")],
            ("01-07-2025", "31-07-2025"),
        ),
        _record(
            "bank-hdfc-karti-2025-07_copy1.pdf",
            [("2025-07-01", -10.0, "a")],
            ("01-07-2025", "31-07-2025"),
        ),
        StatementRecord(
            name="bank-hdfc-karti-2025-09.pdf",
            account="bank-hdfc-karti",
            period=("01-09-2025", "30-09-2025"),
            verified=False,
            problems=("debit sum 1.00 != printed 2.00",),
        ),
    ]
    text = format_report(
        reconcile(records, [_txn("2025-07-01", -10.0, "bank-hdfc-karti")])
    )
    assert "duplicate statement files" in text
    assert "bank-hdfc-karti-2025-07_copy1.pdf" in text
    assert "unverified statements" in text


def test_difference_view_is_json_safe():
    d = Difference("2026-01-01", -10.0, 1, 0, "S", "L", "Food")
    assert d.as_dict()["lcat"] == "Food"


# --------------------------------------------------------------------------
# Sources
# --------------------------------------------------------------------------


class FakeDataSource(DataSourceInterface):
    """Read-only stand-in: one statement PDF and a two-row bank log."""

    def __init__(self, pdfs, log_rows):
        self.pdfs = pdfs
        self.log_rows = log_rows
        self.writes = 0

    def list_statement_file_details(self):
        return [DataSourceFile(name, name) for name in self.pdfs]

    def get_sheet_data(self, source_id, sheet_name, range_spec):
        return []

    def get_transaction_log_data(self, log_type):
        return self.log_rows

    def append_transactions_to_log(self, log_type, data_values):
        self.writes += 1

    def clear_transaction_log_range(self, log_type, start_row=3):
        self.writes += 1

    def write_transactions_to_log(self, log_type, data_values):
        self.writes += 1

    def get_first_sheet_name_from_file(self, file_id):
        return None

    def download_file(self, file_id):
        return self.pdfs[file_id]


@pytest.fixture
def fake_source():
    return FakeDataSource(
        {
            "bank-axis-karti-2024-08.pdf": _pdf_bytes(AXIS_REL),
            "notes.txt": b"",
            "cc-axis-magnus-2024-09.pdf": b"",
            "bank-axis-karti-2019-01.pdf": _pdf_bytes(AXIS_REL),
        },
        [
            [
                "Date",
                "Description",
                "Debit",
                "Credit",
                "Category",
                "Remarks",
                "Account",
            ],
            [
                "2024-07-01",
                "NEFT SALARY CREDIT",
                "",
                "50000",
                "Income",
                "",
                "bank-axis-karti",
            ],
            [
                "2024-07-02",
                "UPI/COFFEE SHOP/1234",
                "1500",
                "",
                "Food",
                "",
                "bank-axis-karti",
            ],
        ],
    )


def test_records_from_data_source_skips_non_pdfs_and_old_files(fake_source):
    records, snapshot = records_from_data_source(fake_source, "bank-", "2024-08")
    assert [r.name for r in records] == ["bank-axis-karti-2024-08.pdf"]
    assert snapshot["bank-axis-karti-2024-08.pdf"]["verified"] is True
    assert fake_source.writes == 0


def test_records_from_data_source_reports_a_failed_extraction(fake_source):
    fake_source.pdfs["bank-axis-karti-2024-09.pdf"] = b"not a pdf"
    records, _ = records_from_data_source(fake_source, "bank-", "2024-08")
    assert [r.name for r in records] == ["bank-axis-karti-2024-08.pdf"]


def test_records_from_pdf_dir_and_snapshot_round_trip(tmp_path):
    (tmp_path / "bank-axis-karti-2024-08.pdf").write_bytes(_pdf_bytes(AXIS_REL))
    (tmp_path / "bank-axis-karti-2019-01.pdf").write_bytes(_pdf_bytes(AXIS_REL))
    (tmp_path / "bank-axis-karti-2024-09.pdf").write_bytes(b"not a pdf")
    records, snapshot = records_from_pdf_dir(str(tmp_path), "bank-", "2024-08")
    assert [r.name for r in records] == ["bank-axis-karti-2024-08.pdf"]
    rebuilt = records_from_snapshot(snapshot)
    assert rebuilt == records


def test_records_from_snapshot_handles_a_periodless_statement():
    (record,) = records_from_snapshot(
        {"bank-axis-karti-2024-08.pdf": {"verified": False, "period": None}}
    )
    assert record.period is None and record.rows == ()


def test_load_ledger_reads_through_the_data_source(fake_source):
    ledger = load_ledger(fake_source, "bank")
    assert sorted(t["amount"] for t in ledger) == [-1500.0, 50000.0]
    assert fake_source.writes == 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def test_cli_reconciles_a_snapshot_against_the_ledger(
    tmp_path, capsys, monkeypatch, fake_source
):
    _, snapshot = records_from_data_source(fake_source, "bank-", "2024-08")
    snap_path = tmp_path / "snapshot.json"
    snap_path.write_text(json.dumps(snapshot))
    monkeypatch.setattr(
        "src.statement_reconciler._make_data_source", lambda csv_db_path: fake_source
    )
    out_path = tmp_path / "report.json"
    code = main(
        [
            "--statements-json",
            str(snap_path),
            "--json",
            str(out_path),
            "--since",
            "2024-07",
        ]
    )
    assert code == 0  # ledger matches the statement exactly
    printed = capsys.readouterr().out
    assert "statements: 1/1 verified" in printed
    assert "account-months compared: 1  with differences: 0" in printed
    assert (
        json.loads(out_path.read_text())["report"]["bank-axis-karti@2024-07"]["items"]
        == []
    )
    assert fake_source.writes == 0


def test_cli_exits_nonzero_when_the_ledger_differs(tmp_path, monkeypatch, fake_source):
    fake_source.log_rows = fake_source.log_rows[:2]  # drop the coffee row
    _, snapshot = records_from_data_source(fake_source, "bank-", "2024-08")
    snap_path = tmp_path / "snapshot.json"
    snap_path.write_text(json.dumps(snapshot))
    monkeypatch.setattr(
        "src.statement_reconciler._make_data_source", lambda csv_db_path: fake_source
    )
    assert main(["--statements-json", str(snap_path), "--since", "2024-07"]) == 1


def test_cli_no_ledger_mode_never_touches_the_data_source(
    tmp_path, capsys, monkeypatch
):
    (tmp_path / "bank-axis-karti-2024-08.pdf").write_bytes(_pdf_bytes(AXIS_REL))
    bare = AXIS_REL.replace("Opening Balance", "Brought forward").replace(
        "Closing Balance", "Carried forward"
    )
    (tmp_path / "bank-axis-karti-2024-09.pdf").write_bytes(_pdf_bytes(bare))

    def explode(csv_db_path):  # pragma: no cover - must never be called
        raise AssertionError("--no-ledger must not build a data source")

    monkeypatch.setattr("src.statement_reconciler._make_data_source", explode)
    snap_path = tmp_path / "snapshot.json"
    code = main(
        ["--pdf-dir", str(tmp_path), "--no-ledger", "--snapshot", str(snap_path)]
    )
    assert code == 0
    printed = capsys.readouterr().out
    assert "statements: 1/2 verified" in printed
    assert "UNVERIFIED bank-axis-karti-2024-09.pdf" in printed
    assert json.loads(snap_path.read_text())["bank-axis-karti-2024-08.pdf"]["n"] == 2


def test_cli_defaults_to_the_data_source_when_no_pdf_dir_is_given(
    monkeypatch, capsys, fake_source
):
    monkeypatch.setattr(
        "src.statement_reconciler._make_data_source", lambda csv_db_path: fake_source
    )
    assert main(["--since", "2024-07"]) == 0
    assert "statements: 1/1 verified" in capsys.readouterr().out


def test_make_data_source_picks_the_backend_from_the_flag(tmp_path, monkeypatch):
    import src.google_data_source as gds
    from src.statement_reconciler import _make_data_source

    assert type(_make_data_source(str(tmp_path))).__name__ == "CSVDataSource"

    class StubGoogle:
        pass

    monkeypatch.setattr(gds, "GoogleDataSource", StubGoogle)
    assert isinstance(_make_data_source(None), StubGoogle)


# --------------------------------------------------------------------------
# Baseline comparison
# --------------------------------------------------------------------------


def _report_with(items):
    """A stored-report dict carrying one month's difference items."""
    return {
        "report": {
            "bank-axis-karti@2024-07": {
                "stmt_n": 2,
                "led_n": 2,
                "items": [
                    {
                        "date": d,
                        "amt": a,
                        "stmt": s,
                        "led": ledger,
                        "sdesc": "",
                        "ldesc": "",
                        "lcat": "",
                    }
                    for d, a, s, ledger in items
                ],
                "splits": {},
            }
        },
        "dupfiles": [],
        "unverified": [],
    }


def _run(statement_rows, ledger_txns, since="2024-07"):
    records = [_record("bank-axis-karti-2024-08.pdf", statement_rows)]
    return reconcile(records, ledger_txns, since=since)


def test_identical_run_against_its_own_baseline_is_empty():
    report = _run([("2024-07-01", 50000.0, "SAL")], [])
    delta = compare_to_baseline(report, report.as_dict())
    assert delta.empty and delta.clean
    assert delta.unchanged == 1
    assert "BASELINE CLEAN" in format_delta(delta)


def test_a_row_that_got_booked_reads_as_fixed():
    baseline = _report_with([("2024-07-01", 50000.0, 1, 0)])
    report = _run([("2024-07-01", 50000.0, "SAL")], [_txn("2024-07-01", 50000.0)])
    delta = compare_to_baseline(report, baseline)
    assert [i.date for i in delta.fixed] == ["2024-07-01"]
    assert delta.clean and not delta.empty
    assert "fixed (1)" in format_delta(delta)


def test_a_narrowed_gap_reads_as_improved_not_fixed():
    # Statement prints the row three times, ledger had none; now it has two.
    baseline = _report_with([("2024-07-05", -10000.0, 3, 0)])
    report = _run(
        [("2024-07-05", -10000.0, "ATM")] * 3,
        [_txn("2024-07-05", -10000.0), _txn("2024-07-05", -10000.0)],
    )
    delta = compare_to_baseline(report, baseline)
    assert delta.fixed == ()
    assert [(i.was, i.now) for i in delta.improved] == [((3, 0), (3, 2))]
    assert delta.clean


def test_a_widened_gap_is_a_regression():
    baseline = _report_with([("2024-07-05", -10000.0, 3, 2)])
    report = _run([("2024-07-05", -10000.0, "ATM")] * 3, [])
    delta = compare_to_baseline(report, baseline)
    assert [(i.was, i.now) for i in delta.worsened] == [((3, 2), (3, 0))]
    assert not delta.clean
    assert "WORSENED" in format_delta(delta)


def test_a_mismatch_the_baseline_never_saw_is_a_regression():
    baseline = _report_with([])
    report = _run([("2024-07-01", 50000.0, "SAL")], [])
    delta = compare_to_baseline(report, baseline)
    assert [i.date for i in delta.newly_broken] == ["2024-07-01"]
    assert not delta.clean
    assert "BASELINE REGRESSION" in format_delta(delta)


def test_a_month_dropping_out_of_the_comparison_is_a_regression():
    # A statement that stops verifying takes its month with it: the month
    # becomes unknown, which is not the same as clean.
    baseline = _report_with([("2024-07-01", 50000.0, 1, 0)])
    delta = compare_to_baseline(reconcile([], []), baseline)
    assert delta.lost_coverage == ("bank-axis-karti@2024-07",)
    assert not delta.clean
    assert "LOST COVERAGE" in format_delta(delta)


def test_a_newly_covered_month_is_reported_but_not_a_regression():
    baseline = {"report": {}, "dupfiles": [], "unverified": []}
    report = _run([("2024-07-01", 50000.0, "SAL")], [_txn("2024-07-01", 50000.0)])
    delta = compare_to_baseline(report, baseline)
    assert delta.new_coverage == ("bank-axis-karti@2024-07",)
    assert delta.clean and not delta.empty
    assert "new coverage (1)" in format_delta(delta)


def test_cli_writes_a_baseline_when_none_exists_then_compares_against_it(
    tmp_path, capsys, monkeypatch, fake_source
):
    _, snapshot = records_from_data_source(fake_source, "bank-", "2024-08")
    snap_path = tmp_path / "snapshot.json"
    snap_path.write_text(json.dumps(snapshot))
    monkeypatch.setattr(
        "src.statement_reconciler._make_data_source", lambda csv_db_path: fake_source
    )
    baseline = tmp_path / "state" / "baseline.json"
    argv = [
        "--statements-json",
        str(snap_path),
        "--since",
        "2024-07",
        "--baseline",
        str(baseline),
    ]
    assert main(argv) == 0
    assert "wrote this run as the reference" in capsys.readouterr().out
    # Second run compares instead of writing, and nothing has moved.
    assert main(argv) == 0
    out = capsys.readouterr().out
    assert "BASELINE CLEAN" in out
    assert "newly broken: 0" in out


def test_cli_exits_nonzero_when_a_run_regresses_against_the_baseline(
    tmp_path, monkeypatch, capsys, fake_source
):
    _, snapshot = records_from_data_source(fake_source, "bank-", "2024-08")
    snap_path = tmp_path / "snapshot.json"
    snap_path.write_text(json.dumps(snapshot))
    monkeypatch.setattr(
        "src.statement_reconciler._make_data_source", lambda csv_db_path: fake_source
    )
    baseline = tmp_path / "baseline.json"
    argv = [
        "--statements-json",
        str(snap_path),
        "--since",
        "2024-07",
        "--baseline",
        str(baseline),
    ]
    assert main(argv) == 0  # writes the reference: ledger matches the statement
    fake_source.log_rows = fake_source.log_rows[:2]  # someone deletes the coffee row
    assert main(argv) == 1
    assert "NEWLY BROKEN (1)" in capsys.readouterr().out


def test_cli_update_baseline_accepts_the_new_reality(
    tmp_path, monkeypatch, capsys, fake_source
):
    _, snapshot = records_from_data_source(fake_source, "bank-", "2024-08")
    snap_path = tmp_path / "snapshot.json"
    snap_path.write_text(json.dumps(snapshot))
    monkeypatch.setattr(
        "src.statement_reconciler._make_data_source", lambda csv_db_path: fake_source
    )
    baseline = tmp_path / "baseline.json"
    argv = [
        "--statements-json",
        str(snap_path),
        "--since",
        "2024-07",
        "--baseline",
        str(baseline),
    ]
    assert main(argv) == 0
    fake_source.log_rows = fake_source.log_rows[:2]
    assert main(argv + ["--update-baseline"]) == 1  # still reports the regression
    capsys.readouterr()
    assert main(argv) == 0  # ... but it is now the reference
    assert "BASELINE CLEAN" in capsys.readouterr().out
