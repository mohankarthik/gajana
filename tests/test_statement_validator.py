import datetime

from src.statement_validator import (
    reconcile_summary,
    validate_statement,
    _amount_in_text,
    _norm,
)

# A compact stand-in for the extracted PDF text layer (the validation oracle).
SOURCE_TEXT = """
Statement Period 20/05/2026 - 18/06/2026
22/05/2026 UPI/MVJ M C AND R H/Q869697156@YBL 20.00 Dr
04/06/2026 BBPS PAYMENT RECEIVED - HD016154 9350.51 Cr
18/06/2026 UPI/ORION UPTOWN BLR/PAYTM-75234692 224.00 Dr
"""

CONFIG = {
    "date_formats": ["%d/%m/%Y"],
    "statement_period_patterns": [
        r"(?P<start>\d{2}/\d{2}/\d{4}) - (?P<end>\d{2}/\d{2}/\d{4})"
    ],
}

TODAY = datetime.datetime(2026, 7, 9)
END = datetime.datetime(2026, 6, 18)


def _txn(date, desc, debit="", credit=""):
    return {"date": date, "description": desc, "debit": debit, "credit": credit}


def test_clean_transaction_passes():
    txns = [_txn("22/05/2026", "UPI/MVJ M C AND R H/Q869697156@YBL", debit="20.00")]
    res = validate_statement(txns, SOURCE_TEXT, CONFIG, END, TODAY)
    assert len(res.passed) == 1
    assert not res.flagged


def test_hallucinated_date_not_in_source_is_flagged():
    # Real amount/description, but a date the statement never printed.
    txns = [_txn("2026-08-06", "UPI/MVJ M C AND R H/Q869697156@YBL", debit="20.00")]
    res = validate_statement(txns, SOURCE_TEXT, CONFIG, END, TODAY)
    assert not res.passed
    _, reasons = res.flagged[0]
    assert any("date_not_in_source" in r for r in reasons)


def test_future_date_after_statement_end_is_flagged():
    # Format matches config and could parse, but it is beyond the period end.
    txns = [_txn("20/08/2026", "UPI/MVJ M C AND R H/Q869697156@YBL", debit="20.00")]
    res = validate_statement(txns, SOURCE_TEXT, CONFIG, END, TODAY)
    _, reasons = res.flagged[0]
    assert any("date_after_end" in r for r in reasons)


def test_end_capped_at_today_even_without_config():
    # No period pattern, no filename end -> end defaults to today.
    future = _txn("22/05/2027", "MVJ", debit="20.00")  # next year, in the future
    res = validate_statement(
        [future], SOURCE_TEXT, {"date_formats": ["%d/%m/%Y"]}, None, TODAY
    )
    _, reasons = res.flagged[0]
    assert any("date_after_end" in r for r in reasons)


def test_near_boundary_date_within_grace_passes():
    # A txn posted 2 days after the statement end (value-date lag) is legit.
    text = SOURCE_TEXT + "\n20/06/2026 UPI/LATE POST/Q1@YBL 33.00 Dr"
    txns = [_txn("20/06/2026", "UPI/LATE POST/Q1@YBL", debit="33.00")]
    res = validate_statement(txns, text, CONFIG, END, TODAY)  # END = 18/06
    assert len(res.passed) == 1
    assert not res.flagged


def test_high_volume_account_no_false_count_mismatch():
    # Many txns sharing few dates must not trip the count heuristic.
    text = "22/05/2026 shop a 10.00\n22/05/2026 shop b 20.00\n"
    txns = [
        _txn("22/05/2026", "shop a", debit="10.00"),
        _txn("22/05/2026", "shop b", debit="20.00"),
    ]
    res = validate_statement(txns, text, {"date_formats": ["%d/%m/%Y"]}, END, TODAY)
    assert not any("count_mismatch" in f for f in res.statement_flags)


def test_reconcile_summary_matches_statement_totals():
    txns = [
        _txn("x", "a", debit="100.00"),
        _txn("x", "b", credit="500.00"),
        _txn("x", "c", debit="50.00"),
    ]
    summary = {"total_debit": "150.00", "total_credit": "500.00"}
    assert reconcile_summary(txns, summary) == []


def test_reconcile_summary_catches_debit_credit_swap():
    # A debit booked as a credit: debit sum falls short, credit sum overshoots.
    txns = [_txn("x", "a", credit="100.00"), _txn("x", "b", credit="500.00")]
    summary = {"total_debit": "100.00", "total_credit": "500.00"}
    labels = [label for label, _ in reconcile_summary(txns, summary)]
    assert "debit" in labels and "credit" in labels


def test_reconcile_summary_no_summary_is_noop():
    assert reconcile_summary([_txn("x", "a", debit="1.00")], {}) == []
    assert reconcile_summary([_txn("x", "a", debit="1.00")], None) == []


def test_reconcile_summary_skips_side_the_statement_omits():
    # Statement prints only total_credit -> debit side is not checked.
    txns = [_txn("x", "a", debit="999.00"), _txn("x", "b", credit="500.00")]
    assert reconcile_summary(txns, {"total_credit": "500.00"}) == []


def test_reconcile_summary_tolerates_paise_rounding():
    txns = [_txn("x", "a", debit="60557.59")]
    assert reconcile_summary(txns, {"total_debit": "60557.16"}) == []  # 0.43 off


def test_reconcile_summary_net_fallback_from_balances():
    # No printed totals, but opening/closing balances -> net-magnitude check.
    # axis-mini: open 66302.94 -> close 82790.88, true net 16487.94.
    summary = {"opening_balance": "66302.94", "closing_balance": "82790.88"}
    good = [_txn("x", "dep", credit="16487.94")]
    assert reconcile_summary(good, summary) == []
    # A bad parse whose net is far off is caught.
    bad = [_txn("x", "dep", credit="80499.94")]
    labels = [label for label, _ in reconcile_summary(bad, summary)]
    assert labels == ["net"]


def test_reconcile_summary_prefers_side_totals_over_net():
    # When per-side totals exist, use them (stronger) and skip the net fallback.
    summary = {
        "total_debit": "100.00",
        "total_credit": "500.00",
        "opening_balance": "0.00",
        "closing_balance": "999.99",
    }
    txns = [_txn("x", "a", debit="100.00"), _txn("x", "b", credit="500.00")]
    assert reconcile_summary(txns, summary) == []  # net ignored


def test_reconcile_summary_mismatch_surfaces_statement_flag():
    txns = [_txn("22/05/2026", "MVJ", debit="20.00")]
    res = validate_statement(
        txns, SOURCE_TEXT, CONFIG, END, TODAY, summary={"total_debit": "9999.00"}
    )
    assert any("reconcile_mismatch" in f for f in res.statement_flags)


def test_amount_digit_drop_is_flagged():
    # Description + date real, but the amount 20.00 was mis-read as 200.00.
    txns = [_txn("22/05/2026", "UPI/MVJ M C AND R H/Q869697156@YBL", debit="200.00")]
    res = validate_statement(txns, SOURCE_TEXT, CONFIG, END, TODAY)
    _, reasons = res.flagged[0]
    assert any("debit_not_in_source" in r for r in reasons)


def test_no_text_layer_quarantines_everything():
    txns = [_txn("22/05/2026", "MVJ", debit="20.00")]
    res = validate_statement(txns, "", CONFIG, END, TODAY)
    assert not res.passed
    assert len(res.flagged) == 1
    assert res.statement_flags


def test_low_confidence_description_is_soft_flag_not_reject():
    # Amount + date corroborated, but the merchant text is garbled. The row still
    # passes (per-txn scope); the statement carries a soft warning.
    txns = [_txn("22/05/2026", "ZZZ QQQ WWW GARBLED", debit="20.00")]
    res = validate_statement(txns, SOURCE_TEXT, CONFIG, END, TODAY)
    assert len(res.passed) == 1
    assert any("low_confidence_descriptions" in f for f in res.statement_flags)


def test_amount_in_text_comma_insensitive():
    assert _amount_in_text("9350.51", _norm("total 9,350.51 cr"))
    assert _amount_in_text("1000", _norm("paid 1,000.00 dr"))
    assert not _amount_in_text("9999", _norm("total 9,350.51 cr"))


def test_date_in_text_tolerates_time_suffix():
    text = _norm("row 20/04/2026 | 08:34 shop 100.00 and 11/04/2026 19:51:23 x")
    txns = [
        _txn("20/04/2026 | 08:34", "shop", debit="100.00"),
    ]
    # date corroborated (ignoring the appended time); amount present -> passes
    res = validate_statement(txns, text, {"date_formats": ["%d/%m/%Y"]}, END, TODAY)
    assert len(res.passed) == 1
