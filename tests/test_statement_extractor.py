"""Layout fixtures for the deterministic statement extractor.

Each fixture is a compact stand-in for pypdf's layout-mode text of one
statement generation: the real header line, summary block and row shapes, with
invented accounts and amounts. Column alignment is load-bearing -- the x-bands
of the header labels are what the extractor reads -- so keep the spacing when
editing.
"""

import datetime
import io
import json
import os

import pytest

from src.statement_extractor import (
    BANK_LAYOUTS,
    CARD_LAYOUTS,
    ExtractedStatement,
    StatementRow,
    extract_bank_statement,
    extract_from_pages,
    extract_statement,
    extract_statement_bytes,
    read_pdf_pages,
    reconcile,
    statement_passwords,
)

# --------------------------------------------------------------------------
# Fixtures: one per registered layout
# --------------------------------------------------------------------------

AXIS_REL = """
Statement of Account for the period (From : 01-07-2024 To : 31-07-2024)
Date          Transaction Details                        Chq        Withdrawal          Deposits        Balance
                                                         No.
                             Opening Balance                                                          10,000.00
01-07-2024    NEFT SALARY CREDIT                                                       50,000.00      60,000.00
02-07-2024    UPI/COFFEE SHOP/1234                                    1,500.00                        58,500.00
                             Closing Balance                                                          58,500.00
                             Total                                    1,500.00         50,000.00
"""

AXIS_COMPACT = """
 Statement of account between 01-02-2026 to 28-02-2026
 Tran Date      Chq No             Particulars              Debit             Credit            Balance          Init.
                                                                                                                 Br
                             OPENING BALANCE                                                  138,918.47
 05-02-2026                  ATM CASH WDL                80,000.00                             58,918.47
 20-02-2026                  IMPS INWARD                                     53,500.00        112,418.47
                             TRANSACTION TOTAL           80,000.00           53,500.00
                             CLOSING BALANCE                                                  112,418.47
"""

AXIS_NEW = """
 Statement Period From : 01-06-2026 To : 30-06-2026
  Txn Date                    Transaction                 Withdrawals        Deposits           Balance      Other Information
                Opening Balance                                                              210,652.32
  02-06-2026    UPI/GROCERY STORE/8891                       2,715.11
  09-06-2026    NEFT REFUND                                                  1,000.00
               Closing Balance                                                               208,937.21
"""

HDFC_REL = """
Statement From : 01-05-2025 To : 31-05-2025
Opening Balance      : 10,000.00                                        Limit       : 0.00
     Txn Date                     Narration                    Withdrawals            Deposits           Closing Balance
    01/05/2025        UPI-MATOSHRI CATTLE                            770.00                0.00                 9,230.00
                      FEED-q052676264@
    17/05/2025        NEFT CR-SALARY                                   0.00           25,000.00                34,230.00
                            Opening Balance          Debit Amount           Credit Amount              Closing Balance

                              10,000.00                  770.00               25,000.00                   34,230.00

                                                     Debit Count            Credit Count
                                                          1                       1
"""

HDFC_CLASSIC = """
Statement From : 01-01-2026 To : 31-01-2026
   Date                     Narration                      Chq./Ref.No.        Value Dt      Withdrawal Amt.       Deposit Amt.      Closing Balance
 01/01/26    NET BANKING SI -A0505                        000000000000000       01/01/26           52,000.00                              151,020.88
 09/01/26    NEFT CR-EMPLOYER                             000000000000000       09/01/26                              9,000.00            160,020.88
                     Opening Balance                        Dr Count           Cr Count            Debits              Credits         Closing Bal
                        203,020.88                              1                  1              52,000.00           9,000.00          160,020.88
"""

AXIS_CARD = """
        Total Payment Due                Minimum Payment Due                Statement Period               Payment Due Date
              189.00   Dr                      4.00   Dr                 20/07/2024 - 18/08/2024               07/09/2024
        Previous Balance - Payments - Credits + Purchase + Cash Advance + Other Debit&Charges =Total Payment Due
        189.00  Dr        11,989.00           0.00            189.00           0.00           11,800.00        189.00   Dr
       DATE                     TRANSACTION DETAILS                       MERCHANT CATEGORY               AMOUNT (Rs.)
 03/08/2024             ANNUAL FEE                                                                          10,000.00 Dr
 03/08/2024             GST                                                                                  1,800.00 Dr
 07/08/2024             PAYMENT RECEIVED                                                                    11,989.00 Cr
 08/08/2024             CARD REPLACEMENT                                                                       189.00 Dr
"""

HDFC_CARD_OLD = """
0      Address    : A1106 Some Street                     Statement Date:12/01/2025
                                                     Account Summary
                                        Opening        Payment/         Purchase/        Finance        Total Dues
                                        Balance          Credits          Debits         Charges

                                       1,000.00        12,000.00        20,500.00          0.00          9,500.00

      12/01/2025 19:07:41    ONLINE GROCER      BENGALURU               20                    500.00
      12/01/2025 12:43:17    RESTAURANT         BENGALURU                8                 20,000.00
      05/01/2025             PAYMENT RECEIVED                                              12,000.00Cr
"""

HDFC_CARD_NEW = """
 BENGALURU 560049 KAR                                Billing Period               13 Jun, 2026 - 12 Jul, 2026
       PREVIOUS STATEMENT DUES        PAYMENTS/CREDITS            PURCHASES/DEBIT              FINANCE CHARGES
                   C93,518.10             C10,000.00                   C25,000.00                    C0.00
                                                    14/06/2026| 16:14    URBAN SERVICES GURGAON        + 5     C 20,000.00
                                                    20/06/2026| 00:00    UTILITY BILL                          C 5,000.00
                                                    25/06/2026| 00:00    PAYMENT RECEIVED                      C 10,000.00 Cr
"""

AXIS_NEW_WITH_FD = """
 Statement Period From : 01-08-2026 To : 31-08-2026
  Txn Date                    Transaction                 Withdrawals        Deposits           Balance      Other Information
                Opening Balance                                                              100,000.00
  02-08-2026    UPI/BOOK STORE/8891                          1,000.00
  09-08-2026    NEFT REFUND                                                  5,000.00
               Closing Balance                                                               104,000.00
                         Term Deposit No. XXXXXXXXXXX1111 - Quick View between 01/08/2026 to 31/08/2026
     Txn Date                            Transaction                          Value Date         Withdrawals            Deposits              Balance
                   Opening Balance                                                                                                             50,000.00
    21-08-2026     Int on TermDep[500.00] Tax[50.00]                         21-08-2026                                        450.00
                   Closing Balance                                                                                                              50,450.00
"""

HDFC_CARD_NEWER = """
 BENGALURU 560049 KAR                                Billing Period               13 Jul, 2026 - 12 Aug, 2026
       PREVIOUS STATEMENT DUES        PAYMENTS/CREDITS            PURCHASES/DEBIT              FINANCE CHARGES
                   C-9,585.00             C10,000.00                   C26,000.00                    C0.00
   Domestic Transactions
                                                    JOHN DOE             [CKYC ID : XXXXXXXXXX9999 ]
14/07/2026| 16:14     EMI    SYNTHETIC GROCER BENGALURU                                    + 50                            C 20,000.00     l
18/07/2026| 10:00            RETURN REFUND MERCHANT                                        - 10                         +  C 1,000.00      l
20/07/2026| 00:00            PAYMENT RECEIVED                                                                           +  C 9,000.00      l
                                                    JANE DOE             [CKYC ID : XXXXXXXXXX1234 ]
25/07/2026| 12:30     EMI    CAFE COFFEE BENGALURU                                         + 25                            C 2,000.00      l
   International Transactions
30/07/2026| 09:15            OVERSEAS BOOKSTORE LONDON                                                                     C 4,000.00      l
   Rewards Program Points Summary
   Opening Balance: 1000
   GST Summary
"""

AXIS_COMPACT_MULTILINE = """
 Statement of account between 01-02-2026 to 28-02-2026
 Tran Date      Chq No             Particulars                                          Debit             Credit            Balance          Init.
                                                                                                                                                 Br
                             OPENING BALANCE                                                                          100,000.00
                             UPI/P2A/112233445566/SYNTHETIC/BANK/P
 02-02-2026                  ayment/                                                                         15,000.00        115,000.00
 05-02-2026                  ATM CASH WDL                                               10,000.00                            105,000.00
                             TRANSACTION TOTAL                                          10,000.00            15,000.00
                             CLOSING BALANCE                                                                          105,000.00
"""


def pages(text: str):
    """One page of layout-mode text, minus the leading blank line."""
    return [text.lstrip("\n")]


# --------------------------------------------------------------------------
# Bank layouts
# --------------------------------------------------------------------------


def test_axis_rel_verifies_and_signs_from_balance():
    st = extract_bank_statement(pages(AXIS_REL))
    assert st.kind == "axis-rel"
    assert st.period == ("01-07-2024", "31-07-2024")
    assert [(r.date, r.amount) for r in st.rows] == [
        (datetime.datetime(2024, 7, 1), 50000.0),
        (datetime.datetime(2024, 7, 2), -1500.0),
    ]
    assert st.rows[0].description == "NEFT SALARY CREDIT"
    assert (st.opening, st.closing) == (10000.0, 58500.0)
    assert (st.total_debit, st.total_credit) == (1500.0, 50000.0)
    assert st.verified
    assert st.problems == []


def test_axis_compact_verifies():
    st = extract_bank_statement(pages(AXIS_COMPACT))
    assert st.kind == "axis-compact"
    assert st.period == ("01-02-2026", "28-02-2026")
    assert [r.amount for r in st.rows] == [-80000.0, 53500.0]
    assert (st.total_debit, st.total_credit) == (80000.0, 53500.0)
    assert st.verified


def test_axis_compact_multiline_narration():
    st = extract_bank_statement(pages(AXIS_COMPACT_MULTILINE))
    assert st.kind == "axis-compact"
    assert len(st.rows) == 2
    assert st.rows[0].description == "UPI/P2A/112233445566/SYNTHETIC/BANK/P ayment/"
    assert st.rows[0].amount == 15000.0
    assert st.rows[1].description == "ATM CASH WDL"
    assert st.rows[1].amount == -10000.0
    assert st.verified


def test_axis_new_uses_column_band_and_opening_closing_identity():
    # The only layout with no per-row balance: the column decides the side and
    # the opening/closing identity is what proves that assignment right.
    st = extract_bank_statement(pages(AXIS_NEW))
    assert st.kind == "axis-new"
    assert [r.amount for r in st.rows] == [-2715.11, 1000.0]
    assert all(r.balance is None for r in st.rows)
    assert st.verified


def test_axis_new_column_swap_breaks_the_identity():
    # Move the withdrawal under the Deposits column: the row reads as a credit,
    # and opening - debits + credits no longer lands on the printed closing.
    swapped = AXIS_NEW.replace(
        "  02-06-2026    UPI/GROCERY STORE/8891                       2,715.11",
        "  02-06-2026    UPI/GROCERY STORE/8891                                          2,715.11",
    )
    st = extract_bank_statement(pages(swapped))
    assert st.rows[0].amount == 2715.11
    assert not st.verified
    assert any("closing" in p for p in st.problems)


def test_axis_new_excludes_trailing_term_deposit_subtables():
    st = extract_bank_statement(pages(AXIS_NEW_WITH_FD))
    assert st.kind == "axis-new"
    assert len(st.rows) == 2
    assert [r.amount for r in st.rows] == [-1000.0, 5000.0]
    assert (st.opening, st.closing) == (100000.0, 104000.0)
    assert st.verified


def test_bank_statement_with_closing_balance_in_narration():
    """A dated transaction row containing 'closing balance' in its narration
    does not prematurely terminate the statement.
    """
    fixture = """
Statement From : 01-08-2026 To : 31-08-2026
Opening Balance      : 10,000.00                                        Limit       : 0.00
     Txn Date                     Narration                    Withdrawals            Deposits           Closing Balance
    01/08/2026        TRANSFER FOR CLOSING BALANCE OF FD              0.00            5,000.00                15,000.00
    02/08/2026        UPI-GROCERY STORE                             500.00                0.00                14,500.00
                      Closing Balance                                                                         14,500.00
"""
    st = extract_bank_statement(pages(fixture))
    assert len(st.rows) == 2
    assert "CLOSING BALANCE" in st.rows[0].description
    assert st.rows[1].description == "UPI-GROCERY STORE"
    assert [r.amount for r in st.rows] == [5000.0, -500.0]


def test_bank_narration_continuation_across_page_break():
    """A narration line wrapping across a page break right under Page 2's repeated
    table header attaches to the last row of Page 1.
    """
    page1 = """
Statement From : 01-08-2026 To : 31-08-2026
Opening Balance      : 10,000.00                                        Limit       : 0.00
     Txn Date                     Narration                    Withdrawals            Deposits           Closing Balance
    01/08/2026        NEFT CR-ACME TECH SOLUTIONS                     0.00           10,000.00                20,000.00
"""
    page2 = """
Statement From : 01-08-2026 To : 31-08-2026
Opening Balance      : 10,000.00                                        Limit       : 0.00
     Txn Date                     Narration                    Withdrawals            Deposits           Closing Balance
                      INDIA PRIVATE LIMITED CONTINUATION
    02/08/2026        UPI-GROCERY STORE                             500.00                0.00                19,500.00
                      Closing Balance                                                                         19,500.00
"""
    st = extract_bank_statement([page1, page2])
    assert len(st.rows) == 2
    assert (
        st.rows[0].description
        == "NEFT CR-ACME TECH SOLUTIONS INDIA PRIVATE LIMITED CONTINUATION"
    )
    assert st.rows[1].description == "UPI-GROCERY STORE"
    assert st.verified


def test_hdfc_rel_verifies_and_folds_continuation_lines():
    st = extract_bank_statement(pages(HDFC_REL))
    assert st.kind == "hdfc-rel"
    assert [r.amount for r in st.rows] == [-770.0, 25000.0]
    # Continuation lines wrap below their row and attach to it directly.
    assert st.rows[0].description == "UPI-MATOSHRI CATTLE FEED-q052676264@"
    assert st.rows[1].description == "NEFT CR-SALARY"
    assert (st.opening, st.closing) == (10000.0, 34230.0)
    assert (st.n_debit, st.n_credit) == (1, 1)
    assert st.verified


def test_continuation_lines_attach_to_current_row():
    fixture = """
Statement From : 01-08-2026 To : 31-08-2026
Opening Balance      : 10,000.00                                        Limit       : 0.00
     Txn Date                     Narration                    Withdrawals            Deposits           Closing Balance
    01/08/2026        NEFT CR-ACME IT                                 0.00           10,000.00                20,000.00
                      SERVICES INTL PVT LTD-SYNTHETIC
                      ACCOUNTING TRANSFER NOTES
    02/08/2026        UPI-GROCERY STORE                               500.00                0.00                19,500.00
HDFC BANK LIMITED
*Closing balance includes funds earmarked for hold and uncleared funds
Registered Office Address: HDFC Bank House, Mumbai
                            Opening Balance          Debit Amount           Credit Amount              Closing Balance

                              10,000.00                  500.00               10,000.00                   19,500.00
"""
    st = extract_bank_statement(pages(fixture))
    assert st.verified
    assert len(st.rows) == 2
    assert (
        st.rows[0].description
        == "NEFT CR-ACME IT SERVICES INTL PVT LTD-SYNTHETIC ACCOUNTING TRANSFER NOTES"
    )
    assert st.rows[1].description == "UPI-GROCERY STORE"


def test_hdfc_classic_verifies_with_counts():
    st = extract_bank_statement(pages(HDFC_CLASSIC))
    assert st.kind == "hdfc-classic"
    assert [r.amount for r in st.rows] == [-52000.0, 9000.0]
    assert st.rows[0].date == datetime.datetime(2026, 1, 1)
    assert (st.total_debit, st.total_credit) == (52000.0, 9000.0)
    assert (st.n_debit, st.n_credit) == (1, 1)
    assert st.verified


def test_hdfc_classic_row_count_mismatch_is_reported():
    dropped = HDFC_CLASSIC.replace(
        " 09/01/26    NEFT CR-EMPLOYER"
        "                             000000000000000       09/01/26"
        "                              9,000.00            160,020.88\n",
        "",
    )
    st = extract_bank_statement(pages(dropped))
    assert len(st.rows) == 1
    assert not st.verified
    assert any("row counts" in p for p in st.problems)


def test_row_amount_disagreeing_with_balance_delta_warns():
    # The printed amount says 1,500 but the balance moved by 2,500. The balance
    # wins (it is the running total), and the disagreement is a row warning,
    # which alone is enough to withhold verification.
    tampered = AXIS_REL.replace(
        "1,500.00                        58,500.00",
        "1,500.00                        57,500.00",
    )
    st = extract_bank_statement(pages(tampered))
    assert st.rows[1].amount == -2500.0
    assert st.rows[1].stated == 1500.0
    assert st.warnings and "stated" in st.warnings[0]
    assert not st.verified


def test_row_before_opening_balance_warns():
    no_opening = AXIS_REL.replace("Opening Balance", "Balance brought forward")
    st = extract_bank_statement(pages(no_opening))
    assert st.opening is None
    assert any("before opening balance" in w for w in st.warnings)
    assert not st.verified


def test_row_with_no_amount_token_derives_it_from_the_balance():
    # The amount can merge into the narration fragment and lose its own
    # x-position; the balance delta still knows what the row did.
    merged = AXIS_REL.replace(
        "02-07-2024    UPI/COFFEE SHOP/1234                                    1,500.00                        58,500.00",
        "02-07-2024    UPI/COFFEE SHOP/1234 1,500.00                                                           58,500.00",
    )
    st = extract_bank_statement(pages(merged))
    assert st.rows[1].amount == -1500.0
    assert st.rows[1].stated is None
    assert any("no amount token" in w for w in st.warnings)


def test_axis_new_row_with_no_amount_left_of_the_balance_band_is_skipped():
    orphan = AXIS_NEW.replace(
        "  02-06-2026    UPI/GROCERY STORE/8891                       2,715.11",
        "  02-06-2026    UPI/GROCERY STORE/8891" + " " * 66 + "2,715.11",
    )
    st = extract_bank_statement(pages(orphan))
    assert [r.amount for r in st.rows] == [1000.0]


def test_column_bands_come_from_the_raw_header_line():
    from src.statement_extractor import _columns

    header = "  Txn Date   Narration   Withdrawals   Deposits   Closing Balance"
    bands = _columns(header, {"dr": r"Withdrawals", "cr": r"Deposits"})
    assert bands is not None
    assert header[slice(*bands["dr"])] == "Withdrawals"
    # A label the line does not carry means this is not that layout's header.
    assert _columns(header, {"dr": r"Withdrawal\s+Amt\."}) is None


def test_period_falls_back_to_the_row_dates():
    undated = AXIS_REL.replace(
        "Statement of Account for the period (From : 01-07-2024 To : 31-07-2024)", ""
    )
    st = extract_bank_statement(pages(undated))
    assert st.period == ("01-07-2024", "02-07-2024")


def test_lines_before_any_header_are_ignored():
    st = extract_bank_statement(pages("stray 01-07-2024 line 1,234.00\n" + AXIS_REL))
    assert len(st.rows) == 2


# --------------------------------------------------------------------------
# The trust contract
# --------------------------------------------------------------------------


def test_statement_with_no_printed_figure_is_unverified():
    # The negative case that the audit tool this module replaces got wrong: with
    # every cross-check absent it declared the file clean.
    bare = "\n".join(
        line
        for line in AXIS_NEW.lstrip("\n").split("\n")
        if "Opening Balance" not in line and "Closing Balance" not in line
    )
    st = extract_bank_statement([bare])
    assert st.rows, "rows should still be extracted"
    assert st.opening is st.closing is st.total_debit is None
    assert not st.verified
    assert st.problems == ["no printed figure to check against"]


def test_empty_extraction_is_never_verified():
    # Zero rows and zero printed figures is the shape of a parse that failed
    # entirely -- the one case it is most dangerous to call clean.
    st = extract_bank_statement(["nothing that looks like a statement"])
    assert st.rows == []
    assert not st.verified
    assert st.problems == ["no printed figure to check against"]


def test_zero_rows_against_printed_totals_fails_loudly():
    st = ExtractedStatement(total_debit=1500.0, total_credit=50000.0)
    verified, problems = reconcile(st)
    assert not verified
    assert len(problems) == 2


def test_last_row_balance_must_match_the_closing_balance():
    st = ExtractedStatement(
        opening=100.0,
        closing=50.0,
        rows=[
            StatementRow(datetime.datetime(2026, 1, 1), "x", -50.0, balance=60.0),
        ],
    )
    verified, problems = reconcile(st)
    assert not verified
    assert any("last row balance" in p for p in problems)


def test_verified_needs_a_check_and_a_clean_row_pass():
    clean = ExtractedStatement(
        opening=100.0,
        closing=50.0,
        rows=[StatementRow(datetime.datetime(2026, 1, 1), "x", -50.0, balance=50.0)],
    )
    assert clean.verified
    clean.warnings.append("something looked odd")
    assert not clean.verified


# --------------------------------------------------------------------------
# Credit cards
# --------------------------------------------------------------------------


def test_axis_card_signs_from_the_printed_marker():
    st = extract_from_pages(pages(AXIS_CARD), "cc-axis-magnus-2024-09.pdf")
    assert st.kind == "axis-cc"
    assert st.period == ("20/07/2024", "18/08/2024")
    assert [r.amount for r in st.rows] == [-10000.0, -1800.0, 11989.0, -189.0]
    assert st.rows[0].description == "ANNUAL FEE"
    # payments+credits and purchases+debits are read as two independent boxes.
    assert st.payments == 11989.0
    assert (st.purchases, st.charges) == (189.0, 11800.0)
    assert st.verified


def test_axis_card_side_swap_cannot_verify():
    swapped = AXIS_CARD.replace("11,989.00 Cr", "11,989.00 Dr").replace(
        "10,000.00 Dr", "10,000.00 Cr"
    )
    st = extract_from_pages(pages(swapped), "cc-axis-magnus-2024-09.pdf")
    assert not st.verified
    assert len(st.problems) == 2


def test_axis_card_row_without_a_marker_is_a_warning():
    unmarked = AXIS_CARD.replace("1,800.00 Dr", "1,800.00   ")
    st = extract_from_pages(pages(unmarked), "cc-axis-magnus-2024-09.pdf")
    assert len(st.rows) == 3
    assert any("no Dr/Cr amount" in w for w in st.warnings)
    assert not st.verified


def test_hdfc_card_old_layout_reads_the_account_summary():
    st = extract_from_pages(pages(HDFC_CARD_OLD), "cc-hdfc-regaliagold-2025-02.pdf")
    assert st.kind == "hdfc-cc"
    # No printed billing period: the cycle is derived from the statement date.
    assert st.period == ("13/12/2024", "12/01/2025")
    assert [r.amount for r in st.rows] == [-500.0, -20000.0, 12000.0]
    assert (st.payments, st.purchases, st.charges) == (12000.0, 20500.0, 0.0)
    assert st.verified


def test_hdfc_card_new_layout_reads_the_dues_boxes():
    st = extract_from_pages(pages(HDFC_CARD_NEW), "cc-hdfc-infiniametal-2026-07.pdf")
    assert st.period == ("13 Jun, 2026", "12 Jul, 2026")
    assert [r.amount for r in st.rows] == [-20000.0, -5000.0, 10000.0]
    assert (st.payments, st.purchases, st.charges) == (10000.0, 25000.0, 0.0)
    assert st.verified


def test_hdfc_card_newer_layout_with_negative_dues_and_addons():
    st = extract_from_pages(pages(HDFC_CARD_NEWER), "cc-hdfc-infiniametal-2026-08.pdf")
    assert st.kind == "hdfc-cc"
    assert st.period == ("13 Jul, 2026", "12 Aug, 2026")
    assert (st.payments, st.purchases, st.charges) == (10000.0, 26000.0, 0.0)
    assert len(st.rows) == 5
    assert [r.amount for r in st.rows] == [-20000.0, 1000.0, 9000.0, -2000.0, -4000.0]
    assert [r.description for r in st.rows] == [
        "SYNTHETIC GROCER BENGALURU",
        "RETURN REFUND MERCHANT",
        "PAYMENT RECEIVED",
        "CAFE COFFEE BENGALURU",
        "OVERSEAS BOOKSTORE LONDON",
    ]
    assert st.verified


def test_hdfc_card_row_without_an_amount_is_a_warning():
    amountless = HDFC_CARD_NEW.replace("C 5,000.00", "          ")
    st = extract_from_pages(pages(amountless), "cc-hdfc-infiniametal-2026-07.pdf")
    assert len(st.rows) == 2
    assert any("no amount" in w for w in st.warnings)
    assert not st.verified


def test_hdfc_card_without_any_period_marker_has_none():
    undated = HDFC_CARD_OLD.replace("Statement Date:12/01/2025", "")
    st = extract_from_pages(pages(undated), "cc-hdfc-regaliagold-2025-02.pdf")
    assert st.period is None
    assert st.verified


def test_hdfc_card_preserves_trailing_c_in_merchant_names():
    """Merchant names ending in 'C' (IRCTC, ACME LLC, WIDGETS INC) are not truncated
    by the optional rupee-glyph 'C' prefix on either old or new HDFC card layouts."""
    old_c = """
0      Address    : A1106 Some Street                     Statement Date:12/01/2025
                                                     Account Summary
                                        Opening        Payment/         Purchase/        Finance        Total Dues
                                        Balance          Credits          Debits         Charges

                                       1,000.00          500.00         1,234.00          0.00          1,734.00

      10/01/2025             IRCTC                                                       1,234.00
      11/01/2025             ACME LLC                                                      500.00Cr
"""
    st_old = extract_from_pages(pages(old_c), "cc-hdfc-regaliagold-2025-02.pdf")
    assert st_old.verified
    assert [(r.description, r.amount) for r in st_old.rows] == [
        ("IRCTC", -1234.0),
        ("ACME LLC", 500.0),
    ]

    new_c = """
 BENGALURU 560049 KAR                                Billing Period               13 Jul, 2026 - 12 Aug, 2026
       PREVIOUS STATEMENT DUES        PAYMENTS/CREDITS            PURCHASES/DEBIT              FINANCE CHARGES
                   C0.00                   C503.48                   C2,034.00                    C0.00
   Domestic Transactions
14/07/2026| 16:14            IRCTC                                                                            C 1,234.00     l
15/07/2026| 12:00            WIDGETS INC                                          + 20                        C 800.00l
16/07/2026| 09:00            ACME LLC                                                                      +  C 500.00       l
17/07/2026| 00:00            GLOBAL VALUE_CASH BACK 18.00% FEE12.34USD                                     +  C 3.48l
"""
    st_new = extract_from_pages(pages(new_c), "cc-hdfc-infiniametal-2026-08.pdf")
    assert st_new.verified
    assert [(r.description, r.amount) for r in st_new.rows] == [
        ("IRCTC", -1234.0),
        ("WIDGETS INC", -800.0),
        ("ACME LLC", 500.0),
        ("GLOBAL VALUE_CASH BACK 18.00% FEE12.34USD", 3.48),
    ]

    unparsed_rate_line = (
        new_c + "18/07/2026| NOTE             GST AT 18.00% APPLIES ON FEE12.34USD\n"
    )
    st_warn = extract_from_pages(
        pages(unparsed_rate_line), "cc-hdfc-infiniametal-2026-08.pdf"
    )
    assert not st_warn.verified
    assert any("no amount" in w for w in st_warn.warnings)


def test_card_family_dispatch_follows_the_registry():
    assert [layout.name for layout in CARD_LAYOUTS] == ["hdfc-cc", "axis-cc"]
    assert (
        extract_from_pages(pages(AXIS_CARD), "cc-axis-neo-2024-09.pdf").kind
        == "axis-cc"
    )
    assert (
        extract_from_pages(pages(HDFC_CARD_OLD), "cc-hdfc-regaliagold-2025-02.pdf").kind
        == "hdfc-cc"
    )
    # Anything not prefixed cc- is a bank statement.
    assert (
        extract_from_pages(pages(AXIS_REL), "bank-axis-karti-2024-08.pdf").kind
        == "axis-rel"
    )


def test_bank_layout_registry_is_complete():
    assert [layout.name for layout in BANK_LAYOUTS] == [
        "axis-rel",
        "axis-compact",
        "axis-new",
        "hdfc-rel",
        "hdfc-classic",
    ]
    # axis-new is the only generation without a running balance to sign from.
    assert [layout.name for layout in BANK_LAYOUTS if not layout.running_balance] == [
        "axis-new"
    ]


def test_summary_round_trips_the_essentials():
    st = extract_bank_statement(pages(AXIS_REL))
    summary = st.summary()
    assert summary["verified"] is True
    assert summary["n"] == 2
    assert summary["dr"] == 1500.0 and summary["cr"] == 50000.0
    assert summary["rows"][0] == {
        "date": "2024-07-01",
        "desc": "NEFT SALARY CREDIT",
        "amt": 50000.0,
    }
    assert json.loads(json.dumps(summary))["period"] == ["01-07-2024", "31-07-2024"]


# --------------------------------------------------------------------------
# PDF reading and passwords
# --------------------------------------------------------------------------


def _pdf_bytes(text: str, password: str = "") -> bytes:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    buf = io.BytesIO()
    kwargs = {}
    if password:
        from reportlab.lib import pdfencrypt

        kwargs["encrypt"] = pdfencrypt.StandardEncryption(password)
    pdf = canvas.Canvas(buf, pagesize=A4, **kwargs)
    pdf.setFont("Courier", 6)
    y = A4[1] - 30
    for line in text.lstrip("\n").split("\n"):
        pdf.drawString(20, y, line)
        y -= 8
    pdf.save()
    return buf.getvalue()


def test_read_pdf_pages_returns_layout_text():
    text = read_pdf_pages(_pdf_bytes("Opening Balance 10,000.00"))[0]
    assert "Opening Balance" in text


def test_encrypted_pdf_opens_with_a_candidate_password():
    data = _pdf_bytes("Opening Balance 10,000.00", password="hunter2")
    assert "Opening Balance" in read_pdf_pages(data, ["wrong", "hunter2"])[0]


def test_encrypted_pdf_without_the_password_raises():
    data = _pdf_bytes("Opening Balance 10,000.00", password="hunter2")
    with pytest.raises(ValueError):
        read_pdf_pages(data, ["wrong"])


def test_extract_statement_bytes_dispatches_on_the_filename():
    st = extract_statement_bytes(
        _pdf_bytes(AXIS_REL), "bank-axis-karti-2024-08.pdf", []
    )
    assert st.kind == "axis-rel"


def test_extract_statement_reads_from_disk(tmp_path, monkeypatch):
    path = tmp_path / "bank-axis-karti-2024-08.pdf"
    path.write_bytes(_pdf_bytes(AXIS_REL))
    monkeypatch.chdir(tmp_path)  # no secrets/passwords.json here
    assert extract_statement(str(path)).kind == "axis-rel"


def test_statement_passwords_prefers_the_account_specific_key(tmp_path):
    pw_file = tmp_path / "passwords.json"
    pw_file.write_text(
        json.dumps({"axis": ["BANKLEVEL"], "axis-secondary": ["CURRENT", "OLD"]})
    )
    assert statement_passwords("bank-axis-secondary-2026-05.pdf", str(pw_file)) == [
        "CURRENT",
        "OLD",
        "BANKLEVEL",
    ]
    assert statement_passwords("bank-axis-karti-2026-05.pdf", str(pw_file)) == [
        "BANKLEVEL"
    ]


def test_statement_passwords_tolerates_a_missing_or_broken_file(tmp_path):
    assert statement_passwords("bank-axis-karti-2026-05.pdf", "no/such/file") == []
    assert statement_passwords("nodashes.pdf", "no/such/file") == []
    broken = tmp_path / "passwords.json"
    broken.write_text("{not json")
    assert statement_passwords("bank-axis-karti-2026-05.pdf", str(broken)) == []


# --------------------------------------------------------------------------
# Multi-page & Add-on Cardholder Tests
# --------------------------------------------------------------------------

HDFC_CARD_MULTIPAGE_P1 = """
 BENGALURU 560049 KAR                                Billing Period               13 Jul, 2026 - 12 Aug, 2026
       PREVIOUS STATEMENT DUES        PAYMENTS/CREDITS            PURCHASES/DEBIT              FINANCE CHARGES
                   C-9,585.00             C10,000.00                   C30,000.00                    C0.00
   Domestic Transactions
                                                    ALEX MORGAN
14/07/2026| 10:00            ONLINE GROCER                                                                 C 10,000.00     l
                                                    PATRICIA MORGAN      [CKYC ID : XXXXXXXXXX1234 ]
15/07/2026| 11:00            PHARMACY STORE                                                                C 5,000.00      l
"""

HDFC_CARD_MULTIPAGE_P2 = """
16/07/2026| 12:00            PHARMEASY VIA SMARTBUYMUMBAI                                                  C 5,000.00      l
   International Transactions
                                                    SAMIRA KHAN
17/07/2026| 13:00            AIRLINE TICKET                                                                C 10,000.00     l
20/07/2026| 00:00            PAYMENT RECEIVED                                                           +  C 10,000.00     l
   Reward Points Summary
   Opening Balance: 1000
   GST Summary
"""

AXIS_CARD_ADDON = """
        Total Payment Due                Minimum Payment Due                Statement Period               Payment Due Date
             12,000.00   Dr                    400.00   Dr                 20/07/2024 - 18/08/2024               07/09/2024
        Previous Balance - Payments - Credits + Purchase + Cash Advance + Other Debit&Charges =Total Payment Due
        10,000.00  Dr        10,000.00           0.00          12,000.00           0.00           0.00             12,000.00   Dr
        Card No: 4111XXXXXXXX1234    Name ALEX MORGAN
        DATE                     TRANSACTION DETAILS                       MERCHANT CATEGORY               AMOUNT (Rs.)
  03/08/2024             ANNUAL FEE                                                                          10,000.00 Dr
  07/08/2024             PAYMENT RECEIVED                                                                    10,000.00 Cr
        Card No: 4111XXXXXXXX5678    Name PATRICIA MORGAN
  04/08/2024             GROCERY STORE                                                                        2,000.00 Dr
"""


def test_hdfc_card_multipage_continuation_and_smartbuy_and_addons():
    pages_list = [
        HDFC_CARD_MULTIPAGE_P1.lstrip("\n"),
        HDFC_CARD_MULTIPAGE_P2.lstrip("\n"),
    ]
    st = extract_from_pages(pages_list, "cc-hdfc-infiniametal-2026-08.pdf")
    assert st.kind == "hdfc-cc"
    assert st.period == ("13 Jul, 2026", "12 Aug, 2026")
    assert (st.payments, st.purchases, st.charges) == (10000.0, 30000.0, 0.0)
    assert len(st.rows) == 5
    assert [r.amount for r in st.rows] == [
        -10000.0,
        -5000.0,
        -5000.0,
        -10000.0,
        10000.0,
    ]
    assert [r.description for r in st.rows] == [
        "ONLINE GROCER",
        "PHARMACY STORE",
        "PHARMEASY VIA SMARTBUYMUMBAI",
        "AIRLINE TICKET",
        "PAYMENT RECEIVED",
    ]
    # Cardholders tracked across page breaks and unaffected by SMARTBUY in description:
    assert [r.cardholder for r in st.rows] == [
        "ALEX MORGAN",
        "PATRICIA MORGAN",
        "PATRICIA MORGAN",
        "SAMIRA KHAN",
        "SAMIRA KHAN",
    ]
    assert st.verified


def test_axis_card_captures_cardholder_names():
    st = extract_from_pages(pages(AXIS_CARD_ADDON), "cc-axis-magnus-2024-08.pdf")
    assert st.kind == "axis-cc"
    assert len(st.rows) == 3
    assert [r.amount for r in st.rows] == [-10000.0, 10000.0, -2000.0]
    assert [r.cardholder for r in st.rows] == [
        "ALEX MORGAN",
        "ALEX MORGAN",
        "PATRICIA MORGAN",
    ]
    assert st.verified


# --------------------------------------------------------------------------
# Opt-in regression against the real corpus
# --------------------------------------------------------------------------


@pytest.mark.skipif(
    not (os.environ.get("GAJANA_PDF_DIR") and os.environ.get("GAJANA_SNAPSHOT")),
    reason="set GAJANA_PDF_DIR and GAJANA_SNAPSHOT to re-check the real corpus",
)
def test_corpus_matches_the_recorded_snapshot():
    """Every statement in the local corpus still extracts to the same rows.

    The corpus is personal data, so it lives outside the repo: point
    GAJANA_PDF_DIR at the statement directory and GAJANA_SNAPSHOT at a snapshot
    written by ``python -m src.statement_reconciler --snapshot``.
    """
    with open(os.environ["GAJANA_SNAPSHOT"], "r", encoding="utf-8") as fh:
        snapshot = json.load(fh)
    pdf_dir = os.environ["GAJANA_PDF_DIR"]
    assert snapshot, "snapshot is empty"
    for name, expected in snapshot.items():
        st = extract_statement(os.path.join(pdf_dir, name))
        got = st.summary()
        assert got["kind"] == expected["kind"], name
        assert got["verified"] == expected["verified"], name
        assert got["rows"] == expected["rows"], name
