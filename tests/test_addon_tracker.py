"""Unit and regression tests for add-on card spend tracking and settlement."""

from __future__ import annotations

import datetime

from src.addon_tracker import (
    _clean_remark_str,
    compute_addon_report,
    format_all_addons_overview,
    format_non_reimbursable_user_message,
    format_reimbursable_user_message,
    format_unconfigured_user_message,
    is_card_bill_payment_or_cashback,
    is_reimbursable_addon_row,
    match_addon_repayment,
    parse_addon_config,
    parse_addon_repay_tag,
    parse_addon_tag,
    resolve_cardholder_tag,
    send_telegram_addon_notification,
)
from src.categorizer import Categorizer
from src.category_index import CategoryIndex
from src.constants import DEFAULT_CATEGORY
from src.utils import redact_telegram_token

# ---------------------------------------------------------------------------
# Synthetic Test Configurations (strictly zero real personal data)
# ---------------------------------------------------------------------------

SYNTHETIC_ADDON_CONFIG = {
    "primary_cardholders": ["ALEX MORGAN"],
    "users": [
        {
            "id": "mom",
            "label": "Mom",
            "greeting_name": "Amma",
            "cardholder_patterns": ["PATRICIA M", "PATRICIA MORGAN"],
            "reimburse": True,
            "reimburse_category": "Transfer",
            "repayment_since": "2026-08-01",
            "repayment_patterns": ["patricia", "patricia@okbank"],
        },
        {
            "id": "partner",
            "label": "Partner",
            "cardholder_patterns": ["SAMIRA K"],
            "reimburse": False,
        },
    ],
}


# ---------------------------------------------------------------------------
# Config Parsing & Cardholder Resolution
# ---------------------------------------------------------------------------


def test_parse_addon_config_empty_and_valid():
    primaries, users = parse_addon_config({})
    assert primaries == []
    assert users == []

    primaries, users = parse_addon_config(SYNTHETIC_ADDON_CONFIG)
    assert primaries == ["ALEX MORGAN"]
    assert len(users) == 2
    u_mom = users[0]
    assert u_mom.id == "mom"
    assert u_mom.label == "Mom"
    assert u_mom.greeting_name == "Amma"
    assert u_mom.reimburse is True
    assert u_mom.reimburse_category == "Transfer"
    assert u_mom.repayment_since == datetime.date(2026, 8, 1)
    assert u_mom.repayment_patterns == ["patricia", "patricia@okbank"]

    u_partner = users[1]
    assert u_partner.id == "partner"
    assert u_partner.reimburse is False


def test_resolve_cardholder_tag():
    cfg = SYNTHETIC_ADDON_CONFIG

    # None or empty
    assert resolve_cardholder_tag(None, cfg) is None
    assert resolve_cardholder_tag("", cfg) is None
    assert resolve_cardholder_tag("   ", cfg) is None

    # Primary cardholder -> None
    assert resolve_cardholder_tag("ALEX MORGAN", cfg) is None
    assert resolve_cardholder_tag("Alex Morgan", cfg) is None
    assert resolve_cardholder_tag("ALEX M", cfg) is None
    assert resolve_cardholder_tag("MR ALEX K MORGAN (PRIMARY)", cfg) is None
    assert resolve_cardholder_tag("ALEX KENNETH MORGAN", cfg) is None
    assert resolve_cardholder_tag("DR. ALEX MORGAN", cfg) is None

    # Configured reimbursable add-on user
    assert resolve_cardholder_tag("PATRICIA MORGAN", cfg) == "addon:Mom"
    assert resolve_cardholder_tag("patricia m", cfg) == "addon:Mom"
    assert resolve_cardholder_tag("MRS PATRICIA MORGAN (ADD-ON)", cfg) == "addon:Mom"
    assert resolve_cardholder_tag("PATRICIA ANN MORGAN", cfg) == "addon:Mom"

    # Configured non-reimbursable add-on user
    assert resolve_cardholder_tag("SAMIRA K", cfg) == "addon:Partner"

    # Unconfigured non-primary name -> title-cased fallback (honorifics/suffixes stripped)
    assert resolve_cardholder_tag("RANDOM COUSIN", cfg) == "addon:Random Cousin"
    assert (
        resolve_cardholder_tag("MR RANDOM COUSIN (ADD-ON)", cfg)
        == "addon:Random Cousin"
    )

    # Configured add-on pattern sharing initial/surname with primary is not swallowed by primary fuzzy matching
    shared_initial_cfg = {
        "primary_cardholders": ["M MORGAN"],
        "users": [
            {
                "id": "mom",
                "label": "Mom",
                "cardholder_patterns": ["M PATRICIA MORGAN"],
                "reimburse": True,
            }
        ],
    }
    assert (
        resolve_cardholder_tag("M PATRICIA MORGAN", shared_initial_cfg) == "addon:Mom"
    )
    assert resolve_cardholder_tag("M MORGAN", shared_initial_cfg) is None
    assert resolve_cardholder_tag("MR M MORGAN (PRIMARY)", shared_initial_cfg) is None

    # Pattern "RAJ K" cannot substring-match "SURAJ K KUMAR"
    substring_boundary_cfg = {
        "primary_cardholders": ["ALEX MORGAN"],
        "users": [
            {
                "id": "brother",
                "label": "Brother",
                "cardholder_patterns": ["RAJ K"],
            }
        ],
    }
    assert (
        resolve_cardholder_tag("SURAJ K KUMAR", substring_boundary_cfg)
        == "addon:Suraj K Kumar"
    )
    assert (
        resolve_cardholder_tag("RAJ K KUMAR", substring_boundary_cfg) == "addon:Brother"
    )


# ---------------------------------------------------------------------------
# Tag Parsing & Reimbursable Row Check
# ---------------------------------------------------------------------------


def test_parse_addon_tag_and_repay_tag():
    assert parse_addon_tag("addon:Mom") == "Mom"
    assert parse_addon_tag("addon: Partner") == "Partner"
    assert parse_addon_tag("addon:Mom; REVIEW: check") == "Mom"
    assert parse_addon_tag("other remark; addon:Mom") == "Mom"
    assert parse_addon_tag("no addon tag here") is None
    assert parse_addon_tag(None) is None
    assert parse_addon_tag(float("nan")) is None

    assert parse_addon_repay_tag("addon-repay:Mom") == "Mom"
    assert parse_addon_repay_tag("addon-repay: Partner") == "Partner"
    assert parse_addon_repay_tag("review: note; addon-repay:Mom") == "Mom"
    assert parse_addon_repay_tag("addon:Mom") is None


def test_is_reimbursable_addon_row():
    cfg = SYNTHETIC_ADDON_CONFIG

    # CC swipe tagged addon:Mom with Transfer category -> True
    row1 = {
        "description": "PHARMACY",
        "amount": -500.0,
        "category": "Transfer",
        "remarks": "addon:Mom",
    }
    assert is_reimbursable_addon_row(row1, cfg) is True

    # Bank repayment tagged addon-repay:Mom with Transfer category -> True
    row2 = {
        "description": "UPI/CR/123/PATRICIA@OKBANK",
        "amount": 500.0,
        "category": "Transfer",
        "remarks": "addon-repay:Mom",
    }
    assert is_reimbursable_addon_row(row2, cfg) is True

    # Non-reimbursable add-on swipe (addon:Partner) -> False
    row3 = {
        "description": "CLOTHING STORE",
        "amount": -2000.0,
        "category": "Expense:Clothing",
        "remarks": "addon:Partner",
    }
    assert is_reimbursable_addon_row(row3, cfg) is False

    # Standard primary row -> False
    row4 = {
        "description": "GROCERY",
        "amount": -1000.0,
        "category": "Expense:Groceries",
        "remarks": "",
    }
    assert is_reimbursable_addon_row(row4, cfg) is False


# ---------------------------------------------------------------------------
# Bank UPI Repayment Auto-Matching
# ---------------------------------------------------------------------------


def test_match_addon_repayment():
    _, users = parse_addon_config(SYNTHETIC_ADDON_CONFIG)

    # Valid incoming credit on or after repayment_since matching pattern
    txn_match = {
        "date": datetime.date(2026, 8, 15),
        "amount": 10000.0,
        "description": "UPI/CR/123456789012/PATRICIA@OKBANK/PAYMENT",
        "remarks": "",
    }
    matched = match_addon_repayment(txn_match, users)
    assert matched is not None
    assert matched.id == "mom"

    # Handles space around @ in UPI handle
    txn_space_at = {
        "date": datetime.date(2026, 8, 15),
        "amount": 5000.0,
        "description": "UPI/CR/123456789012/PATRICIA @ OKBANK/PAYMENT",
        "remarks": "",
    }
    assert match_addon_repayment(txn_space_at, users) is not None

    # Date before repayment_since -> ignored
    txn_old = {
        "date": datetime.date(2026, 7, 20),
        "amount": 10000.0,
        "description": "UPI/CR/123456789012/PATRICIA@OKBANK/PAYMENT",
        "remarks": "",
    }
    assert match_addon_repayment(txn_old, users) is None

    # Debit (amount <= 0) -> ignored
    txn_debit = {
        "date": datetime.date(2026, 8, 15),
        "amount": -15000.0,
        "description": "UPI/DR/123456789012/PATRICIA@OKBANK/PAYMENT",
        "remarks": "",
    }
    assert match_addon_repayment(txn_debit, users) is None

    # Human remarks opt-out (e.g. 'gift' or 'travel') -> blocked
    txn_opt_out = {
        "date": datetime.date(2026, 8, 15),
        "amount": 10000.0,
        "description": "UPI/CR/123456789012/PATRICIA@OKBANK/PAYMENT",
        "remarks": "birthday gift",
    }
    assert match_addon_repayment(txn_opt_out, users) is None

    # Category check: recategorization away from reimburse_category (e.g. Income:Other) opts out
    txn_recategorized = {
        "date": datetime.date(2026, 8, 15),
        "amount": 10000.0,
        "description": "UPI/CR/123456789012/PATRICIA@OKBANK/PAYMENT",
        "category": "Income:Other",
        "remarks": "",
    }
    assert match_addon_repayment(txn_recategorized, users) is None

    # Matching category ("Transfer") or empty/missing category allows match
    txn_matching_cat = {
        "date": datetime.date(2026, 8, 15),
        "amount": 10000.0,
        "description": "UPI/CR/123456789012/PATRICIA@OKBANK/PAYMENT",
        "category": "Transfer",
        "remarks": "",
    }
    assert match_addon_repayment(txn_matching_cat, users) is not None

    txn_empty_cat = {
        "date": datetime.date(2026, 8, 15),
        "amount": 10000.0,
        "description": "UPI/CR/123456789012/PATRICIA@OKBANK/PAYMENT",
        "category": "",
        "remarks": "",
    }
    assert match_addon_repayment(txn_empty_cat, users) is not None

    # Review remarks allowed
    txn_review_rem = {
        "date": datetime.date(2026, 8, 15),
        "amount": 10000.0,
        "description": "UPI/CR/123456789012/PATRICIA@OKBANK/PAYMENT",
        "remarks": "REVIEW: check account",
    }
    assert match_addon_repayment(txn_review_rem, users) is not None

    # Auto-tagged row with category changed away from Transfer (e.g. Income:Other) opts out
    txn_tagged_opt_out_cat = {
        "date": datetime.date(2026, 5, 1),
        "amount": 5000.0,
        "description": "NEFT TRANSFER FROM MOTHER",
        "category": "Income:Other",
        "remarks": "addon-repay:Mom",
    }
    assert match_addon_repayment(txn_tagged_opt_out_cat, users) is None

    # Auto-tagged row with free-text remarks added (e.g. birthday gift) opts out
    txn_tagged_opt_out_rem = {
        "date": datetime.date(2026, 5, 1),
        "amount": 5000.0,
        "description": "NEFT TRANSFER FROM MOTHER",
        "category": "Transfer",
        "remarks": "addon-repay:Mom; birthday gift",
    }
    assert match_addon_repayment(txn_tagged_opt_out_rem, users) is None

    # Auto-tagged row with review flag preserved matches
    txn_tagged_review = {
        "date": datetime.date(2026, 5, 1),
        "amount": 5000.0,
        "description": "NEFT TRANSFER FROM MOTHER",
        "category": "Transfer",
        "remarks": "addon-repay:Mom; REVIEW: low-confidence",
    }
    matched_review = match_addon_repayment(txn_tagged_review, users)
    assert matched_review is not None
    assert matched_review.id == "mom"


# ---------------------------------------------------------------------------
# Categorizer Layer 0 Override
# ---------------------------------------------------------------------------


def test_categorizer_layer0_override_and_preservation(tmp_path):
    matchers_file = tmp_path / "matchers.json"
    matchers_file.write_text(
        '[{"category": "Expense:Groceries", "description": ["SUPERMARKET"]}]'
    )

    categorizer = Categorizer(
        matchers_file=str(matchers_file),
        addon_cards=SYNTHETIC_ADDON_CONFIG,
    )

    # Rule 0a: Reimbursable CC swipe overrides exact matcher rule -> Transfer
    txn_reimb = {
        "date": "2026-08-10",
        "description": "SUPERMARKET CITY CENTER",
        "amount": -3000.0,
        "account": "cc-hdfc",
        "category": DEFAULT_CATEGORY,
        "remarks": "addon:Mom",
    }
    categorizer.categorize([txn_reimb])
    assert txn_reimb["category"] == "Transfer"
    assert txn_reimb["remarks"] == "addon:Mom"

    # Rule 0b: Bank credit repayment auto-matches -> Transfer, remarks tagged
    txn_bank = {
        "date": "2026-08-12",
        "description": "UPI/CR/998877665544/PATRICIA@OKBANK",
        "amount": 3000.0,
        "account": "bank-axis",
        "category": DEFAULT_CATEGORY,
        "remarks": "",
    }
    categorizer.categorize([txn_bank])
    assert txn_bank["category"] == "Transfer"
    assert "addon-repay:Mom" in txn_bank["remarks"]

    # Non-reimbursable user CC swipe: does NOT get Transfer; normal rules apply
    txn_non_reimb = {
        "date": "2026-08-10",
        "description": "SUPERMARKET CITY CENTER",
        "amount": -1500.0,
        "account": "cc-hdfc",
        "category": DEFAULT_CATEGORY,
        "remarks": "addon:Partner",
    }
    categorizer.categorize([txn_non_reimb])
    assert txn_non_reimb["category"] == "Expense:Groceries"
    assert txn_non_reimb["remarks"] == "addon:Partner"


# ---------------------------------------------------------------------------
# CategoryIndex Isolation
# ---------------------------------------------------------------------------


def test_category_index_isolation():
    # Historical training rows:
    # 1. Mom's purchase tagged addon:Mom with Transfer
    # 2. Partner's purchase tagged addon:Partner with Expense:Clothing
    # 3. Primary's purchase at PHARMACY with Expense:Medical
    history = [
        {
            "description": "CITY PHARMACY BENGALURU",
            "amount": -500.0,
            "category": "Transfer",
            "remarks": "addon:Mom",
        },
        {
            "description": "TRENDS CLOTHING STORE",
            "amount": -2000.0,
            "category": "Expense:Clothing",
            "remarks": "addon:Partner",
        },
        {
            "description": "CITY PHARMACY BENGALURU",
            "amount": -500.0,
            "category": "Expense:Medical",
            "remarks": "",
        },
    ]

    index = CategoryIndex().build(history, addon_cards=SYNTHETIC_ADDON_CONFIG)

    # City pharmacy exact lookup should be Expense:Medical (Transfer row was skipped!)
    exact_hit, conf, method = index.lookup("CITY PHARMACY BENGALURU", -500.0)
    assert exact_hit == "Expense:Medical"
    assert method == "exact"

    # Partner's clothing store should be indexed
    partner_hit, conf, method = index.lookup("TRENDS CLOTHING STORE", -2000.0)
    assert partner_hit == "Expense:Clothing"
    assert method == "exact"


# ---------------------------------------------------------------------------
# FIFO Settlement Math & Formatting
# ---------------------------------------------------------------------------


def test_compute_addon_report_fifo_settlement():
    cc_txns = [
        # Mom purchases
        {
            "date": datetime.date(2026, 8, 2),
            "description": "GROCERIES",
            "amount": -4000.0,
            "remarks": "addon:Mom",
        },
        {
            "date": datetime.date(2026, 8, 5),
            "description": "MEDICINES",
            "amount": -2000.0,
            "remarks": "addon:Mom",
        },
        {
            "date": datetime.date(2026, 8, 10),
            "description": "UTILITY BILL",
            "amount": -3000.0,
            "remarks": "addon:Mom",
        },
        # Partner purchase
        {
            "date": datetime.date(2026, 8, 12),
            "description": "BOOK STORE",
            "amount": -1500.0,
            "remarks": "addon:Partner",
        },
    ]

    # Scenario 1: Zero repayments
    rep1 = compute_addon_report(cc_txns, [], SYNTHETIC_ADDON_CONFIG)
    mom_rep1 = next(u for u in rep1.users if u.user.id == "mom")
    assert mom_rep1.total_spend == 9000.0
    assert mom_rep1.total_repaid == 0.0
    assert mom_rep1.net_due == 9000.0
    assert len(mom_rep1.open_items) == 3
    assert [i.unsettled_amount for i in mom_rep1.open_items] == [
        4000.0,
        2000.0,
        3000.0,
    ]

    # Scenario 2: Partial repayment of ₹5,000 via UPI
    # Covers GROCERIES (4,000) fully, leaves MEDICINES (2,000 - 1,000 = 1,000 open), UTILITY BILL (3,000 open)
    bank_partial = [
        {
            "date": datetime.date(2026, 8, 8),
            "amount": 5000.0,
            "description": "UPI/CR/123/PATRICIA@OKBANK",
            "remarks": "addon-repay:Mom",
        }
    ]
    rep2 = compute_addon_report(cc_txns, bank_partial, SYNTHETIC_ADDON_CONFIG)
    mom_rep2 = next(u for u in rep2.users if u.user.id == "mom")
    assert mom_rep2.total_spend == 9000.0
    assert mom_rep2.total_repaid == 5000.0
    assert mom_rep2.net_due == 4000.0
    assert len(mom_rep2.open_items) == 2
    assert mom_rep2.open_items[0].description == "MEDICINES"
    assert mom_rep2.open_items[0].gross_amount == 2000.0
    assert mom_rep2.open_items[0].unsettled_amount == 1000.0
    assert mom_rep2.open_items[1].description == "UTILITY BILL"
    assert mom_rep2.open_items[1].gross_amount == 3000.0
    assert mom_rep2.open_items[1].unsettled_amount == 3000.0

    # Strict on-screen arithmetic invariant
    msg2 = format_reimbursable_user_message(mom_rep2)
    assert "Items total: ₹5,000" in msg2
    assert "Already received via UPI: -₹1,000" in msg2
    assert "Amount due: ₹4,000" in msg2

    # Scenario 3: Exact full repayment of ₹9,000
    bank_full = [
        {
            "date": datetime.date(2026, 8, 15),
            "amount": 9000.0,
            "description": "UPI/CR/123/PATRICIA@OKBANK",
            "remarks": "addon-repay:Mom",
        }
    ]
    rep3 = compute_addon_report(cc_txns, bank_full, SYNTHETIC_ADDON_CONFIG)
    mom_rep3 = next(u for u in rep3.users if u.user.id == "mom")
    assert mom_rep3.net_due == 0.0
    assert len(mom_rep3.open_items) == 0
    msg3 = format_reimbursable_user_message(mom_rep3)
    assert "credit card spend is all settled up (₹0 due)" in msg3

    # Scenario 4: Overpayment of ₹10,000 (₹1,000 credit balance)
    bank_over = [
        {
            "date": datetime.date(2026, 8, 15),
            "amount": 10000.0,
            "description": "UPI/CR/123/PATRICIA@OKBANK",
            "remarks": "addon-repay:Mom",
        }
    ]
    rep4 = compute_addon_report(cc_txns, bank_over, SYNTHETIC_ADDON_CONFIG)
    mom_rep4 = next(u for u in rep4.users if u.user.id == "mom")
    assert mom_rep4.net_due == -1000.0
    msg4 = format_reimbursable_user_message(mom_rep4)
    assert "credit balance" in msg4
    assert "₹1,000" in msg4

    # Scenario 5: CC merchant refund (amount > 0 in CC)
    cc_with_refund = cc_txns + [
        {
            "date": datetime.date(2026, 8, 11),
            "description": "MEDICINES RETURN",
            "amount": 1000.0,  # +1000 credit refund on card
            "remarks": "addon:Mom",
        }
    ]
    rep5 = compute_addon_report(cc_with_refund, [], SYNTHETIC_ADDON_CONFIG)
    mom_rep5 = next(u for u in rep5.users if u.user.id == "mom")
    assert mom_rep5.total_spend == 8000.0  # 9000 - 1000
    assert mom_rep5.net_due == 8000.0
    assert mom_rep5.total_refunds == 1000.0
    # Because merchant refund contributed to settling open items, use neutral label
    msg5 = format_reimbursable_user_message(mom_rep5)
    assert "Items total: ₹9,000" in msg5
    assert "Already paid / adjusted: -₹1,000" in msg5
    assert "Amount due: ₹8,000" in msg5


def test_format_non_reimbursable_and_overview():
    cc_txns = [
        {
            "date": datetime.date(2026, 8, 5),
            "description": "CLOTHING STORE",
            "amount": -4500.0,
            "remarks": "addon:Partner",
        }
    ]
    rep = compute_addon_report(cc_txns, [], SYNTHETIC_ADDON_CONFIG)
    partner_rep = next(u for u in rep.users if u.user.id == "partner")
    msg = format_non_reimbursable_user_message(partner_rep)
    assert "Partner Spend Summary:" in msg
    assert "Total spend: ₹4,500" in msg
    assert "CLOTHING STORE" in msg

    overview = format_all_addons_overview(rep)
    assert "Add-on Cards Overview:" in overview
    assert "Mom (reimbursable):" in overview
    assert "Partner:" in overview


def test_format_unconfigured_user():
    cc_txns = [
        {
            "date": datetime.date(2026, 8, 10),
            "description": "CAFE",
            "amount": -650.0,
            "remarks": "addon:Friend",
        }
    ]
    rep = compute_addon_report(cc_txns, [], SYNTHETIC_ADDON_CONFIG)
    assert len(rep.unconfigured_users) == 1
    unconf = rep.unconfigured_users[0]
    assert unconf.label == "Friend"
    assert unconf.total_spend == 650.0
    msg = format_unconfigured_user_message(unconf)
    assert "Friend Spend Summary:" in msg
    assert "Total spend: ₹650" in msg


# ---------------------------------------------------------------------------
# Chat Bridge Notification Helper
# ---------------------------------------------------------------------------


def test_send_telegram_addon_notification_flow(tmp_path, caplog):
    from src.chat_client import ChatGatewayUnavailableError, ChatSocketError

    class _FakeChatClient:
        def __init__(self):
            self.outbox = []
            self.raise_on_call = None

        def send_message(self, channel, text, reply_to=None):
            if callable(self.raise_on_call):
                self.raise_on_call(len(self.outbox), text)
            self.outbox.append({"channel": channel, "text": text})
            return {"ok": True}

    client = _FakeChatClient()
    missing_settings = tmp_path / "missing_settings.json"

    # Case 1: No add-on swipes -> False, no messages
    res1 = send_telegram_addon_notification(
        all_cc=[],
        all_bank=[],
        newly_added_cc=[
            {
                "description": "GROCERIES",
                "amount": -1000.0,
                "remarks": "",
            }
        ],
        addon_config=SYNTHETIC_ADDON_CONFIG,
        settings_path=str(missing_settings),
        chat_client=client,
    )
    assert res1 is False
    assert len(client.outbox) == 0

    # Case 2: New add-on swipe for Mom (settings.json absent -> still sends via bridge)
    new_cc = [
        {
            "date": datetime.date(2026, 8, 10),
            "description": "PHARMACY",
            "amount": -2500.0,
            "remarks": "addon:Mom",
        }
    ]
    res2 = send_telegram_addon_notification(
        all_cc=new_cc,
        all_bank=[],
        newly_added_cc=new_cc,
        addon_config=SYNTHETIC_ADDON_CONFIG,
        settings_path=str(missing_settings),
        chat_client=client,
    )
    assert res2 is True
    assert len(client.outbox) == 2
    assert client.outbox[0]["channel"] == "finance"
    assert (
        "New CC statement add-on spend booked: Mom ₹2,500 (1 txn)."
        in client.outbox[0]["text"]
    )
    assert client.outbox[1]["channel"] == "finance"
    assert "Hi Amma, credit card spend summary:" in client.outbox[1]["text"]
    assert "• 10 Aug – PHARMACY: ₹2,500" in client.outbox[1]["text"]

    # Case 3: Batch containing a purchase (-3000), a refund (+500), and card bill payment (+10000)
    client.outbox.clear()
    new_cc_mixed = [
        {
            "date": datetime.date(2026, 8, 12),
            "description": "CLOTHING STORE",
            "amount": -3000.0,
            "remarks": "addon:Mom",
        },
        {
            "date": datetime.date(2026, 8, 13),
            "description": "CLOTHING RETURN",
            "amount": 500.0,
            "remarks": "addon:Mom",
        },
        {
            "date": datetime.date(2026, 8, 14),
            "description": "PAYMENT RECEIVED - AUTOPAY",
            "amount": 10000.0,
            "remarks": "addon:Mom",
        },
    ]
    res3 = send_telegram_addon_notification(
        all_cc=new_cc_mixed,
        all_bank=[],
        newly_added_cc=new_cc_mixed,
        addon_config=SYNTHETIC_ADDON_CONFIG,
        settings_path=str(missing_settings),
        chat_client=client,
    )
    assert res3 is True
    assert (
        "New CC statement add-on spend booked: Mom ₹2,500 (2 txns)."
        in client.outbox[0]["text"]
    )

    # Case 4: Net refund activity (refund exceeds purchase)
    client.outbox.clear()
    new_cc_refund = [
        {
            "date": datetime.date(2026, 8, 12),
            "description": "COFFEE",
            "amount": -500.0,
            "remarks": "addon:Mom",
        },
        {
            "date": datetime.date(2026, 8, 13),
            "description": "STORE REFUND",
            "amount": 1000.0,
            "remarks": "addon:Mom",
        },
    ]
    res4 = send_telegram_addon_notification(
        all_cc=new_cc_refund,
        all_bank=[],
        newly_added_cc=new_cc_refund,
        addon_config=SYNTHETIC_ADDON_CONFIG,
        settings_path=str(missing_settings),
        chat_client=client,
    )
    assert res4 is True
    assert (
        "New CC statement add-on spend booked: Mom net -₹500 (2 txns)."
        in client.outbox[0]["text"]
    )

    # Case 5: Respects enabled: false in settings.json
    client.outbox.clear()
    disabled_settings = tmp_path / "disabled_settings.json"
    disabled_settings.write_text('{"enabled": false}', encoding="utf-8")
    res5 = send_telegram_addon_notification(
        all_cc=new_cc,
        all_bank=[],
        newly_added_cc=new_cc,
        addon_config=SYNTHETIC_ADDON_CONFIG,
        settings_path=str(disabled_settings),
        chat_client=client,
    )
    assert res5 is False
    assert len(client.outbox) == 0

    # Case 6: Header send error catches ChatSocketError / OSError without raising
    client.outbox.clear()

    def _fail_first(idx, txt):
        raise ChatGatewayUnavailableError("bridge unavailable")

    client.raise_on_call = _fail_first
    with caplog.at_level("WARNING"):
        res6 = send_telegram_addon_notification(
            all_cc=new_cc,
            all_bank=[],
            newly_added_cc=new_cc,
            addon_config=SYNTHETIC_ADDON_CONFIG,
            settings_path=str(missing_settings),
            chat_client=client,
        )
    assert res6 is False
    assert "Failed to send add-on header notification" in caplog.text

    # Case 7: Forwardable message error catches ChatSocketError without raising
    client.outbox.clear()

    def _fail_second(idx, txt):
        if idx >= 1:
            raise ChatSocketError("send failed")

    client.raise_on_call = _fail_second
    with caplog.at_level("WARNING"):
        res7 = send_telegram_addon_notification(
            all_cc=new_cc,
            all_bank=[],
            newly_added_cc=new_cc,
            addon_config=SYNTHETIC_ADDON_CONFIG,
            settings_path=str(missing_settings),
            chat_client=client,
        )
    assert res7 is True
    assert len(client.outbox) == 1
    assert "Failed to send forwardable add-on message" in caplog.text

    # Case 8: Corrupt settings.json fails closed (does not bypass enabled: false)
    client.outbox.clear()
    caplog.clear()
    corrupt_settings = tmp_path / "corrupt_settings.json"
    corrupt_settings.write_text("{not valid json", encoding="utf-8")
    with caplog.at_level("WARNING"):
        res8 = send_telegram_addon_notification(
            all_cc=new_cc,
            all_bank=[],
            newly_added_cc=new_cc,
            addon_config=SYNTHETIC_ADDON_CONFIG,
            settings_path=str(corrupt_settings),
            chat_client=client,
        )
    assert res8 is False
    assert len(client.outbox) == 0
    assert "Could not read chat bot settings" in caplog.text

    # Case 9: Non-object JSON in settings.json also fails closed
    client.outbox.clear()
    caplog.clear()
    list_settings = tmp_path / "list_settings.json"
    list_settings.write_text("[]", encoding="utf-8")
    with caplog.at_level("WARNING"):
        res9 = send_telegram_addon_notification(
            all_cc=new_cc,
            all_bank=[],
            newly_added_cc=new_cc,
            addon_config=SYNTHETIC_ADDON_CONFIG,
            settings_path=str(list_settings),
            chat_client=client,
        )
    assert res9 is False
    assert len(client.outbox) == 0
    assert "not a JSON object" in caplog.text


def test_send_telegram_addon_notification_no_live_socket_fallback(tmp_path):
    """Ensure conftest isolates AGENTIC_CHAT_SOCKET so unmocked calls fail closed cleanly."""
    new_cc = [
        {
            "date": datetime.date(2026, 8, 10),
            "description": "PHARMACY",
            "amount": -2500.0,
            "remarks": "addon:Mom",
        }
    ]
    res = send_telegram_addon_notification(
        all_cc=new_cc,
        all_bank=[],
        newly_added_cc=new_cc,
        addon_config=SYNTHETIC_ADDON_CONFIG,
        settings_path=str(tmp_path / "missing.json"),
    )
    assert res is False


# ---------------------------------------------------------------------------
# Card-level Bill Payment vs Merchant Refund Tests
# ---------------------------------------------------------------------------


def test_is_card_bill_payment_or_cashback():
    # True for bill payment / card cashback credit descriptions (amount > 0)
    assert is_card_bill_payment_or_cashback("PAYMENT RECEIVED", 5000.0) is True
    assert (
        is_card_bill_payment_or_cashback("CC PAYMENT VIA NETBANKING", 12000.0) is True
    )
    assert is_card_bill_payment_or_cashback("AUTOPAY DEBIT", 2000.0) is True
    assert is_card_bill_payment_or_cashback("BBPS PAYMENT RECEIVED", 1000.0) is True
    assert is_card_bill_payment_or_cashback("NEFT CREDIT-AXIS", 15000.0) is True
    assert is_card_bill_payment_or_cashback("GLOBAL VALUE_CASH BACK", 500.0) is True
    assert is_card_bill_payment_or_cashback("MONTHLY CASHBACK", 250.0) is True

    # False for merchant refunds / reversals (amount > 0)
    assert is_card_bill_payment_or_cashback("AMAZON REFUND", 1200.0) is False
    assert is_card_bill_payment_or_cashback("IRCTC REVERSAL", 450.0) is False
    assert is_card_bill_payment_or_cashback("ACME STORE REFUND", 300.0) is False
    assert is_card_bill_payment_or_cashback("PHARMACY RETURN", 150.0) is False

    # False for debits (amount <= 0) even if description mentions payment
    assert is_card_bill_payment_or_cashback("PAYMENT TO VENDOR", -500.0) is False


def test_bill_payment_does_not_wipe_out_addon_dues_in_report_or_categorizer(tmp_path):
    # A statement with Mom purchases + a card-level bill payment tagged addon:Mom
    # and a merchant refund tagged addon:Mom
    cc_txns = [
        {
            "date": datetime.date(2026, 8, 5),
            "description": "PHARMACY STORE",
            "amount": -5000.0,
            "remarks": "addon:Mom",
            "account": "cc-hdfc",
        },
        {
            "date": datetime.date(2026, 8, 10),
            "description": "PAYMENT RECEIVED",
            "amount": 5000.0,  # Card bill payment
            "remarks": "addon:Mom",
            "account": "cc-hdfc",
        },
        {
            "date": datetime.date(2026, 8, 12),
            "description": "STORE REFUND",
            "amount": 500.0,  # Legitimate merchant refund
            "remarks": "addon:Mom",
            "account": "cc-hdfc",
        },
    ]

    # Report check:
    # 5,000 spend - 500 merchant refund = 4,500 net due (the 5,000 bill payment is EXCLUDED)
    rep = compute_addon_report(cc_txns, [], SYNTHETIC_ADDON_CONFIG)
    mom_rep = next(u for u in rep.users if u.user.id == "mom")
    assert mom_rep.total_spend == 4500.0
    assert mom_rep.net_due == 4500.0
    assert len(mom_rep.open_items) == 1
    assert mom_rep.open_items[0].description == "PHARMACY STORE"

    # Categorizer check:
    matchers_file = tmp_path / "matchers.json"
    matchers_file.write_text(
        '[{"category": "Expense:Medical", "description": ["PHARMACY"]}]'
    )
    categorizer = Categorizer(
        matchers_file=str(matchers_file),
        addon_cards=SYNTHETIC_ADDON_CONFIG,
    )
    txns_copy = [dict(t) for t in cc_txns]
    categorizer.categorize(txns_copy)
    # PHARMACY gets Transfer
    assert txns_copy[0]["category"] == "Transfer"
    # PAYMENT RECEIVED does NOT get Transfer via Rule 0a
    assert txns_copy[1]["category"] == DEFAULT_CATEGORY
    # STORE REFUND does get Transfer (merchant refund reduces dues)
    assert txns_copy[2]["category"] == "Transfer"


# ---------------------------------------------------------------------------
# Word-Boundary & Token Redaction Tests
# ---------------------------------------------------------------------------


def test_repayment_word_boundary_matching():
    _, users = parse_addon_config(SYNTHETIC_ADDON_CONFIG)

    # Word-boundary check for name pattern "patricia"
    txn_exact = {
        "date": datetime.date(2026, 8, 15),
        "amount": 1000.0,
        "description": "UPI/CR/123/PATRICIA/PAYMENT",
    }
    assert match_addon_repayment(txn_exact, users) is not None

    # Substring in longer word "patrician" MUST NOT match
    txn_subword = {
        "date": datetime.date(2026, 8, 15),
        "amount": 1000.0,
        "description": "UPI/CR/123/PATRICIAN/PAYMENT",
    }
    assert match_addon_repayment(txn_subword, users) is None

    # Handle pattern "patricia@okbank" matches
    txn_handle = {
        "date": datetime.date(2026, 8, 15),
        "amount": 1000.0,
        "description": "UPI/CR/123/PATRICIA@OKBANK/PAYMENT",
    }
    assert match_addon_repayment(txn_handle, users) is not None


def test_redact_telegram_token():
    raw_url = "https://api.telegram.org/bot123456:SECRET_ABC_xyz-99/sendMessage"
    redacted = redact_telegram_token(raw_url)
    assert "SECRET_ABC" not in redacted
    assert "https://api.telegram.org/bot<REDACTED>/sendMessage" == redacted

    # Safe on non-string / exceptions
    exc = Exception(f"Failed to post to {raw_url}")
    redacted_exc = redact_telegram_token(exc)
    assert "SECRET_ABC" not in redacted_exc
    assert "bot<REDACTED>" in redacted_exc


def test_clean_remark_str():
    import pandas as pd

    assert _clean_remark_str(None) == ""
    assert _clean_remark_str("") == ""
    assert _clean_remark_str("   ") == ""
    assert _clean_remark_str(pd.NA) == ""
    assert _clean_remark_str(float("nan")) == ""
    assert _clean_remark_str("<NA>") == ""
    assert _clean_remark_str("nan") == ""
    assert _clean_remark_str("addon:Mom") == "addon:Mom"
