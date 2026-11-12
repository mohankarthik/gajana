"""Per-user add-on credit card spend tracking, reimbursable categorization,
repayment matching, FIFO settlement, and Telegram notifications.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import datetime
import json
import logging
import math
import os
import re
from typing import Any, Hashable, Optional, Sequence

import requests

from src.settings import ADDON_CARDS

logger = logging.getLogger(__name__)

API_URL = "https://api.telegram.org/bot{token}/{method}"
HTTP_TIMEOUT = 30

_DEFAULT_SETTINGS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "plugins", "telegram_bot", "settings.json"
)
_DEFAULT_TOKEN_PATH = os.path.join(
    os.path.dirname(__file__), "..", "secrets", "telegram.json"
)


@dataclass(frozen=True)
class AddonUser:
    """Configuration for an add-on card user."""

    id: str
    label: str
    greeting_name: str = ""
    cardholder_patterns: list[str] = field(default_factory=list)
    reimburse: bool = False
    reimburse_category: str = "Transfer"
    repayment_since: Optional[datetime.date] = None
    repayment_patterns: list[str] = field(default_factory=list)


@dataclass
class OpenItem:
    """An open (unsettled or partially settled) CC debit transaction."""

    date: datetime.date
    description: str
    gross_amount: float
    unsettled_amount: float


@dataclass
class UserReport:
    """Settlement and spend report for a configured add-on card user."""

    user: AddonUser
    total_spend: float
    total_repaid: float
    net_due: float
    open_items: list[OpenItem]
    recent_swipes: list[dict[Hashable, Any]]
    current_month_spend: float
    previous_month_spend: float


@dataclass
class UnconfiguredUserReport:
    """Spend report for an unconfigured add-on cardholder tag."""

    label: str
    total_spend: float
    recent_swipes: list[dict[Hashable, Any]]
    current_month_spend: float
    previous_month_spend: float


@dataclass
class AddonReport:
    """Full add-on tracking report across all users."""

    users: list[UserReport]
    unconfigured_users: list[UnconfiguredUserReport]


def parse_addon_config(
    config: Optional[dict[str, Any]] = None,
) -> tuple[list[str], list[AddonUser]]:
    """Parses addon_cards config dictionary into (primary_cardholders, users)."""
    if config is None:
        config = ADDON_CARDS
    primaries = [
        str(p).strip() for p in config.get("primary_cardholders", []) if str(p).strip()
    ]
    users: list[AddonUser] = []
    for u in config.get("users", []):
        if not isinstance(u, dict):
            continue
        uid = str(u.get("id", "")).strip()
        label = str(u.get("label", "")).strip() or uid
        greeting = str(u.get("greeting_name", "")).strip() or label
        reimburse = bool(u.get("reimburse", False))
        reimburse_cat = (
            str(u.get("reimburse_category", "Transfer")).strip() or "Transfer"
        )
        rep_since_raw = u.get("repayment_since")
        rep_since: Optional[datetime.date] = None
        if rep_since_raw:
            try:
                rep_since = datetime.date.fromisoformat(str(rep_since_raw).strip())
            except Exception:
                rep_since = None
        card_pats = [
            str(p).strip() for p in u.get("cardholder_patterns", []) if str(p).strip()
        ]
        rep_pats = [
            str(p).strip() for p in u.get("repayment_patterns", []) if str(p).strip()
        ]
        users.append(
            AddonUser(
                id=uid,
                label=label,
                greeting_name=greeting,
                cardholder_patterns=card_pats,
                reimburse=reimburse,
                reimburse_category=reimburse_cat,
                repayment_since=rep_since,
                repayment_patterns=rep_pats,
            )
        )
    return primaries, users


def resolve_cardholder_tag(
    cardholder: Optional[str], addon_config: Optional[dict[str, Any]] = None
) -> Optional[str]:
    """Resolves raw statement cardholder string to 'addon:<Label>' or None for primary."""
    if not cardholder:
        return None
    ch = " ".join(str(cardholder).split()).strip()
    if not ch:
        return None

    primaries, users = parse_addon_config(addon_config)
    ch_upper = ch.upper()

    def _matches_name(query: str, target: str) -> bool:
        if (
            query == target
            or query.startswith(target + " ")
            or target.startswith(query + " ")
        ):
            return True
        q_toks = [t.rstrip(".") for t in query.split() if t.rstrip(".")]
        t_toks = [t.rstrip(".") for t in target.split() if t.rstrip(".")]
        if len(q_toks) == len(t_toks) and len(q_toks) >= 2:
            if all(
                q == t
                or (len(q) == 1 and t.startswith(q))
                or (len(t) == 1 and q.startswith(t))
                for q, t in zip(q_toks, t_toks)
            ):
                return True
        return False

    # Check primary cardholders: exact match or token prefix match
    for p in primaries:
        p_norm = " ".join(p.split()).strip().upper()
        if _matches_name(ch_upper, p_norm):
            return None

    # Check configured add-on users
    for user in users:
        for pat in user.cardholder_patterns:
            pat_norm = " ".join(pat.split()).strip().upper()
            if not pat_norm:
                continue
            if _matches_name(ch_upper, pat_norm):
                return f"addon:{user.label}"
            if len(pat_norm) >= 4 and pat_norm in ch_upper:
                return f"addon:{user.label}"

    # If primary cardholders is non-empty and cardholder is non-empty and did not match
    if primaries:
        return f"addon:{ch.title()}"

    return None


def parse_addon_tag(remarks: Optional[str]) -> Optional[str]:
    """Extracts label from 'addon:<Label>' in remarks (e.g. 'addon:Mom' -> 'Mom').
    Handles combined suffixes like 'addon:Partner; REVIEW: low-confidence'."""
    if not remarks or (isinstance(remarks, float) and math.isnan(remarks)):
        return None
    for part in str(remarks).split(";"):
        m = re.match(r"^\s*addon:(.+)$", part.strip(), re.I)
        if m:
            return m.group(1).strip()
    return None


def parse_addon_repay_tag(remarks: Optional[str]) -> Optional[str]:
    """Extracts label from 'addon-repay:<Label>' in remarks (e.g. 'addon-repay:Mom' -> 'Mom')."""
    if not remarks or (isinstance(remarks, float) and math.isnan(remarks)):
        return None
    for part in str(remarks).split(";"):
        m = re.match(r"^\s*addon-repay:(.+)$", part.strip(), re.I)
        if m:
            return m.group(1).strip()
    return None


def match_addon_repayment(
    txn: dict[Hashable, Any], users: Sequence[AddonUser]
) -> Optional[AddonUser]:
    """Matches an incoming bank credit to a reimbursable add-on user."""
    try:
        amt = float(txn.get("amount", 0.0) or 0.0)
    except (ValueError, TypeError):
        return None
    if amt <= 0:
        return None

    raw_remarks = txn.get("remarks")
    rem_str = ""
    if raw_remarks is not None and not (
        isinstance(raw_remarks, float) and math.isnan(raw_remarks)
    ):
        rem_str = str(raw_remarks).strip()

    repay_tag = parse_addon_repay_tag(rem_str)
    desc = str(txn.get("description", "")).lower()
    norm_desc = re.sub(r"\s*@\s*", "@", desc)

    raw_date = txn.get("date")
    txn_date: Optional[datetime.date] = None
    if raw_date is not None:
        if isinstance(raw_date, datetime.datetime):
            txn_date = raw_date.date()
        elif isinstance(raw_date, datetime.date):
            txn_date = raw_date
        elif isinstance(raw_date, str) and raw_date.strip():
            try:
                txn_date = datetime.date.fromisoformat(raw_date.strip()[:10])
            except Exception:
                txn_date = None

    for user in users:
        if not user.reimburse:
            continue

        # Condition 1: explicit tag in remarks
        if repay_tag and (
            repay_tag.lower() == user.label.lower()
            or repay_tag.lower() == user.id.lower()
        ):
            return user

        if not user.repayment_patterns:
            continue

        # Condition 2:
        # a) date check
        if user.repayment_since is not None:
            if txn_date is None:
                continue
            if txn_date < user.repayment_since:
                continue

        # b) remarks check
        if rem_str:
            low_rem = rem_str.lower()
            allowed_rem = (
                low_rem.startswith("review:")
                or low_rem.startswith(f"addon-repay:{user.label.lower()}")
                or low_rem.startswith(f"addon-repay:{user.id.lower()}")
            )
            if not allowed_rem:
                continue

        # c) description check
        for pat in user.repayment_patterns:
            p_clean = re.sub(r"\s*@\s*", "@", pat.lower().strip())
            if p_clean and p_clean in norm_desc:
                return user

    return None


def is_reimbursable_addon_row(
    txn: dict[Hashable, Any], addon_config: Optional[dict[str, Any]] = None
) -> bool:
    """True if transaction is an add-on swipe or repayment tagged with Transfer/reimburse_category."""
    rem = txn.get("remarks")
    if not rem or (isinstance(rem, float) and math.isnan(rem)):
        return False
    rem_str = str(rem).strip()
    addon_tag = parse_addon_tag(rem_str)
    repay_tag = parse_addon_repay_tag(rem_str)
    if not addon_tag and not repay_tag:
        return False

    cat = str(txn.get("category", "")).strip()
    if not cat:
        return False

    _, users = parse_addon_config(addon_config)
    reimburse_cats = {"Transfer"}
    for u in users:
        if u.reimburse:
            reimburse_cats.add(u.reimburse_category)

    return cat in reimburse_cats


def _to_date_obj(val: Any) -> datetime.date:
    """Normalizes datetime/date/iso string into a datetime.date."""
    if isinstance(val, datetime.datetime):
        return val.date()
    if isinstance(val, datetime.date):
        return val
    if isinstance(val, str) and val.strip():
        try:
            return datetime.date.fromisoformat(val.strip()[:10])
        except Exception:
            pass
    return datetime.date.min


def _format_currency(val: float) -> str:
    """Formats an amount in INR: integer if round, else 2 decimals."""
    if round(val, 2) % 1 != 0:
        return f"₹{val:,.2f}"
    return f"₹{int(round(val)):,}"


def compute_addon_report(
    cc_txns: Sequence[dict[Hashable, Any]],
    bank_txns: Sequence[dict[Hashable, Any]],
    addon_config: Optional[dict[str, Any]] = None,
) -> AddonReport:
    """Computes spend, repayments, FIFO settlement, and summaries across add-on cards."""
    primaries, users = parse_addon_config(addon_config)
    user_by_label: dict[str, AddonUser] = {}
    for u in users:
        user_by_label[u.label.lower()] = u
        user_by_label[u.id.lower()] = u

    user_cc_txns: dict[str, list[dict[Hashable, Any]]] = defaultdict(list)
    unconf_cc_txns: dict[str, list[dict[Hashable, Any]]] = defaultdict(list)

    for txn in cc_txns:
        tag = parse_addon_tag(txn.get("remarks"))
        if not tag:
            continue
        matched_user = user_by_label.get(tag.lower())
        if matched_user:
            user_cc_txns[matched_user.id].append(txn)
        else:
            unconf_cc_txns[tag].append(txn)

    user_repayments: dict[str, list[dict[Hashable, Any]]] = defaultdict(list)
    for txn in bank_txns:
        matched_user = match_addon_repayment(txn, users)
        if matched_user:
            user_repayments[matched_user.id].append(txn)

    today = datetime.date.today()
    curr_year, curr_month = today.year, today.month
    prev_year = curr_year if curr_month > 1 else curr_year - 1
    prev_month = curr_month - 1 if curr_month > 1 else 12

    user_reports: list[UserReport] = []
    for user in users:
        u_cc = user_cc_txns.get(user.id, [])
        u_rep = user_repayments.get(user.id, [])

        # Sort CC txns chronologically: oldest first
        u_cc_sorted = sorted(
            u_cc,
            key=lambda t: (
                _to_date_obj(t.get("date")),
                str(t.get("description", "")),
                float(t.get("amount", 0.0)),
            ),
        )

        total_spend = round(sum(-float(t.get("amount", 0.0)) for t in u_cc_sorted), 2)
        total_repaid = round(sum(float(r.get("amount", 0.0)) for r in u_rep), 2)
        net_due = round(total_spend - total_repaid, 2)

        curr_spend = round(
            sum(
                -float(t.get("amount", 0.0))
                for t in u_cc_sorted
                if _to_date_obj(t.get("date")).year == curr_year
                and _to_date_obj(t.get("date")).month == curr_month
            ),
            2,
        )
        prev_spend = round(
            sum(
                -float(t.get("amount", 0.0))
                for t in u_cc_sorted
                if _to_date_obj(t.get("date")).year == prev_year
                and _to_date_obj(t.get("date")).month == prev_month
            ),
            2,
        )

        # Recent swipes: up to 5 newest first
        recent_swipes = sorted(
            u_cc_sorted,
            key=lambda t: _to_date_obj(t.get("date")),
            reverse=True,
        )[:5]

        # FIFO settlement for reimbursable users
        open_items: list[OpenItem] = []
        if user.reimburse:
            # CC refunds (amount > 0 in Gajana) add to covered pool
            refund_pool = sum(
                float(t.get("amount", 0.0))
                for t in u_cc_sorted
                if float(t.get("amount", 0.0)) > 0
            )
            covered_pool = total_repaid + refund_pool

            # Positive purchases (amount < 0 in Gajana, spend > 0)
            purchases = [t for t in u_cc_sorted if -float(t.get("amount", 0.0)) > 0]
            for t in purchases:
                gross = round(-float(t.get("amount", 0.0)), 2)
                t_date = _to_date_obj(t.get("date"))
                t_desc = str(t.get("description", ""))

                if covered_pool >= gross - 0.005:
                    covered_pool -= gross
                elif covered_pool > 0.005:
                    unsettled = round(gross - covered_pool, 2)
                    covered_pool = 0.0
                    open_items.append(
                        OpenItem(
                            date=t_date,
                            description=t_desc,
                            gross_amount=gross,
                            unsettled_amount=unsettled,
                        )
                    )
                else:
                    open_items.append(
                        OpenItem(
                            date=t_date,
                            description=t_desc,
                            gross_amount=gross,
                            unsettled_amount=gross,
                        )
                    )

        user_reports.append(
            UserReport(
                user=user,
                total_spend=total_spend,
                total_repaid=total_repaid,
                net_due=net_due,
                open_items=open_items,
                recent_swipes=recent_swipes,
                current_month_spend=curr_spend,
                previous_month_spend=prev_spend,
            )
        )

    unconfigured_reports: list[UnconfiguredUserReport] = []
    for label, txns in unconf_cc_txns.items():
        sorted_txns = sorted(
            txns,
            key=lambda t: (
                _to_date_obj(t.get("date")),
                str(t.get("description", "")),
                float(t.get("amount", 0.0)),
            ),
        )
        total_spend = round(sum(-float(t.get("amount", 0.0)) for t in sorted_txns), 2)
        curr_spend = round(
            sum(
                -float(t.get("amount", 0.0))
                for t in sorted_txns
                if _to_date_obj(t.get("date")).year == curr_year
                and _to_date_obj(t.get("date")).month == curr_month
            ),
            2,
        )
        prev_spend = round(
            sum(
                -float(t.get("amount", 0.0))
                for t in sorted_txns
                if _to_date_obj(t.get("date")).year == prev_year
                and _to_date_obj(t.get("date")).month == prev_month
            ),
            2,
        )
        recent = sorted(
            sorted_txns, key=lambda t: _to_date_obj(t.get("date")), reverse=True
        )[:5]
        unconfigured_reports.append(
            UnconfiguredUserReport(
                label=label,
                total_spend=total_spend,
                recent_swipes=recent,
                current_month_spend=curr_spend,
                previous_month_spend=prev_spend,
            )
        )

    return AddonReport(users=user_reports, unconfigured_users=unconfigured_reports)


def format_reimbursable_user_message(report: UserReport) -> str:
    """Formats a clean, copy-pasteable message for a reimbursable user (e.g. Mom).
    Strict on-screen arithmetic invariant: items_total - partial_credit == net_due.
    """
    greeting = report.user.greeting_name or report.user.label
    if report.net_due <= 0.005:
        if report.net_due < -0.5:
            credit_val = abs(report.net_due)
            return (
                f"Hi {greeting}, credit card spend is all settled up "
                f"(₹0 due, {_format_currency(credit_val)} credit balance)."
            )
        return f"Hi {greeting}, credit card spend is all settled up (₹0 due)."

    lines = [f"Hi {greeting}, credit card spend summary:", ""]
    for item in report.open_items:
        date_str = item.date.strftime("%d %b")
        amt_str = _format_currency(item.gross_amount)
        lines.append(f"• {date_str} – {item.description}: {amt_str}")

    open_items_gross = round(sum(i.gross_amount for i in report.open_items), 2)
    partial_credit = round(open_items_gross - report.net_due, 2)

    lines.append("")
    if partial_credit > 0.01:
        lines.append(f"Items total: {_format_currency(open_items_gross)}")
        lines.append(f"Already received via UPI: -{_format_currency(partial_credit)}")
        lines.append(f"Amount due: {_format_currency(report.net_due)}")
    else:
        if len(report.open_items) > 1:
            if report.total_repaid == 0:
                lines.append(f"Total spend: {_format_currency(open_items_gross)}")
                lines.append("Received via UPI: ₹0")
                lines.append(f"Amount due: {_format_currency(report.net_due)}")
            else:
                lines.append(f"Total due: {_format_currency(report.net_due)}")
        else:
            lines.append(f"Amount due: {_format_currency(report.net_due)}")

    return "\n".join(lines)


def format_non_reimbursable_user_message(report: UserReport) -> str:
    """Formats a spend summary for a non-reimbursable add-on user (e.g. Partner)."""
    label = report.user.label
    lines = [
        f"{label} Spend Summary:",
        f"• Current month: {_format_currency(report.current_month_spend)}",
        f"• Previous month: {_format_currency(report.previous_month_spend)}",
        f"• Total spend: {_format_currency(report.total_spend)}",
    ]
    if report.recent_swipes:
        lines.append("• Recent swipes:")
        for t in report.recent_swipes:
            d_str = _to_date_obj(t.get("date")).strftime("%d %b")
            amt = abs(float(t.get("amount", 0.0)))
            lines.append(
                f"  - {d_str}: {t.get('description', '')} ({_format_currency(amt)})"
            )
    return "\n".join(lines)


def format_unconfigured_user_message(report: UnconfiguredUserReport) -> str:
    """Formats a spend summary for an unconfigured add-on cardholder."""
    lines = [
        f"{report.label} Spend Summary:",
        f"• Current month: {_format_currency(report.current_month_spend)}",
        f"• Previous month: {_format_currency(report.previous_month_spend)}",
        f"• Total spend: {_format_currency(report.total_spend)}",
    ]
    if report.recent_swipes:
        lines.append("• Recent swipes:")
        for t in report.recent_swipes:
            d_str = _to_date_obj(t.get("date")).strftime("%d %b")
            amt = abs(float(t.get("amount", 0.0)))
            lines.append(
                f"  - {d_str}: {t.get('description', '')} ({_format_currency(amt)})"
            )
    return "\n".join(lines)


def format_all_addons_overview(report: AddonReport) -> str:
    """Formats a multi-user overview for /addon all."""
    lines = ["Add-on Cards Overview:", ""]
    if not report.users and not report.unconfigured_users:
        return "No add-on cards configured or found in transactions."

    for u_rep in report.users:
        u = u_rep.user
        reimb_tag = " (reimbursable)" if u.reimburse else ""
        lines.append(f"{u.label}{reimb_tag}:")
        lines.append(
            f"  • Current month: {_format_currency(u_rep.current_month_spend)}"
        )
        lines.append(
            f"  • Previous month: {_format_currency(u_rep.previous_month_spend)}"
        )
        if u.reimburse:
            lines.append(
                f"  • Net due: {_format_currency(u_rep.net_due)} "
                f"(repaid via UPI: {_format_currency(u_rep.total_repaid)})"
            )
        else:
            lines.append(f"  • Total spend: {_format_currency(u_rep.total_spend)}")
        if u_rep.recent_swipes:
            lines.append("  • Recent swipes:")
            for t in u_rep.recent_swipes[:3]:
                d_str = _to_date_obj(t.get("date")).strftime("%d %b")
                amt = abs(float(t.get("amount", 0.0)))
                lines.append(
                    f"    - {d_str}: {t.get('description', '')} ({_format_currency(amt)})"
                )
        lines.append("")

    for unconf in report.unconfigured_users:
        lines.append(f"{unconf.label} (unconfigured):")
        lines.append(
            f"  • Current month: {_format_currency(unconf.current_month_spend)}"
        )
        lines.append(
            f"  • Previous month: {_format_currency(unconf.previous_month_spend)}"
        )
        lines.append(f"  • Total spend: {_format_currency(unconf.total_spend)}")
        if unconf.recent_swipes:
            lines.append("  • Recent swipes:")
            for t in unconf.recent_swipes[:3]:
                d_str = _to_date_obj(t.get("date")).strftime("%d %b")
                amt = abs(float(t.get("amount", 0.0)))
                lines.append(
                    f"    - {d_str}: {t.get('description', '')} ({_format_currency(amt)})"
                )
        lines.append("")

    return "\n".join(lines).strip()


def send_telegram_addon_notification(
    all_cc: Sequence[dict[Hashable, Any]],
    all_bank: Sequence[dict[Hashable, Any]],
    newly_added_cc: Sequence[dict[Hashable, Any]],
    addon_config: Optional[dict[str, Any]] = None,
    settings_path: Optional[str] = None,
    token_path: Optional[str] = None,
) -> bool:
    """Sends header notification and separate forwardable messages when new CC add-on rows land.
    Returns True if notification was sent, False if unconfigured or no add-on rows.
    """
    # 1. Filter newly added CC rows that have an addon: tag
    new_addon_rows: list[tuple[str, float]] = []
    for t in newly_added_cc:
        tag = parse_addon_tag(t.get("remarks"))
        if tag:
            new_addon_rows.append((tag, abs(float(t.get("amount", 0.0)))))

    if not new_addon_rows:
        return False

    # 2. Check Telegram configuration
    st_path = settings_path or _DEFAULT_SETTINGS_PATH
    if not os.path.exists(st_path):
        logger.info("Telegram settings not found; skipping add-on notification.")
        return False

    try:
        with open(st_path, "r", encoding="utf-8") as f:
            st_data = json.load(f)
    except Exception as e:
        logger.warning(f"Could not read Telegram settings: {e}")
        return False

    chat_id = st_data.get("allowed_chat_id")
    if not chat_id:
        return False

    tk_path = token_path or _DEFAULT_TOKEN_PATH
    token = ""
    if os.path.exists(tk_path):
        try:
            with open(tk_path, "r", encoding="utf-8") as f:
                tk_data = json.load(f)
            token = str(tk_data.get("token") or tk_data.get("bot_token") or "").strip()
        except Exception as e:
            logger.warning(f"Could not read Telegram token: {e}")

    if not token:
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

    if not token:
        logger.info("No Telegram bot token found; skipping add-on notification.")
        return False

    # 3. Compute full report
    report = compute_addon_report(all_cc, all_bank, addon_config)

    # 4. Group new add-on rows by tag
    user_swipes: dict[str, list[float]] = defaultdict(list)
    for tag, amt in new_addon_rows:
        user_swipes[tag].append(amt)

    summary_parts = []
    for tag, amounts in user_swipes.items():
        total_amt = sum(amounts)
        fmt_a = _format_currency(total_amt)
        n = len(amounts)
        summary_parts.append(f"{tag} {fmt_a} ({n} txn{'s' if n != 1 else ''})")

    header_msg = f"New CC statement add-on spend booked: {', '.join(summary_parts)}."

    url = API_URL.format(token=token, method="sendMessage")

    def _send(txt: str) -> None:
        resp = requests.post(
            url, json={"chat_id": chat_id, "text": txt}, timeout=HTTP_TIMEOUT
        )
        resp.raise_for_status()

    # Send header message
    try:
        _send(header_msg)
    except Exception as e:
        logger.warning(f"Failed to send Telegram header notification: {e}")
        return False

    # 5. Send separate clean message for each reimbursable user with new rows
    reimb_users_with_new_rows = {
        u_rep.user.label.lower(): u_rep
        for u_rep in report.users
        if u_rep.user.reimburse
    }
    for tag in user_swipes:
        u_rep = reimb_users_with_new_rows.get(tag.lower())
        if u_rep:
            msg = format_reimbursable_user_message(u_rep)
            try:
                _send(msg)
            except Exception as e:
                logger.warning(
                    f"Failed to send forwardable add-on message for {tag}: {e}"
                )

    return True
