"""Telegram cash-entry bot for gajana.

Cron-polled (no webhook, no exposed port): each run fetches new group messages
via ``getUpdates``, parses a cash entry ("Coffee 250" or "+2000 ATM"),
categorizes it from the Cash Transactions history, appends a row to that tab,
and replies in the group. Only the configured group chat and allowed users are
processed. ``/undo`` removes the most recent entry.

Cash rows live in the "Cash Transactions" tab, columns B:G
(Date | Description | Debit | Credit | Category | Remarks) -- there is no
Account column (a single shared cash ledger).
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
from typing import Any, Hashable, Optional

import requests

from src.categorizer import Categorizer
from src.constants import DEFAULT_CATEGORY, TRANSACTIONS_SHEET_ID

logger = logging.getLogger(__name__)

CASH_SHEET_NAME = "Cash Transactions"
API_URL = "https://api.telegram.org/bot{token}/{method}"
HTTP_TIMEOUT = 30

# A leading-token that flags the entry as cash-in (a Credit) rather than a spend.
CREDIT_FIRST_WORDS = {"atm", "received", "deposit", "credit", "cashin", "refund"}
# Amount: optional leading +/-, digits with optional thousands commas + decimals.
_AMOUNT_RE = re.compile(r"([+-]?)(\d[\d,]*(?:\.\d+)?)")
_ISO_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")


class ParsedEntry:
    """A parsed cash entry from a message (None amount = not an entry)."""

    def __init__(
        self,
        description: str,
        amount: float,
        is_credit: bool,
        entry_date: datetime.date,
    ) -> None:
        self.description = description
        self.amount = amount
        self.is_credit = is_credit
        self.entry_date = entry_date


def parse_entry(text: str) -> Optional[ParsedEntry]:
    """Parse a message into a cash entry, or None if it has no amount.

    Accepts an optional ``/cash`` or ``/add`` prefix, an optional date
    (``yesterday`` / ``today`` / ``YYYY-MM-DD``), and an amount anywhere in the
    text. A ``+`` before the amount, or a leading credit word (atm, received,
    ...), marks the entry as cash-in.
    """
    text = text.strip()
    text = re.sub(r"^/(?:cash|add)\b", "", text, flags=re.IGNORECASE).strip()
    if not text:
        return None

    first_word = text.split()[0].lower() if text.split() else ""

    entry_date = datetime.date.today()
    m = _ISO_DATE_RE.search(text)
    if m:
        try:
            entry_date = datetime.date.fromisoformat(m.group(1))
            text = text.replace(m.group(1), " ")
        except ValueError:
            pass
    elif re.search(r"(?i)\byesterday\b", text):
        entry_date = datetime.date.today() - datetime.timedelta(days=1)
        text = re.sub(r"(?i)\byesterday\b", " ", text)
    else:
        text = re.sub(r"(?i)\btoday\b", " ", text)

    am = _AMOUNT_RE.search(text)
    if not am:
        return None  # no amount -> not a cash entry, ignore silently
    sign, digits = am.group(1), am.group(2).replace(",", "")
    try:
        amount = abs(float(digits))
    except ValueError:
        return None
    if amount == 0:
        return None
    # Remove the matched amount token (once) from the text -> description.
    text = text[: am.start()] + " " + text[am.end() :]

    is_credit = sign == "+" or first_word in CREDIT_FIRST_WORDS
    description = re.sub(r"\s+", " ", text).strip(" -").strip()
    if not description:
        description = "Cash"
    return ParsedEntry(description, amount, is_credit, entry_date)


class TelegramCashBot:
    """Polls Telegram, records cash entries to the Cash Transactions sheet."""

    def __init__(
        self,
        data_source: Any,
        settings: dict[str, Any],
        token: str,
        state_path: str,
    ) -> None:
        self.ds = data_source
        self.sheets = data_source.sheets_service
        self.token = token
        self.state_path = state_path
        self.allowed_chat_id = settings.get("allowed_chat_id")
        # {telegram_user_id (int): "Label"} -- who may log, and their name.
        self.allowed_users = {
            int(k): v for k, v in settings.get("allowed_users", {}).items()
        }
        self.state = self._load_state()

    # --- Telegram transport --------------------------------------------------
    def _api(self, method: str) -> str:
        return API_URL.format(token=self.token, method=method)

    def get_updates(self) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "timeout": 0,
            "allowed_updates": json.dumps(["message"]),
        }
        if self.state.get("offset"):
            params["offset"] = self.state["offset"]
        resp = requests.get(
            self._api("getUpdates"), params=params, timeout=HTTP_TIMEOUT
        )
        resp.raise_for_status()
        return resp.json().get("result", [])

    def send_message(self, chat_id: int, text: str) -> None:
        try:
            requests.post(
                self._api("sendMessage"),
                json={"chat_id": chat_id, "text": text},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            logger.warning(f"Failed to send Telegram reply: {e}")

    # --- State ---------------------------------------------------------------
    def _load_state(self) -> dict[str, Any]:
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                logger.warning("Bad telegram state file; starting fresh.")
        return {}

    def _save_state(self) -> None:
        os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)

    # --- Authorization -------------------------------------------------------
    def _authorized(self, message: dict[str, Any]) -> bool:
        chat_id = message.get("chat", {}).get("id")
        user_id = message.get("from", {}).get("id")
        if self.allowed_chat_id in (None, 0, ""):
            # Setup mode: help the operator discover the chat id, process nothing.
            logger.info(
                f"[setup] saw message in chat_id={chat_id} from user_id={user_id} "
                f"({message.get('from', {}).get('first_name')}). Set allowed_chat_id "
                "in settings to enable."
            )
            return False
        return chat_id == self.allowed_chat_id and user_id in self.allowed_users

    # --- Categorization ------------------------------------------------------
    def _build_categorizer(self) -> Categorizer:
        """Categorizer with an index learned from the Cash tab history."""
        rows = self.ds.get_sheet_data(TRANSACTIONS_SHEET_ID, CASH_SHEET_NAME, "B3:G")
        history: list[dict[Hashable, Any]] = []
        for r in rows:
            if len(r) < 5:
                continue
            desc, debit, credit, cat = r[1], r[2], r[3], r[4]
            if not cat or not str(desc).strip():
                continue
            amount = _to_float(credit) - _to_float(debit)
            history.append(
                {"description": desc, "amount": amount, "category": str(cat)}
            )
        cat = Categorizer()
        cat.build_index(history, enable_llm=False)
        return cat

    # --- Cash ledger writes --------------------------------------------------
    def append_cash_row(self, entry: ParsedEntry, category: str, remarks: str) -> int:
        debit = "" if entry.is_credit else round(entry.amount, 2)
        credit = round(entry.amount, 2) if entry.is_credit else ""
        values = [
            [
                entry.entry_date.isoformat(),
                entry.description,
                debit,
                credit,
                category,
                remarks,
            ]
        ]
        resp = (
            self.sheets.spreadsheets()
            .values()
            .append(
                spreadsheetId=TRANSACTIONS_SHEET_ID,
                range=f"'{CASH_SHEET_NAME}'!B:G",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": values},
            )
            .execute()
        )
        updated = resp.get("updates", {}).get("updatedRange", "")
        m = re.search(r"![A-Z]+(\d+)", updated)
        return int(m.group(1)) if m else -1

    def _cash_gid(self) -> Optional[int]:
        meta = (
            self.sheets.spreadsheets()
            .get(spreadsheetId=TRANSACTIONS_SHEET_ID)
            .execute()
        )
        for s in meta.get("sheets", []):
            if s["properties"]["title"] == CASH_SHEET_NAME:
                return int(s["properties"]["sheetId"])
        return None

    def delete_row(self, row: int) -> bool:
        gid = self._cash_gid()
        if gid is None or row < 1:
            return False
        self.sheets.spreadsheets().batchUpdate(
            spreadsheetId=TRANSACTIONS_SHEET_ID,
            body={
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": gid,
                                "dimension": "ROWS",
                                "startIndex": row - 1,
                                "endIndex": row,
                            }
                        }
                    }
                ]
            },
        ).execute()
        return True

    # --- Message handling ----------------------------------------------------
    def _handle_undo(self, chat_id: int) -> None:
        last = self.state.get("last_appended")
        if not last:
            self.send_message(chat_id, "Nothing to undo.")
            return
        if self.delete_row(int(last["row"])):
            self.send_message(chat_id, f"✗ Removed: {last['summary']}")
            self.state["last_appended"] = None
        else:
            self.send_message(chat_id, "Could not undo (row not found).")

    def _handle_entry(
        self, chat_id: int, user_id: int, text: str, categorizer: Categorizer
    ) -> None:
        entry = parse_entry(text)
        if entry is None:
            return  # not a cash entry (chatter) -> stay silent
        label = self.allowed_users.get(user_id, "")
        signed = entry.amount if entry.is_credit else -entry.amount
        txn: dict[Hashable, Any] = {
            "description": entry.description,
            "amount": signed,
            "account": "cash",
            "category": DEFAULT_CATEGORY,
        }
        categorizer.categorize([txn])
        category = str(txn.get("category") or DEFAULT_CATEGORY)

        row = self.append_cash_row(entry, category, remarks=label)
        flow = "+" if entry.is_credit else ""
        summary = f"{entry.description} {flow}₹{entry.amount:.0f}"
        self.state["last_appended"] = {
            "row": row,
            "summary": summary,
            "chat_id": chat_id,
        }
        by = f" · {label}" if label else ""
        self.send_message(
            chat_id, f"✓ {summary} → {category} (cash){by}. /undo to remove."
        )

    def process_message(
        self, message: dict[str, Any], categorizer: Categorizer
    ) -> None:
        text = str(message.get("text", "")).strip()
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]
        low = text.lower().split("@")[0]  # strip @botname from group commands
        if low in ("/undo",):
            self._handle_undo(chat_id)
        elif low in ("/start", "/help"):
            self.send_message(
                chat_id,
                "Log cash: e.g. `Coffee 250` (spend) or `+2000 ATM` (cash in). "
                "Add `yesterday` or `YYYY-MM-DD` for a past date. /undo removes "
                "the last entry.",
            )
        elif text.startswith("/"):
            return  # unknown command -> ignore
        else:
            self._handle_entry(chat_id, user_id, text, categorizer)

    # --- Orchestration -------------------------------------------------------
    def run_once(self) -> int:
        """Poll once, process authorized messages, persist state. Returns count
        of entries handled."""
        updates = self.get_updates()
        if not updates:
            return 0
        # Advance the offset past every fetched update so nothing is reprocessed,
        # even messages we ignore.
        self.state["offset"] = max(u["update_id"] for u in updates) + 1

        authorized = [
            u["message"]
            for u in updates
            if "message" in u and self._authorized(u["message"])
        ]
        handled = 0
        if authorized:
            categorizer = self._build_categorizer()
            for message in authorized:
                try:
                    self.process_message(message, categorizer)
                    handled += 1
                except Exception as e:  # never let one bad message wedge the loop
                    logger.error(f"Error processing message: {e}", exc_info=True)
        self._save_state()
        return handled


def _to_float(v: Any) -> float:
    try:
        return float(str(v).replace(",", "")) if str(v).strip() else 0.0
    except (TypeError, ValueError):
        return 0.0
