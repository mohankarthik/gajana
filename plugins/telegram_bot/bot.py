"""Chat-bridge cash-entry bot for gajana.

Cron-polled (no webhook, no exposed port): each run registers the ``finance``
channel over the host unified chat bridge Unix socket (``src.chat_client``),
pulls pre-authorized events since ``last_seq``, parses cash entries
("Coffee 250" or "+2000 ATM"), categorizes them from the Cash Transactions
history, appends a row to that tab, replies via the chat bridge, and
acknowledges processed events. ``/undo`` removes the most recent entry and
``/addon`` queries add-on card spend and settlement.

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

from src.categorizer import Categorizer
from src.chat_client import (
    ChatGatewayUnavailableError,
    ChatSocketClient,
    ChatSocketError,
)
from src.constants import ADDON_CARDS, DEFAULT_CATEGORY, TRANSACTIONS_SHEET_ID

logger = logging.getLogger(__name__)

CASH_SHEET_NAME = "Cash Transactions"
CHANNEL = "finance"
MAX_EVENT_RETRIES = 3

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
    """Polls the host chat bridge ``finance`` channel and records cash entries."""

    def __init__(
        self,
        data_source: Any,
        settings: Optional[dict[str, Any]],
        state_path: str,
        chat_client: Optional[ChatSocketClient] = None,
        addon_cards: Optional[dict[str, Any]] = None,
        channel: str = CHANNEL,
    ) -> None:
        cfg = settings or {}
        self.ds = data_source
        self.sheets = getattr(data_source, "sheets_service", None)
        self.state_path = state_path
        self.channel = channel
        self.enabled: bool = cfg.get("enabled", True) is not False
        self.transport: str = (
            str(cfg.get("transport") or "telegram").strip() or "telegram"
        )
        self.chat_client: ChatSocketClient = (
            chat_client if chat_client is not None else ChatSocketClient()
        )
        # Optional {sender_id_str: "Label"} for populating sheet Remarks;
        # channel authorization itself is enforced on the host by chat-bridge.
        raw_users = cfg.get("allowed_users") or cfg.get("user_labels") or {}
        self.allowed_users: dict[str, str] = {
            str(k): str(v) for k, v in raw_users.items()
        }
        self.addon_cards = (
            addon_cards
            if addon_cards is not None
            else ADDON_CARDS or cfg.get("addon_cards") or {}
        )
        self.state = self._load_state()
        self._last_backend_error: Optional[str] = None
        self._suppress_error_replies: bool = False

    def _record_backend_error(self, user_message: str) -> None:
        """Record a transient backend error and notify immediately unless suppressed."""
        self._last_backend_error = user_message
        if not self._suppress_error_replies:
            self.send_message(user_message)

    # --- Chat bridge transport -----------------------------------------------
    def send_message(self, text: str) -> bool:
        """Send a reply via chat-bridge without raising on transport failures."""
        try:
            self.chat_client.send_message(self.channel, text)
            return True
        except (ChatSocketError, OSError) as e:
            logger.warning(f"Failed to send chat bridge reply: {e}")
            return False

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
        if not self.sheets:
            raise RuntimeError("Google Sheets service is not configured on data_source")
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
        if not self.sheets:
            return None
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
        if not self.sheets:
            return False
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
    def _handle_undo(self) -> None:
        last = self.state.get("last_appended")
        if not last:
            self.send_message("Nothing to undo.")
            return
        try:
            deleted = self.delete_row(int(last["row"]))
        except Exception as e:
            logger.error(f"Failed to delete cash row for /undo: {e}")
            self._record_backend_error(
                "Failed to undo entry in Google Sheets. Please try again shortly."
            )
            return
        if deleted:
            self.send_message(f"✗ Removed: {last['summary']}")
            self.state["last_appended"] = None
        else:
            self.send_message("Could not undo (row not found).")

    def _handle_addon(self, arg: str = "") -> None:
        try:
            from src.addon_tracker import (
                compute_addon_report,
                format_all_addons_overview,
                format_non_reimbursable_user_message,
                format_reimbursable_user_message,
                format_unconfigured_user_message,
            )
            from src.transaction_processor import TransactionProcessor

            processor = TransactionProcessor(self.ds, addon_cards=self.addon_cards)
            cc_txns = processor.get_old_transactions("cc")
            bank_txns = processor.get_old_transactions("bank")
            report = compute_addon_report(cc_txns, bank_txns, self.addon_cards)

            arg_clean = arg.strip().lower()

            if arg_clean == "all":
                self.send_message(format_all_addons_overview(report))
                return

            reimbursable_users = [u for u in report.users if u.user.reimburse]

            if not arg_clean:
                if not reimbursable_users:
                    self.send_message(format_all_addons_overview(report))
                    return
                for u_rep in reimbursable_users:
                    self.send_message(format_reimbursable_user_message(u_rep))
                return

            # Specific user requested
            for u_rep in report.users:
                u = u_rep.user
                candidates = [
                    u.id.lower(),
                    u.label.lower(),
                    u.greeting_name.lower(),
                ] + [p.lower() for p in u.cardholder_patterns]
                if arg_clean in candidates:
                    if u.reimburse:
                        self.send_message(format_reimbursable_user_message(u_rep))
                    else:
                        self.send_message(format_non_reimbursable_user_message(u_rep))
                    return

            for unconf in report.unconfigured_users:
                if arg_clean == unconf.label.lower():
                    self.send_message(format_unconfigured_user_message(unconf))
                    return

            available = [u.user.label for u in report.users] + [
                u.label for u in report.unconfigured_users
            ]
            names_str = ", ".join(available) if available else "none"
            self.send_message(
                f"Unknown add-on user '{arg.strip()}'. Available add-on users: {names_str}. "
                "Use `/addon all` for full overview."
            )
        except Exception as e:
            logger.warning(f"Failed to handle /addon: {e}")
            self._record_backend_error(
                "Could not fetch add-on spend right now; please try again shortly."
            )

    def _handle_entry(
        self,
        text: str,
        categorizer: Categorizer,
        sender_label: str = "",
    ) -> bool:
        """Record a cash entry. Returns True if a row was written."""
        entry = parse_entry(text)
        if entry is None:
            return False  # not a cash entry (chatter) -> stay silent
        signed = entry.amount if entry.is_credit else -entry.amount
        txn: dict[Hashable, Any] = {
            "description": entry.description,
            "amount": signed,
            "account": "cash",
            "category": DEFAULT_CATEGORY,
        }
        categorizer.categorize([txn])
        category = str(txn.get("category") or DEFAULT_CATEGORY)

        try:
            row = self.append_cash_row(entry, category, remarks=sender_label)
        except Exception as e:
            logger.error(f"Failed to append cash row: {e}")
            self._record_backend_error(
                "Failed to record entry in Google Sheets. Please check bot logs or configuration."
            )
            return False

        flow = "+" if entry.is_credit else ""
        summary = f"{entry.description} {flow}₹{entry.amount:.0f}"
        self.state["last_appended"] = {
            "row": row,
            "summary": summary,
        }
        by = f" · {sender_label}" if sender_label else ""
        self.send_message(f"✓ {summary} → {category} (cash){by}. /undo to remove.")
        return True

    def _resolve_sender_label(
        self, event: dict[str, Any], payload: dict[str, Any]
    ) -> str:
        sender_id = event.get("sender_id")
        if sender_id is None:
            sender_id = payload.get("sender_id")
        if sender_id is None and isinstance(event.get("from"), dict):
            sender_id = event["from"].get("id")
        if sender_id is not None:
            mapped = self.allowed_users.get(str(sender_id), "")
            if mapped:
                return mapped
        return str(
            event.get("sender_label")
            or payload.get("sender_label")
            or event.get("sender_name")
            or payload.get("sender_name")
            or ""
        ).strip()

    def process_message(
        self, event: dict[str, Any], categorizer: Optional[Categorizer] = None
    ) -> bool:
        """Handle one chat bridge message event. Returns True if a cash entry was recorded."""
        self._last_backend_error = None
        raw_payload = event.get("payload")
        payload: dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else {}
        text = str(event.get("text") or payload.get("text") or "").strip()
        if not text:
            return False
        sender_label = self._resolve_sender_label(event, payload)
        parts = text.split(None, 1)
        first_token = parts[0] if parts else ""
        cmd = first_token.split("@")[0].lower()
        args = parts[1].strip() if len(parts) > 1 else ""

        if cmd in ("/undo",):
            self._handle_undo()
        elif cmd in ("/addon",):
            self._handle_addon(args)
        elif cmd in ("/start", "/help"):
            self.send_message(
                "Log cash: e.g. `Coffee 250` (spend) or `+2000 ATM` (cash in). "
                "Add `yesterday` or `YYYY-MM-DD` for a past date. /undo removes "
                "the last entry. /addon shows add-on card spend and settlement; "
                "/addon all for full overview."
            )
        elif text.startswith("/"):
            return False  # unknown command -> ignore
        else:
            if categorizer is None:
                categorizer = self._build_categorizer()
            return self._handle_entry(text, categorizer, sender_label=sender_label)
        return False

    def _should_retry_event(self, seq: int, backend_error: str) -> bool:
        """Persist bounded retry state for a failed event; return True to defer ack."""
        prev_seq = self.state.get("retry_seq")
        try:
            prev_count = (
                int(self.state.get("retry_count") or 0) if prev_seq == seq else 0
            )
        except (TypeError, ValueError):
            prev_count = 0
        retry_count = prev_count + 1
        if retry_count < MAX_EVENT_RETRIES:
            self.state["retry_seq"] = seq
            self.state["retry_count"] = retry_count
            self._save_state()
            logger.warning(
                f"Backend error on event seq={seq} "
                f"(attempt {retry_count}/{MAX_EVENT_RETRIES}); "
                "leaving unacked for next poll."
            )
            return True
        logger.error(
            f"Giving up on event seq={seq} after {retry_count} attempts: "
            f"{backend_error}"
        )
        self.send_message(backend_error)
        return False

    # --- Orchestration -------------------------------------------------------
    def run_once(self) -> int:
        """Poll the host chat bridge once, process message events, and persist state.

        Returns the number of cash entries actually written (not events seen).
        """
        if not self.enabled:
            logger.info("Finance chat bot disabled in settings; skipping.")
            return 0

        try:
            self.chat_client.register_channel(self.channel, transport=self.transport)
            last_seq = int(self.state.get("last_seq", 0))
            events = self.chat_client.pull_events(
                self.channel, after_seq=last_seq, wait_s=0
            )
        except (ChatGatewayUnavailableError, OSError) as e:
            logger.warning(f"Chat bridge unavailable during {self.channel} poll: {e}")
            return 0

        if not events:
            return 0

        categorizer: Optional[Categorizer] = None
        recorded = 0
        for event in events:
            seq = int(event["seq"])
            backend_error: Optional[str] = None
            if event.get("event_type") == "message":
                self._last_backend_error = None
                self._suppress_error_replies = True
                try:
                    raw_payload = event.get("payload")
                    payload: dict[str, Any] = (
                        raw_payload if isinstance(raw_payload, dict) else {}
                    )
                    text = str(event.get("text") or payload.get("text") or "").strip()
                    if text and not text.startswith("/") and categorizer is None:
                        categorizer = self._build_categorizer()
                    if self.process_message(event, categorizer):
                        recorded += 1
                    backend_error = self._last_backend_error
                except Exception as e:
                    logger.error(
                        f"Error processing message seq={seq}: {e}", exc_info=True
                    )
                    backend_error = (
                        self._last_backend_error
                        or "Failed to record entry in Google Sheets. Please check bot logs or configuration."
                    )
                finally:
                    self._suppress_error_replies = False

            if backend_error is not None and self._should_retry_event(
                seq, backend_error
            ):
                break

            self.state.pop("retry_seq", None)
            self.state.pop("retry_count", None)
            self.state["last_seq"] = seq
            self._save_state()
            try:
                self.chat_client.ack_event(self.channel, seq)
            except (ChatSocketError, OSError) as e:
                logger.warning(f"Failed to ack chat bridge event seq={seq}: {e}")
        return recorded


def _to_float(v: Any) -> float:
    try:
        return float(str(v).replace(",", "")) if str(v).strip() else 0.0
    except (TypeError, ValueError):
        return 0.0
