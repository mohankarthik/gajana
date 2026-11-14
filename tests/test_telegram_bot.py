from __future__ import annotations

import datetime
import json
import socket
import threading
from pathlib import Path
from typing import Any

import pytest

import run_telegram_bot
from plugins.telegram_bot.bot import MAX_EVENT_RETRIES, TelegramCashBot, parse_entry
from src.chat_client import (
    ChatGatewayUnavailableError,
    ChatSocketClient,
    ChatSocketError,
)

TODAY = datetime.date.today()

SETTINGS = {
    "enabled": True,
    "transport": "telegram",
    "allowed_users": {"111": "Karti", "222": "Mini"},
}

# --- parse_entry ---------------------------------------------------------------


def test_parse_spend():
    e = parse_entry("Coffee 250")
    assert e is not None
    assert e.description == "Coffee"
    assert e.amount == 250.0
    assert e.is_credit is False
    assert e.entry_date == TODAY


def test_parse_credit_plus_prefix():
    e = parse_entry("+2000 ATM")
    assert e is not None
    assert e.is_credit is True
    assert e.amount == 2000.0
    assert "ATM" in e.description


def test_parse_credit_first_word():
    e = parse_entry("received 5000 from dad")
    assert e is not None
    assert e.is_credit is True
    assert e.amount == 5000.0


def test_parse_yesterday():
    e = parse_entry("Groceries 640 yesterday")
    assert e is not None
    assert e.entry_date == TODAY - datetime.timedelta(days=1)
    assert e.description == "Groceries"


def test_parse_explicit_date():
    e = parse_entry("Rent 12000 2026-07-01")
    assert e is not None
    assert e.entry_date == datetime.date(2026, 7, 1)


def test_parse_commas_and_decimals():
    e = parse_entry("Fridge 1,250.50")
    assert e is not None
    assert e.amount == 1250.50


def test_parse_cash_prefix_ignored():
    e = parse_entry("/cash Auto 80")
    assert e is not None
    assert e.description == "Auto" and e.amount == 80.0


def test_parse_no_amount_returns_none():
    assert parse_entry("running 5 min late lol") is not None  # has a number
    assert parse_entry("let's have dinner tonight") is None
    assert parse_entry("") is None


def test_parse_zero_is_not_entry():
    assert parse_entry("free sample 0") is None


def test_parse_missing_description_defaults():
    e = parse_entry("500")
    assert e is not None
    assert e.description == "Cash" and e.amount == 500.0


# --- fakes ---------------------------------------------------------------------


class _Exec:
    def __init__(self, result):
        self._r = result

    def execute(self):
        return self._r


class FakeSheets:
    def __init__(self):
        self.appended = []
        self.deleted = []
        self.next_row = 2320

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def append(
        self,
        spreadsheetId=None,
        range=None,
        valueInputOption=None,
        insertDataOption=None,
        body=None,
    ):
        self.appended.append(body["values"][0])
        rng = f"'Cash Transactions'!B{self.next_row}:G{self.next_row}"
        self.next_row += 1
        return _Exec({"updates": {"updatedRange": rng}})

    def get(self, spreadsheetId=None):
        return _Exec(
            {
                "sheets": [
                    {"properties": {"title": "Cash Transactions", "sheetId": 208405317}}
                ]
            }
        )

    def batchUpdate(self, spreadsheetId=None, body=None):
        self.deleted.append(body["requests"][0]["deleteDimension"]["range"])
        return _Exec({})


class FakeDataSource:
    def __init__(self, cash_history, cc_history=None, bank_history=None):
        self.sheets_service = FakeSheets()
        self._history = cash_history
        self._cc_history = cc_history or []
        self._bank_history = bank_history or []

    def get_sheet_data(self, sheet_id, sheet_name, rng):
        return self._history

    def get_transaction_log_data(self, account_type):
        if account_type == "cc":
            return self._cc_history
        if account_type == "bank":
            return self._bank_history
        return []


class FakeChatClient:
    def __init__(self):
        self.registered: list[tuple[str, str | None]] = []
        self.pulls: list[tuple[str, int, float]] = []
        self.sent: list[dict[str, Any]] = []
        self.acked: list[tuple[str, int]] = []
        self.events: list[dict[str, Any]] = []
        self.register_error: Exception | None = None
        self.pull_error: Exception | None = None
        self.send_error: Exception | None = None
        self.ack_error: Exception | None = None

    def register_channel(self, channel: str, transport: str | None = None) -> dict:
        if self.register_error is not None:
            raise self.register_error
        self.registered.append((channel, transport))
        return {"ok": True, "channel": channel, "transport": transport}

    def pull_events(
        self,
        channel: str,
        after_seq: int = 0,
        wait_s: float = 0.0,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if self.pull_error is not None:
            raise self.pull_error
        self.pulls.append((channel, after_seq, wait_s))
        return [e for e in self.events if int(e["seq"]) > after_seq][:limit]

    def send_message(
        self, channel: str, text: str, reply_to: str | int | None = None
    ) -> dict:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append({"channel": channel, "text": text, "reply_to": reply_to})
        return {"ok": True}

    def ack_event(self, channel: str, seq: int) -> dict:
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.append((channel, seq))
        return {"ok": True}


class _LocalUnixChatServer:
    def __init__(self, sock_path: Path, handler):
        self.sock_path = sock_path
        self.handler = handler
        self.requests: list[dict[str, Any]] = []
        self._srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._srv.bind(str(sock_path))
        self._srv.listen(8)
        self._stop = False
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop:
            try:
                conn, _ = self._srv.accept()
            except OSError:
                break
            with conn:
                data = b""
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                    if b"\n" in data:
                        break
                line = data.splitlines()[0].decode("utf-8") if data else "{}"
                req = json.loads(line)
                self.requests.append(req)
                resp = self.handler(req)
                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))

    def close(self) -> None:
        self._stop = True
        try:
            self._srv.close()
        except OSError:
            pass
        self._thread.join(timeout=2.0)


CASH_HISTORY = [
    # B:G rows: Date, Description, Debit, Credit, Category, Remarks
    ["2026-06-01", "Coffee", "200.00", "", "Expense:Dining", ""],
    ["2026-06-02", "Auto", "80.00", "", "Expense:Local Travel", ""],
    ["2026-06-03", "ATM", "", "3000.00", "Transfer:Cash", ""],
]

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


def make_bot(
    tmp_path,
    cash_history=CASH_HISTORY,
    cc_history=None,
    bank_history=None,
    addon_cards=SYNTHETIC_ADDON_CONFIG,
    settings=None,
    chat_client=None,
):
    ds = FakeDataSource(cash_history, cc_history=cc_history, bank_history=bank_history)
    state = str(tmp_path / "state.json")
    client = chat_client if chat_client is not None else FakeChatClient()
    bot = TelegramCashBot(
        ds,
        SETTINGS if settings is None else settings,
        state,
        chat_client=client,
        addon_cards=addon_cards,
    )
    return bot, ds, client


# --- entry handling ------------------------------------------------------------


def test_entry_appends_row_and_replies(tmp_path):
    bot, ds, client = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "Coffee 250"}, cat
    )
    row = ds.sheets_service.appended[-1]
    assert row[1] == "Coffee"  # description
    assert row[2] == 250.0  # debit
    assert row[3] == ""  # credit blank
    assert row[4] == "Expense:Dining"  # categorized from cash history
    assert row[5] == "Karti"  # remarks = sender label
    assert client.sent[-1]["channel"] == "finance"
    assert "Coffee" in client.sent[-1]["text"] and "✓" in client.sent[-1]["text"]


def test_entry_reads_nested_payload_and_matrix_sender(tmp_path):
    bot, ds, client = make_bot(
        tmp_path,
        settings={
            "transport": "matrix",
            "allowed_users": {"@mini:matrix.example": "Mini"},
        },
    )
    cat = bot._build_categorizer()
    recorded = bot.process_message(
        {
            "event_type": "message",
            "payload": {"text": "Auto 80", "sender_id": "@mini:matrix.example"},
        },
        cat,
    )
    assert recorded is True
    row = ds.sheets_service.appended[-1]
    assert row[1] == "Auto"
    assert row[5] == "Mini"
    assert "Mini" in client.sent[-1]["text"]


def test_entry_without_allowed_users_mapping_leaves_remarks_blank(tmp_path):
    bot, ds, client = make_bot(tmp_path, settings={})
    cat = bot._build_categorizer()
    recorded = bot.process_message(
        {"event_type": "message", "sender_id": "999", "text": "Coffee 120"}, cat
    )
    assert recorded is True
    row = ds.sheets_service.appended[-1]
    assert row[5] == ""
    assert "✓ Coffee ₹120" in client.sent[-1]["text"]


def test_credit_entry_writes_credit_column(tmp_path):
    bot, ds, _ = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"event_type": "message", "sender_id": "222", "text": "+2000 ATM"}, cat
    )
    row = ds.sheets_service.appended[-1]
    assert row[2] == "" and row[3] == 2000.0  # debit blank, credit set
    assert row[5] == "Mini"


def test_chatter_without_amount_is_silent(tmp_path):
    bot, ds, client = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "dinner tonight?"}, cat
    )
    assert ds.sheets_service.appended == []
    assert client.sent == []


def test_undo_deletes_last_row(tmp_path):
    bot, ds, client = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "Coffee 250"}, cat
    )
    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "/undo"}, cat
    )
    assert ds.sheets_service.deleted  # a deleteDimension was issued
    assert bot.state["last_appended"] is None
    assert "Removed" in client.sent[-1]["text"]


def test_undo_with_nothing_to_undo(tmp_path):
    bot, _, client = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "/undo"}, cat
    )
    assert "Nothing to undo" in client.sent[-1]["text"]


def test_bot_mention_suffix_stripped_on_commands(tmp_path):
    bot, _, client = make_bot(tmp_path)
    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "/help@gajana_bot"}
    )
    assert "/addon" in client.sent[-1]["text"]


# --- run_once & state persistence ----------------------------------------------


def test_run_once_registers_pulls_processes_saves_last_seq_and_acks(tmp_path):
    bot, ds, client = make_bot(tmp_path)
    client.events = [
        {
            "seq": 10,
            "channel": "finance",
            "transport": "telegram",
            "event_type": "message",
            "sender_id": "111",
            "text": "Coffee 250",
        },
        {
            "seq": 11,
            "channel": "finance",
            "transport": "telegram",
            "event_type": "attachment",
            "sender_id": "111",
            "text": "100",
        },
        {
            "seq": 12,
            "channel": "finance",
            "transport": "telegram",
            "event_type": "message",
            "payload": {"text": "Auto 80", "sender_id": "222"},
        },
    ]
    handled = bot.run_once()
    assert handled == 2
    assert client.registered == [("finance", "telegram")]
    assert client.pulls == [("finance", 0, 0)]
    assert client.acked == [("finance", 10), ("finance", 11), ("finance", 12)]
    assert bot.state["last_seq"] == 12
    assert bot.state["last_appended"]["summary"] == "Auto ₹80"
    assert len(ds.sheets_service.appended) == 2

    # Next run resumes from last_seq=12 and preserves last_appended for /undo
    client.events.append(
        {
            "seq": 13,
            "channel": "finance",
            "transport": "telegram",
            "event_type": "message",
            "sender_id": "111",
            "text": "/undo",
        }
    )
    bot2 = TelegramCashBot(
        ds,
        SETTINGS,
        str(tmp_path / "state.json"),
        chat_client=client,
        addon_cards=SYNTHETIC_ADDON_CONFIG,
    )
    handled2 = bot2.run_once()
    assert handled2 == 0
    assert client.pulls[-1] == ("finance", 12, 0)
    assert client.acked[-1] == ("finance", 13)
    assert ds.sheets_service.deleted
    assert bot2.state["last_seq"] == 13
    assert bot2.state["last_appended"] is None


def test_run_once_disabled_in_settings_skips_bridge(tmp_path):
    bot, _, client = make_bot(tmp_path, settings={"enabled": False})
    client.events = [
        {"seq": 1, "event_type": "message", "text": "Coffee 250", "sender_id": "111"}
    ]
    assert bot.run_once() == 0
    assert client.registered == []
    assert client.pulls == []


def test_run_once_gateway_unavailable_exits_cleanly_without_corrupting_state(
    tmp_path, caplog
):
    state_file = tmp_path / "state.json"
    initial_state = {"last_seq": 42, "last_appended": {"row": 10, "summary": "Tea ₹20"}}
    state_file.write_text(json.dumps(initial_state), encoding="utf-8")

    bot, _, client = make_bot(tmp_path)
    client.register_error = ChatGatewayUnavailableError("bridge restarting")
    with caplog.at_level("WARNING"):
        assert bot.run_once() == 0
    assert "Chat bridge unavailable" in caplog.text
    assert json.loads(state_file.read_text(encoding="utf-8")) == initial_state

    client.register_error = None
    client.pull_error = ChatGatewayUnavailableError("socket down")
    with caplog.at_level("WARNING"):
        assert bot.run_once() == 0
    assert json.loads(state_file.read_text(encoding="utf-8")) == initial_state


def test_send_message_catches_chat_socket_error_and_oserror(tmp_path, caplog):
    bot, _, client = make_bot(tmp_path)
    client.send_error = ChatSocketError("send failed")
    with caplog.at_level("WARNING"):
        assert bot.send_message("hi") is False
    assert "Failed to send chat bridge reply" in caplog.text

    client.send_error = OSError("broken pipe")
    assert bot.send_message("hi") is False


def test_run_once_ack_error_persists_last_seq_and_continues(tmp_path, caplog):
    bot, ds, client = make_bot(tmp_path)
    client.events = [
        {"seq": 5, "event_type": "message", "sender_id": "111", "text": "Coffee 100"},
    ]
    client.ack_error = ChatSocketError("transient ack error")
    with caplog.at_level("WARNING"):
        assert bot.run_once() == 1
    assert "Failed to ack chat bridge event seq=5" in caplog.text
    assert len(ds.sheets_service.appended) == 1
    saved = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert saved["last_seq"] == 5


def test_run_once_transient_build_categorizer_failure_retries_and_recovers(tmp_path):
    bot, ds, client = make_bot(tmp_path)
    state_file = tmp_path / "state.json"
    client.events = [
        {"seq": 20, "event_type": "message", "sender_id": "111", "text": "Coffee 250"},
    ]

    orig_get_sheet_data = ds.get_sheet_data
    calls = {"n": 0}

    def _flaky_sheet_data(sheet_id, sheet_name, rng):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("Google Sheets 503 transient")
        return orig_get_sheet_data(sheet_id, sheet_name, rng)

    ds.get_sheet_data = _flaky_sheet_data  # type: ignore[method-assign]

    # Attempt 1: transient _build_categorizer failure leaves last_seq & acked untouched
    assert bot.run_once() == 0
    assert client.acked == []
    assert client.sent == []
    assert ds.sheets_service.appended == []
    assert "last_seq" not in bot.state
    saved1 = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved1 == {"retry_seq": 20, "retry_count": 1}

    # Attempt 2 (new bot instance loading persisted state): succeeds, clears retry, acks
    bot2 = TelegramCashBot(
        ds,
        SETTINGS,
        str(state_file),
        chat_client=client,
        addon_cards=SYNTHETIC_ADDON_CONFIG,
    )
    assert bot2.run_once() == 1
    assert len(ds.sheets_service.appended) == 1
    assert ds.sheets_service.appended[0][1] == "Coffee"
    assert client.acked == [("finance", 20)]
    assert len(client.sent) == 1 and "Coffee ₹250" in client.sent[0]["text"]
    saved2 = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved2["last_seq"] == 20
    assert "retry_seq" not in saved2
    assert "retry_count" not in saved2


def test_run_once_transient_append_cash_row_failure_retries_and_recovers(tmp_path):
    bot, ds, client = make_bot(tmp_path)
    state_file = tmp_path / "state.json"
    client.events = [
        {"seq": 30, "event_type": "message", "sender_id": "111", "text": "Tea 40"},
        {"seq": 31, "event_type": "message", "sender_id": "222", "text": "Auto 80"},
    ]

    orig_append = bot.append_cash_row
    attempts = {"n": 0}

    def _flaky_append(entry, category, remarks=""):
        attempts["n"] += 1
        if attempts["n"] == 2:  # fail on second event (seq=31) first attempt
            raise RuntimeError("Sheets timeout")
        return orig_append(entry, category, remarks)

    bot.append_cash_row = _flaky_append  # type: ignore[method-assign]

    # Run 1: seq=30 succeeds & acks; seq=31 fails -> breaks before advancing to 31
    assert bot.run_once() == 1
    assert len(ds.sheets_service.appended) == 1
    assert ds.sheets_service.appended[0][1] == "Tea"
    assert client.acked == [("finance", 30)]
    assert len(client.sent) == 1 and "Tea ₹40" in client.sent[0]["text"]
    saved1 = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved1["last_seq"] == 30
    assert saved1["retry_seq"] == 31
    assert saved1["retry_count"] == 1

    # Run 2: seq=31 succeeds, records, clears retry state, advances last_seq & acks
    assert bot.run_once() == 1
    assert len(ds.sheets_service.appended) == 2
    assert ds.sheets_service.appended[1][1] == "Auto"
    assert client.acked == [("finance", 30), ("finance", 31)]
    assert len(client.sent) == 2 and "Auto ₹80" in client.sent[1]["text"]
    saved2 = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved2["last_seq"] == 31
    assert "retry_seq" not in saved2
    assert "retry_count" not in saved2


def test_run_once_capped_retries_advances_after_max_attempts(tmp_path, caplog):
    bot, ds, client = make_bot(tmp_path)
    state_file = tmp_path / "state.json"
    ds.sheets_service = None  # permanent failure on append_cash_row
    bot.sheets = None
    client.events = [
        {"seq": 40, "event_type": "message", "sender_id": "111", "text": "Coffee 100"},
        {"seq": 41, "event_type": "message", "sender_id": "111", "text": "dinner?"},
    ]

    assert MAX_EVENT_RETRIES == 3

    # Attempts 1 and 2: deferred without advancing last_seq, acking, or sending failure
    assert bot.run_once() == 0
    assert client.acked == []
    assert client.sent == []
    assert json.loads(state_file.read_text(encoding="utf-8")) == {
        "retry_seq": 40,
        "retry_count": 1,
    }

    assert bot.run_once() == 0
    assert client.acked == []
    assert client.sent == []
    assert json.loads(state_file.read_text(encoding="utf-8")) == {
        "retry_seq": 40,
        "retry_count": 2,
    }

    # Attempt 3: logs error, sends user failure once, clears retry state, advances & acks,
    # and continues processing seq=41 without wedging the queue
    with caplog.at_level("ERROR"):
        assert bot.run_once() == 0
    assert "Giving up on event seq=40 after 3 attempts" in caplog.text
    assert len(client.sent) == 1
    assert "Failed to record entry in Google Sheets" in client.sent[0]["text"]
    assert client.acked == [("finance", 40), ("finance", 41)]
    saved3 = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved3["last_seq"] == 41
    assert "retry_seq" not in saved3
    assert "retry_count" not in saved3


def test_run_once_transient_undo_and_addon_failures_retry_without_acking(tmp_path):
    bot, ds, client = make_bot(
        tmp_path, cc_history=SAMPLE_CC_LOG, bank_history=SAMPLE_BANK_LOG
    )
    bot.state["last_appended"] = {"row": 2320, "summary": "Coffee ₹250"}
    bot._save_state()

    client.events = [
        {"seq": 50, "event_type": "message", "sender_id": "111", "text": "/undo"},
        {"seq": 51, "event_type": "message", "sender_id": "111", "text": "/addon"},
    ]

    orig_delete = bot.delete_row
    delete_calls = {"n": 0}

    def _flaky_delete(row):
        delete_calls["n"] += 1
        if delete_calls["n"] == 1:
            raise RuntimeError("Sheets 503 on delete")
        return orig_delete(row)

    bot.delete_row = _flaky_delete  # type: ignore[method-assign]

    orig_txn_log = ds.get_transaction_log_data
    addon_calls = {"n": 0}

    def _flaky_addon(acc):
        addon_calls["n"] += 1
        if addon_calls["n"] == 1:
            raise RuntimeError("Sheets 503 on addon")
        return orig_txn_log(acc)

    ds.get_transaction_log_data = _flaky_addon  # type: ignore[method-assign]

    # Run 1: /undo fails -> stops before acking 50 or running 51
    assert bot.run_once() == 0
    assert client.acked == []
    assert client.sent == []
    assert bot.state["retry_seq"] == 50
    assert bot.state["retry_count"] == 1

    # Run 2: /undo succeeds & acks 50; /addon fails -> stops before acking 51
    assert bot.run_once() == 0
    assert client.acked == [("finance", 50)]
    assert len(client.sent) == 1 and "Removed: Coffee ₹250" in client.sent[0]["text"]
    assert bot.state["last_seq"] == 50
    assert bot.state["retry_seq"] == 51
    assert bot.state["retry_count"] == 1

    # Run 3: /addon succeeds, clears retry state, acks 51
    assert bot.run_once() == 0
    assert client.acked == [("finance", 50), ("finance", 51)]
    assert len(client.sent) == 2 and "Hi Amma" in client.sent[1]["text"]
    assert bot.state["last_seq"] == 51
    assert "retry_seq" not in bot.state
    assert "retry_count" not in bot.state


# --- addon commands ------------------------------------------------------------


SAMPLE_CC_LOG = [
    ["Date", "Description", "Debit", "Credit", "Category", "Remarks", "Account"],
    ["2026-08-05", "SUPERMARKET", "3500.00", "", "Transfer", "addon:Mom", "cc-hdfc"],
    [
        "2026-08-10",
        "BOUTIQUE",
        "4000.00",
        "",
        "Expense:Clothing",
        "addon:Partner",
        "cc-hdfc",
    ],
]

SAMPLE_BANK_LOG = [
    ["Date", "Description", "Debit", "Credit", "Category", "Remarks", "Account"],
    [
        "2026-08-08",
        "UPI/CR/123/PATRICIA@OKBANK",
        "",
        "1500.00",
        "Transfer",
        "addon-repay:Mom",
        "bank-axis",
    ],
]


def test_addon_command_reimbursable_forwardable_reply(tmp_path):
    bot, _, client = make_bot(
        tmp_path, cc_history=SAMPLE_CC_LOG, bank_history=SAMPLE_BANK_LOG
    )
    bot.process_message({"event_type": "message", "sender_id": "111", "text": "/addon"})
    assert len(client.sent) == 1
    reply = client.sent[-1]["text"]
    assert "Hi Amma, credit card spend summary:" in reply
    assert "Items total: ₹3,500" in reply
    assert "Already received via UPI: -₹1,500" in reply
    assert "Amount due: ₹2,000" in reply


def test_addon_all_command_overview_reply(tmp_path):
    bot, _, client = make_bot(
        tmp_path, cc_history=SAMPLE_CC_LOG, bank_history=SAMPLE_BANK_LOG
    )
    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "/addon all"}
    )
    assert len(client.sent) == 1
    reply = client.sent[-1]["text"]
    assert "Add-on Cards Overview:" in reply
    assert "Mom (reimbursable):" in reply
    assert "Net due: ₹2,000" in reply
    assert "Partner:" in reply
    assert "Total spend: ₹4,000" in reply


def test_addon_specific_user_reimbursable_and_non_reimbursable(tmp_path):
    bot, _, client = make_bot(
        tmp_path, cc_history=SAMPLE_CC_LOG, bank_history=SAMPLE_BANK_LOG
    )

    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "/addon mom"}
    )
    assert "Hi Amma, credit card spend summary:" in client.sent[-1]["text"]

    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "/addon partner"}
    )
    assert "Partner Spend Summary:" in client.sent[-1]["text"]
    assert "Total spend: ₹4,000" in client.sent[-1]["text"]


def test_addon_unknown_user_suggests_available(tmp_path):
    bot, _, client = make_bot(
        tmp_path, cc_history=SAMPLE_CC_LOG, bank_history=SAMPLE_BANK_LOG
    )
    bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "/addon nonexistent"}
    )
    reply = client.sent[-1]["text"]
    assert "Unknown add-on user 'nonexistent'" in reply
    assert "Available add-on users: Mom, Partner" in reply
    assert "/addon all" in reply


def test_help_mentions_addon(tmp_path):
    bot, _, client = make_bot(tmp_path)
    bot.process_message({"event_type": "message", "sender_id": "111", "text": "/help"})
    reply = client.sent[-1]["text"]
    assert "/addon" in reply
    assert "/addon all" in reply


def test_addon_command_handles_data_source_error_gracefully(tmp_path):
    bot, ds, client = make_bot(tmp_path)

    def _err(acc):
        raise RuntimeError("Google Sheets 503 Service Unavailable")

    ds.get_transaction_log_data = _err  # type: ignore[method-assign]

    bot.process_message({"event_type": "message", "sender_id": "111", "text": "/addon"})
    assert len(client.sent) == 1
    reply = client.sent[-1]["text"]
    assert "Could not fetch add-on spend right now; please try again shortly." in reply


def test_append_cash_row_missing_sheets_service_raises_and_handles_gracefully(
    tmp_path,
):
    ds = FakeDataSource(CASH_HISTORY)
    ds.sheets_service = None  # simulate missing sheets service
    client = FakeChatClient()
    state = str(tmp_path / "state.json")
    bot = TelegramCashBot(
        ds,
        SETTINGS,
        state,
        chat_client=client,
        addon_cards=SYNTHETIC_ADDON_CONFIG,
    )
    cat = bot._build_categorizer()

    entry = parse_entry("Coffee 100")
    assert entry is not None
    with pytest.raises(RuntimeError, match="Google Sheets service is not configured"):
        bot.append_cash_row(entry, "Expense:Dining", "Karti")

    res = bot.process_message(
        {"event_type": "message", "sender_id": "111", "text": "Coffee 100"}, cat
    )
    assert res is False
    assert bot.state.get("last_appended") is None
    assert len(client.sent) == 1
    assert "Failed to record entry in Google Sheets" in client.sent[-1]["text"]


def test_telegram_bot_addon_cards_config_precedence(tmp_path):
    ds = FakeDataSource(CASH_HISTORY)
    state = str(tmp_path / "state.json")
    client = FakeChatClient()

    b1 = TelegramCashBot(
        ds,
        SETTINGS,
        state,
        chat_client=client,
        addon_cards={"custom": "passed"},
    )
    assert b1.addon_cards == {"custom": "passed"}

    b2 = TelegramCashBot(
        ds,
        {**SETTINGS, "addon_cards": {"from": "settings"}},
        state,
        chat_client=client,
        addon_cards=None,
    )
    from src.constants import ADDON_CARDS

    expected = ADDON_CARDS or {"from": "settings"}
    assert b2.addon_cards == expected


# --- run_telegram_bot entrypoint -----------------------------------------------


def test_run_telegram_bot_main_without_settings_uses_defaults(tmp_path, monkeypatch):
    missing_settings = tmp_path / "missing_settings.json"
    state_file = tmp_path / "state.json"
    called = {}

    class _FakeBot:
        def __init__(self, ds, settings, state_path):
            called["settings"] = settings
            called["state_path"] = state_path

        def run_once(self):
            called["ran"] = True
            return 1

    monkeypatch.setattr(run_telegram_bot, "SETTINGS_PATH", str(missing_settings))
    monkeypatch.setattr(run_telegram_bot, "STATE_PATH", str(state_file))
    monkeypatch.setattr(
        run_telegram_bot, "GoogleDataSource", lambda: FakeDataSource(CASH_HISTORY)
    )
    monkeypatch.setattr(run_telegram_bot, "TelegramCashBot", _FakeBot)

    run_telegram_bot.main()
    assert called == {"settings": {}, "state_path": str(state_file), "ran": True}


def test_run_telegram_bot_main_respects_disabled_flag(tmp_path, monkeypatch):
    settings_file = tmp_path / "settings.json"
    settings_file.write_text('{"enabled": false}', encoding="utf-8")
    monkeypatch.setattr(run_telegram_bot, "SETTINGS_PATH", str(settings_file))
    monkeypatch.setattr(
        run_telegram_bot,
        "GoogleDataSource",
        lambda: pytest.fail("GoogleDataSource should not be initialized when disabled"),
    )
    run_telegram_bot.main()


# --- real AF_UNIX socket round-trip with src/chat_client.py --------------------


def test_real_unix_socket_round_trip(tmp_path, monkeypatch):
    sock_path = tmp_path / "chat.sock"
    monkeypatch.setenv("AGENTIC_CHAT_SOCKET", str(sock_path))

    events_db = [
        {
            "seq": 1,
            "channel": "finance",
            "transport": "telegram",
            "event_type": "message",
            "sender_id": "111",
            "text": "Coffee 250",
            "payload": {"text": "Coffee 250", "sender_id": "111"},
        }
    ]
    acked_seqs: list[int] = []
    sent_texts: list[str] = []

    def handler(req: dict[str, Any]) -> dict[str, Any]:
        op = req.get("op")
        if op == "register_channel":
            return {
                "ok": True,
                "channel": req["channel"],
                "transport": req["transport"],
            }
        if op == "pull_events":
            after = int(req.get("after_seq", 0))
            return {
                "ok": True,
                "events": [e for e in events_db if e["seq"] > after],
            }
        if op == "send_message":
            sent_texts.append(req["text"])
            return {"ok": True}
        if op == "ack_event":
            acked_seqs.append(int(req["seq"]))
            return {"ok": True}
        return {"ok": False, "error": f"unknown op {op}"}

    srv = _LocalUnixChatServer(sock_path, handler)
    try:
        ds = FakeDataSource(CASH_HISTORY)
        state_path = str(tmp_path / "bot_state.json")
        client = ChatSocketClient()  # resolves AGENTIC_CHAT_SOCKET from env
        bot = TelegramCashBot(
            ds,
            {"transport": "telegram", "allowed_users": {"111": "Karti"}},
            state_path,
            chat_client=client,
            addon_cards=SYNTHETIC_ADDON_CONFIG,
        )
        handled = bot.run_once()
        assert handled == 1
        assert [r["op"] for r in srv.requests] == [
            "register_channel",
            "pull_events",
            "send_message",
            "ack_event",
        ]
        assert srv.requests[0]["channel"] == "finance"
        assert srv.requests[0]["transport"] == "telegram"
        assert ds.sheets_service.appended[-1][1] == "Coffee"
        assert ds.sheets_service.appended[-1][5] == "Karti"
        assert len(sent_texts) == 1 and "Coffee ₹250" in sent_texts[0]
        assert acked_seqs == [1]
        persisted = json.loads(Path(state_path).read_text(encoding="utf-8"))
        assert persisted["last_seq"] == 1
        assert persisted["last_appended"]["summary"] == "Coffee ₹250"
    finally:
        srv.close()
