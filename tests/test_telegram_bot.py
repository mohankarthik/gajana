from __future__ import annotations

import datetime

import pytest

from plugins.telegram_bot import bot as bot_mod
from plugins.telegram_bot.bot import TelegramCashBot, parse_entry

TODAY = datetime.date.today()

SETTINGS = {
    "allowed_chat_id": -100123,
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
    assert e.is_credit is True
    assert e.amount == 2000.0
    assert "ATM" in e.description


def test_parse_credit_first_word():
    e = parse_entry("received 5000 from dad")
    assert e.is_credit is True
    assert e.amount == 5000.0


def test_parse_yesterday():
    e = parse_entry("Groceries 640 yesterday")
    assert e.entry_date == TODAY - datetime.timedelta(days=1)
    assert e.description == "Groceries"


def test_parse_explicit_date():
    e = parse_entry("Rent 12000 2026-07-01")
    assert e.entry_date == datetime.date(2026, 7, 1)


def test_parse_commas_and_decimals():
    e = parse_entry("Fridge 1,250.50")
    assert e.amount == 1250.50


def test_parse_cash_prefix_ignored():
    e = parse_entry("/cash Auto 80")
    assert e.description == "Auto" and e.amount == 80.0


def test_parse_no_amount_returns_none():
    assert parse_entry("running 5 min late lol") is not None  # has a number
    assert parse_entry("let's have dinner tonight") is None
    assert parse_entry("") is None


def test_parse_zero_is_not_entry():
    assert parse_entry("free sample 0") is None


def test_parse_missing_description_defaults():
    e = parse_entry("500")
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
    def __init__(self, cash_history):
        self.sheets_service = FakeSheets()
        self._history = cash_history

    def get_sheet_data(self, sheet_id, sheet_name, rng):
        return self._history


CASH_HISTORY = [
    # B:G rows: Date, Description, Debit, Credit, Category, Remarks
    ["2026-06-01", "Coffee", "200.00", "", "Expense:Dining", ""],
    ["2026-06-02", "Auto", "80.00", "", "Expense:Local Travel", ""],
    ["2026-06-03", "ATM", "", "3000.00", "Transfer:Cash", ""],
]


@pytest.fixture
def sent(monkeypatch):
    outbox = []

    class _Resp:
        def __init__(self, data):
            self._d = data

        def raise_for_status(self):
            pass

        def json(self):
            return self._d

    self_updates = {"result": []}

    def fake_get(url, params=None, timeout=None):
        return _Resp(self_updates)

    def fake_post(url, json=None, timeout=None):
        outbox.append(json)
        return _Resp({})

    monkeypatch.setattr(bot_mod.requests, "get", fake_get)
    monkeypatch.setattr(bot_mod.requests, "post", fake_post)
    return outbox, self_updates


def make_bot(tmp_path, cash_history=CASH_HISTORY):
    ds = FakeDataSource(cash_history)
    state = str(tmp_path / "state.json")
    return TelegramCashBot(ds, SETTINGS, "TOKEN", state), ds


# --- authorization -------------------------------------------------------------


def test_authorized_accepts_configured_chat_and_user(tmp_path, sent):
    bot, _ = make_bot(tmp_path)
    msg = {"chat": {"id": -100123}, "from": {"id": 111}}
    assert bot._authorized(msg) is True


def test_authorized_rejects_other_chat_or_user(tmp_path, sent):
    bot, _ = make_bot(tmp_path)
    assert bot._authorized({"chat": {"id": -999}, "from": {"id": 111}}) is False
    assert bot._authorized({"chat": {"id": -100123}, "from": {"id": 999}}) is False


def test_setup_mode_processes_nothing(tmp_path, sent):
    ds = FakeDataSource(CASH_HISTORY)
    bot = TelegramCashBot(
        ds,
        {"allowed_chat_id": 0, "allowed_users": {"111": "Karti"}},
        "TOKEN",
        str(tmp_path / "s.json"),
    )
    assert bot._authorized({"chat": {"id": -100123}, "from": {"id": 111}}) is False


# --- entry handling ------------------------------------------------------------


def test_entry_appends_row_and_replies(tmp_path, sent):
    outbox, _ = sent
    bot, ds = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"chat": {"id": -100123}, "from": {"id": 111}, "text": "Coffee 250"}, cat
    )
    row = ds.sheets_service.appended[-1]
    assert row[1] == "Coffee"  # description
    assert row[2] == 250.0  # debit
    assert row[3] == ""  # credit blank
    assert row[4] == "Expense:Dining"  # categorized from cash history
    assert row[5] == "Karti"  # remarks = sender label
    assert "Coffee" in outbox[-1]["text"] and "✓" in outbox[-1]["text"]


def test_credit_entry_writes_credit_column(tmp_path, sent):
    bot, ds = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"chat": {"id": -100123}, "from": {"id": 222}, "text": "+2000 ATM"}, cat
    )
    row = ds.sheets_service.appended[-1]
    assert row[2] == "" and row[3] == 2000.0  # debit blank, credit set


def test_chatter_without_amount_is_silent(tmp_path, sent):
    outbox, _ = sent
    bot, ds = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"chat": {"id": -100123}, "from": {"id": 111}, "text": "dinner tonight?"}, cat
    )
    assert ds.sheets_service.appended == []
    assert outbox == []


def test_undo_deletes_last_row(tmp_path, sent):
    outbox, _ = sent
    bot, ds = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"chat": {"id": -100123}, "from": {"id": 111}, "text": "Coffee 250"}, cat
    )
    bot.process_message(
        {"chat": {"id": -100123}, "from": {"id": 111}, "text": "/undo"}, cat
    )
    assert ds.sheets_service.deleted  # a deleteDimension was issued
    assert bot.state["last_appended"] is None
    assert "Removed" in outbox[-1]["text"]


def test_undo_with_nothing_to_undo(tmp_path, sent):
    outbox, _ = sent
    bot, _ = make_bot(tmp_path)
    cat = bot._build_categorizer()
    bot.process_message(
        {"chat": {"id": -100123}, "from": {"id": 111}, "text": "/undo"}, cat
    )
    assert "Nothing to undo" in outbox[-1]["text"]


# --- run_once ------------------------------------------------------------------


def test_run_once_advances_offset_and_filters(tmp_path, sent):
    outbox, updates = sent
    bot, ds = make_bot(tmp_path)
    updates["result"] = [
        {
            "update_id": 10,
            "message": {
                "chat": {"id": -100123},
                "from": {"id": 111},
                "text": "Coffee 250",
            },
        },
        {
            "update_id": 11,
            "message": {"chat": {"id": -999}, "from": {"id": 111}, "text": "Hack 999"},
        },
        {
            "update_id": 12,
            "message": {
                "chat": {"id": -100123},
                "from": {"id": 111},
                "text": "Auto 80",
            },
        },
    ]
    handled = bot.run_once()
    assert handled == 2  # unauthorized chat ignored
    assert bot.state["offset"] == 13  # max(update_id)+1
    assert len(ds.sheets_service.appended) == 2
