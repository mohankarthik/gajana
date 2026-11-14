# Cash entry chat bot

Log cash transactions to the gajana **Cash Transactions** sheet from a Telegram
group or Matrix room via the host unified chat bridge (`/run/agentic/chat.sock`,
channel `"finance"`).

## How it works

- Cron-polled every minute (`run_telegram_bot.py`): connects to `/run/agentic/chat.sock`
  via `src/chat_client.py` (`ChatSocketClient`), registers channel `"finance"`
  with the configured transport (`"telegram"` or `"matrix"`), pulls new
  pre-authorized events (`after_seq`), and acknowledges each processed event.
- Host `chat-bridge` holds the bot token, `allowed_chat_id`, and user
  authorization; `gajana` never stores chat tokens or calls `api.telegram.org`.
- Parses a message into a cash entry, categorizes it from the Cash tab history,
  appends a row (`Date | Description | Debit | Credit | Category | Remarks`), and
  replies. `/undo` deletes the most recent entry; `/addon` queries add-on card
  spend and settlement.

## Usage (in the group)

```
Coffee 250            spend ₹250 on Coffee            -> Debit
Auto 80               spend ₹80                       -> Debit
+2000 ATM             cash in ₹2000 (ATM withdrawal)  -> Credit
Groceries 640 yesterday                               -> dated yesterday
Rent 12000 2026-07-01                                 -> explicit date
/undo                 remove the last entry
/addon                add-on card spend and settlement
```

A leading `+`, or a first word like `atm` / `received` / `deposit` / `refund`,
marks the entry as cash-in (Credit). Everything else is a spend (Debit).
Messages without a number are ignored, so an optional `/cash` prefix also works.

## Configuration

`plugins/telegram_bot/settings.json` is optional (defaults to `"enabled": true`,
`"transport": "telegram"`). Copy `settings.example.json` if you want to
customize:

- `"enabled"`: `true` or `false` (disables both bot polling and outbound add-on
  notifications when `false`).
- `"transport"`: `"telegram"` (default) or `"matrix"`.
- `"allowed_users"`: optional `{sender_id: "Label"}` map used only to populate
  the sheet `Remarks` column (channel authorization is enforced on the host by
  `chat-bridge`).
