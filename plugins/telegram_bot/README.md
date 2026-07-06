# Telegram cash bot

Log cash transactions to the gajana **Cash Transactions** sheet from a Telegram
group — no app, no exposed port. Both partners can post in a shared group.

## How it works

- Cron-polled every minute (`run_telegram_bot.py`): fetches new group messages
  via Telegram `getUpdates`, so there is **no webhook and no inbound port**.
- Parses a message into a cash entry, categorizes it from the Cash tab history,
  appends a row (`Date | Description | Debit | Credit | Category | Remarks`), and
  replies. The sender's name goes in Remarks.
- Only the configured group chat + allowed users are processed. `/undo` deletes
  the most recent entry.

## Usage (in the group)

```
Coffee 250            spend ₹250 on Coffee            -> Debit
Auto 80               spend ₹80                       -> Debit
+2000 ATM             cash in ₹2000 (ATM withdrawal)  -> Credit
Groceries 640 yesterday                               -> dated yesterday
Rent 12000 2026-07-01                                 -> explicit date
/undo                 remove the last entry
```

A leading `+`, or a first word like `atm` / `received` / `deposit` / `refund`,
marks the entry as cash-in (Credit). Everything else is a spend (Debit).
Messages without a number are ignored, so an optional `/cash` prefix also works.

> **Use a dedicated group** (just the two of you + the bot). With Privacy Mode
> off the bot sees every message, so a chat used only for logging avoids
> accidental entries from ordinary conversation that happens to contain numbers.

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather) → copy the token into
   `secrets/telegram.json`:
   ```json
   { "token": "123456:ABC-..." }
   ```
2. BotFather → `/setprivacy` → **Disable** (so the bot sees plain messages, not
   just commands).
3. Create a group, add the bot and both partners.
4. Copy `settings.example.json` → `settings.json` and fill in `allowed_users`
   (Telegram numeric user id → display name). Leave `allowed_chat_id: 0` for
   now.
5. Post any message in the group, then run `python run_telegram_bot.py` once —
   with `allowed_chat_id` still 0 it logs the group's `chat_id` (setup mode,
   nothing is written). Put that id into `settings.json` as `allowed_chat_id`.
6. Done — the monthly image already schedules a 1-minute poll.

Finding your user id: message [@userinfobot](https://t.me/userinfobot), or read
it from the setup-mode log line.
