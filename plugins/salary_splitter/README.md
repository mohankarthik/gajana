# Salary Splitter plugin

Bespoke, opt-in add-on (not part of gajana's core pipeline). Turns a Google
payslip PDF into:

1. **The "Google Salary" tracking sheet** — fills the payslip-derived input
   cells for that month (Basic, HRA, SA, Bonus, Medical Insurance, GSU Value,
   Credited GSU Value, Credited to A/C).
2. **The gajana bank ledger** — appends the month's salary as categorized split
   rows (Income, Tax, Equity, Small Savings, Insurance) instead of one lump
   credit.

The sheet's own formulas compute the bottom block (Total Salary, Total Tax,
Equity, Total In Hand); the plugin reads those back and writes them to the
ledger, so the tax/equity math lives in one place — the sheet.

## Run

```bash
# A specific month (used for manual backfill)
python run_salary_splitter.py 2026-06

# Fill the salary sheet + print planned rows without writing the ledger
python run_salary_splitter.py 2026-06 --dry-run

# No month -> the previous calendar month (what the monthly cron runs)
python run_salary_splitter.py
```

Already-split months are a no-op, so re-runs are safe.

On the homelab container it runs monthly (crontab: 5th, 08:00 IST).

## Safety

- **Net-pay guard**: aborts before writing any ledger row if the sheet's
  computed `Total In Hand` ≠ the payslip `Net Pay`.
- **Idempotent**: re-running a month that is already in the ledger is a no-op
  (detected by the synthetic row description).
- **No lump double-booking**: the raw single-line salary NEFT is kept out of
  the ledger by gajana's ignore-list (`data/ignore.json`). This plugin is the
  sole writer of Google-salary ledger rows.

## Config

Copy `settings.example.json` → `settings.json` (gitignored) and fill in:

- `payslip_folder_id` — Drive folder holding `YYYY-MM.pdf` payslips.
- `salary_sheet_id` — the "Google Salary" tracking sheet (one tab per year).
- `input_row_map` — sheet row label → payslip field expression.
- `salary_account` — the gajana bank account the salary lands in.
- `split_map` — sheet bottom-block row → ledger category + `credit`/`debit`.

The Google service account (`secrets/google.json`) needs **Viewer** on the
payslip folder and **Editor** on the salary sheet.

## Known limits

- GSU **share-count** rows (Num GSUs, Stock Price USD, Credited GSUs) come from
  equity-vesting data, not the payslip, and stay manual. The money rows
  (GSU Value, Credited GSU Value) are payslip-derived, so the ledger split is
  correct; only the sheet's Stock-Price-INR display cell may look stale.
- `Credited GSU Value` is written as a literal (overwriting its formula), since
  the plugin cannot fill the share counts that formula depends on.
