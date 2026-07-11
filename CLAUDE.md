# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
pytest

# Run a single test file
pytest tests/test_transaction_processor.py

# Run a single test by name
pytest tests/test_categorizer.py::TestCategorizer::test_exact_match

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Type check
mypy src/ main.py

# Lint
flake8 src/ tests/ main.py

# Format
black src/ tests/ main.py

# Run the app (normal mode)
python main.py

# Run with local CSV instead of Google Sheets
python main.py --csv-db-path /path/to/csv/root
```

Pre-commit hooks run black, flake8, mypy, and the full test suite on every `git commit`. Fix failures before trying again — never use `--no-verify`.

## Architecture

Gajana is a personal finance pipeline: fetches bank/CC statements → parses → categorizes → writes to Google Sheets (or CSV). Entry point is `main.py`.

### Data flow

```
statements (Google Drive / local CSV / PDF)
  → TransactionProcessor  (parses raw data into dicts)
  → Categorizer           (applies matchers.json rules)
  → DataSourceInterface   (writes to Google Sheets or CSV)
```

### Key abstractions

**`DataSourceInterface`** (`src/interfaces.py`) — abstract backend. Two implementations:
- `GoogleDataSource` (`src/google_data_source.py`) — default; reads from Google Drive folder, writes to Google Sheets. Requires `secrets/google.json` service account key.
- `CSVDataSource` (`src/csv_data_source.py`) — local CSV files; used with `--csv-db-path`.

**`TransactionProcessor`** (`src/transaction_processor.py`) — parses statement files. For `.pdf` files it delegates to `PDFParser`; for Google Sheets files it uses pandas + config-driven header detection. Standardizes all transactions to the internal dict schema: `{date, description, amount, category, remarks, account}`.

**Processed-statements cache** — statements are immutable, so re-parsing the newest one every day (until next month's arrives) just burns LLM calls; the filename-derived month-end watermark check can't skip it because the last real txn is before month-end. So a cache (`data/state/processed_statements.json`, GoogleDataSource-only; CSV/tests no-op via interface defaults) records a **cleanly-parsed** PDF's file ID → its latest booked txn date. A file is skipped (no download, no LLM) when the account watermark already covers that date. Self-healing: delete booked rows → watermark drops below the cached date → it re-parses. Only *clean* (0-flagged) PDFs are cached, so a statement with rows in review keeps re-parsing until a retry rescues them.

**`PDFParser`** (`src/pdf_parser.py`) — uses LiteLLM. Primary model: `gemini/gemini-2.5-flash`. Fallback: `anthropic/claude-sonnet-4-6`. Falls back to text extraction if PDF vision fails. API keys loaded from `secrets/gemini.json` and `secrets/anthropic.json` (or env vars `GEMINI_API_KEY` / `ANTHROPIC_API_KEY`). The prompt asks the model to copy every field **verbatim** (raw date/amount tokens as printed, no reformatting) — deterministic code does all parsing — and to also read the statement's own printed totals into a `summary` (`total_debit`/`total_credit`). `parse_pdf_with_text()` returns `(txns, extracted_text, summary)`; the text layer is the token oracle and the summary is the reconcile cross-check (see `StatementValidator`). `models=[...]` overrides the model order for retries.

**`StatementValidator`** (`src/statement_validator.py`) — deterministic guard against LLM hallucination on PDF parses (no LLM calls). Vision owns *structure* (which column), the PDF text layer owns *tokens* (never mangled), and this crosses them. Per-txn **hard** checks → row is quarantined to the Review tab: date/amount not present in the text, unparseable date, or date after the statement end + a small grace window (`STATEMENT_END_GRACE_DAYS`, for value-date lag; still capped at today — a real txn can't be future-dated). Per-statement **soft** flags (logged + noted, don't block rows): row-count mismatch (gross order-of-magnitude only), low-confidence descriptions, and `reconcile` mismatch. `TransactionProcessor._parse_validate_pdf` runs it, retries once with the fallback model if anything is flagged, and routes survivors: pass → ledger, fail → `write_review_rows` (Review tab; no-op on CSV).

**Reconcile** (`reconcile_summary`) cross-checks the transaction sums against the statement's own printed figures — the LLM-read `summary` — **no per-bank regex, no per-account config**. Preferred: `total_debit`/`total_credit` checked independently, so a debit/credit column swap fails even when the net is unchanged. Fallback when a statement prints no such totals (e.g. axis-mini): `opening_balance`/`closing_balance` → net-magnitude check `|closing−opening|` vs `|sum_debit−sum_credit|` (sign-agnostic, works for banks and CCs; weaker — can't see a net-preserving swap — but catches dropped/misread rows). Semantic extraction ("find the total debits / the closing balance") is robust to layout and model changes; missing figures → side skipped (fail-safe, no false flag). Gross-error only — relative tolerance `max(RECONCILE_ABS_TOLERANCE, RECONCILE_REL_TOLERANCE·stated)` ignores paise rounding. Summary and transactions come from the same call, but a table column-swap doesn't corrupt the separately-read summary box, so the check stays meaningful (verified live: HDFC Infinia summary caught a ~₹1400 txn-parse error the per-token checks couldn't).

Other config keys: `statement_period_patterns` (upper-bound date), `date_formats`. Verbatim dates are parsed with these plus an ISO fallback; a trailing time / `|`-separator (HDFC Infinia's date+time column) is stripped before parsing and before the date-in-text check (`_DATE_TOKEN_RE` in `utils.py`) so the row isn't quarantined over a time.

**`Categorizer`** (`src/categorizer.py`) — rule-based categorization from `data/matchers.json`. Each matcher specifies `category`, `description` (list of keywords), and optionally `debit: true/false`.

**`TransactionMatcher`** (`src/transaction_matcher.py`) — deduplication logic. Prevents adding transactions already in the log.

**`SQLiteBackupManager`** (`src/backup_manager.py`) — backs up/restores transaction log to `backups/gajana.db`.

**Cash mirror** (`src/cash_mirror.py`) — after new **bank** txns are booked in normal/daily mode, cash movements are mirrored into the shared "Cash Transactions" tab: an ATM withdrawal (bank debit) becomes a Cash **credit** (wallet gains cash); a cash deposit into the bank (bank credit) becomes a Cash **debit**. Which categories mirror, and their direction, comes from `data/cash_mirror.json` (`{category: "in"|"out"}`; `in` = wallet gains → Cash credit). Detection is category-based (categorizer must first label the txn, e.g. `Transfer:Cash` / `Transfer:Cash Deposit`). Idempotent across daily runs via a stable Remarks marker `auto:{account}:{date}:{amount}:{deschash}` — existing Cash rows are scanned and already-mirrored txns skipped. Only runs on data sources exposing a cash ledger (`GoogleDataSource`); no-op for CSV. Same tab is also written by the Telegram bot.

**Gmail plugin** (`plugins/gmail_fetcher/`) — optional; downloads statements from Gmail before processing. Activated with `--fetch-emails` (requires `GoogleDataSource`).

### Statement parsing configuration

`data/configs/` holds one JSON per account type (e.g. `bank-axis.json`, `cc-hdfc.json`). Config key is derived from the statement filename: `{type}-{bank}` (e.g. a file named `bank-axis-primary-2026-05.pdf` → key `bank-axis`).

Each config has:
- `header_patterns` — list of column-name lists; used to auto-detect the header row (75% match threshold, checked in first 30 rows)
- `column_map` — maps source column names to internal keys (`date`, `description`, `debit`, `credit`, or `amount`)
- `date_formats` — strptime format strings tried in order
- Optional `special_handling: "hdfc_cc_tilde"` — splits tilde-delimited single-column data

### Storage format

Sheets: `Date | Description | Debit | Credit | Category | Remarks | Account` starting at B2. Internally transactions use signed `amount` (negative = debit); the split to debit/credit happens only when writing to storage.

### Secrets directory

`secrets/` (gitignored):
- `google.json` — Google service account key
- `gemini.json` — `{"api_key": "..."}`
- `anthropic.json` — `{"api_key": "..."}`
- `passwords.json` — `{"axis": "password", "axis-secondary": "specific-password"}` for encrypted PDFs

### Adding a new bank/CC

1. Add account identifier to `BANK_ACCOUNTS` or `CC_ACCOUNTS` in `src/constants.py` — format: `{type}-{bank}-{name}` (e.g. `bank-newbank-savings`)
2. Create `data/configs/{type}-{bank}.json` with `header_patterns`, `column_map`, `date_formats`
3. Add test in `tests/test_transaction_processor.py`

No Python code changes needed for new banks — only config + constants.
