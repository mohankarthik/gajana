# gajana/main.py
from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
from collections import Counter, defaultdict
from operator import itemgetter
from typing import Any, Hashable

from src import monitor
from src.backup_manager import SQLiteBackupManager
from src.cash_mirror import mirror_bank_cash_txns
from src.categorizer import Categorizer
from src.constants import (
    DEFAULT_CATEGORY,
    load_ignore_rules,
    txn_matches_ignore_rule,
)
from src.llm_categorizer import LLMCategorizer
from src.google_data_source import GoogleDataSource
from src.transaction_matcher import TransactionMatcher
from src.transaction_processor import TransactionProcessor
from src.utils import log_and_exit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# A full-overwrite must never shrink a sheet far below the last local backup:
# that signals a truncated/partial read and would delete real rows.
RECAT_MIN_READ_RATIO = 0.9

# Descriptions the pipeline must never book (e.g. the Google salary NEFT, which
# plugins/salary_splitter re-books as categorized split rows).
IGNORE_RULES = load_ignore_rules()


def apply_ignore_rules(
    txns: list[dict[Hashable, Any]],
) -> list[dict[Hashable, Any]]:
    """Drops transactions matching any configured ignore rule."""
    if not IGNORE_RULES:
        return txns
    kept = [t for t in txns if not txn_matches_ignore_rule(dict(t), IGNORE_RULES)]
    dropped = len(txns) - len(kept)
    if dropped:
        logger.info(f"Ignore-list dropped {dropped} transaction(s) before booking.")
    return kept


def partition_by_sheet(
    txns: list[dict[Any, Any]],
) -> tuple[dict[str, list[dict[Any, Any]]], list[dict[Any, Any]]]:
    """Split transactions into bank/cc buckets by account PREFIX (not a fixed
    allow-list), so legacy/renamed accounts are never silently dropped on an
    overwrite. Anything without a bank-/cc- prefix is returned as ``unknown``."""
    buckets: dict[str, list[dict[Any, Any]]] = {"bank": [], "cc": []}
    unknown: list[dict[Any, Any]] = []
    for t in txns:
        acc = str(t.get("account", "")).strip().lower()
        if acc.startswith("bank"):
            buckets["bank"].append(t)
        elif acc.startswith("cc"):
            buckets["cc"].append(t)
        else:
            unknown.append(t)
    return buckets, unknown


def backup_baseline_counts() -> dict[str, int] | None:
    """Per-sheet row counts from the latest local SQLite backup, or None if no
    backup exists / it can't be read. Used as a safety baseline before an
    overwrite to detect a truncated read."""
    import os
    import sqlite3
    from contextlib import closing

    from src.constants import DB_FILE_PATH

    if not os.path.exists(DB_FILE_PATH):
        return None
    try:
        with closing(sqlite3.connect(DB_FILE_PATH)) as conn:
            rows = conn.execute(
                "SELECT account, COUNT(*) FROM transactions GROUP BY account"
            ).fetchall()
    except Exception as e:
        logger.warning(f"Could not read backup baseline from {DB_FILE_PATH}: {e}")
        return None
    counts = {"bank": 0, "cc": 0}
    for acc, n in rows:
        a = str(acc or "").lower()
        if a.startswith("bank"):
            counts["bank"] += n
        elif a.startswith("cc"):
            counts["cc"] += n
    return counts


def assert_safe_to_overwrite(
    buckets: dict[str, list[dict[Any, Any]]],
    unknown: list[dict[Any, Any]],
    require_baseline: bool,
) -> None:
    """Abort (log_and_exit) if an overwrite would lose data.

    - unknown-prefix accounts present -> refuse (would be dropped).
    - live row count far below the backup baseline -> refuse (truncated read).
    - require_baseline and no backup exists -> refuse (no safety net).
    """
    if unknown:
        sample = {str(t.get("account")) for t in unknown[:5]}
        log_and_exit(
            logger,
            f"Refusing overwrite: {len(unknown)} txns have an unrecognized account "
            f"prefix (not bank-/cc-): {sample}. They would be dropped. Fix the data "
            f"or account naming first.",
        )
    baseline = backup_baseline_counts()
    if baseline is None:
        if require_baseline:
            log_and_exit(
                logger,
                "Refusing overwrite: no local backup found as a safety baseline. "
                "Run `python main.py --backup-db` first.",
            )
        return
    for sheet in ("bank", "cc"):
        have, base = len(buckets[sheet]), baseline.get(sheet, 0)
        if base and have < base * RECAT_MIN_READ_RATIO:
            log_and_exit(
                logger,
                f"Refusing overwrite: {sheet} read has {have} rows but the backup "
                f"baseline has {base} (< {RECAT_MIN_READ_RATIO:.0%}). This looks like "
                f"a truncated read; aborting to avoid deleting rows. Re-run, and if the "
                f"drop is legitimate, refresh the backup with --backup-db.",
            )


def find_latest_transaction_by_account(
    txns: list[dict[Hashable, Any]],
) -> dict[str, datetime.datetime]:
    latest: dict[str, datetime.datetime] = {}
    if not txns:
        return latest
    for txn in txns:
        try:
            account, current_date = txn["account"], txn["date"]
            assert isinstance(
                current_date, datetime.datetime
            ), f"Date not datetime: {current_date}"
            latest[account] = max(
                latest.get(account, datetime.datetime.min), current_date
            )
        except (KeyError, AssertionError, TypeError) as e:
            log_and_exit(logger, f"Error finding latest date: {e}. Txn: {txn}")
            assert False, f"Error finding latest date: {e}"
    logger.debug(f"Latest transaction dates: {latest}")
    return latest


def run_normal_mode(processor: TransactionProcessor, categorizer: Categorizer):
    logger.info("Running in NORMAL mode.")
    all_new_txns_added_count = 0

    # Fetch existing history for both account types up front and build the
    # category lookup index from it (exact + fuzzy layers).
    old_by_type = {t: processor.get_old_transactions(t) for t in ["bank", "cc"]}
    history = [txn for txns in old_by_type.values() for txn in txns]
    categorizer.build_index(history, enable_llm=categorizer.llm is not None)

    for acc_type in ["bank", "cc"]:
        logger.info(f"--- Processing {acc_type.upper()} Transactions ---")
        try:
            old_txns = old_by_type[acc_type]
            latest_by_account = find_latest_transaction_by_account(old_txns)
            potential_new_txns = processor.get_new_transactions_from_statements(
                acc_type, latest_by_account
            )

            # TransactionMatcher.find_new_txns now returns only new_txns
            new_txns_to_add = TransactionMatcher.find_new_txns(
                old_txns, potential_new_txns
            )
            new_txns_to_add = apply_ignore_rules(new_txns_to_add)

            if new_txns_to_add:
                logger.info(
                    f"Categorizing {len(new_txns_to_add)} new {acc_type} transactions..."
                )
                categorized_txns = categorizer.categorize(new_txns_to_add)
                processor.add_new_transactions_to_log(categorized_txns, acc_type)
                all_new_txns_added_count += len(categorized_txns)
                # Bank cash movements (ATM withdrawals, cash deposits) mirror
                # into the shared Cash Transactions ledger. No-op for cc.
                if acc_type == "bank":
                    mirror_bank_cash_txns(processor.data_source, categorized_txns)
            else:
                logger.info(f"No new {acc_type} transactions found to add.")
        except Exception as e:  # Catch exceptions per account type processing
            logger.error(
                f"Error processing {acc_type} transactions in normal mode: {e}", e
            )
            # Decide if you want to continue to the next account type or assert False here

    logger.info(
        f"Normal mode finished. Added {all_new_txns_added_count} new transactions."
    )


def run_recategorize_mode(processor: TransactionProcessor, categorizer: Categorizer):
    logger.info("Running in RECATEGORIZE mode.")
    all_existing_txns = processor.get_all_transactions_for_recategorize()
    if not all_existing_txns:
        logger.warning("No existing transactions for recategorization.")
        return

    # Safety gate FIRST, before anything else: a full overwrite will rewrite the
    # whole sheet, so validate the read up front (truncated read or unknown-prefix
    # accounts -> refuse) regardless of whether there is anything to recategorize.
    buckets, unknown = partition_by_sheet(all_existing_txns)
    assert_safe_to_overwrite(buckets, unknown, require_baseline=True)

    txns_to_categorize = [
        txn
        for txn in all_existing_txns
        if txn.get("category", DEFAULT_CATEGORY) == DEFAULT_CATEGORY
        or not txn.get("category")
    ]
    if not txns_to_categorize:
        logger.info("No transactions found needing recategorization.")
        return

    logger.info(
        f"Recategorizing {len(txns_to_categorize)} '{DEFAULT_CATEGORY}' transactions..."
    )
    # Build the lookup index from the already-labeled history (build() skips
    # Uncategorized rows) before recategorizing the leftovers.
    categorizer.build_index(all_existing_txns, enable_llm=categorizer.llm is not None)
    categorizer.categorize(txns_to_categorize)  # Modifies in-place

    for sheet in ("bank", "cc"):
        buckets[sheet].sort(key=itemgetter("date", "account", "amount", "description"))
        logger.info(f"--- Overwriting {sheet.upper()} Transactions Sheet ---")
        processor.overwrite_transaction_log(buckets[sheet], sheet)
    logger.info("Recategorize mode finished.")


def run_learn_mode(processor: TransactionProcessor):
    logger.info("Running in LEARN CATEGORIES mode.")
    all_existing_txns = processor.get_all_transactions_for_recategorize()
    if not all_existing_txns:
        logger.warning("No existing transactions to learn from.")
        return

    categorized_txns = [
        txn
        for txn in all_existing_txns
        if txn.get("category") and txn.get("category") != DEFAULT_CATEGORY
    ]
    if not categorized_txns:
        logger.warning(
            f"No transactions with categories other than '{DEFAULT_CATEGORY}'."
        )
        return

    logger.info(f"Analyzing {len(categorized_txns)} categorized transactions...")
    category_patterns: dict[str, dict[str, Counter]] = defaultdict(
        lambda: defaultdict(Counter)
    )
    for txn in categorized_txns:
        try:
            cat, desc, amt = (
                txn["category"],
                str(txn.get("description", "")).lower(),
                float(txn["amount"]),
            )
            _, dc_key = amt < 0, "debit" if amt < 0 else "credit"
            words = re.findall(r"\b[a-z0-9]{3,}\b", desc)  # Words 3+ chars
            for word in words:
                if not word.isdigit():
                    category_patterns[cat][dc_key][word] += 1
        except Exception as e:
            logger.warning(f"Skipping txn during learning: {e}. Txn: {txn}")

    logger.info("--- Suggested New Categorization Rules ---")
    print(
        "\n--- Suggested New Categorization Rules ---\n(Review and manually add to matchers.json)\n"
    )
    suggestions_found = 0
    for cat, dc_data in category_patterns.items():
        for dc_key, word_counter in dc_data.items():
            suggested_keywords = [
                w for w, c in word_counter.most_common(10) if c >= 3
            ]  # Min count 3
            if suggested_keywords:
                suggestions_found += 1
                rule = {
                    "category": cat,
                    "description": sorted(suggested_keywords),
                    "debit": (dc_key == "debit"),
                }
                print(
                    f"# Suggestion for Category: {cat} ({dc_key.upper()})\n{json.dumps(rule, indent=2)}\n{'-'*20}"
                )
    if not suggestions_found:
        print("No significant patterns found.")
    logger.info("Learn categories mode finished.")


def run_backup_mode(
    processor: TransactionProcessor, backup_manager: SQLiteBackupManager
):
    """Fetches all transactions from Google Sheets and backs them up to the local SQLite DB."""
    logger.info("Running in BACKUP mode.")
    all_transactions = processor.get_all_transactions_for_recategorize()
    if not all_transactions:
        logger.warning("No transactions found in Google Sheets to back up.")
        return

    backup_manager.backup(all_transactions)
    logger.info("Backup mode finished.")


def run_backup_mode_monitored(
    processor: TransactionProcessor, backup_manager: SQLiteBackupManager
) -> None:
    """Weekly backup wrapped in an Uptime-Kuma heartbeat push (own monitor)."""
    try:
        run_backup_mode(processor, backup_manager)
        monitor.push_backup(True, "OK | backup complete")
    except Exception as e:
        monitor.push_backup(False, f"backup failed: {e}")
        raise


def _latest_txn_date(
    processor: TransactionProcessor,
) -> datetime.datetime | None:
    """Newest transaction date across both logs, or None if the logs are empty."""
    latest: datetime.datetime | None = None
    for acc_type in ("bank", "cc"):
        by_account = find_latest_transaction_by_account(
            processor.get_old_transactions(acc_type)
        )
        for dt in by_account.values():
            if latest is None or dt > latest:
                latest = dt
    return latest


def run_daily_mode(
    data_source: Any,
    processor: TransactionProcessor,
    categorizer: Categorizer,
    fetch_days: int,
) -> None:
    """Consolidated daily job: fetch statements, update the log, then push a
    single Uptime-Kuma heartbeat summarizing overall pipeline health."""
    logger.info("Running in DAILY mode.")
    pipeline_error: str | None = None
    new_pdf_count = 0

    try:
        if isinstance(data_source, GoogleDataSource):
            from plugins.gmail_fetcher.fetcher import run_plugin

            new_pdf_count = run_plugin(data_source.drive_service, days_back=fetch_days)
        else:
            logger.warning("Daily mode without GoogleDataSource; skipping email fetch.")
        monitor.record_pdf_fetch(new_pdf_count)

        run_normal_mode(processor, categorizer)
    except Exception as e:
        pipeline_error = str(e)
        logger.error(f"Daily pipeline error: {e}", exc_info=e)

    latest_txn_date: datetime.datetime | None = None
    try:
        latest_txn_date = _latest_txn_date(processor)
    except Exception as e:
        pipeline_error = pipeline_error or f"could not read latest txn: {e}"
        logger.error(f"Failed reading latest txn date: {e}", exc_info=e)

    is_up, msg = monitor.evaluate_health(
        pipeline_error=pipeline_error,
        new_pdf_count=new_pdf_count,
        latest_txn_date=latest_txn_date,
    )
    monitor.push_daily(is_up, msg)
    logger.info(f"Daily mode finished. health={'UP' if is_up else 'DOWN'} | {msg}")


def run_restore_mode(
    processor: TransactionProcessor, backup_manager: SQLiteBackupManager
):
    """Restores all transactions from the local SQLite DB to Google Sheets."""
    logger.info("Running in RESTORE mode.")

    # Critical confirmation step due to destructive nature of the operation
    confirm = input(
        "WARNING: This will ERASE all data in the 'Bank transactions' and 'CC Transactions' sheets "
        "and replace it with the data from the local database. This action cannot be undone.\n"
        "Are you sure you want to continue? (yes/no): "
    )
    if confirm.lower() != "yes":
        logger.warning("Restore operation cancelled by user.")
        return

    restored_transactions = backup_manager.restore()
    if not restored_transactions:
        logger.warning("No transactions found in the local database to restore.")
        return

    # Route by account prefix so legacy accounts are restored too (not dropped
    # by a fixed allow-list). Refuse if any account has an unknown prefix.
    buckets, unknown = partition_by_sheet(restored_transactions)
    assert_safe_to_overwrite(buckets, unknown, require_baseline=False)

    for sheet in ("bank", "cc"):
        buckets[sheet].sort(key=itemgetter("date", "account", "amount", "description"))
        logger.info(
            f"--- Overwriting {sheet.upper()} Transactions Sheet from Restore ---"
        )
        processor.overwrite_transaction_log(buckets[sheet], sheet)
    logger.info("Restore mode finished.")


def main():
    parser = argparse.ArgumentParser(
        description="Gajana - Personal Finance Transaction Processor"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--recategorize-only",
        action="store_true",
        help="Recategorize 'Uncategorized' txns.",
    )
    mode_group.add_argument(
        "--learn-categories",
        action="store_true",
        help="Suggest new categorization rules.",
    )
    mode_group.add_argument(
        "--backup-db",
        action="store_true",
        help="Backup all data from Google Sheets to a local SQLite database.",
    )
    mode_group.add_argument(
        "--restore-db",
        action="store_true",
        help="Restore all data from the local SQLite database to Google Sheets (DESTRUCTIVE).",
    )
    mode_group.add_argument(
        "--daily",
        action="store_true",
        help="Consolidated scheduled run: fetch statements + update the log, "
        "then push one Uptime-Kuma health heartbeat. Enables LLM categorization.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Run in normal mode (fetches and processes new statements). Default.",
    )
    parser.add_argument(
        "--csv-db-path",
        type=str,
        help="Path to local CSV database root. If provided, uses CSV instead of Google Sheets.",
    )
    parser.add_argument(
        "--fetch-emails",
        action="store_true",
        help="Run the optional Gmail Fetcher plugin to download statements before processing.",
    )
    parser.add_argument(
        "--fetch-days",
        type=int,
        default=7,
        help="How many days back the Gmail Fetcher searches for statements. "
        "Increase to backfill missed months (e.g. --fetch-days 180).",
    )
    parser.add_argument(
        "--llm-categorize",
        action="store_true",
        help="Enable the LLM fallback layer for novel merchants the lookup "
        "index and rules miss (costs API calls; results are cached).",
    )
    args = parser.parse_args()

    logger.info("Gajana script started.")
    start_time = datetime.datetime.now()
    try:
        if args.csv_db_path:
            from src.csv_data_source import CSVDataSource

            data_source = CSVDataSource(args.csv_db_path)
        else:
            data_source = GoogleDataSource()

        # --daily runs its own fetch so it can capture the new-PDF count for
        # monitoring; skip the standalone fetch block for it.
        if args.fetch_emails and not args.daily:
            if isinstance(data_source, GoogleDataSource):
                from plugins.gmail_fetcher.fetcher import run_plugin

                run_plugin(data_source.drive_service, days_back=args.fetch_days)
            else:
                logger.warning(
                    "--fetch-emails requires GoogleDataSource. Skipping email fetch."
                )

        processor = TransactionProcessor(data_source)

        if args.backup_db:
            backup_manager = SQLiteBackupManager()
            run_backup_mode_monitored(processor, backup_manager)
        elif args.restore_db:
            backup_manager = SQLiteBackupManager()
            run_restore_mode(processor, backup_manager)
        elif args.learn_categories:
            run_learn_mode(processor)
        elif args.daily:
            categorizer = Categorizer(llm=LLMCategorizer())
            run_daily_mode(data_source, processor, categorizer, args.fetch_days)
        else:
            llm = LLMCategorizer() if args.llm_categorize else None
            categorizer = Categorizer(llm=llm)
            if args.recategorize_only:
                run_recategorize_mode(processor, categorizer)
            else:
                run_normal_mode(processor, categorizer)

    except AssertionError as e:
        log_and_exit(logger, f"Critical assertion failed: {e}", e)
    except Exception as e:
        log_and_exit(logger, f"Unexpected error in main workflow: {e}", e)
    duration = datetime.datetime.now() - start_time
    logger.info(f"Gajana script finished. Total execution time: {duration}")


if __name__ == "__main__":
    main()
