import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("salary_splitter")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from plugins.salary_splitter.splitter import SalarySplitter  # noqa: E402
from src.google_data_source import GoogleDataSource  # noqa: E402

SETTINGS_PATH = os.path.join(
    os.path.dirname(__file__), "plugins", "salary_splitter", "settings.json"
)


# Kill switch. The splitter wrote wrong ledger rows for 10 of the last 16
# months -- including two it never wrote at all, and one (2026-04) whose legs
# netted exactly to the bank credit while understating income by Rs 4.92M, so a
# net-level check could not see it. Until it reads the manual "Google Salary"
# sheet and asserts every component against it, running it makes the ledger
# worse. Set GAJANA_ALLOW_SALARY_SPLITTER=1 to override deliberately.
DISABLED_REASON = (
    "The salary splitter is disabled: it mis-booked 10 of the last 16 months "
    "and the ledger is being conformed to the manual 'Google Salary' sheet. "
    "Re-enable only once it validates every split component against that "
    "sheet, not just the net. Override with GAJANA_ALLOW_SALARY_SPLITTER=1."
)


def main() -> None:
    """Split a Google payslip into the salary sheet and the gajana ledger.

    Usage: python run_salary_splitter.py [YYYY-MM] [--dry-run]
    With no month, defaults to the previous calendar month (the monthly cron
    splits the month that just ended). Pass an explicit YYYY-MM for manual
    backfill. --dry-run fills the salary sheet and prints the planned ledger
    rows without writing them to the ledger. Already-split months are a no-op.
    """
    import datetime

    if os.environ.get("GAJANA_ALLOW_SALARY_SPLITTER") != "1":
        logger.error(DISABLED_REASON)
        sys.exit(1)

    argv = [a for a in sys.argv[1:] if a != "--dry-run"]
    dry_run = "--dry-run" in sys.argv
    if argv:
        ym = argv[0]
    else:
        # Previous calendar month, e.g. run on 2026-08-05 -> "2026-07".
        first_of_this_month = datetime.date.today().replace(day=1)
        prev_month = first_of_this_month - datetime.timedelta(days=1)
        ym = prev_month.strftime("%Y-%m")
        logger.info(f"No month given; defaulting to previous month {ym}.")

    if not os.path.exists(SETTINGS_PATH):
        logger.error(
            f"{SETTINGS_PATH} not found. Copy settings.example.json and fill it in."
        )
        sys.exit(1)
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        settings = json.load(f)

    logger.info("Starting Gajana Salary Splitter...")
    ds = GoogleDataSource()
    SalarySplitter(ds, settings).run(ym, dry_run=dry_run)
    logger.info("Salary Splitter finished.")


if __name__ == "__main__":
    main()
