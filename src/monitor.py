# gajana/src/monitor.py
"""Health monitoring + Uptime-Kuma push for the containerized daily/backup runs.

The daily pipeline (fetch + update) emits a single consolidated push so a
missed run, a stale statement fetch, or a stale transaction log all surface as
one monitor going DOWN with a human-readable reason. The weekly backup pushes
to its own monitor.

Push URLs come from the environment (set via the container's env_file):
  GAJANA_UPTIME_PUSH_URL  - daily pipeline monitor
  GAJANA_BACKUP_PUSH_URL  - weekly backup monitor
An empty/unset URL makes the push a no-op (safe for local/dev runs).
"""

from __future__ import annotations

import datetime
import json
import logging
import os
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

logger = logging.getLogger(__name__)

# A statement fetch or transaction older than this many days means the pipeline
# has silently stalled (missed a monthly statement) -> monitor goes DOWN.
STALE_DAYS = 45

# Persisted across runs (mounted + B2-backed) so we remember when we last saw a
# genuinely new statement PDF, independent of any single run.
STATE_FILE = os.path.join("data", "state", "monitor_state.json")

ENV_DAILY_PUSH_URL = "GAJANA_UPTIME_PUSH_URL"
ENV_BACKUP_PUSH_URL = "GAJANA_BACKUP_PUSH_URL"

_PUSH_TIMEOUT_SECONDS = 15


def load_state() -> dict:
    """Load the persisted monitor state, or an empty dict if none exists."""
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
        logger.debug(f"No usable monitor state at {STATE_FILE}: {e}")
        return {}


def save_state(state: dict) -> None:
    """Persist the monitor state, creating the state directory if needed."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


def record_pdf_fetch(new_pdf_count: int, today: Optional[datetime.date] = None) -> None:
    """Record the outcome of a statement fetch. Only a genuinely new PDF
    (count > 0) advances ``last_new_pdf_date``; the timestamp of the attempt is
    always updated for observability."""
    today = today or datetime.date.today()
    state = load_state()
    state["last_fetch_attempt"] = today.isoformat()
    if new_pdf_count > 0:
        state["last_new_pdf_date"] = today.isoformat()
        state["last_new_pdf_count"] = new_pdf_count
    save_state(state)


def days_since(
    date_str: Optional[str], today: Optional[datetime.date] = None
) -> Optional[int]:
    """Whole days between an ISO date string and today, or None if unparseable."""
    if not date_str:
        return None
    today = today or datetime.date.today()
    try:
        then = datetime.date.fromisoformat(str(date_str)[:10])
    except ValueError:
        return None
    return (today - then).days


def evaluate_health(
    *,
    pipeline_error: Optional[str],
    new_pdf_count: int,
    latest_txn_date: Optional[datetime.datetime],
    today: Optional[datetime.date] = None,
) -> tuple[bool, str]:
    """Fold the daily run's signals into a single (is_up, message) verdict.

    DOWN if the pipeline raised, if no new statement PDF has arrived in
    STALE_DAYS, or if the newest transaction in the log is older than STALE_DAYS.
    """
    today = today or datetime.date.today()
    state = load_state()
    problems: list[str] = []

    if pipeline_error:
        problems.append(f"pipeline error: {pipeline_error}")

    pdf_age = days_since(state.get("last_new_pdf_date"), today)
    if pdf_age is None:
        # No PDF ever recorded and none fetched now -> can't confirm freshness.
        if new_pdf_count == 0:
            problems.append("no statement PDF ever fetched")
    elif pdf_age > STALE_DAYS:
        problems.append(f"no new statement in {pdf_age}d")

    if latest_txn_date is None:
        problems.append("no transactions in log")
    else:
        txn_age = (today - latest_txn_date.date()).days
        if txn_age > STALE_DAYS:
            problems.append(f"latest txn {txn_age}d old")

    if problems:
        return False, "; ".join(problems)

    latest_str = latest_txn_date.date().isoformat() if latest_txn_date else "n/a"
    return True, f"OK | +{new_pdf_count} pdfs | latest txn {latest_str}"


def push(url: Optional[str], is_up: bool, msg: str) -> None:
    """Push a status heartbeat to an Uptime-Kuma push monitor.

    No-ops when ``url`` is empty so local runs don't need monitoring configured.
    Never raises - a monitoring failure must not fail the pipeline.
    """
    if not url:
        logger.info(f"No Uptime-Kuma push URL configured; skipping. Status: {msg}")
        return
    status = "up" if is_up else "down"
    # Merge into any existing query so a URL pasted with its own
    # ?status=up&msg=OK&ping= template doesn't produce a double query string.
    parts = urlsplit(url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params["status"] = status
    params["msg"] = msg
    full_url = urlunsplit(parts._replace(query=urlencode(params)))
    try:
        resp = requests.get(full_url, timeout=_PUSH_TIMEOUT_SECONDS)
        resp.raise_for_status()
        logger.info(f"Pushed Uptime-Kuma status={status} msg={msg!r}")
    except requests.RequestException as e:
        logger.error(f"Failed to push Uptime-Kuma heartbeat: {e}")


def push_daily(is_up: bool, msg: str) -> None:
    push(os.environ.get(ENV_DAILY_PUSH_URL), is_up, msg)


def push_backup(is_up: bool, msg: str) -> None:
    push(os.environ.get(ENV_BACKUP_PUSH_URL), is_up, msg)
