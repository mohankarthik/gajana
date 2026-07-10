# gajana/utils.py
from __future__ import annotations

import datetime
import logging
import re
import sys
from typing import Optional, Any, List

import pandas as pd

# A leading date token, used to salvage dates the LLM decorated with a trailing
# time / separator (e.g. "20/04/2026 | 08:34", "11/04/2026 19:51:23").
_DATE_TOKEN_RE = re.compile(
    r"\d{4}-\d{1,2}-\d{1,2}"
    r"|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}"
    r"|\d{1,2}\s+[A-Za-z]{3,}\s+\d{2,4}"
)


def log_and_exit(
    logger_instance: logging.Logger,
    message: str,
    exception_instance: Optional[Exception] = None,
    exit_code: int = 1,
) -> None:
    """
    Logs a critical message (with optional exception info) and then exits the program.

    Args:
        logger_instance: The logger instance to use (e.g., logger = logging.getLogger(__name__)).
        message: The critical error message to log.
        exception_instance: Optional. The exception instance that occurred.
                            If provided, exc_info will be based on this.
        exit_code: The exit code for sys.exit(). Defaults to 1.
    """
    if exception_instance:
        # Log the exception with its traceback
        logger_instance.critical(message, exc_info=exception_instance)
    else:
        logger_instance.critical(message)

    sys.exit(exit_code)


def parse_mixed_datetime(
    logger_instance: logging.Logger, date_str: Any, formats: List[str]
) -> Optional[datetime.datetime]:
    """
    Attempts to parse a date string using a list of provided formats.
    Falls back to pandas inference if specific formats fail.
    """
    if pd.isna(date_str) or date_str == "":
        return None
    try:
        cleaned_str = str(date_str).strip("' ")

        # Candidates: the full string, plus (if different) just the leading date
        # token. LLM PDF parsing sometimes appends a transaction time or a "|"
        # separator to the date column ("20/04/2026 | 08:34"); we only care about
        # the date, so fall back to the bare token rather than failing the parse.
        candidates = [cleaned_str]
        m = _DATE_TOKEN_RE.search(cleaned_str)
        if m and m.group() != cleaned_str:
            candidates.append(m.group())

        if not formats:
            logger_instance.warning(
                "No specific date formats provided, attempting inference only."
            )

        # Try explicit formats first, across candidates.
        for cand in candidates:
            for fmt in formats:
                try:
                    return datetime.datetime.strptime(cand, fmt)
                except ValueError:
                    continue

        # Fall back to pandas inference, across candidates.
        for cand in candidates:
            dt_obj = pd.to_datetime(cand, dayfirst=True, errors="coerce")
            if not pd.isna(dt_obj):
                logger_instance.debug(f"Parsed '{cand}' using pandas inference.")
                return dt_obj.to_pydatetime()

        logger_instance.warning(
            f"Could not parse date string with any provided format or inference: "
            f"'{cleaned_str}'"
        )
        return None
    except Exception as e:
        log_and_exit(
            logger_instance,
            f"Unexpected error parsing date string '{date_str}': {e}",
            e,
        )
        return None
