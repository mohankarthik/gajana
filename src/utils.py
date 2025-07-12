# gajana/utils.py
from __future__ import annotations

import datetime
import logging
import sys
from typing import Optional, Any, List

import pandas as pd


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
        parsed_date = None

        # Try explicit formats first
        if formats:
            for fmt in formats:
                try:
                    # Use Python's datetime.strptime for explicit format matching
                    parsed_date = datetime.datetime.strptime(cleaned_str, fmt)
                    logger_instance.debug(
                        f"Parsed '{cleaned_str}' using format '{fmt}'"
                    )
                    break
                except ValueError:
                    continue
        else:
            logger_instance.warning(
                "No specific date formats provided, attempting inference only."
            )
            pass

        # If specific formats fail or weren't provided, try pandas inference
        if not parsed_date:
            dt_obj = pd.to_datetime(cleaned_str, dayfirst=True, errors="coerce")
            if pd.isna(dt_obj):
                logger_instance.warning(
                    f"Could not parse date string with any provided format or inference: '{cleaned_str}'"
                )
                return None
            logger_instance.debug(f"Parsed '{cleaned_str}' using pandas inference.")
            return dt_obj.to_pydatetime()
        else:
            return parsed_date
    except Exception as e:
        log_and_exit(
            logger_instance,
            f"Unexpected error parsing date string '{date_str}': {e}",
            e,
        )
        return None
