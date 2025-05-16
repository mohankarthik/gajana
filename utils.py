# gajana/utils.py
from __future__ import annotations

import logging
import sys
from typing import Optional


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
