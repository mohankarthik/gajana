# tests/test_utils.py
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

# Assuming utils.py is in gajana package (src/gajana/utils.py)
from src.utils import log_and_exit


@pytest.fixture
def mock_logger():
    """Fixture to create a mock logger instance."""
    logger = MagicMock(spec=logging.Logger)
    return logger


def test_log_and_exit_no_exception(mock_logger):
    """Test log_and_exit when no exception instance is provided."""
    test_message = "Critical error occurred!"
    test_exit_code = 5

    # We need to mock sys.exit to prevent the test from actually exiting
    with patch("sys.exit") as mock_sys_exit:
        log_and_exit(mock_logger, test_message, exit_code=test_exit_code)

        # Check that sys.exit was called with the correct code
        mock_sys_exit.assert_called_once_with(test_exit_code)

    # Check that logger.critical was called correctly
    mock_logger.critical.assert_called_once_with(test_message)


def test_log_and_exit_with_exception(mock_logger):
    """Test log_and_exit when an exception instance is provided."""
    test_message = "Critical error with exception!"
    test_exit_code = 2
    test_exception = ValueError("Something went wrong")

    with patch("sys.exit") as mock_sys_exit:
        log_and_exit(
            mock_logger,
            test_message,
            exception_instance=test_exception,
            exit_code=test_exit_code,
        )
        mock_sys_exit.assert_called_once_with(test_exit_code)

    # Check that logger.critical was called with exc_info set to the exception
    mock_logger.critical.assert_called_once_with(test_message, exc_info=test_exception)


def test_log_and_exit_default_exit_code(mock_logger):
    """Test log_and_exit uses the default exit code if none is provided."""
    test_message = "Default exit code test"

    with patch("sys.exit") as mock_sys_exit:
        log_and_exit(mock_logger, test_message)  # No exit_code specified

        # Default exit_code in log_and_exit is 1
        mock_sys_exit.assert_called_once_with(1)

    mock_logger.critical.assert_called_once_with(test_message)
