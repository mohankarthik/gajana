# tests/test_utils.py
from __future__ import annotations

import datetime
import logging
from unittest.mock import MagicMock, patch

import pytest

# Assuming utils.py is in gajana package (src/gajana/utils.py)
from src.utils import log_and_exit, parse_mixed_datetime


@pytest.fixture
def mock_logger():
    """Fixture to create a mock logger instance."""
    logger = MagicMock(spec=logging.Logger)
    return logger


@pytest.fixture(autouse=True)
def mock_log_and_exit_fixture(mocker):
    """Mocks the log_and_exit utility to prevent tests from exiting."""
    return mocker.patch("src.utils.log_and_exit", side_effect=SystemExit)


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


def test_parse_mixed_datetime_pandas_failure(mock_logger, caplog):
    """Tests that a completely unparseable date returns None and logs a warning."""
    # This string format will cause pd.to_datetime with errors='coerce' to return NaT
    result = parse_mixed_datetime(mock_logger, "not a real date", [])
    assert result is None


def test_parse_mixed_datetime_unexpected_exception(
    mock_logger, mock_log_and_exit_fixture, mocker
):
    """Tests the main exception handler in date parsing."""
    mocker.patch("pandas.to_datetime", side_effect=Exception("Unexpected error"))
    with pytest.raises(SystemExit):
        parse_mixed_datetime(mock_logger, "any-date", [])
    mock_log_and_exit_fixture.assert_called_once()
    assert (
        "Unexpected error parsing date string"
        in mock_log_and_exit_fixture.call_args[0][1]
    )


@pytest.mark.parametrize(
    "date_str, formats, expected_date",
    [
        ("23-01-2024", ["%d-%m-%Y"], datetime.datetime(2024, 1, 23)),
        ("01/23/2024", ["%m/%d/%Y"], datetime.datetime(2024, 1, 23)),
        ("23-Jan-2024", ["%d-%b-%Y"], datetime.datetime(2024, 1, 23)),
        ("23/01/24", [], datetime.datetime(2024, 1, 23)),  # Fallback to pandas
        ("invalid-date", [], None),
        (None, [], None),
    ],
)
def test_parse_mixed_datetime(mock_logger, date_str, formats, expected_date):
    """Tests the standalone date parsing helper."""
    assert parse_mixed_datetime(mock_logger, date_str, formats) == expected_date
