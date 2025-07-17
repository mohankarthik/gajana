# tests/conftest.py
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def mock_settings_fixture(mocker) -> MagicMock:
    """
    Automatically mocks the singleton 'settings' object from the ConfigManager
    for all test functions.

    This fixture creates a mock that can be configured on a per-test basis
    to simulate different settings from settings.ini.
    """
    # Create a mock object that simulates the ConfigManager instance
    mock_settings = MagicMock()

    # Set default attributes that the application expects
    mock_settings.bank_accounts = []
    mock_settings.cc_accounts = []
    mock_settings.parser_configs = {}

    # Mock the get_setting method
    def get_setting_side_effect(section, key):
        # Default return values for common settings to avoid errors
        if key == "sheets_id":
            return "dummy_sheets_id"
        if key == "drive_folder_id":
            return "dummy_drive_folder_id"
        # Add other defaults as needed
        return f"dummy_{section}_{key}"

    mock_settings.get_setting.side_effect = get_setting_side_effect

    # Patch the settings object where it is defined, so all modules
    # that import it will get this mock instead.
    mocker.patch("src.config_manager.settings", mock_settings)

    return mock_settings
