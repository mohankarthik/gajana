# gajana/config_manager.py
from __future__ import annotations

import configparser
import json
import logging
import os
from typing import Any, Dict, List

from src.constants import CONFIG_DIR, SETTINGS_FILE
from src.utils import log_and_exit

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Handles loading and providing access to all user-specific configurations
    from settings.ini and the parser config JSON files.
    """

    def __init__(self, settings_path: str = SETTINGS_FILE):
        if not os.path.exists(settings_path):
            log_and_exit(
                logger,
                f"Configuration file '{settings_path}' not found. "
                "Please copy 'settings.example.ini' to 'settings.ini' and fill it out, "
                "or run the setup.",
            )

        self.config = configparser.ConfigParser()
        self.config.read(settings_path)
        logger.info(f"Successfully loaded settings from '{settings_path}'.")

        # Load parser configurations dynamically
        self.parser_configs = self._load_parsing_config()

        # Load and parse account lists
        self.bank_accounts = self._parse_list_setting("accounts", "bank_accounts")
        self.cc_accounts = self._parse_list_setting("accounts", "cc_accounts")

    def get_setting(self, section: str, key: str) -> str:
        """Safely gets a setting from the loaded configuration."""
        try:
            return self.config.get(section, key)
        except (configparser.NoSectionError, configparser.ParsingError) as e:
            log_and_exit(
                logger,
                f"Missing required setting in '{SETTINGS_FILE}': Section='{section}', Key='{key}'",
                e,
            )
            raise

    def _parse_list_setting(self, section: str, key: str) -> List[str]:
        """Parses a comma-separated string from settings into a list of strings."""
        raw_value = self.get_setting(section, key)
        if not raw_value:
            return []
        # Split by comma and strip whitespace from each item
        return [item.strip() for item in raw_value.split(",")]

    def _load_parsing_config(self) -> Dict[str, Any]:
        """Loads all .json parsing configurations from the specified directory."""
        config_dir = CONFIG_DIR
        loaded_config = {}
        logger.info(f"Loading parsing configs from: {config_dir}")
        if not os.path.exists(config_dir):
            log_and_exit(
                logger, f"Parser configuration directory not found: {config_dir}"
            )

        for filename in os.listdir(config_dir):
            if filename.endswith(".json"):
                config_key = filename[:-5]
                file_path = os.path.join(config_dir, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        loaded_config[config_key] = json.load(f)
                    logger.debug(f"Successfully loaded parser config: {config_key}")
                except (json.JSONDecodeError, IOError) as e:
                    log_and_exit(
                        logger,
                        f"Failed to load or parse config file {filename}: {e}",
                        e,
                    )

        logger.info(f"Loaded {len(loaded_config)} parsing configurations.")
        return loaded_config


# Create a singleton instance to be used throughout the application
settings = ConfigManager()
