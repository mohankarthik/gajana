# gajana/config_manager.py
from __future__ import annotations

import configparser
import json
import logging
import os
from typing import Any, Dict, List, Optional

from src.constants import CONFIG_DIR, SETTINGS_FILE
from src.utils import log_and_exit

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Handles loading and providing access to all user-specific configurations
    from settings.ini and the parser config JSON files.
    """

    def __init__(self, settings_path: str = SETTINGS_FILE):
        self.config = configparser.ConfigParser()
        self.config.read(settings_path)
        logger.info(f"Successfully loaded settings from '{settings_path}'.")

        self.parser_configs = self._load_parsing_config()
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
        return [item.strip() for item in raw_value.split(",")]

    def _load_parsing_config(self) -> Dict[str, Any]:
        """Loads all .json parsing configurations from the specified directory."""
        loaded_config = {}
        logger.info(f"Loading parsing configs from: {CONFIG_DIR}")
        if not os.path.exists(CONFIG_DIR):
            log_and_exit(
                logger, f"Parser configuration directory not found: {CONFIG_DIR}"
            )

        for filename in os.listdir(CONFIG_DIR):
            if filename.endswith(".json"):
                config_key = filename[:-5]
                file_path = os.path.join(CONFIG_DIR, filename)
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


# --- Singleton Accessor Function ---
_settings_instance: Optional[ConfigManager] = None


def get_settings() -> ConfigManager:
    """
    Returns the singleton instance of the ConfigManager.
    The instance is created on the first call.
    """
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = ConfigManager()
    return _settings_instance
