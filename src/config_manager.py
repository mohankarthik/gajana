# src/config_manager.py
from __future__ import annotations
import configparser, json, logging, os
from typing import Any, Dict, List, Optional
from src.utils import log_and_exit

logger = logging.getLogger(__name__)
SETTINGS_FILE = "settings.ini"

class ConfigManager:
    def __init__(self, settings_path: str = SETTINGS_FILE):
        self.config = configparser.ConfigParser()
        self.config.read(settings_path)
        # ... (rest of the class implementation from previous steps) ...
        #gcp
        self.service_account_key_file = self.config['gcp']['service_account_key_file']
        self.sheets_id = self.config['gcp']['sheets_id']
        self.drive_folder_id = self.config['gcp']['drive_folder_id']
        #database
        self.db_file_path = self.config['database']['db_file_path']
        #data
        self.parser_configs_dir = self.config['data']['parser_configs_dir']
        self.matchers_file = self.config['data']['matchers_file']
        #accounts
        self.bank_accounts = self.config['accounts']['bank_accounts'].split()
        self.cc_accounts = self.config['accounts']['cc_accounts'].split()


# --- Singleton Accessor Function ---
_settings_instance: Optional[ConfigManager] = None

def get_settings() -> ConfigManager:
    """Returns the singleton instance of the ConfigManager."""
    global _settings_instance
    if _settings_instance is None:
        if not os.path.exists(SETTINGS_FILE):
             # This path triggers the setup flow in main.py
            log_and_exit(logger, f"Settings file '{SETTINGS_FILE}' not found. Please run the setup using `python src/main.py --setup`.")
        _settings_instance = ConfigManager()
    return _settings_instance
