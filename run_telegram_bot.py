import json
import logging
import os
import sys
from typing import Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("telegram_bot")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from plugins.telegram_bot.bot import TelegramCashBot  # noqa: E402
from src.google_data_source import GoogleDataSource  # noqa: E402

SETTINGS_PATH = os.path.join(
    os.path.dirname(__file__), "plugins", "telegram_bot", "settings.json"
)
STATE_PATH = os.path.join(
    os.path.dirname(__file__), "data", "state", "telegram_bot_state.json"
)


def _load_settings(settings_path: str = SETTINGS_PATH) -> dict[str, Any]:
    if not os.path.exists(settings_path):
        logger.info(f"{settings_path} not found; using default chat bridge settings.")
        return {}
    with open(settings_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def main() -> None:
    """Poll the host chat bridge once and record any new cash entries.

    Runs on a 1-minute cron (see crontab). No webhook / exposed port.
    """
    settings = _load_settings(SETTINGS_PATH)
    if settings.get("enabled") is False:
        logger.info("Finance chat bot disabled in settings; skipping.")
        return

    ds = GoogleDataSource()
    bot = TelegramCashBot(ds, settings, STATE_PATH)
    handled = bot.run_once()
    if handled:
        logger.info(f"Finance bot recorded {handled} cash entr(y/ies).")


if __name__ == "__main__":
    main()
