import json
import logging
import os
import sys

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
TOKEN_PATH = os.path.join(os.path.dirname(__file__), "secrets", "telegram.json")
STATE_PATH = os.path.join(
    os.path.dirname(__file__), "data", "state", "telegram_bot_state.json"
)


def _load_token() -> str:
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return str(data.get("token") or data.get("bot_token") or "").strip()
    return os.environ.get("TELEGRAM_BOT_TOKEN", "")


def main() -> None:
    """Poll Telegram once and record any new cash entries.

    Runs on a 1-minute cron (see crontab). No webhook / exposed port.
    """
    # Unconfigured is a clean no-op, not an error: the 1-minute cron can ship
    # before the bot is set up without spamming failures.
    if not os.path.exists(SETTINGS_PATH):
        logger.info(f"{SETTINGS_PATH} not found; telegram bot not configured yet.")
        return
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        settings = json.load(f)

    token = _load_token()
    if not token:
        logger.info("No Telegram bot token yet (secrets/telegram.json); skipping.")
        return

    ds = GoogleDataSource()
    bot = TelegramCashBot(ds, settings, token, STATE_PATH)
    handled = bot.run_once()
    if handled:
        logger.info(f"Telegram bot recorded {handled} cash entr(y/ies).")


if __name__ == "__main__":
    main()
