import os

import pytest

# Point every test (and any main.py subprocess they spawn) at a committed,
# non-personal settings file so the suite never depends on the user's real
# settings.json / account names. Set at import time, before src.settings loads.
os.environ.setdefault(
    "GAJANA_SETTINGS_FILE",
    os.path.join(os.path.dirname(__file__), "fixtures", "settings.test.json"),
)


@pytest.fixture(autouse=True)
def _disable_gemini_throttle():
    """Disable the PDF parser's primary-model rate-limit throttle in tests so
    multi-call paths don't sleep."""
    os.environ["GAJANA_GEMINI_MIN_INTERVAL"] = "0"
    yield
