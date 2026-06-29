import os

import pytest


@pytest.fixture(autouse=True)
def _disable_gemini_throttle():
    """Disable the PDF parser's primary-model rate-limit throttle in tests so
    multi-call paths don't sleep."""
    os.environ["GAJANA_GEMINI_MIN_INTERVAL"] = "0"
    yield
