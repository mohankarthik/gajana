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
def _isolate_llm_gateway_socket(monkeypatch, tmp_path_factory):
    """Point LLM_GATEWAY_SOCKET / AGENTIC_LLM_SOCKET at a non-existent path inside
    tmp_path_factory unless explicitly overridden so offline pytest never touches
    /run/agentic/llm.sock or ~/.local/state/agentic/gateway/llm.sock."""
    sock_dir = tmp_path_factory.mktemp("llm_gateway")
    dead_sock = str(sock_dir / "llm.sock")
    if not os.environ.get("GAJANA_LIVE_LLM_SOCKET"):
        monkeypatch.setenv("LLM_GATEWAY_SOCKET", dead_sock)
        monkeypatch.setenv("AGENTIC_LLM_SOCKET", dead_sock)
        monkeypatch.setenv("AGENTIC_STATE_DIR", str(sock_dir / "state"))
    yield
