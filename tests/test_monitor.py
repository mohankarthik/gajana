import datetime
import json
import os

import pytest

from src import monitor


@pytest.fixture(autouse=True)
def _state_in_tmp(tmp_path, monkeypatch):
    """Redirect the monitor state file into a tmp dir so tests never touch the
    real data/state/monitor_state.json."""
    monkeypatch.setattr(
        monitor, "STATE_FILE", str(tmp_path / "state" / "monitor_state.json")
    )


TODAY = datetime.date(2026, 7, 3)


def _dt(y, m, d):
    return datetime.datetime(y, m, d)


def test_days_since_handles_none_and_bad_input():
    assert monitor.days_since(None) is None
    assert monitor.days_since("not-a-date") is None
    assert monitor.days_since("2026-07-01", today=TODAY) == 2


def test_record_pdf_fetch_advances_only_on_new_pdf():
    monitor.record_pdf_fetch(0, today=TODAY)
    state = monitor.load_state()
    assert state["last_fetch_attempt"] == TODAY.isoformat()
    assert "last_new_pdf_date" not in state

    monitor.record_pdf_fetch(2, today=TODAY)
    state = monitor.load_state()
    assert state["last_new_pdf_date"] == TODAY.isoformat()
    assert state["last_new_pdf_count"] == 2


def test_save_state_creates_directory():
    monitor.save_state({"a": 1})
    assert os.path.exists(monitor.STATE_FILE)
    with open(monitor.STATE_FILE) as f:
        assert json.load(f) == {"a": 1}


def test_health_up_when_fresh():
    monitor.record_pdf_fetch(1, today=TODAY)
    is_up, msg = monitor.evaluate_health(
        pipeline_error=None,
        new_pdf_count=1,
        latest_txn_date=_dt(2026, 7, 2),
        today=TODAY,
    )
    assert is_up is True
    assert "latest txn 2026-07-02" in msg


def test_health_down_on_pipeline_error():
    monitor.record_pdf_fetch(1, today=TODAY)
    is_up, msg = monitor.evaluate_health(
        pipeline_error="boom",
        new_pdf_count=1,
        latest_txn_date=_dt(2026, 7, 2),
        today=TODAY,
    )
    assert is_up is False
    assert "boom" in msg


def test_health_down_on_stale_pdf():
    # Last new PDF was 60 days ago (> STALE_DAYS) and none fetched now.
    monitor.record_pdf_fetch(1, today=TODAY - datetime.timedelta(days=60))
    is_up, msg = monitor.evaluate_health(
        pipeline_error=None,
        new_pdf_count=0,
        latest_txn_date=_dt(2026, 7, 2),
        today=TODAY,
    )
    assert is_up is False
    assert "no new statement in 60d" in msg


def test_health_down_on_stale_txn():
    monitor.record_pdf_fetch(1, today=TODAY)
    is_up, msg = monitor.evaluate_health(
        pipeline_error=None,
        new_pdf_count=1,
        latest_txn_date=_dt(2026, 4, 1),  # ~93 days old
        today=TODAY,
    )
    assert is_up is False
    assert "latest txn" in msg and "old" in msg


def test_health_down_when_no_txns():
    monitor.record_pdf_fetch(1, today=TODAY)
    is_up, msg = monitor.evaluate_health(
        pipeline_error=None,
        new_pdf_count=1,
        latest_txn_date=None,
        today=TODAY,
    )
    assert is_up is False
    assert "no transactions in log" in msg


def test_push_noop_when_url_empty(monkeypatch):
    called = {"n": 0}

    def _fail_get(*a, **k):  # pragma: no cover - must not be called
        called["n"] += 1
        raise AssertionError("should not be called")

    monkeypatch.setattr(monitor.requests, "get", _fail_get)
    monitor.push("", True, "msg")  # empty url -> no HTTP
    assert called["n"] == 0


def test_push_merges_into_existing_query(monkeypatch):
    captured = {}

    class _Resp:
        def raise_for_status(self):
            pass

    def _get(url, timeout=None):
        captured["url"] = url
        return _Resp()

    monkeypatch.setattr(monitor.requests, "get", _get)
    # URL already carries a Kuma template query; must not double the '?'.
    monitor.push("https://kuma/api/push/abc?status=up&msg=OK&ping=", False, "boom down")
    url = captured["url"]
    assert url.count("?") == 1
    assert "status=down" in url
    assert "msg=boom+down" in url
    assert "status=up" not in url


def test_push_swallows_request_errors(monkeypatch):
    def _raise(*a, **k):
        raise monitor.requests.RequestException("down")

    monkeypatch.setattr(monitor.requests, "get", _raise)
    # Must not raise despite the request failing.
    monitor.push("http://kuma/push/abc", False, "boom")
