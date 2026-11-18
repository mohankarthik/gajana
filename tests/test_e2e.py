"""End-to-end tests for Gajana using CSVDataSource, Golden Files, and dual-mode LLM."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any, Callable, List

# Ensure standalone runs (e.g. `python -m tests.test_e2e --record-llm`) use
# the committed test settings before importing any `src.*` module.
os.environ.setdefault(
    "GAJANA_SETTINGS_FILE",
    os.path.join(os.path.dirname(__file__), "fixtures", "settings.test.json"),
)

from src.constants import (  # noqa: E402
    BANK_TRANSACTIONS_SHEET_NAME,
    CC_TRANSACTIONS_SHEET_NAME,
)

LIVE_GATEWAY_SOCKET = "/root/.local/state/agentic/gateway/llm.sock"
RECORDED_FIXTURE_PATH = os.path.join(
    os.path.dirname(__file__), "fixtures", "e2e", "recorded_llm.json"
)
_RECORD_PROXY_ENV = "GAJANA_E2E_RECORD_SOCKET"


def prompt_schema_sha256() -> str:
    """Compute SHA-256 over the PDF extraction and categorizer system prompts."""
    from src.llm_categorizer import LLMCategorizer
    from src.pdf_parser import _EXTRACTION_PROMPT

    cat_instance = LLMCategorizer.__new__(LLMCategorizer)
    cat_prompt = cat_instance._build_system_prompt(
        ["Groceries", "Dining", "Shopping", "Utilities"], {}
    )
    combined = f"{_EXTRACTION_PROMPT}\n---\n{cat_prompt}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def _strip_json_fences(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    return stripped


class _UnixGatewayServer:
    """Minimal in-process AF_UNIX newline-delimited JSON gateway server."""

    def __init__(
        self, sock_path: Path, handler: Callable[[dict[str, Any]], dict[str, Any]]
    ):
        self.sock_path = sock_path
        self.handler = handler
        self.requests: list[dict[str, Any]] = []
        self._srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._srv.bind(str(sock_path))
        self._srv.listen(8)
        self._stop = False
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop:
            try:
                conn, _ = self._srv.accept()
            except OSError:
                break
            with conn:
                data = b""
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                    if b"\n" in data:
                        break
                if not data.strip():
                    continue
                req = json.loads(data.splitlines()[0].decode("utf-8"))
                self.requests.append(req)
                resp = self.handler(req)
                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))

    def close(self) -> None:
        self._stop = True
        try:
            self._srv.close()
        except OSError:
            pass
        self._thread.join(timeout=2.0)


def _forward_unix_request(
    target_sock: Path, req: dict[str, Any], timeout_s: float = 300.0
) -> dict[str, Any]:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout_s)
        sock.connect(str(target_sock))
        sock.sendall((json.dumps(req, ensure_ascii=False) + "\n").encode("utf-8"))
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        chunks: list[bytes] = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            nl = chunk.find(b"\n")
            if nl != -1:
                chunks.append(chunk[: nl + 1])
                break
            chunks.append(chunk)
        return json.loads(b"".join(chunks).decode("utf-8"))
    finally:
        sock.close()


def _build_replay_handler(
    recorded: dict[str, Any],
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    pdf_text = str(recorded["pdf_extraction_text"])
    pdf_data = json.loads(_strip_json_fences(pdf_text))
    categorizer_text = str(recorded["categorizer_text"])
    models = recorded.get("recorded_models", {})

    def handler(req: dict[str, Any]) -> dict[str, Any]:
        op = req.get("op")
        if op == "ping":
            return {"ok": True, "service": "llm_gateway", "op": "ping"}
        if op == "complete":
            title = req.get("title", "")
            is_pdf = (
                title in ("gajana_pdf", "gajana_pdf_text")
                or bool(req.get("attachments"))
                or req.get("response_format") == "json"
            )
            if is_pdf:
                return {
                    "ok": True,
                    "data": pdf_data,
                    "text": pdf_text,
                    "meta": {"model": models.get("pdf", ""), "family": "gemini"},
                }
            return {
                "ok": True,
                "text": categorizer_text,
                "meta": {"model": models.get("categorizer", ""), "family": "claude"},
            }
        return {"ok": False, "error_code": "bad_request", "error": f"unknown op {op!r}"}

    return handler


class TestGajanaE2E(unittest.TestCase):
    def setUp(self):
        """Set up a temporary working directory."""
        self.repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.fixtures_dir = os.path.join(self.repo_dir, "tests", "fixtures", "e2e")
        self.working_dir = tempfile.mkdtemp(prefix="gajana_e2e_")

        # Create statements dir as expected by CSVDataSource
        self.statements_work_dir = os.path.join(self.working_dir, "statements")
        os.makedirs(self.statements_work_dir)

    def tearDown(self):
        """Clean up the temporary working directory."""
        if os.path.exists(self.working_dir):
            shutil.rmtree(self.working_dir)

    def _run_main(self, args: List[str], env: dict[str, str] | None = None):
        """Runs main.py with the provided arguments."""
        cmd = [sys.executable, "main.py", "--csv-db-path", self.working_dir] + args
        return subprocess.run(
            cmd, capture_output=True, text=True, cwd=self.repo_dir, env=env
        )

    def _compare_csv_files(self, actual_path, expected_path):
        """Compares two CSV files row by row."""
        with open(actual_path, "r", encoding="utf-8") as f_act, open(
            expected_path, "r", encoding="utf-8"
        ) as f_exp:
            actual_lines = f_act.readlines()
            expected_lines = f_exp.readlines()

            self.assertEqual(
                actual_lines,
                expected_lines,
                f"CSV mismatch between {actual_path} and {expected_path}",
            )

    def _is_live_mode(self) -> bool:
        return os.environ.get("AGENTIC_LIVE_LLM_E2E") == "1"

    def _is_record_mode(self) -> bool:
        return bool(os.environ.get(_RECORD_PROXY_ENV))

    def _load_recorded_fixture(self) -> dict[str, Any]:
        with open(RECORDED_FIXTURE_PATH, "r", encoding="utf-8") as f:
            recorded = json.load(f)
        self.assertEqual(
            recorded.get("prompt_schema_sha256"),
            prompt_schema_sha256(),
            "recorded_llm.json prompt_schema_sha256 is stale; re-run "
            "`./venv/bin/python -m tests.test_e2e --record-llm`",
        )
        return recorded

    def _setup_llm_mode(
        self, default_caller: str
    ) -> tuple[_UnixGatewayServer | None, dict[str, str]]:
        """Configure environment and optional replay server for LLM E2E tests."""
        saved_env = {
            k: os.environ.get(k)
            for k in ("AGENTIC_LLM_SOCKET", "LLM_GATEWAY_SOCKET", "AGENTIC_CALLER")
        }

        def _restore():
            for k, v in saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

        self.addCleanup(_restore)

        if self._is_record_mode():
            sock_path = os.environ[_RECORD_PROXY_ENV]
            os.environ["AGENTIC_LLM_SOCKET"] = sock_path
            os.environ["LLM_GATEWAY_SOCKET"] = sock_path
            os.environ["AGENTIC_CALLER"] = default_caller
            return None, os.environ.copy()

        if self._is_live_mode():
            sock_path = os.environ.get("AGENTIC_LLM_SOCKET") or LIVE_GATEWAY_SOCKET
            os.environ["AGENTIC_LLM_SOCKET"] = sock_path
            os.environ["LLM_GATEWAY_SOCKET"] = sock_path
            os.environ["AGENTIC_CALLER"] = (
                os.environ.get("AGENTIC_CALLER") or "e2e-test"
            )
            return None, os.environ.copy()

        recorded = self._load_recorded_fixture()
        replay_sock = Path(self.working_dir) / "llm.sock"
        srv = _UnixGatewayServer(replay_sock, _build_replay_handler(recorded))
        self.addCleanup(srv.close)
        os.environ["AGENTIC_LLM_SOCKET"] = str(replay_sock)
        os.environ["LLM_GATEWAY_SOCKET"] = str(replay_sock)
        os.environ["AGENTIC_CALLER"] = default_caller
        return srv, os.environ.copy()

    def test_e2e_csv_flow(self):
        """
        Runs the E2E test for the CSV statement pathway.
        """
        bank_initial = os.path.join(self.fixtures_dir, "bank_initial.csv")
        cc_initial = os.path.join(self.fixtures_dir, "cc_initial.csv")

        bank_work_path = os.path.join(
            self.working_dir, f"{BANK_TRANSACTIONS_SHEET_NAME}.csv"
        )
        cc_work_path = os.path.join(
            self.working_dir, f"{CC_TRANSACTIONS_SHEET_NAME}.csv"
        )

        shutil.copy(bank_initial, bank_work_path)
        shutil.copy(cc_initial, cc_work_path)

        statements_fixture_dir = os.path.join(self.fixtures_dir, "statements")
        for filename in os.listdir(statements_fixture_dir):
            if filename.endswith(".csv"):
                shutil.copy(
                    os.path.join(statements_fixture_dir, filename),
                    os.path.join(self.statements_work_dir, filename),
                )

        result = self._run_main(["--update"])
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)

        self.assertEqual(result.returncode, 0, "main.py failed to run")

        bank_expected = os.path.join(self.fixtures_dir, "bank_expected.csv")
        cc_expected = os.path.join(self.fixtures_dir, "cc_expected.csv")

        self._compare_csv_files(bank_work_path, bank_expected)
        self._compare_csv_files(cc_work_path, cc_expected)

    def test_e2e_pdf_flow(self):
        """
        Runs the E2E test for the PDF statement pathway (replay by default, live when AGENTIC_LIVE_LLM_E2E=1).
        """
        _srv, env = self._setup_llm_mode(default_caller="e2e-test")

        bank_initial = os.path.join(self.fixtures_dir, "bank_initial.csv")
        cc_initial = os.path.join(self.fixtures_dir, "cc_initial.csv")

        bank_work_path = os.path.join(
            self.working_dir, f"{BANK_TRANSACTIONS_SHEET_NAME}.csv"
        )
        cc_work_path = os.path.join(
            self.working_dir, f"{CC_TRANSACTIONS_SHEET_NAME}.csv"
        )

        shutil.copy(bank_initial, bank_work_path)
        shutil.copy(cc_initial, cc_work_path)

        statements_fixture_dir = os.path.join(self.fixtures_dir, "statements")
        shutil.copy(
            os.path.join(statements_fixture_dir, "bank-axis-primary-2026-05.pdf"),
            os.path.join(self.statements_work_dir, "bank-axis-primary-2026-05.pdf"),
        )
        shutil.copy(
            os.path.join(statements_fixture_dir, "cc-axis-neo-2026-05.csv"),
            os.path.join(self.statements_work_dir, "cc-axis-neo-2026-05.csv"),
        )

        result = self._run_main(["--update"], env=env)
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)

        self.assertEqual(result.returncode, 0, "main.py failed to run")

        bank_expected = os.path.join(self.fixtures_dir, "bank_expected.csv")
        cc_expected = os.path.join(self.fixtures_dir, "cc_expected.csv")

        self._compare_csv_files(bank_work_path, bank_expected)
        self._compare_csv_files(cc_work_path, cc_expected)

    def test_e2e_llm_categorizer_flow(self):
        """
        Runs the E2E test for LLMCategorizer (categorization, cache hit, and caller tag).
        """
        from src.llm_categorizer import LLMCategorizer

        srv, _env = self._setup_llm_mode(default_caller="gajana")
        cache_file = os.path.join(self.working_dir, "llm_category_cache.json")
        categorizer = LLMCategorizer(cache_file=cache_file, enabled=True)
        self.assertTrue(categorizer._client_ready)

        txns = [
            {
                "description": "SWIGGY ORDER BANGALORE",
                "amount": -450.0,
                "account": "CC",
            },
            {
                "description": "BIGBASKET GROCERIES",
                "amount": -1200.0,
                "account": "Bank",
            },
        ]
        allowed_categories = ["Groceries", "Dining & Food", "Utilities", "Shopping"]

        res_first = categorizer.classify(txns, allowed_categories)
        self.assertEqual(
            res_first,
            {
                0: ("Dining & Food", "llm"),
                1: ("Groceries", "llm"),
            },
        )

        if srv is not None:
            complete_reqs = [r for r in srv.requests if r.get("op") == "complete"]
            self.assertEqual(len(complete_reqs), 1)
            self.assertEqual(complete_reqs[0].get("caller"), "gajana")

        req_count_before_second = len(srv.requests) if srv is not None else 0
        res_second = categorizer.classify(txns, allowed_categories)
        self.assertEqual(
            res_second,
            {
                0: ("Dining & Food", "llm-cache"),
                1: ("Groceries", "llm-cache"),
            },
        )
        if srv is not None:
            self.assertEqual(len(srv.requests), req_count_before_second)


def _record_llm_fixtures() -> int:
    """Run live PDF and categorizer flows against host llm.sock and save recorded_llm.json."""
    target_sock = Path(os.environ.get("AGENTIC_LLM_SOCKET") or LIVE_GATEWAY_SOCKET)
    if not target_sock.exists():
        print(
            f"ERROR: live LLM gateway socket not found: {target_sock}", file=sys.stderr
        )
        return 1

    tmp_dir = Path(tempfile.mkdtemp(prefix="gajana_record_llm_"))
    captured: dict[str, Any] = {
        "recorded_models": {},
        "pdf_extraction_text": None,
        "categorizer_text": None,
    }

    def proxy_handler(req: dict[str, Any]) -> dict[str, Any]:
        resp = _forward_unix_request(target_sock, req)
        if req.get("op") == "complete" and resp.get("ok"):
            title = req.get("title", "")
            meta = resp.get("meta") or {}
            if title in ("gajana_pdf", "gajana_pdf_text") or req.get("attachments"):
                captured["recorded_models"]["pdf"] = str(meta.get("model") or "")
                captured["pdf_extraction_text"] = (
                    resp.get("text")
                    if resp.get("text") is not None
                    else json.dumps(resp.get("data"))
                )
            elif title == "gajana_categorize":
                captured["recorded_models"]["categorizer"] = str(
                    meta.get("model") or ""
                )
                captured["categorizer_text"] = str(resp.get("text") or "")
        return resp

    proxy_sock = tmp_dir / "record.sock"
    srv = _UnixGatewayServer(proxy_sock, proxy_handler)
    old_proxy_env = os.environ.get(_RECORD_PROXY_ENV)
    os.environ[_RECORD_PROXY_ENV] = str(proxy_sock)
    try:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestGajanaE2E)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        if not result.wasSuccessful():
            return 1
    finally:
        if old_proxy_env is None:
            os.environ.pop(_RECORD_PROXY_ENV, None)
        else:
            os.environ[_RECORD_PROXY_ENV] = old_proxy_env
        srv.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)

    if not captured["pdf_extraction_text"] or not captured["categorizer_text"]:
        print(
            f"ERROR: incomplete capture during --record-llm: {captured}",
            file=sys.stderr,
        )
        return 1

    payload = {
        "prompt_schema_sha256": prompt_schema_sha256(),
        "recorded_models": captured["recorded_models"],
        "pdf_extraction_text": captured["pdf_extraction_text"],
        "categorizer_text": captured["categorizer_text"],
    }
    os.makedirs(os.path.dirname(RECORDED_FIXTURE_PATH), exist_ok=True)
    with open(RECORDED_FIXTURE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"Saved recorded LLM fixtures to {RECORDED_FIXTURE_PATH}")
    print(f"Recorded models: {payload['recorded_models']}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Gajana E2E test suite")
    parser.add_argument(
        "--record-llm",
        action="store_true",
        help="Run against live host llm.sock and record tests/fixtures/e2e/recorded_llm.json",
    )
    args, remaining = parser.parse_known_args()
    if args.record_llm:
        sys.exit(_record_llm_fixtures())
    unittest.main(argv=[sys.argv[0]] + remaining)


if __name__ == "__main__":
    main()
