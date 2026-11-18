#!/usr/bin/env bash
set -euo pipefail

if [ "${SKIP_LIVE_LLM_E2E:-0}" = "1" ]; then
  exit 0
fi

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

RUNNER="/root/.local/share/agentic/deploy/automations/e2e_live_llm.py"
if [ -f "$RUNNER" ]; then
  exec python3 "$RUNNER" --suite gajana --for-push "$ROOT" HEAD
fi

exec env AGENTIC_LIVE_LLM_E2E=1 ./venv/bin/pytest -q tests/test_e2e.py
