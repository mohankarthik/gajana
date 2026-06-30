"""Comprehensive E2E for the layered categorizer (exact/rule/fuzzy/review).

Runs the full main.py pipeline over CSV statements crafted so each new
transaction exercises a specific categorization layer, then validates the
output against golden files AND the per-source counts reported in the logs.
LLM layer stays off (default) so the run is fully deterministic.
"""

import os
import re
import shutil
import subprocess
import sys
import unittest
from typing import List

from src.constants import BANK_TRANSACTIONS_SHEET_NAME, CC_TRANSACTIONS_SHEET_NAME


class TestGajanaE2ELayers(unittest.TestCase):
    def setUp(self):
        self.fixtures_dir = "/root/gajana/tests/fixtures/e2e_layers"
        self.working_dir = "/tmp/gajana_e2e_layers_work"
        if os.path.exists(self.working_dir):
            shutil.rmtree(self.working_dir)
        os.makedirs(os.path.join(self.working_dir, "statements"))

    def tearDown(self):
        if os.path.exists(self.working_dir):
            shutil.rmtree(self.working_dir)

    def _run_main(self, args: List[str]):
        cmd = [sys.executable, "main.py", "--csv-db-path", self.working_dir] + args
        return subprocess.run(cmd, capture_output=True, text=True, cwd="/root/gajana")

    def _compare(self, actual_path, expected_path):
        with open(actual_path, encoding="utf-8") as a, open(
            expected_path, encoding="utf-8"
        ) as e:
            self.assertEqual(
                a.readlines(),
                e.readlines(),
                f"CSV mismatch: {actual_path} vs {expected_path}",
            )

    def test_layered_flow(self):
        bank_work = os.path.join(
            self.working_dir, f"{BANK_TRANSACTIONS_SHEET_NAME}.csv"
        )
        cc_work = os.path.join(self.working_dir, f"{CC_TRANSACTIONS_SHEET_NAME}.csv")
        shutil.copy(os.path.join(self.fixtures_dir, "bank_initial.csv"), bank_work)
        shutil.copy(os.path.join(self.fixtures_dir, "cc_initial.csv"), cc_work)
        for fn in os.listdir(os.path.join(self.fixtures_dir, "statements")):
            if fn.endswith(".csv"):
                shutil.copy(
                    os.path.join(self.fixtures_dir, "statements", fn),
                    os.path.join(self.working_dir, "statements", fn),
                )

        result = self._run_main(["--update"])
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)
        self.assertEqual(result.returncode, 0, "main.py failed to run")

        # Golden comparison
        self._compare(bank_work, os.path.join(self.fixtures_dir, "bank_expected.csv"))
        self._compare(cc_work, os.path.join(self.fixtures_dir, "cc_expected.csv"))

        # Per-layer assertions from the source-count log lines.
        sources = re.findall(r"By source: (\{[^}]*\})", result.stderr + result.stdout)
        joined = " ".join(sources)
        self.assertIn("'exact'", joined, "exact-lookup layer never fired")
        self.assertIn("'rule'", joined, "rule layer never fired")
        self.assertIn("'fuzzy'", joined, "fuzzy layer never fired")
        # Two transactions are genuinely novel -> flagged for review.
        self.assertEqual(result.stderr.count("flagged for review"), 2)


if __name__ == "__main__":
    unittest.main()
