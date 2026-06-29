"""End-to-end tests for Gajana using CSVDataSource and Golden Files."""

import os
import shutil
import unittest
import subprocess
import sys
from typing import List

from src.constants import BANK_TRANSACTIONS_SHEET_NAME, CC_TRANSACTIONS_SHEET_NAME


class TestGajanaE2E(unittest.TestCase):
    def setUp(self):
        """Set up a temporary working directory."""
        self.fixtures_dir = "/root/gajana/tests/fixtures/e2e"
        self.working_dir = "/tmp/gajana_e2e_work"
        if os.path.exists(self.working_dir):
            shutil.rmtree(self.working_dir)
        os.makedirs(self.working_dir)

        # Create statements dir as expected by CSVDataSource
        self.statements_work_dir = os.path.join(self.working_dir, "statements")
        os.makedirs(self.statements_work_dir)

    def tearDown(self):
        """Clean up the temporary working directory."""
        if os.path.exists(self.working_dir):
            shutil.rmtree(self.working_dir)

    def _run_main(self, args: List[str]):
        """Runs main.py with the provided arguments."""
        cmd = [sys.executable, "main.py", "--csv-db-path", self.working_dir] + args
        result = subprocess.run(cmd, capture_output=True, text=True, cwd="/root/gajana")
        return result

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

    def test_e2e_csv_flow(self):
        """
        Runs the E2E test for the CSV statement pathway.
        """
        # 1. Setup initial state by copying fixtures
        bank_initial = os.path.join(self.fixtures_dir, "bank_initial.csv")
        cc_initial = os.path.join(self.fixtures_dir, "cc_initial.csv")

        # CSVDataSource expects files named after sheet names
        bank_work_path = os.path.join(
            self.working_dir, f"{BANK_TRANSACTIONS_SHEET_NAME}.csv"
        )
        cc_work_path = os.path.join(
            self.working_dir, f"{CC_TRANSACTIONS_SHEET_NAME}.csv"
        )

        shutil.copy(bank_initial, bank_work_path)
        shutil.copy(cc_initial, cc_work_path)

        # Copy ONLY CSV statement fixtures
        statements_fixture_dir = os.path.join(self.fixtures_dir, "statements")
        for filename in os.listdir(statements_fixture_dir):
            if filename.endswith(".csv"):
                shutil.copy(
                    os.path.join(statements_fixture_dir, filename),
                    os.path.join(self.statements_work_dir, filename),
                )

        # 2. Run the entire main.py in --update mode
        result = self._run_main(["--update"])

        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)

        self.assertEqual(result.returncode, 0, "main.py failed to run")

        # 3. Compare against expected output
        bank_expected = os.path.join(self.fixtures_dir, "bank_expected.csv")
        cc_expected = os.path.join(self.fixtures_dir, "cc_expected.csv")

        self._compare_csv_files(bank_work_path, bank_expected)
        self._compare_csv_files(cc_work_path, cc_expected)

    def test_e2e_pdf_flow(self):
        """
        Runs the E2E test for the PDF statement pathway (live LLM call).
        """
        from src.pdf_parser import configure_api_keys, has_any_api_key

        configure_api_keys()
        if not has_any_api_key():
            raise unittest.SkipTest(
                "No LLM API keys configured (GEMINI_API_KEY or ANTHROPIC_API_KEY). Skipping live LLM E2E test."
            )

        # 1. Setup initial state by copying fixtures
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

        # Copy the PDF statement and credit card CSV statement
        statements_fixture_dir = os.path.join(self.fixtures_dir, "statements")
        shutil.copy(
            os.path.join(statements_fixture_dir, "bank-axis-karti-2026-05.pdf"),
            os.path.join(self.statements_work_dir, "bank-axis-karti-2026-05.pdf"),
        )
        shutil.copy(
            os.path.join(statements_fixture_dir, "cc-axis-magnus-2026-05.csv"),
            os.path.join(self.statements_work_dir, "cc-axis-magnus-2026-05.csv"),
        )

        # 2. Run the entire main.py in --update mode
        result = self._run_main(["--update"])

        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)

        self.assertEqual(result.returncode, 0, "main.py failed to run")

        # 3. Compare against expected output
        bank_expected = os.path.join(self.fixtures_dir, "bank_expected.csv")
        cc_expected = os.path.join(self.fixtures_dir, "cc_expected.csv")

        self._compare_csv_files(bank_work_path, bank_expected)
        self._compare_csv_files(cc_work_path, cc_expected)


if __name__ == "__main__":
    unittest.main()
