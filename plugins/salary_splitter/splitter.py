"""Google salary splitter.

For a given month:
  1. Find and parse that month's payslip PDF (Drive).
  2. Fill the payslip-derived input cells of the "Google Salary" sheet.
  3. Read back that sheet's computed bottom block (Total Salary, Total Tax,
     Equity, ...) — the sheet's own formulas do the tax/equity math.
  4. Guard: the bottom block's "Total In Hand" must equal the payslip Net Pay.
  5. Append the split rows to the gajana bank ledger, categorized.

The raw single-line salary NEFT is kept out of the ledger by gajana's
ignore-list (data/ignore.json), so this plugin is the sole writer of
Google-salary ledger rows. See README.md.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

from src.transaction_processor import TransactionProcessor

from .payslip_parser import parse_payslip

logger = logging.getLogger(__name__)

# The split rows must reconcile to the payslip's cash Net Pay. Tolerance is a
# fraction of net pay, sized to absorb GSU share-count/price rounding in the
# payslip during vesting months (see README / project memory).
GUARD_TOLERANCE_FRACTION = 0.001  # 0.1% of net pay


class SalarySplitError(Exception):
    """Raised when a month cannot be split safely (parse/guard failure)."""


def _col_to_num(letter: str) -> int:
    num = 0
    for ch in letter.strip().upper():
        num = num * 26 + (ord(ch) - ord("A") + 1)
    return num


def _num_to_col(num: int) -> str:
    letters = ""
    while num > 0:
        num, rem = divmod(num - 1, 26)
        letters = chr(ord("A") + rem) + letters
    return letters


class SalarySplitter:
    def __init__(self, data_source: Any, settings: dict[str, Any]):
        self.ds = data_source
        self.s = settings
        self.sheets = data_source.sheets_service
        self.processor = TransactionProcessor(data_source)
        self.month_col_start = settings.get("month_col_start", "B")

    # --- Drive ---------------------------------------------------------------
    def find_payslip(self, ym: str) -> tuple[str, str]:
        """Returns (file_id, name) for the ``YYYY-MM.pdf`` payslip, or raises."""
        folder = self.s["payslip_folder_id"]
        query = (
            f"'{folder}' in parents and trashed=false and " "mimeType='application/pdf'"
        )
        resp = (
            self.ds.drive_service.files()
            .list(q=query, fields="files(id,name)", pageSize=200)
            .execute()
        )
        target = f"{ym}.pdf"
        for f in resp.get("files", []):
            if f["name"] == target:
                return f["id"], f["name"]
        raise SalarySplitError(f"No payslip named {target} in Drive folder {folder}.")

    def list_payslip_months(self) -> list[str]:
        """All ``YYYY-MM`` months that have a payslip PDF, newest first."""
        folder = self.s["payslip_folder_id"]
        query = (
            f"'{folder}' in parents and trashed=false and " "mimeType='application/pdf'"
        )
        resp = (
            self.ds.drive_service.files()
            .list(q=query, fields="files(name)", pageSize=200)
            .execute()
        )
        months = []
        for f in resp.get("files", []):
            name = f["name"]
            if name.endswith(".pdf"):
                stem = name[:-4]
                if len(stem) == 7 and stem[4] == "-" and stem[:4].isdigit():
                    months.append(stem)
        return sorted(set(months), reverse=True)

    # --- Sheet #1: Google Salary --------------------------------------------
    def _label_rows(self, year: int) -> dict[str, int]:
        """Maps a row label in column A to its 1-based row number."""
        rng = f"{year}!A1:A80"
        result = (
            self.sheets.spreadsheets()
            .values()
            .get(spreadsheetId=self.s["salary_sheet_id"], range=rng)
            .execute()
        )
        rows = result.get("values", [])
        mapping: dict[str, int] = {}
        for i, row in enumerate(rows):
            if row and str(row[0]).strip():
                mapping.setdefault(str(row[0]).strip(), i + 1)
        return mapping

    def _month_col(self, month: int) -> str:
        return _num_to_col(_col_to_num(self.month_col_start) + month - 1)

    def _eval_expr(self, expr: str, fields: dict[str, float]) -> float:
        return float(eval(expr, {"__builtins__": {}}, dict(fields)))  # noqa: S307

    def fill_salary_sheet(self, ym: str, fields: dict[str, Any]) -> None:
        year, month = int(ym[:4]), int(ym[5:7])
        label_rows = self._label_rows(year)
        col = self._month_col(month)
        numeric = {k: v for k, v in fields.items() if isinstance(v, (int, float))}

        data = []
        for label, expr in self.s["input_row_map"].items():
            if label not in label_rows:
                raise SalarySplitError(
                    f"Row label '{label}' not found in {year} tab of salary sheet."
                )
            value = self._eval_expr(expr, numeric)
            cell = f"{year}!{col}{label_rows[label]}"
            data.append({"range": cell, "values": [[round(value)]]})
            logger.info(f"Salary sheet {cell} = {round(value)} ({label})")

        self.sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=self.s["salary_sheet_id"],
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

    def read_bottom_block(self, ym: str) -> dict[str, float]:
        """Reads the computed values in the month's column, label → number."""
        year, month = int(ym[:4]), int(ym[5:7])
        rng = f"{year}!A1:AB80"
        result = (
            self.sheets.spreadsheets()
            .values()
            .get(
                spreadsheetId=self.s["salary_sheet_id"],
                range=rng,
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
        )
        idx = _col_to_num(self.month_col_start) - 2 + month
        block: dict[str, float] = {}
        for row in result.get("values", []):
            if not row or not str(row[0]).strip():
                continue
            label = str(row[0]).strip()
            if len(row) > idx and isinstance(row[idx], (int, float)):
                block[label] = float(row[idx])
        return block

    def _gsu_rounding_residual(self, block: dict[str, float]) -> float:
        """Payslip GSU-value rounding: gross GSU value carried into income minus
        the credited + taxed GSU values deducted. Zero when there is no
        ``gsu_rounding`` config or the month has no vesting rows."""
        cfg = self.s.get("gsu_rounding")
        if not cfg:
            return 0.0
        return (
            float(block.get(cfg["gross"], 0.0))
            - float(block.get(cfg["credited"], 0.0))
            - float(block.get(cfg["taxed"], 0.0))
        )

    # --- Sheet #2: gajana ledger --------------------------------------------
    def _synthetic_desc(self, ym: str) -> str:
        mon = datetime.datetime.strptime(ym + "-01", "%Y-%m-%d").strftime("%b-%y")
        return f"Google Salary {mon} (auto-split from payslip)"

    def already_split(self, ym: str) -> bool:
        desc = self._synthetic_desc(ym)
        raw = self.ds.get_transaction_log_data("bank")
        for row in raw[1:]:
            if len(row) >= 2 and str(row[1]).strip() == desc:
                return True
        return False

    def build_split_txns(
        self, ym: str, block: dict[str, float], pay_date: datetime.datetime
    ) -> list[dict[str, Any]]:
        desc = self._synthetic_desc(ym)
        account = self.s["salary_account"]
        txns = []
        for label, spec in self.s["split_map"].items():
            if label not in block:
                raise SalarySplitError(
                    f"Split source row '{label}' missing from salary sheet block."
                )
            magnitude = block[label]
            if abs(magnitude) < 0.5:
                continue  # skip zero-value components (e.g. Equity in off months)
            signed = magnitude if spec["sign"] == "credit" else -magnitude
            txns.append(
                {
                    "date": pay_date,
                    "description": desc,
                    "amount": float(signed),
                    "category": spec["category"],
                    "remarks": "",
                    "account": account,
                }
            )
        return txns

    # --- Orchestration -------------------------------------------------------
    def run(self, ym: str | None = None, dry_run: bool = False) -> None:
        if ym is None:
            for candidate in self.list_payslip_months():
                if not self.already_split(candidate):
                    ym = candidate
                    break
            if ym is None:
                logger.info("No unsplit payslip months found. Nothing to do.")
                return
            logger.info(f"Auto-selected month {ym}.")

        if self.already_split(ym):
            logger.info(f"{ym} already split into the ledger. Skipping.")
            return

        file_id, name = self.find_payslip(ym)
        logger.info(f"Processing payslip {name} ({file_id}).")
        pdf_bytes = self.ds.download_file(file_id)
        fields = parse_payslip(pdf_bytes)

        self.fill_salary_sheet(ym, fields)
        block = self.read_bottom_block(ym)

        net_pay = float(fields["net_pay"])
        pay_date = self._resolve_pay_date(ym, fields)
        txns = self.build_split_txns(ym, block, pay_date)

        # Guard: the split rows (what lands in the ledger) must reconcile to the
        # payslip's cash Net Pay. The only legitimate discrepancy is GSU-value
        # rounding in the payslip -- "GSU Value" carries into income at full
        # value while "Credited GSU Value" + "Taxed GSU Value" (the deductions)
        # round independently, so they differ by a few hundred to a few thousand
        # rupees that scales with GSU size, not net pay. Subtract that exact
        # residual, then the cash side must reconcile within a tight tolerance;
        # a missing or mis-signed component is far larger and still trips.
        split_sum = sum(t["amount"] for t in txns)
        gsu_residual = self._gsu_rounding_residual(block)
        cash_diff = (split_sum - gsu_residual) - net_pay
        tolerance = max(1.0, GUARD_TOLERANCE_FRACTION * abs(net_pay))
        if abs(cash_diff) > tolerance:
            raise SalarySplitError(
                f"Net-pay guard failed for {ym}: split rows sum to "
                f"{split_sum:.0f}, GSU rounding residual {gsu_residual:.0f}, "
                f"cash-reconciled {split_sum - gsu_residual:.0f} != payslip Net "
                f"Pay {net_pay:.0f} (diff {cash_diff:.0f}, tolerance "
                f"{tolerance:.0f}). No ledger rows written."
            )

        if dry_run:
            logger.info(
                f"[DRY RUN] Guard OK (cash-reconciled "
                f"{split_sum - gsu_residual:.0f} vs net pay {net_pay:.0f}, GSU "
                f"residual {gsu_residual:.0f}). Salary sheet was filled; ledger "
                f"NOT written. Planned {len(txns)} split rows for {ym}:"
            )
            for t in txns:
                logger.info(
                    f"[DRY RUN]   {t['date']:%Y-%m-%d}  {t['amount']:>12.2f}  "
                    f"{t['category']:<32}  {t['account']}"
                )
            return

        logger.info(
            f"Guard OK (cash-reconciled {split_sum - gsu_residual:.0f} vs net "
            f"pay {net_pay:.0f}, GSU residual {gsu_residual:.0f}). Appending "
            f"{len(txns)} split rows for {ym}."
        )
        self.processor.add_new_transactions_to_log(txns, "bank")
        logger.info(f"Salary split for {ym} complete.")

    def _resolve_pay_date(self, ym: str, fields: dict[str, Any]) -> datetime.datetime:
        raw = str(fields.get("date_of_payment", "")).strip()
        for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y"):
            try:
                return datetime.datetime.strptime(raw, fmt)
            except ValueError:
                continue
        logger.warning(
            f"Could not parse date_of_payment '{raw}' for {ym}; "
            "falling back to month-end."
        )
        year, month = int(ym[:4]), int(ym[5:7])
        if month == 12:
            return datetime.datetime(year, 12, 31)
        return datetime.datetime(year, month + 1, 1) - datetime.timedelta(days=1)
