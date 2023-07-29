from web import Web
from selenium.webdriver.common.by import By
from datetime import datetime
from datetime import timedelta
import time
import os
from constants import DOWNLOAD_DIR
from dotenv import load_dotenv
import xlrd
from mf_holding import MutualFundHolding
from mf_transaction import MutualFundTransaction
from typing import Any

MFUTILITY_URL = "https://www.mfuonline.com/"
MFUTILITY_HOLDING = DOWNLOAD_DIR + "CANHoldingReport.xls"
MFUTILITY_NORMAL_ORDER = DOWNLOAD_DIR + "NormalOrderBook.xls"


class MFUtility:
    def __init__(
        self, username: str, password: str, txn_password: str, config: dict[str, Any]
    ) -> None:
        self._username = username
        self._password = password
        self._txn_password = txn_password
        self._config = config
        self._holding: list[MutualFundHolding] = []
        self._orders: list[MutualFundTransaction] = []

    def update(self) -> None:
        web = Web()
        web.navigate(MFUTILITY_URL)

        if os.path.exists(MFUTILITY_HOLDING):
            os.remove(MFUTILITY_HOLDING)
        if os.path.exists(MFUTILITY_NORMAL_ORDER):
            os.remove(MFUTILITY_NORMAL_ORDER)

        web.get_element(By.ID, "loginid").send_keys(self._username)
        web.get_element(By.ID, "password").send_keys(self._password)
        web.click_element(By.ID, "submitButton")

        web.get_element(By.ID, "txnPassword").send_keys(self._txn_password)
        web.click_element(By.ID, "cnfrmBtn")

        # Export holding
        web.click_element(
            By.XPATH, "/html/body/div[1]/div[7]/div[4]/div[1]/div/div[1]/div[3]/div[3]"
        )
        web.click_element(By.ID, "exportSection")
        time.sleep(2)

        # Navigate to Normal Order Book
        web.click_element(By.XPATH, "/html/body/div[1]/div[6]/nav/div/ul/li[4]")
        web.click_element(
            By.XPATH, "/html/body/div[1]/div[6]/nav/div/ul/li[4]/ul/li[1]/a"
        )

        # Download data since last 30 days
        start_date = datetime.now() - timedelta(days=29)
        date = web.get_element(By.ID, "startDateIdDisp")
        date.clear()
        date.send_keys(start_date.strftime("%d-%m-%Y"))
        web.click_element(
            By.XPATH,
            "/html/body/div[1]/div[7]/form[1]/div[1]/div[1]/div[1]/table/tbody/tr/td[7]/button",
        )
        time.sleep(2)
        web.click_element(By.ID, "exportSection")
        time.sleep(2)

        # clean up
        del web

        # Parse the downloaded data
        self._parse_holding()
        self._parse_orders()

    def _parse_holding(self):
        if os.path.exists(MFUTILITY_HOLDING):
            book = xlrd.open_workbook(MFUTILITY_HOLDING)
            sheet = book.sheet_by_index(0)

            index = -1
            while True:
                index += 1
                can_name = sheet.cell_value(rowx=index, colx=0)
                if can_name == "":
                    break

                if not [conf for conf in self._config if conf["id"] in can_name]:
                    continue

                self._holding.append(
                    MutualFundHolding(
                        account=[
                            conf for conf in self._config if conf["id"] in can_name
                        ][0]["name"],
                        fund=sheet.cell_value(rowx=index, colx=1),
                        scheme=sheet.cell_value(rowx=index, colx=4),
                        category=sheet.cell_value(rowx=index, colx=5),
                        folio=sheet.cell_value(rowx=index, colx=6),
                        units=sheet.cell_value(rowx=index, colx=8),
                        nav=sheet.cell_value(rowx=index, colx=9),
                        date=sheet.cell_value(rowx=index, colx=10),
                        value=sheet.cell_value(rowx=index, colx=11),
                    )
                )

    def _parse_orders(self):
        if os.path.exists(MFUTILITY_NORMAL_ORDER):
            book = xlrd.open_workbook(MFUTILITY_NORMAL_ORDER)
            sheet = book.sheet_by_index(0)

            index = -1
            while True:
                index += 1
                try:
                    can_id = sheet.cell_value(rowx=index, colx=0)
                except:
                    break
                if can_id == None or can_id == "":
                    break

                if can_id == "CAN":
                    continue

                if sheet.cell_value(rowx=index, colx=12) != "RTA Processed":
                    continue

                self._orders.append(
                    MutualFundTransaction(
                        account=[conf for conf in self._config if conf["id"] == can_id][
                            0
                        ]["name"],
                        type=sheet.cell_value(rowx=index, colx=4),
                        folio=sheet.cell_value(rowx=index, colx=5),
                        scheme=sheet.cell_value(rowx=index, colx=8),
                        units=sheet.cell_value(rowx=index, colx=15),
                        value=sheet.cell_value(rowx=index, colx=16),
                        nav=sheet.cell_value(rowx=index, colx=17),
                        date=datetime.strptime("01-01-1900", "%d-%m-%Y")
                        + timedelta(days=sheet.cell_value(rowx=index, colx=18)),
                        stamp_duty=sheet.cell_value(rowx=index, colx=21),
                        fund_house=None,
                        sanitize_name=True,
                    )
                )


if __name__ == "__main__":
    load_dotenv()
    stub = MFUtility(
        os.getenv("MFU_USERNAME"),
        os.getenv("MFU_PASSWORD"),
        os.getenv("MFU_TXN_PASSWORD"),
        [
            {
                "id": os.getenv(f"MFU_CAN_ID{idx+1}"),
                "name": os.getenv(f"MFU_CAN_NAME{idx+1}"),
            }
            for idx in range(int(os.getenv("MFU_CAN_COUNT")))
        ],
    )
    # stub.update()
    stub._parse_holding()
    stub._parse_orders()
    input("Press Enter to continue")
    del stub
