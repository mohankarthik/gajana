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

MFUTILITY_URL = "https://www.mfuonline.com/"
MFUTILITY_HOLDING = DOWNLOAD_DIR + "CANHoldingReport.xls"
MFUTILITY_NORMAL_ORDER = DOWNLOAD_DIR + "NormalOrderBook.xls"

class MFUtility:
  def __init__(self, username: str, password: str, txn_password: str) -> None:
    self._username = username
    self._password = password
    self._txn_password = txn_password
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
    web.click_element(By.XPATH, "/html/body/div[1]/div[7]/div[4]/div[1]/div/div[1]/div[3]/div[3]")
    web.click_element(By.ID, "exportSection")
    time.sleep(2)
    
    # Navigate to Normal Order Book
    web.click_element(By.XPATH, "/html/body/div[1]/div[6]/nav/div/ul/li[4]")
    web.click_element(By.XPATH, "/html/body/div[1]/div[6]/nav/div/ul/li[4]/ul/li[1]/a")
    
    # Download data since last 30 days
    start_date = datetime.now() - timedelta(days=29)
    date = web.get_element(By.ID, "startDateIdDisp")
    date.clear()
    date.send_keys(start_date.strftime("%d-%m-%Y"))
    web.click_element(By.XPATH, "/html/body/div[1]/div[7]/form[1]/div[1]/div[1]/div[1]/table/tbody/tr/td[7]/button")
    time.sleep(2)
    web.click_element(By.ID, "exportSection")
    time.sleep(2)
    
    # clean up
    del web
  
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
        
        if "ILA" not in can_name and "MOHAN" not in can_name:
          continue
        
        self._holding.append(MutualFundHolding(
          account="mfu-karti" if "MOHAN" in can_name else "mfu-joint",
          fund=sheet.cell_value(rowx=index, colx=1),
          scheme=sheet.cell_value(rowx=index, colx=4),
          category=sheet.cell_value(rowx=index, colx=5),
          folio=sheet.cell_value(rowx=index, colx=6),
          units=sheet.cell_value(rowx=index, colx=8),
          nav=sheet.cell_value(rowx=index, colx=9),
          date=sheet.cell_value(rowx=index, colx=10),
          value=sheet.cell_value(rowx=index, colx=11),
        ))
        
  def _parse_orders(self):
    if os.path.exists(MFUTILITY_NORMAL_ORDER):
      book = xlrd.open_workbook(MFUTILITY_NORMAL_ORDER)
      sheet = book.sheet_by_index(0)
      
      index = -1
      while True:
        index += 1
        can_id = sheet.cell_value(rowx=index, colx=0)
        if can_id == "":
          break
        
        if can_id == "CAN":
          continue
        
        if sheet.cell_value(rowx=index, colx=12) != "RTA Processed":
          continue
        
        self._orders.append(MutualFundTransaction(
          account="mfu-karti" if "MOHAN" in sheet.cell_value(rowx=index, colx=1) else "mfu-joint",
          type=sheet.cell_value(rowx=index, colx=4),
          folio=sheet.cell_value(rowx=index, colx=5),
          scheme=sheet.cell_value(rowx=index, colx=8),
          units=sheet.cell_value(rowx=index, colx=15),
          value=sheet.cell_value(rowx=index, colx=16),
          nav=sheet.cell_value(rowx=index, colx=17),
          time=datetime.strptime("01-01-1900", "%d-%m-%Y") + timedelta(days=sheet.cell_value(rowx=index, colx=18)),
          stamp_duty=sheet.cell_value(rowx=index, colx=21),
        ))


if __name__ == "__main__":
  load_dotenv()
  stub = MFUtility(os.getenv("MFU_USERNAME"), os.getenv("MFU_PASSWORD"), os.getenv("MFU_TXN_PASSWORD"))
  # stub.update()
  stub._parse_holding()
  stub._parse_orders()
  input("Press Enter to continue")
  del stub