from selenium.webdriver.common.by import By
import os
import time
import csv
import re
from web import Web
from transaction import Transaction
from constants import DOWNLOAD_DIR


AXIS_CSV_PATH= DOWNLOAD_DIR + "009010101527735.csv"
AXIS_HOME_URL="https://omni.axisbank.co.in/axisretailbanking/"

class AxisBank:
  def __init__(self, account: str, username: str, password: str) -> None:
    self._account = account
    self._username = username
    self._password = password
    self._data = []
  
  def update(self) -> None:
    # If csv already exists, then delete it
    if os.path.exists(AXIS_CSV_PATH):
      os.remove(AXIS_CSV_PATH)
      
    # Instantiate the web client
    web = Web()
    web.navigate(AXIS_HOME_URL)
    
    # Scrape    
    self._login()
    self._captcha()
    self._disable_popup()
    self._get_statements()
    
    # Wait for the download to finish
    self._wait_for_download()

    # Delete the web client, we no longer need it      
    del web
    
    # Parse the csv
    self._parse()
  
  def _parse(self) -> None:
    with open(AXIS_CSV_PATH, "r") as csv_file:
      reader = csv.reader(csv_file, delimiter=',')
      for row in reader:
        if len(row) > 1 and re.search(r'\d{2}\-\d{2}\-\d{4}', row[0]):
          self._data.append(Transaction(row[0], row[2], row[3], row[4], row[5], self._account))
    
    for row in self._data:
      print (row)
  
  def _wait_for_download(self) -> bool:
    timeout = 5 * 60
    start = time.time()

    while not os.path.exists(AXIS_CSV_PATH):
      if time.time() - start > timeout:
        break
    
    return os.path.exists(AXIS_CSV_PATH)
  
  def _login(self, web: Web) -> None:
    web.get_element(By.ID, "custid").send_keys(self._username)
    web.get_element(By.ID, "pass").send_keys(self._password)
    web.click_element(By.ID, "APLOGIN")
    
  def _captcha(self, web: Web) -> None:
    try:
      question = web.get_element(By.XPATH, "/html/body/app-root/mat-sidenav-container/mat-sidenav-content/div/app-homepage/div/div/div/div/app-s504/div/div/mat-card/mat-card-content/div/div[5]/div/div/div").text
    except:
      print("Captcha skipped ...")
      return
  
    print(question)
  
    elem = web.get_element(By.XPATH, "/html/body/app-root/mat-sidenav-container/mat-sidenav-content/div/app-homepage/div/div/div/div/app-s504/div/div/mat-card/mat-card-content/div/div[5]/div/div/div/div/mat-form-field/div/div[1]/div/input")
    if "In which year did you graduate from High School?" in question:
      elem.send_keys("2004")
    elif "What is your spouse`s name?" in question:
      elem.send_keys("mini")
    elif "What is your maternal grandmother`s name?" in question:
      elem.send_keys("kamala")
  
    web.click_element(By.XPATH, "/html/body/app-root/mat-sidenav-container/mat-sidenav-content/div/app-homepage/div/div/div/div/app-s504/div/div/mat-card/mat-card-content/div/div[6]/button[1]")

  def _disable_popup(self, web: Web) -> None:
    # If the amazon pop-up shows up, disable it
    try:
      web.click_element(By.ID, "wzrk-cancel")
    except:
      print("No pop-up")

  def _get_statements(self, web: Web) -> None:
    # Click on Account
    web.click_element(By.ID, "navList1")
    
    # Click on statements
    web.click_element(By.XPATH, '//*[@id="mat-tab-label-1-1"]/div')
    
    # Dropdown
    web.click_element(By.XPATH, '//*[@id="selectedValue"]')
    web.click_element(By.XPATH, '//*[@id="1_0"]')
    
    # Select last 3 months
    web.click_element(By.XPATH, '//*[@id="nextMonth2"]')
    
    # Dropdown to download
    web.click_element(By.XPATH, '//*[@id="topDownload"]')
    web.click_element(By.XPATH, '//*[@id="3_3"]')
    
    # Click on Go
    web.click_element(By.XPATH, '//*[@id="StatementInputFilter0"]')