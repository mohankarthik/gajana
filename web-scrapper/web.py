from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
import time

class Web:
  def __init__(self) -> None:
    chromedriver_autoinstaller.install()
    self._driver = webdriver.Chrome()
    # self._driver.maximize_window()
    time.sleep(1)
  
  def __del__(self) -> None:
    self._driver.close()
    self._driver.quit()
    
  def navigate(self, url: str) -> None:
    self._driver.get(url)
    
  def get_element(self, by: By, value: str) -> WebElement:
    return WebDriverWait(self._driver, timeout=3).until(lambda d: d.find_element(by, value))

  def click_element(self, by: By, value: str) -> None:
    WebDriverWait(self._driver, timeout=3).until(EC.element_to_be_clickable((by, value))).click()
