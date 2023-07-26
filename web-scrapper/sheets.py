import httplib2
import os

from apiclient import discovery
from google.oauth2 import service_account
from dotenv import load_dotenv
from mf_transaction import MutualFundTransaction
from transaction import Transaction
import logging
from datetime import datetime
from utils import sanitize_currency

class Sheets:
  def __init__(self, mf_sheet_id: str, bank_sheet_id: str) -> None:
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"] 
    secret_file = os.path.join(os.getcwd(), 'google_secret.json')
    if not os.path.exists(secret_file):
      logging.error("Secret file missing")
      raise RuntimeError()
    
    credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
    self._service = discovery.build('sheets', 'v4', credentials=credentials)
    self._mf_sheet_id = mf_sheet_id
    self._bank_sheet_id = bank_sheet_id

  def get_mf_transactions(self) -> list[MutualFundTransaction]:
    result: list[MutualFundTransaction] = []
    for row in self._service.spreadsheets().values().get(spreadsheetId=self._mf_sheet_id, range="MF Transactions!B2:M").execute().get('values', []):
      result.append(MutualFundTransaction(
        scheme=row[1].strip(),
        date=datetime.strptime(row[2].strip(), "%Y-%m-%d"),
        units=sanitize_currency(row[3]),
        nav=sanitize_currency(row[4]),
        value=sanitize_currency(row[5]),
        stamp_duty=sanitize_currency(row[6]),
        type=row[7].strip(),
        folio=row[8].strip(),
        account=row[9].strip()
      ))
    return result
  
  def get_bank_transactions(self) -> list[Transaction]:
    result: list[Transaction] = []
    for row in self._service.spreadsheets().values().get(spreadsheetId=self._bank_sheet_id, range="Bank Transactions!B3:J").execute().get('values', []):
      result.append(Transaction(
        date=datetime.strptime(row[2].strip(), "%Y-%m-%d"),
        units=sanitize_currency(row[3]),
        nav=sanitize_currency(row[4]),
        value=sanitize_currency(row[5]),
        stamp_duty=sanitize_currency(row[6]),
        type=row[7].strip(),
        folio=row[8].strip(),
        account=row[9].strip()
      ))
    return result
    
  
if __name__ == "__main__":
  load_dotenv()
  stub = Sheets(os.getenv("SHEETS_MF_ID"), os.getenv("SHEETS_BANK_ID"))
  stub.get_mf_transactions()