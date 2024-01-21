from operator import itemgetter
import os.path

import httplib2
import sys
import json
from difflib import SequenceMatcher
import datetime
import logging
import time
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_KEY_FILE = 'secrets/google.json'
CSV_FOLDER = '1DwJGCYydYikP7eWxMWD6mA84Mj7fO7-3'
TRANSACTIONS_SHEET_ID = '1I1NkOf2L5hVB6_yV896x9H-s1CIsRYWTR2T0ioBZDZU'
BANK_TRANSACTIONS_RANGE = 'Bank transactions!B3:H'
AXIS_BANK_STATEMENT_RANGE = 'transactions!A19:G'
HDFC_BANK_STATEMENT_RANGE = 'transactions!A4:G'

class GoogleWrapper:
  def __init__(self) -> None:
    self.creds = self._get_credential()
    self.drive_service = self._get_drive_service()
    self.sheets_service = self._get_sheets_service()
    self.statement_files = self._get_statement_files()
  
  def get_old_bank_txns(self) -> list[dict]:
    values = self._get_sheet_data(TRANSACTIONS_SHEET_ID, BANK_TRANSACTIONS_RANGE)

    txns = []
    for row in values:
      txns.append({
        "date": datetime.datetime.strptime(row[0], "%Y-%m-%d"),
        "description": row[1],
        "amount": self._parse_amount(row[3]) - self._parse_amount(row[2]),
        "category": row[4],
        "remarks": row[5],
        "account": row[6]
      })
    txns = sorted(txns, key=itemgetter('date', 'account', 'amount', 'description'))
    logging.info(f"Found total of {len(txns)} bank transactions already processed with latest date of {txns[-1]['date']}")
    return txns
  
  def get_all_bank_txns(self) -> list[dict]:
    txns = []
    for file in self.statement_files:
      if 'bank-axis-karti' in file['name']:
        txns += self._get_axis_bank_txns(file['id'], 'bank-axis-karti')
      elif 'bank-axis-mini' in file['name']:
        txns += self._get_axis_bank_txns(file['id'], 'bank-axis-mini')
      elif 'bank-hdfc-karti' in file['name']:
        txns += self._get_hdfc_bank_txns(file['id'], 'bank-hdfc-karti')
    txns = sorted(txns, key=itemgetter('date', 'account', 'amount', 'description'))
    logging.info(f"Found total of {len(txns)} bank transactions in CSV statements with latest date of {txns[-1]['date']}")
    return txns
  
  def add_new_bank_txns(self, txns: list[dict]) -> None:
    values = []
    for txn in txns:
      debit = ''
      credit = ''
      if txn['amount'] < 0:
        debit = str(-txn['amount'])
      else:
        credit = str(txn['amount'])
      values.append([txn['date'].strftime("%Y-%m-%d"), txn['description'], debit, credit, txn['category'], '', txn['account']])
    
    self._update_sheet_data(TRANSACTIONS_SHEET_ID, BANK_TRANSACTIONS_RANGE, values)

  def _get_axis_bank_txns(self, sheet_id: str, account_name: str) -> list[dict]:
    txns = []
    values = self._get_sheet_data(sheet_id, AXIS_BANK_STATEMENT_RANGE)
    for row in values:
      if len(row) != 7:
        continue
      txns.append({
        "date": datetime.datetime.strptime(row[0], "%d-%m-%Y"),
        "description": row[2],
        "amount": self._parse_amount(row[4]) - self._parse_amount(row[3]),
        "category": None,
        "remarks": None,
        "account": account_name
      })
    return txns
  
  def _get_hdfc_bank_txns(self, sheet_id: str, account_name: str) -> list[dict]:
    txns = []
    values = self._get_sheet_data(sheet_id, HDFC_BANK_STATEMENT_RANGE)
    for row in values:
      if len(row) != 7:
        continue
      txns.append({
        "date": datetime.datetime.strptime(row[0], "%d/%m/%y"),
        "description": row[1],
        "amount": self._parse_amount(row[4]) - self._parse_amount(row[3]),
        "category": None,
        "remarks": None,
        "account": account_name
      })
    return txns

  def _get_credential(self) -> ServiceAccountCredentials:
    """Creates a Credential object with the correct OAuth2 authorization.

    Uses the service account key stored in SERVICE_ACCOUNT_KEY_FILE.

    Returns:
      Credentials, the user's credential.
    """
    credential = ServiceAccountCredentials.from_json_keyfile_name(
      SERVICE_ACCOUNT_KEY_FILE, SCOPES)

    if not credential or credential.invalid:
      print('Unable to authenticate using service account key.')
      sys.exit()
    return credential
  
  def _get_drive_service(self) -> any:
    return build("drive", "v3", credentials=self.creds)
  
  def _get_sheets_service(self) -> any:
    return build("sheets", "v4", credentials=self.creds)
  
  def _get_statement_files(self) -> list:
    files = []
    page_token = None
    while True:
      response = (
        self.drive_service.files()
        .list(
          q=f"parents in '{CSV_FOLDER}'",
          spaces="drive",
          fields="nextPageToken, files(id, name)",
          pageToken=page_token,
        )
        .execute()
      )
      for file in response.get("files", []):
        logging.debug(f'Found file: {file.get("name")}, {file.get("id")}')
      files.extend(response.get("files", []))
      page_token = response.get("nextPageToken", None)
      if page_token is None:
        break
    logging.info(f'Found a total of {len(files)} statement files')
    return files
  
  def _get_sheet_data(self, sheet_id: str, range: str, retry_count: int = 0) -> list:
    if retry_count > 3:
      raise SystemError(f'Unable to update sheet after {retry_count} attempts')
    sheet = self.sheets_service.spreadsheets()
    try:
      result = (
          sheet.values()
          .get(spreadsheetId=sheet_id, range=range)
          .execute()
      )
      values = result.get("values", [])
      assert(values)
      return values
    except:
      logging.warning(f'Hit API resource limits, waiting for 1 minute and retrying')
      time.sleep(60)
      return self._get_sheet_data(sheet_id, range, retry_count + 1)
    
  def _update_sheet_data(self, spreadsheet_id: str, range_name: str, values: list, value_input_option: str="USER_ENTERED", retry_count: int=0) -> None:
    if retry_count > 3:
      raise SystemError(f'Unable to update sheet after {retry_count} attempts')
    try:
      body = {"values": values}
      result = (
          self.sheets_service.spreadsheets()
          .values()
          .append(
              spreadsheetId=spreadsheet_id,
              range=range_name,
              valueInputOption=value_input_option,
              body=body,
          )
          .execute()
      )
      logging.info(f"{(result.get('updates').get('updatedCells'))} cells appended.")
      return result
    except:
      logging.warning(f'Failed to update sheet, retrying')
      time.sleep(60)
      return self._update_sheet_data(spreadsheet_id, range_name, values, value_input_option, retry_count+1)

  @staticmethod
  def _parse_amount(value: str) -> float:
    if value == '':
      return 0
    
    return float(value.replace(',', ''))

class TransactionMatcher:
  @staticmethod
  def _is_txn_same(txn_a: dict, txn_b: dict) -> bool:
    return (txn_a['date'] == txn_b['date']) and (txn_a['account'] == txn_b['account']) and (txn_a['amount'] == txn_b['amount']) and SequenceMatcher(None, txn_a['description'].lower(), txn_b['description'].lower()).ratio() > 0.5

  @staticmethod
  def _is_ignored_txn(txn: dict) -> bool:
    if 'ANALOG DE' in txn['description'] and txn['category'] != 'Reversal':
      return True
    if 'GOOGLE IT' in txn['description'] and txn['category'] != 'Reversal':
      return True
    return False

  @staticmethod
  def find_new_txns(old_txns: list[dict], all_txns: list[dict]) -> list[dict]:
    missing_txns = []
    old_idx = 0
    all_idx = 0
    while (old_idx < len(old_txns) and all_idx < len(all_txns)):
      if all_txns[all_idx]['date'] > old_txns[old_idx]['date']:
        # Skip past old transactions that no longer exist in the CSVs
        old_idx += 1
        continue
      if TransactionMatcher._is_txn_same(old_txns[old_idx], all_txns[all_idx]):
        old_idx += 1
        all_idx += 1
        continue
      if TransactionMatcher._is_ignored_txn(old_txns[old_idx]):
        old_idx += 1
        continue
      if TransactionMatcher._is_ignored_txn(all_txns[all_idx]):
        all_idx += 1
        continue
      missing_txns.append(all_txns[all_idx])
      all_idx += 1

    logging.info(f"Found total of {len(missing_txns)} missing bank transactions")
    for txn in missing_txns:
      print(txn)

    new_txns = all_txns[all_idx:]
    logging.info(f"Found total of {len(new_txns)} new bank transactions")
    return missing_txns, new_txns

class Categorizer:
  def __init__(self) -> None:
    self.matchers = None
    with open("data/matchers.json", "r") as f:
      self.matchers = json.load(f)

  def categorize(self, txns: list[dict]) -> list[dict]:
    for txn in txns:
      found = False
      is_debit = txn['amount'] < 0
      txn_description = txn['description'].lower()
      for matcher in self.matchers:
        if found:
          break
        if ('debit' in matcher) and ((matcher['debit'] and not is_debit) or (not matcher['debit'] and is_debit)):
          continue
        if ('account' in matcher) and (matcher['account'] not in txn['account']):
          continue
        for matcher_description in matcher['description']:
          if matcher_description.lower() in txn_description:
            found = True
            txn['category'] = matcher['category']
            logging.info(f"Found matcher {matcher} for transaction {txn}")
            break
      if not found:
        logging.warning(f"Could not categorize transaction {txn}")
    return txns

def main():
  """Shows basic usage of the Drive Activity API.

  Prints information about the last 10 events that occured the user's Drive.
  """
  logging.basicConfig(level=logging.INFO)
  google_stub = GoogleWrapper()
  categorizer = Categorizer()
  old_bank_txns = google_stub.get_old_bank_txns()
  all_bank_txns = google_stub.get_all_bank_txns()
  missing_txns, new_txns = TransactionMatcher.find_new_txns(old_bank_txns, all_bank_txns)
  new_txns = categorizer.categorize(new_txns)
  google_stub.add_new_bank_txns(new_txns)


if __name__ == "__main__":
  main()