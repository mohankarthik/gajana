from axis_bank import AxisBank
from mfutility import MFUtility
from sheets import Sheets
from dotenv import load_dotenv
import os

def update_mf(sheet_stub: Sheets) -> None:
  # Get existing transactions
  existing_mf_transactions = sheet_stub.get_mf_transactions()

  # Get updated data from MF Utility
  mf_stub = MFUtility(
    os.getenv("MFU_USERNAME"),
    os.getenv("MFU_PASSWORD"),
    os.getenv("MFU_TXN_PASSWORD"),
    [{
      "id": os.getenv(f"MFU_CAN_ID{idx+1}"),
      "name": os.getenv(f"MFU_CAN_NAME{idx+1}")
    } for idx in range(int(os.getenv("MFU_CAN_COUNT")))]
  )
  #mf_stub.update()
  mf_stub._parse_orders()

  # Get new MF transactions and push it to sheets
  new_mf_txns = [txn for txn in mf_stub._orders if not txn.is_present(existing_mf_transactions)]
  sheet_stub.update_mf_transactions(new_mf_txns)

if __name__ == "__main__":
  load_dotenv()

  # Instantiate link to sheets
  sheet_stub = Sheets(
    os.getenv("SHEETS_MF_ID"),
    os.getenv("SHEETS_BANK_ID"),
  )

  # update_mf(sheet_stub)



