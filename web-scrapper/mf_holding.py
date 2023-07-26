from datetime import datetime

class MutualFundHolding:
  def __init__(self, account: str, date: str, scheme: str, fund: str, category: str, folio: str, units: float, nav: float, value: float) -> None:
    self._date = datetime.strptime(date.strip(), "%d-%m-%Y")
    self._account = account.strip()
    self._scheme = scheme.strip()
    self._fund = fund.strip()
    self._category = category.strip()
    self._folio = folio.strip()
    self._units = units
    self._nav = nav
    self._value = value
