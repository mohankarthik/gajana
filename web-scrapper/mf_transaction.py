from datetime import datetime
from datetime import timedelta

class MutualFundTransaction:
  def __init__(self, account: str, date: datetime, scheme: str, folio: str, type: str, units: float, nav: float, value: float, stamp_duty: float) -> None:
    self.account = account
    self.date = date
    self.scheme = scheme
    self.folio = folio
    self.type = type
    self.units = units
    self.nav = nav
    self.value = value
    self.stamp_duty = stamp_duty
