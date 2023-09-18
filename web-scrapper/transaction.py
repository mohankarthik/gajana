import datetime


class Transaction:
    def __init__(
        self,
        date: datetime.datetime,
        description: str,
        credit: str,
        debit: str,
        category: str,
        account: str,
    ) -> None:
        self.date = date
        self.description = description
        self.credit = credit
        self.debit = debit
        self.category = category
        self.account = account
