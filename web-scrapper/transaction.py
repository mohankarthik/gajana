import datetime


class Transaction:
    def __init__(
        self,
        date: str,
        description: str,
        credit: str,
        debit: str,
        balance: str,
        account: str,
    ) -> None:
        self.date: datetime.datetime = datetime.datetime.strptime(
            date.strip(), "%d-%m-%Y"
        )
        self.description: str = description.strip()
        self.credit: float = float(credit) if credit.strip() != "" else None
        self.debit: float = float(debit) if debit.strip() != "" else None
        self.balance: float = float(balance) if balance.strip() != "" else None
        self.account: str = account.strip()
