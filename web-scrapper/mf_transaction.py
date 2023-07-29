from datetime import datetime
from typing import Self
import logging


FUND_HOUSES = [
    [
        "Axis Mutual Fund",
        [
            "Axis Liquid Fund - Direct Plan - Growth Option",
            "Axis Bluechip Fund - Direct Plan - Growth",
            "Axis Midcap Fund - Direct Plan - Growth",
        ],
    ],
    [
        "Canara Robeco Mutual Fund",
        [
            "CANARA ROBECO LIQUID FUND - DIRECT PLAN - GROWTH OPTION",
            "CANARA ROBECO BLUE CHIP EQUITY FUND - DIRECT PLAN - GROWTH OPTION",
        ],
    ],
    [
        "Invesco Mutual Fund",
        [
            "Invesco India Liquid Fund - Direct Plan - Growth",
            "Invesco India Growth Opportunities Fund - Direct Plan - Growth",
        ],
    ],
    [
        "Mirae Asset Mutual Fund",
        [
            "Mirae Asset Cash Management Fund - Direct Plan - Growth",
            "Mirae Asset Large Cap Fund - Direct Plan - Growth",
        ],
    ],
    [
        "Motilal Oswal Mutual Fund",
        [
            "Motilal Oswal Liquid Fund - Direct Growth",
            "Motilal Oswal Nifty Smallcap 250 Index Fund- Direct Plan",
        ],
    ],
    [
        "PPFAS Mutual Fund",
        [
            "Parag Parikh Liquid Fund- Direct Plan- Growth",
            "Parag Parikh Flexi Cap Fund - Direct Plan - Growth",
        ],
    ],
    [
        "ICICI Prudential Mutual Fund",
        [
            "ICICI Prudential Liquid Fund - Direct Plan - Growth",
            "ICICI Prudential Balanced Advantage Fund - Direct Plan -  Growth",
        ],
    ],
]

MATCHERS = [
    [
        "Axis Liquid Fund - Direct Growth",
        "Axis Liquid Fund - Direct Plan - Growth Option",
    ],
    [
        "Axis Bluechip Fund - Direct Plan - Growth",
        "Axis Bluechip Fund - Direct Plan - Growth",
    ],
    ["Axis Mid Cap Fund - Direct Growth", "Axis Midcap Fund - Direct Plan - Growth"],
    [
        "Canara Robeco Liquid Fund -Direct-Growth",
        "CANARA ROBECO LIQUID FUND - DIRECT PLAN - GROWTH OPTION",
    ],
    [
        "Canara Robeco Bluechip Equity Fund Direct Growth Growth",
        "CANARA ROBECO BLUE CHIP EQUITY FUND - DIRECT PLAN - GROWTH OPTION",
    ],
    [
        "Invesco India Liquid Fund - Direct Plan Growth",
        "Invesco India Liquid Fund - Direct Plan - Growth",
    ],
    [
        "Invesco India Growth Opportunities Fund- Direct Plan Growth",
        "Invesco India Growth Opportunities Fund - Direct Plan - Growth",
    ],
    [
        "Mirae Asset Cash Management Fund - Direct Plan - Growth",
        "Mirae Asset Cash Management Fund - Direct Plan - Growth",
    ],
    [
        "Mirae Asset Large Cap Fund Direct Plan Growth",
        "Mirae Asset Large Cap Fund - Direct Plan - Growth",
    ],
    [
        "Motilal Oswal Liquid Fund - Direct Growth",
        "Motilal Oswal Liquid Fund - Direct Growth",
    ],
    [
        "Motilal Oswal Nifty Smallcap 250 Index Fund-Direct Growth",
        "Motilal Oswal Nifty Smallcap 250 Index Fund- Direct Plan",
    ],
    [
        "Parag Parikh Liquid Fund Direct Plan Growth",
        "Parag Parikh Liquid Fund- Direct Plan- Growth",
    ],
    [
        "Parag Parikh Flexi Cap Fund-Direct-Growth",
        "Parag Parikh Flexi Cap Fund - Direct Plan - Growth",
    ],
    [
        "ICICI Prudential Liquid Fund -Direct-Growth",
        "ICICI Prudential Liquid Fund - Direct Plan - Growth",
    ],
    [
        "ICICI Prudential Balanced Advantage Fund - Direct Plan - Growth",
        "ICICI Prudential Balanced Advantage Fund - Direct Plan -  Growth",
    ],
    [
        "Parag Parikh Flexi Cap Fund-Direct-Growth",
        "Parag Parikh Flexi Cap Fund - Direct Plan - Growth",
    ],
]


class MutualFundTransaction:
    def __init__(
        self,
        account: str,
        date: datetime,
        scheme: str,
        folio: str,
        type: str,
        units: float,
        nav: float,
        value: float,
        stamp_duty: float,
        fund_house: str = "",
        sanitize_name=False,
    ) -> None:
        self.account = account
        self.date = date
        found = False
        if sanitize_name:
            for matcher in MATCHERS:
                if scheme == matcher[0]:
                    self.scheme = matcher[1]
                    found = True
                    break
                elif scheme == matcher[1]:
                    self.scheme = scheme
                    found = True
                    break
            if not found:
                logging.fatal("Cannot find scheme {}".format(scheme))
                raise ValueError("Cannot find scheme")
        else:
            self.scheme = scheme

        if not fund_house:
            self.fund_house = [
                house for house in FUND_HOUSES if self.scheme in house[1]
            ][0][0]
        else:
            self.fund_house = fund_house

        self.folio = folio
        self.type = type.replace("-", " ")
        self.units = units
        self.nav = nav
        self.value = value
        self.stamp_duty = stamp_duty

    def is_present(self, old_transactions: list[Self]) -> bool:
        if [
            txn
            for txn in old_transactions
            if txn.date == self.date
            and txn.folio == self.folio
            and txn.type == self.type
            and txn.scheme == self.scheme
            and txn.units == self.units
        ]:
            return True
        return False
