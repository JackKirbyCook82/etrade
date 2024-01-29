# -*- coding: utf-8 -*-
"""
Created on Sat Jan 27 2024
@name:   ETrade Portfolio Objects
@author: Jack Kirby Cook

"""

import pytz
import numpy as np
import pandas as pd
from enum import IntEnum
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone
from collections import namedtuple as ntuple

from webscraping.weburl import WebURL
from webscraping.webdatas import WebJSON
from webscraping.webpages import WebJsonPage
from support.pipelines import CycleProducer
from finance.variables import Securities, Instruments, Positions

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeAccountDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


Status = IntEnum("Status", ["CLOSED", "ACTIVE"], start=0)
timestamp_parser = lambda x: Datetime.fromtimestamp(int(x), Timezone.utc).astimezone(pytz.timezone("US/Central"))
date_parser = lambda x: np.datetime64(timestamp_parser(x).date(), "D")
datetime_parser = lambda x: np.datetime64(timestamp_parser(x))
strike_parser = lambda x: np.round(x, 2).astype(np.float32)
position_parser = lambda string: Positions[string]
option_parser = lambda string: Instruments[string]
status_parser = lambda string: Status[string]


class ETradeAccountURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"
    def path(cls, *args, **kwargs): return "/v1/accounts/list.json"

class ETradeBalanceURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"
    def path(cls, *args, account, **kwargs): return f"/v1/accounts/{str(account)}/balance.json"
    def parms(cls, *args, **kwargs): return {"instType": "BROKERAGE", "realTimeNAV": "true"}

class ETradePortfolioURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"
    def path(cls, *args, account, **kwargs): return f"/v1/accounts/{str(account)}/portfolio.json"
    def parms(cls, *args, **kwargs): return {"count": "1000", "view": "QUICK"}


class ETradeAccountData(WebJSON, locator="//AccountListResponse/Accounts/Account[]", collections=True):
    class AccountID(WebJSON.Text, locator="//accountId", key="id", parser=np.int32): pass
    class AccountKey(WebJSON.Text, locator="//accountIdKey", key="key", parser=str): pass
    class Status(WebJSON.Text, locator="//accountStatus", key="status", parser=status_parser): pass


class ETradeBalanceData(WebJSON, locator="//BalanceResponse"):
    class AccountID(WebJSON.Text, locator="//accountId", key="id", parser=np.int32): pass
    class Cash(WebJSON.Text, locator="//Computed/cashBuyingPower", key="cash", parser=np.float32): pass
    class Margin(WebJSON.Text, locator="//Computed/marginBuyingPower", key="margin", parser=np.float32): pass
    class Value(WebJSON.Text, locator="//Computed/RealTimeValues/totalAccountValue", key="value", parser=np.float32): pass
    class Market(WebJSON.Text, locator="//Computed/RealTimeValues/netMv", key="market", parser=np.float32): pass


class ETradePortfolioData(WebJSON, locator="//PortfolioResponse/AccountPortfolio/Position[]", collections=True):
    class Current(WebJSON.Text, locator="//dateTimeUTC", key="current", parser=datetime_parser): pass
    class Acquired(WebJSON.Text, locator="//dateAcquired", key="acquired", parser=datetime_parser): pass
    class Quantity(WebJSON.Text, locator="//quantity", key="quantity", parser=np.int32): pass
    class Paid(WebJSON.Text, locator="//pricePaid", key="paid", parser=np.float32): pass
    class Price(WebJSON.Text, locator="//price", key="price", parser=np.float32): pass
    class Cost(WebJSON.Text, locator="//totalCost", key="cost", parser=np.float): pass
    class Value(WebJSON.Text, locator="//marketValue", key="value", parser=np.float): pass
    class Yield(WebJSON.Text, locator="//totalGainPct", key="yield", parser=np.float): pass

    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Security(WebJSON, key="security"):
        class Option(WebJSON.Text, locator="//Product/callPut", key="option", parser=option_parser, optional=True): pass
        class Position(WebJSON.Text, locator="//positionType", key="position", parser=position_parser): pass

    class Strike(WebJSON.Text, locator="//Product/strikePrice", key="strike", parser=strike_parser, optional=True): pass
    class Expire(WebJSON, key="expire", optional=True):
        class Year(WebJSON.Text, locator="//Product/expiryYear", key="year", parser=np.int16, optional=True): pass
        class Month(WebJSON.Text, locator="//Product/expiryMonth", key="month", parser=np.int16, optional=True): pass
        class Day(WebJSON.Text, locator="//Product/expiryDay", key="day", parser=np.int16, optional=True): pass


class ETradeAccountPage(WebJsonPage):
    def __call__(self, ticker, *args, account, **kwargs):
        assert isinstance(account, int)
        curl = ETradeAccountURL()
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeAccountData(self.source)
        accounts = {int(content["id"].data): str(content["key"].data) for content in iter(contents)}
        return accounts[account]


class ETradeBalancePage(WebJsonPage):
    def __call__(self, ticker, *args, account, **kwargs):
        curl = ETradeAccountURL(account=account)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeAccountData(self.source)
        balances = {key: value.data for key, value in iter(contents)}
        return balances


class ETradePortfolioPage(WebJsonPage):
    def __call__(self, ticker, *args, account, **kwargs):
        curl = ETradePortfolioURL(account=account)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradePortfolioData(self.source)
        columns = ["current", "acquired", "quantity", "ticker", "security", "strike", "expire", "paid", "price", "value", "cost", "yield"]
        portfolio = [self.transaction(content) for content in iter(contents)]
        portfolio = pd.DataFrame.from_records(portfolio)
        return portfolio[columns]

    @staticmethod
    def transaction(content):
        expire = {"expire": Date(year=content["year"].data, month=content["month"].data, day=content["day"].data) if "expire" in content else np.NaT}
        instrument = Instruments.STOCK if ("option" not in content["security"]) else content["security"]["option"].data
        position = content["security"]["position"].data
        security = {"security": Securities[(instrument, position)]}
        contents = {key: value.data for key, value in iter(content)}
        return contents | security | expire


class ETradeAccountQuery(ntuple("Query", "current balance portfolio")): pass
class ETradeAccountDownloader(CycleProducer, title="Downloaded"):
    def __init__(self, *args, name, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        pages = {"account": ETradeAccountPage, "balance": ETradeBalancePage, "portfolio": ETradePortfolioPage}
        pages = {key: page(*args, **kwargs) for key, page in pages.items()}
        self.pages = pages

    def prepare(self, *args, account, **kwargs):
        account = self.pages["account"](*args, **kwargs)[account]
        return {"account": account}

    def execute(self, *args, account, **kwargs):
        current = Datetime.now()
        balances = self.pages["balance"](*args, account=account, **kwargs)
        portfolio = self.pages["portfolio"](*args, acccount=account, **kwargs)
        yield ETradeAccountQuery(current, balances, portfolio)



