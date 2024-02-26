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
from finance.variables import Query, Contract, Instruments, Options, Positions

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradePortfolioDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


Status = IntEnum("Status", ["CLOSED", "ACTIVE"], start=0)
timestamp_parser = lambda x: Datetime.fromtimestamp(int(x), Timezone.utc).astimezone(pytz.timezone("US/Central"))
date_parser = lambda x: np.datetime64(timestamp_parser(x).date(), "D")
datetime_parser = lambda x: np.datetime64(timestamp_parser(x))
strike_parser = lambda x: np.round(x, 2).astype(np.float32)
stock_parser = lambda string: Instruments.STOCK if str(string).upper() == "EQ" else None
option_parser = lambda string: Options[str(string).upper()]
position_parser = lambda string: Positions[str(string).upper()]
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
    def parms(cls, *args, **kwargs): return {"count": "1000", "view": "COMPLETE"}


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
    class Date(WebJSON.Text, locator="//dateTimeUTC", key="date", parser=date_parser): pass
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Strike(WebJSON.Text, locator="//Product/strikePrice", key="strike", parser=strike_parser, optional=True): pass
    class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
    class Underlying(WebJSON.Text, locator="//baseSymbolAndPrice", key="underlying", parser=np.float32, optional=True): pass
    class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
    class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
    class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32, optional=True): pass
    class Acquired(WebJSON.Text, locator="//dateAcquired", key="acquired", parser=date_parser): pass
    class Quantity(WebJSON.Text, locator="//quantity", key="quantity", parser=np.int32): pass
    class Spent(WebJSON.Text, locator="//totalCost", key="spent", parser=np.float32): pass

    class Position(WebJSON.Text, locator="//positionType", key="position", parser=position_parser): pass
    class Instrument(WebJSON.Text, key="instrument"):
        class Stock(WebJSON.Text, locator="//Product/securityType", key="stock", parser=stock_parser): pass
        class Option(WebJSON.Text, locator="//Product/callPut", key="option", parser=option_parser, optional=True): pass

        def execute(self, *args, **kwargs):
            instrument = self["instrument"]["option"].data if "option" in self["instrument"] else self["instrument"]["stock"].data
            return instrument

    class Expire(WebJSON, key="expire"):
        class Year(WebJSON.Text, locator="//Product/expiryYear", key="year", parser=np.int16, optional=True): pass
        class Month(WebJSON.Text, locator="//Product/expiryMonth", key="month", parser=np.int16, optional=True): pass
        class Day(WebJSON.Text, locator="//Product/expiryDay", key="day", parser=np.int16, optional=True): pass

        def execute(self, *args, **kwargs):
            return Date(year=self["year"].data, month=self["month"].data, day=self["day"].data)


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
        curl = ETradeBalanceURL(account=account)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeBalanceData(self.source)
        balances = {key: value(*args, **kwargs) for key, value in iter(contents)}
        return balances


class ETradePortfolioPage(WebJsonPage):
    def __init__(self, *args, **kwargs):
        Columns = ntuple("Columns", "markets holdings")
        securities = ["instrument", "position", "ticker", "expire", "strike", "date", "price", "size", "volume", "interest", "underlying"]
        holdings = ["instrument", "position", "quantity"]
        self.columns = Columns(securities, holdings)
        super().__init__(*args, **kwargs)

    def __call__(self, ticker, *args, account, **kwargs):
        curl = ETradePortfolioURL(account=account)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradePortfolioData(self.source)
        for (ticker, expire), (securities, holdings) in self.portfolio(contents, *args, **kwargs):
            securities = securities[self.columns.securities]
            holdings = holdings[self.columns.holdings]
            yield (ticker, expire), (securities, holdings)

    @staticmethod
    def portfolio(contents, *args, **kwargs):
        position_function = lambda cols: Positions.LONG if cols["position"] == Positions.SHORT else Positions.SHORT
        size_function = lambda cols: cols["supply"] if cols["position"] == Positions.LONG else cols["demand"]
        price_function = lambda cols: cols["ask"] if cols["position"] == Positions.LONG else cols["bid"]
        string_function = lambda x: str(x.name).lower()
        contents = [{key: value(*args, **kwargs) for key, value in iter(content)} for content in iter(contents)]
        portfolio = pd.DataFrame.from_records(contents)
        for (ticker, expire), dataframe in iter(portfolio.groupby(["ticker", "expire"])):
            securities = dataframe.copy(deep=False)
            holdings = dataframe.copy(deep=False)
            securities = securities.where(securities["security"] == Instruments.STOCK).dropna(how="all", inplace=False)
            securities["position"] = securities.apply(position_function)
            securities["price"] = securities.apply(price_function)
            securities["size"] = securities.apply(size_function)
            securities["instrument"] = securities["instrument"].apply(string_function)
            securities["position"] = securities["instrument"].apply(string_function)
            holdings["instrument"] = holdings["instrument"].apply(string_function)
            holdings["position"] = holdings["instrument"].apply(string_function)
            yield (ticker, expire), (securities, holdings)


class ETradePortfolioDownloader(CycleProducer, title="Downloaded"):
    def __init__(self, *args, name, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        pages = {"account": ETradeAccountPage, "portfolio": ETradePortfolioPage}
        pages = {key: page(*args, **kwargs) for key, page in pages.items()}
        self.pages = pages

    def prepare(self, *args, account, **kwargs):
        account = self.pages["account"](*args, **kwargs)[account]
        return {"account": account}

    def execute(self, *args, account, **kwargs):
        inquiry = Datetime.now()
        portfolio = self.pages["portfolio"](*args, acccount=account, **kwargs)
        for (ticker, expire), (securities, holdings) in iter(portfolio):
            contract = Contract(ticker, expire)
            yield Query(inquiry, contract, securities=securities, holdings=holdings)



