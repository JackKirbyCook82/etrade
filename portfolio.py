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
from itertools import chain
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone
from collections import namedtuple as ntuple

from webscraping.weburl import WebURL
from webscraping.webdatas import WebJSON
from webscraping.webpages import WebJsonPage
from support.processes import Downloader
from support.pipelines import Processor
from finance.variables import Instruments, Options, Positions

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeAccountDownloader", "ETradePortfolioDownloader"]
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
    class Quantity(WebJSON.Text, locator="//quantity", key="quantity", parser=np.int32): pass
    class Paid(WebJSON.Text, locator="pricePaid", key="paid", parser=np.float32): pass

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
    def __call__(self, ticker, *args, account, **kwargs):
        Columns = ntuple("Columns", "securities holdings")
        security = ["instrument", "position", "ticker", "expire", "strike", "date", "price", "underlying", "size", "volume", "interest"]
        holding = ["instrument", "position", "ticker", "expire", "strike", "date", "quantity"]
        columns = Columns(security, holding)
        string = lambda x: str(x.name).lower()
        curl = ETradePortfolioURL(account=account)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradePortfolioData(self.source)
        for contract, portfolio in self.portfolio(contents, *args, **kwargs):
            securities = self.securities(portfolio, *args, **kwargs)
            securities["instrument"] = securities["instrument"].apply(string)
            securities["position"] = securities["instrument"].apply(string)
            securities = securities[columns.securities]
            holdings = self.holdings(portfolio, *args, **kwargs)
            holdings["instrument"] = holdings["instrument"].apply(string)
            holdings["position"] = holdings["instrument"].apply(string)
            holdings = holdings[columns.holdings]
            yield contract, (securities, holdings)

    @staticmethod
    def portfolio(contents, *args, **kwargs):
        contents = [{key: value(*args, **kwargs) for key, value in iter(content)} for content in iter(contents)]
        portfolio = pd.DataFrame.from_records(contents)
        for contract, dataframe in iter(portfolio.groupby(["ticker", "expire"])):
            yield contract, dataframe

    @staticmethod
    def securities(dataframe, *args, **kwargs):
        position = lambda cols: Positions.LONG if cols["position"] == Positions.SHORT else Positions.SHORT
        size = lambda cols: cols["supply"] if cols["position"] == Positions.LONG else cols["demand"]
        price = lambda cols: cols["ask"] if cols["position"] == Positions.LONG else cols["bid"]
        mask = dataframe["instrument"] != Instruments.STOCK
        options = dataframe.where(mask).dropna(how="all", inplace=False)
        options["position"] = options.apply(position)
        options["price"] = options.apply(price)
        options["size"] = options.apply(size)
        return options

    @staticmethod
    def holdings(dataframe, *args, **kwargs):
        index = ["instrument", "position", "ticker", "expire", "strike", "date"]
        invert = lambda x: Positions.SHORT if x == Positions.LONG else Positions.LONG
        quantity = lambda x: np.floor(np.divide(x, 100)).astype(np.float32)
        strike = lambda x: np.round(x, 2).astype(np.float32)
        parameters = lambda record: {"strike": strike(record["paid"]), "quantity": quantity(record["quantity"])}
        call = lambda record: {"instrument": Instruments.CALL, "position": record["position"]}
        put = lambda record: {"instrument": Instruments.PUT, "position": invert(record["position"])}
        left = lambda record: record | put(record) | parameters(record)
        right = lambda record: record | call(record) | parameters(record)
        stocks = dataframe["instrument"] == Instruments.STOCKS
        stocks = dataframe.where(stocks).dropna(axis=0, how="all")
        virtuals = [[left(record), right(record)] for record in stocks.to_dict("records")]
        virtuals = pd.DataFrame.from_records(list(chain(*virtuals)))
        virtuals["strike"] = virtuals["strike"].apply(strike)
        mask = dataframe["instrument"] != Instruments.STOCK
        options = dataframe.where(mask).dropna(axis=0, how="all")
        options = pd.concat([options, virtuals], axis=0)[index + ["quantity"]]
        options = options.groupby(index)["quantity"].sum()
        options = options.reset_index(drop=False, inplace=False)
        return options


class ETradeAccountDownloader(Downloader, Processor, pages={"account": ETradeAccountPage, "portfolio": ETradePortfolioPage}):
    def execute(self, query, *args, **kwargs):
        pass


class ETradePortfolioDownloader(Downloader, Processor, pages={"account": ETradeAccountPage, "portfolio": ETradePortfolioPage}):
    def execute(self, query, *args, **kwargs):
        pass


# class ETradePortfolioDownloader(CycleDownloader, pages={"account": ETradeAccountPage, "portfolio": ETradePortfolioPage}):
#     def prepare(self, *args, account, **kwargs):
#         account = self.pages["account"](*args, **kwargs)[account]
#        return {"account": account}

#     def execute(self, *args, account, **kwargs):
#         portfolio = self.pages["portfolio"](*args, acccount=account, **kwargs)
#         for (ticker, expire), (securities, holdings) in iter(portfolio):
#             contract = Contract(ticker, expire)
#             yield Query(contract, securities=securities, holdings=holdings)



