# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2023
@name:   ETrade Market Objects
@author: Jack Kirby Cook

"""

import logging
import numpy as np
import pandas as pd
from abc import ABC
from datetime import date as Date

from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"
__logger__ = logging.getLogger(__name__)


class ETradeURL(WebURL, domain="https://api.etrade.com"): pass
class ETradeExpireURL(ETradeURL, path=["v1", "market", "optionexpiredate" + ".json"], parms={"expiryType": "ALL"}):
    @staticmethod
    def parms(*args, ticker, **kwargs): return {"symbol": f"{str(ticker).upper()}"}


class ETradeStockURL(ETradeURL, path=["v1", "market", "quote"]):
    @staticmethod
    def path(*args, ticker, **kwargs):
        assert isinstance(ticker, (str, set, tuple, list))
        ticker = [ticker] if isinstance(ticker, str) else list(ticker)
        return [",".join(list(ticker)) + ".json"]


class ETradeOptionURL(ETradeURL, path=["v1", "market", "optionchains" + ".json"]):
    @staticmethod
    def expires(*args, expire, **kwargs): return {"expiryYear": f"{expire.year:04.0f}", "expiryMonth": f"{expire.month:02.0f}", "expiryDay": f"{expire.day:02.0f}", "expiryType": "ALL"}
    @staticmethod
    def strikes(*args, strike, **kwargs): return {"strikePriceNear": str(int(strike)), "noOfStrikes": "1000", "priceType": "ALL"}
    @staticmethod
    def options(*args, **kwargs): return {"optionCategory": "STANDARD", "chainType": "CALLPUT", "skipAdjusted": "true"}

    @classmethod
    def parms(cls, *args, ticker, **kwargs):
        options = cls.options(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        strikes = cls.strikes(*args, **kwargs)
        return {"symbol": str(ticker).upper(), **options, **expires, **strikes}


class ETradeExpireData(WebJSON, locator="//OptionExpireDateResponse/ExpirationDate[]", multiple=True, optional=True):
    class Year(WebJSON.Text, locator="//year", key="year", parser=np.int16): pass
    class Month(WebJSON.Text, locator="//month", key="month", parser=np.int16): pass
    class Day(WebJSON.Text, locator="//day", key="day", parser=np.int16): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        return Date(**contents)


class ETradeStockData(WebJSON, locator="//QuoteResponse/QuoteData[]", multiple=True, optional=True):
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str.upper): pass
    class Current(WebJSON.Text, locator="//dateTimeUTC", key="current", parser=PARSER): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        stocks = pd.DataFrame.from_records([contents])
        stocks["instrument"] = Insturments.STOCK
        return stocks

class ETradeStockTradeData(ETradeStockData):
    class Price(WebJSON.Text, locator="//lastTrade", key="price", parser=np.float32): pass

class ETradeStockQuoteData(ETradeStockData):
    class Bid(WebJSON.Text, locator="//All/bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//All/ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//All/askSize", key="supply", parser=np.int32): pass


class ETradeOptionData(WebJSON, ABC):
    class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
    class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=PARSER): pass
    class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=np.float32): pass
    class Option(WebJSON.Text, locator="//optionType", key="option", parser=PARSER): pass
    class Current(WebJSON.Text, locator="//timeStamp", key="current", parser=PARSER): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        options = pd.DataFrame.from_records([contents])
        options["instrument"] = Instruments.OPTION
        function = lambda column: np.round(column, 2)
        options["strike"] = options["strike"].apply(function).astype(np.float32)
        return options

class ETradeOptionTradeData(ETradeOptionData):
    class Price(WebJSON.Text, locator="//lastPrice", key="price", parser=np.float32): pass

class ETradeOptionQuoteData(ETradeOptionData):
    class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass


class ETradeOptionsData(WebJSON, locator="//OptionChainResponse/OptionPair[]", multiple=True, optional=True):
    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        options = list(contents.values())
        options = pd.concat(options, axis=0)
        return options

class ETradeOptionsTradeData(ETradeOptionsData):
    class Call(ETradeOptionTradeData, locator="//Call", key="call"): pass
    class Put(ETradeOptionTradeData, locator="//Put", key="put"): pass

class ETradeOptionsQuoteData(ETradeOptionsData):
    class Call(ETradeOptionQuoteData, locator="//Call", key="call"): pass
    class Put(ETradeOptionQuoteData, locator="//Put", key="put"): pass


class ETradeExpirePage(WebJSONPage, url=ETradeExpireURL, data=ETradeExpireData): pass
class ETradeStockPage(WebJSONPage, url=ETradeStockURL, data={(STOCK, TRADE): ETradeStockTradeData, (STOCK, QUOTE): ETradeStockQuoteData}): pass
class ETradeOptionPage(WebJSONPage, url=ETradeOptionURL, data={(OPTION, TRADE): ETradeOptionsTradeData, (OPTION, QUOTE): ETradeOptionQuoteData}): pass


class ETradeSettlementDownloader(Emptying, Partition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page = ETradeExpirePage(*args, **kwargs)

    def execute(self, stocks, *args, expires, **kwargs):
        assert isinstance(stocks, pd.DataFrame)
        if self.empty(stocks): return
        for partition, dataframe in self.partition(stocks, by=Symbol):
            series = dataframe.squeeze()
            ticker = str(series["ticker"]).upper()
            underlying = np.round(series["price"], 2).astype(np.float32)
            expires = [expire for expire in self.page(*args, **kwargs) if expire in expires]
            settlements = {Settlement(ticker, expire): underlying for expire in expires}
            string = f"Downloaded: {repr(self)}|{str(SETTLEMENT)}|{str(partition)}[{len(settlements):.0f}]"
            __logger__.info(string)
            if not bool(settlements): continue
            yield {(SETTLEMENT,): settlements}


class ETradeStockDownloader(Sizing, Emptying):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page = ETradeStockPage(*args, **kwargs)

    def execute(self, *args, symbols, **kwargs):
        assert isinstance(symbols, list)
        for symbol in iter(symbols):
            parameters = dict(ticker=symbol.ticker)
            contents = self.page(*args, **parameters, **kwargs)
            assert isinstance(contents, dict)
            for dataset, content in contents.items():
                string = "|".join(list(map(str, dataset)))
                size = self.size(content)
                string = f"Downloaded: {repr(self)}|{str(string)}|{str(symbol)}[{int(size):.0f}]"
                __logger__.info(string)
            if self.empty(contents): continue
            yield contents


class ETradeOptionDownloader(Sizing, Emptying):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page = ETradeOptionPage(*args, **kwargs)

    def execute(self, *args, settlements, **kwargs):
        assert isinstance(settlements, dict)
        for settlement, underlying in settlements.items():
            parameters = dict(ticker=settlement.ticker, expire=settlement.expire, strike=underlying, underlying=underlying)
            contents = self.page(*args, **parameters, **kwargs)
            assert isinstance(contents, dict)
            for dataset, content in contents.items():
                string = "|".join(list(map(str, dataset)))
                size = self.size(content)
                string = f"Downloaded: {repr(self)}|{str(string)}|{str(settlement)}[{int(size):.0f}]"
                __logger__.info(string)
            if self.empty(contents): continue
            yield contents




