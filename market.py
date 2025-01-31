# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2023
@name:   ETrade Market Objects
@author: Jack Kirby Cook

"""

import pytz
import numpy as np
import pandas as pd
from abc import ABC
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone

from finance.variables import Querys, Variables
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Mixin

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


timestamp_parser = lambda integer: Datetime.fromtimestamp(integer, Timezone.utc).astimezone(pytz.timezone("US/Central"))
current_parser = lambda integer: np.datetime64(timestamp_parser(integer))
contract_parser = lambda string: Querys.Contract.fromOSI(str(string).replace("---", ""))
strike_parser = lambda content: np.round(content, 2).astype(np.float32)
expire_parser = lambda string: contract_parser(string).expire


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
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Current(WebJSON.Text, locator="//dateTimeUTC", key="current", parser=current_parser): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        stocks = pd.DataFrame.from_records([contents])
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
    class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=expire_parser): pass
    class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=strike_parser): pass
    class Option(WebJSON.Text, locator="//optionType", key="option", parser=Variables.Securities.Option): pass
    class Current(WebJSON.Text, locator="//timeStamp", key="current", parser=current_parser): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        options = pd.DataFrame.from_records([contents])
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
class ETradeStockPage(WebJSONPage, url=ETradeStockURL, data=[ETradeStockTradeData, ETradeStockQuoteData]): pass
class ETradeOptionPage(WebJSONPage, url=ETradeOptionURL, data=[ETradeOptionsTradeData, ETradeOptionQuoteData]): pass


class ETradeSettlementDownloader(Mixin, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = ETradeExpirePage(*args, **kwargs)

    def execute(self, symbols, *args, expires, **kwargs):
        assert isinstance(symbols, (list, Querys.Symbol))
        assert all([isinstance(symbol, Querys.Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        symbols = list(symbols) if isinstance(symbols, list) else [symbols]
        for symbol in list(symbols):
            expires = [expire for expire in self.download(symbol, *args, **kwargs) if expire in expires]
            settlements = [Querys.Settlement(symbol.ticker, expire) for expire in expires]
            string = f"{str(symbol)}[{len(settlements):.0f}]"
            self.console(string)
            if not bool(settlements): continue
            yield settlements

    def download(self, symbol, *args, **kwargs):
        parameters = dict(ticker=symbol.ticker)
        expires = self.page(*args, **parameters, **kwargs)
        assert isinstance(expires, list)
        return expires

    @property
    def page(self): return self.__page


class ETradeStockDownloader(Sizing, Emptying, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = ETradeStockPage(*args, **kwargs)

    def execute(self, symbols, *args, **kwargs):
        assert isinstance(symbols, (list, Querys.Symbol))
        assert all([isinstance(symbol, Querys.Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        symbols = list(symbols) if isinstance(symbols, list) else [symbols]
        for symbol in list(symbols):
            stocks = self.download(symbol, *args, **kwargs)
            size = self.size(stocks)
            string = f"{str(symbol)}[{int(size):.0f}]"
            self.console(string)
            if self.empty(stocks): return
            return stocks

    def download(self, symbol, *args, **kwargs):
        parameters = dict(ticker=symbol.ticker)
        trade, quote = self.page(*args, **parameters, **kwargs)
        assert isinstance(trade, pd.DataFrame) and isinstance(quote, pd.DataFrame)
        stocks = trade.merge(quote, how="outer", on=list(Querys.Symbol), sort=False, suffixes=("", "_"))
        return stocks

    @property
    def page(self): return self.__page


class ETradeOptionDownloader(Sizing, Emptying, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = ETradeOptionPage(*args, **kwargs)

    def execute(self, settlements, *args, **kwargs):
        assert isinstance(settlements, (list, Querys.Settlement))
        assert all([isinstance(settlement, Querys.Settlement) for settlement in settlements]) if isinstance(settlements, list) else True
        settlements = list(settlements) if isinstance(settlements, list) else [settlements]
        for settlement in list(settlements):
            options = self.download(settlement, *args, **kwargs)
            size = self.size(options)
            string = f"{str(settlement)}[{int(size):.0f}]"
            self.console(string)
            if self.empty(options): return
            return options

    def download(self, settlement, *args, underlying={}, **kwargs):
        assert isinstance(underlying, dict)
        parameters = dict(ticker=settlement.ticker, expire=settlement.expire, strike=underlying[settlement.ticker])
        trade, quote = self.page(*args, **parameters, **kwargs)
        assert isinstance(trade, pd.DataFrame) and isinstance(quote, pd.DataFrame)
        options = trade.merge(quote, how="outer", on=list(Querys.Contract), sort=False, suffixes=("", "_"))
        options["underlying"] = underlying[settlement.ticker]
        return options

    @property
    def page(self): return self.__page


