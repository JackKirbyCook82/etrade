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
from itertools import product
from datetime import date as Date
from datetime import timezone as Timezone
from datetime import datetime as Datetime

from finance.variables import Querys, Variables, OSI
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition, Logging
from support.custom import SliceOrderedDict as SODict

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeStockDownloader", "ETradeOptionDownloader", "ETradeExpireDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


timestamp_parser = lambda integer: Datetime.fromtimestamp(int(integer), tz=Timezone.utc).astimezone(pytz.timezone("US/Central"))
osi_parser = lambda string: OSI(str(string).replace("-", ""))
contract_parser = lambda string: Querys.Contract(list(osi_parser(string).values()))
strike_parser = lambda content: np.round(float(content), 2).astype(np.float32)
expire_parser = lambda string: contract_parser(string).expire


class ETradeMarketURL(WebURL, domain="https://api.etrade.com"): pass
class ETradeStockURL(ETradeMarketURL, path=["v1", "market", "quote"]):
    @staticmethod
    def path(*args, tickers, **kwargs): return [",".join(list(map(str, tickers))) + ".json"]

class ETradeOptionURL(ETradeMarketURL, path=["v1", "market", "optionchains" + ".json"], parameters={"noOfStrikes": "1000", "priceType": "ALL"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        tickers = cls.tickers(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        return tickers | expires

    @staticmethod
    def tickers(*args, ticker, **kwargs): return {"symbol": str(ticker).upper()}
    @staticmethod
    def expires(*args, expire, **kwargs): return {"expiryYear": f"{expire.year:04.0f}", "expiryMonth": f"{expire.month:02.0f}", "expiryDay": f"{expire.day:02.0f}"}

class ETradeExpireURL(ETradeMarketURL, path=["v1", "market", "optionexpiredate" + ".json"], parameters={"expiryType": "ALL"}):
    @staticmethod
    def parameters(*args, ticker, **kwargs): return {"symbol": str(ticker).upper()}


class ETradeStocksData(WebJSON, locator="//QuoteResponse/QuoteData[]", multiple=True, optional=False):
    class Quoting(WebJSON.Text, locator="//quoteStatus", key="quoting", parser=Variables.Markets.Quoting): pass
    class Timing(WebJSON.Text, locator="//dateTimeUTC", key="timing", parser=timestamp_parser): pass
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Last(WebJSON.Text, locator="//All/lastTrade", key="last", parser=np.float32): pass
    class Bid(WebJSON.Text, locator="//All/bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//All/ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//All/askSize", key="supply", parser=np.int32): pass

    def execute(self, *args, **kwargs):
        stocks = super().execute(*args, **kwargs)
        assert isinstance(stocks, dict)
        stocks = pd.DataFrame.from_records([stocks])
        stocks["instrument"] = Variables.Securities.Instrument.STOCK
        stocks["option"] = Variables.Securities.Option.EMPTY
        return stocks


class ETradeOptionData(WebJSON, ABC, multiple=False, optional=False):
    class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
    class Expire(WebJSON.Text, locator="//osiKey", key="expire", parser=expire_parser): pass
    class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=strike_parser): pass
    class Option(WebJSON.Text, locator="//optionType", key="option", parser=Variables.Securities.Option): pass
    class Last(WebJSON.Text, locator="//lastPrice", key="last", parser=np.float32): pass
    class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass

    def execute(self, *args, **kwargs):
        options = super().execute(*args, **kwargs)
        assert isinstance(options, dict)
        options = pd.DataFrame.from_records([options])
        options["instrument"] = Variables.Securities.Instrument.OPTION
        return options


class ETradeOptionsData(WebJSON, ABC, locator="//OptionChainResponse", multiple=False, optional=False):
    class Quoting(WebJSON.Text, locator="//quoteType", key="quoting", parser=Variables.Markets.Quoting): pass
    class Timing(WebJSON.Text, locator="//timeStamp", key="timing", parser=timestamp_parser): pass
    class Options(WebJSON, locator="OptionPair[]", key="option", multiple=True, optional=False):
        class Call(ETradeOptionData, locator="//Call", key="call"): pass
        class Put(ETradeOptionData, locator="//Put", key="put"): pass

        def execute(self, *args, **kwargs):
            calls = self["call"](*args, **kwargs)
            puts = self["put"](*args, **kwargs)
            options = pd.concat([calls, puts], axis=0)
            return options

    def execute(self, *args, **kwargs):
        options = [data(*args, **kwargs) for data in self["option"]]
        options = pd.concat(options, axis=0)
        options["quoting"] = self["quoting"](*args, **kwargs)
        options["timing"] = self["timing"](*args, **kwargs)
        return options


class ETradeExpireData(WebJSON, locator="//OptionExpireDateResponse/ExpirationDate[]", multiple=True, optional=False):
    class Year(WebJSON.Text, locator="//year", key="year", parser=np.int16): pass
    class Month(WebJSON.Text, locator="//month", key="month", parser=np.int16): pass
    class Day(WebJSON.Text, locator="//day", key="day", parser=np.int16): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        return Date(**contents)


class ETradeStockPage(WebJSONPage):
    def execute(self, *args, **kwargs):
        url = ETradeStockURL(*args, **kwargs)
        self.load(url, *args, **kwargs)
        stocks = ETradeStocksData(self.json, *args, **kwargs)
        stocks = [data(*args, **kwargs) for data in iter(stocks)]
        stocks = pd.concat(stocks, axis=0)
        return stocks

class ETradeOptionPage(WebJSONPage):
    def execute(self, *args, **kwargs):
        url = ETradeOptionURL(*args, **kwargs)
        self.load(url, *args, **kwargs)
        options = ETradeOptionsData(self.json, *args, **kwargs)
        options = options(*args, **kwargs)
        assert isinstance(options, pd.DataFrame)
        return options

class ETradeExpirePage(WebJSONPage):
    def execute(self, *args, expiry=None, **kwargs):
        url = ETradeExpireURL(*args, **kwargs)
        self.load(url, *args, **kwargs)
        datas = ETradeExpireData(self.json, *args, **kwargs)
        assert isinstance(datas, list)
        contents = [data(*args, **kwargs) for data in datas]
        expiry = expiry if expiry is not None else contents
        contents = [content for content in contents if content in expiry]
        return contents


class ETradeDownloader(Sizing, Emptying, Partition, Logging, ABC, title="Downloaded"):
    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.__pagetype__ = kwargs.get("page", getattr(cls, "__pagetype__", None))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = self.pagetype(*args, **kwargs)

    @property
    def pagetype(self): return type(self).__pagetype__
    @property
    def page(self): return self.__page


class ETradeSecurityDownloader(ETradeDownloader):
    def download(self, *args, **kwargs):
        securities = self.page(*args, **kwargs)
        assert isinstance(securities, pd.DataFrame)
        assert not self.empty(securities)
        return securities

    @staticmethod
    def querys(querys, querytype):
        assert isinstance(querys, (list, dict, querytype))
        assert all([isinstance(query, querytype) for query in querys]) if isinstance(querys, (list, dict)) else True
        if isinstance(querys, querytype): querys = [querys]
        elif isinstance(querys, dict): querys = SODict(querys)
        else: querys = list(querys)
        return querys


class ETradeStockDownloader(ETradeSecurityDownloader, page=ETradeStockPage):
    def execute(self, symbols, *args, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        symbols = [symbols[index:index+25] for index in range(0, len(symbols), 100)]
        for symbols in iter(symbols):
            parameters = {"tickers": [str(symbol.ticker) for symbol in symbols]}
            stocks = self.download(*args, **parameters, **kwargs)
            assert isinstance(stocks, pd.DataFrame)
            if isinstance(symbols, dict):
                function = lambda series: symbols[Querys.Symbol(series.to_dict())]
                values = stocks[list(Querys.Symbol)].apply(function, axis=1, result_type="expand")
                stocks = pd.concat([stocks, values], axis=1)
            symbols = ",".join(list(map(str, symbols)))
            size = self.size(stocks)
            self.console(f"{str(symbols)}[{int(size):.0f}]")
            if self.empty(stocks): return
            yield stocks


class ETradeOptionDownloader(ETradeSecurityDownloader, page=ETradeOptionPage):
    def execute(self, symbols, expires, *args, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        for symbol, expire in product(list(symbols), list(expires)):
            parameters = {"ticker": str(symbol.ticker), "expire": expire}
            options = self.download(*args, **parameters, **kwargs)
            assert isinstance(options, pd.DataFrame)
            if isinstance(symbols, dict):
                function = lambda series: symbols[Querys.Symbol(series.to_dict())]
                values = options[list(Querys.Symbol)].apply(function, axis=1, result_type="expand")
                options = pd.concat([options, values], axis=1)
            size = self.size(options)
            self.console(f"{str(symbol)}|{str(expire.strftime('%Y%m%d'))}[{int(size):.0f}]")
            if self.empty(options): return
            yield options


class ETradeExpireDownloader(ETradeDownloader, page=ETradeExpirePage, title="Downloaded"):
    def execute(self, symbols, *args, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        for symbol in iter(symbols):
            parameters = {"ticker": str(symbol.ticker)}
            expires = self.download(*args, **parameters, **kwargs)
            assert isinstance(expires, list)
            self.console(f"{str(symbol)}[{len(expires):.0f}]")
            if not bool(expires): return
            yield expires

    def download(self, *args, **kwargs):
        expires = self.page(*args, **kwargs)
        assert isinstance(expires, list)
        expires.sort()
        return expires

    @staticmethod
    def querys(querys, querytype):
        assert isinstance(querys, (list, querytype))
        assert all([isinstance(query, querytype) for query in querys]) if isinstance(querys, list) else True
        querys = list(querys) if isinstance(querys, list) else [querys]
        return querys


