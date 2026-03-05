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

from finance.concepts import Querys, Concepts, OSI
from webscraping.webpages import WebJSONPage, WebDownloader
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL

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
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Last(WebJSON.Text, locator="//All/lastTrade", key="last", parser=np.float32): pass
    class Bid(WebJSON.Text, locator="//All/bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//All/ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//All/askSize", key="supply", parser=np.int32): pass

    def execute(self, *args, **kwargs):
        stock = super().execute(*args, **kwargs)
        assert isinstance(stock, dict)
        stock = pd.DataFrame.from_records([stock])
        stock["instrument"] = Concepts.Securities.Instrument.STOCK
        stock["option"] = Concepts.Securities.Option.EMPTY
        return stock


class ETradeOptionData(WebJSON, ABC, multiple=False, optional=False):
    class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
    class Expire(WebJSON.Text, locator="//osiKey", key="expire", parser=expire_parser): pass
    class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=strike_parser): pass
    class Option(WebJSON.Text, locator="//optionType", key="option", parser=Concepts.Securities.Option): pass
    class Last(WebJSON.Text, locator="//lastPrice", key="last", parser=np.float32): pass
    class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
    class Implied(WebJSON.Text, locator="//OptionGreeks/iv", key="implied", parser=np.float32): pass

    def execute(self, *args, **kwargs):
        option = super().execute(*args, **kwargs)
        assert isinstance(option, dict)
        option = pd.DataFrame.from_records([option])
        option["instrument"] = Concepts.Securities.Instrument.OPTION
        return option


class ETradeOptionsData(WebJSON, ABC, locator="//OptionChainResponse", multiple=False, optional=False):
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
    def execute(self, *args, symbols, **kwargs):
        tickers = [str(symbol.ticker) for symbol in symbols]
        parameters = dict(tickers=tickers)
        url = ETradeStockURL(*args, **parameters, **kwargs)
        self.load(url, *args, **kwargs)
        stocks = ETradeStocksData(self.json, *args, **kwargs)
        stocks = [data(*args, **kwargs) for data in iter(stocks)]
        stocks = pd.concat(stocks, axis=0)
        return stocks

class ETradeOptionPage(WebJSONPage):
    def execute(self, *args, symbol, expire, **kwargs):
        parameters = dict(ticker=str(symbol.ticker), expire=expire)
        url = ETradeOptionURL(*args, **parameters, **kwargs)
        self.load(url, *args, **kwargs)
        options = ETradeOptionsData(self.json, *args, **kwargs)
        options = options(*args, **kwargs)
        assert isinstance(options, pd.DataFrame)
        return options

class ETradeExpirePage(WebJSONPage):
    def execute(self, *args, symbol, expiry=None, **kwargs):
        parameters = dict(ticker=str(symbol.ticker))
        url = ETradeExpireURL(*args, **parameters, **kwargs)
        self.load(url, *args, **kwargs)
        datas = ETradeExpireData(self.json, *args, **kwargs)
        assert isinstance(datas, list)
        contents = [data(*args, **kwargs) for data in datas]
        expiry = expiry if expiry is not None else contents
        contents = [content for content in contents if content in expiry]
        return contents


class ETradeSecurityDownloader(WebDownloader, ABC):
    def download(self, /, **kwargs):
        securities = self.page(**kwargs)
        assert isinstance(securities, pd.DataFrame)
        assert not self.empty(securities)
        return securities


class ETradeStockDownloader(ETradeSecurityDownloader, page=ETradeStockPage):
    def execute(self, symbols, /, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        if self.limit:
            symbols = [symbols[index:index+self.limit] for index in range(0, len(symbols), self.limit)]
        for symbols in iter(symbols):
            stocks = self.download(symbols=symbols, **kwargs)
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
    def execute(self, symbols, expires, /, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        for symbol, expire in product(list(symbols), list(expires)):
            options = self.download(symbol=symbol, expire=expire, **kwargs)
            assert isinstance(options, pd.DataFrame)
            if isinstance(symbols, dict):
                function = lambda series: symbols[Querys.Symbol(series.to_dict())]
                values = options[list(Querys.Symbol)].apply(function, axis=1, result_type="expand")
                options = pd.concat([options, values], axis=1)
            size = self.size(options)
            self.console(f"{str(symbol)}|{str(expire.strftime('%Y%m%d'))}[{int(size):.0f}]")
            if self.empty(options): return
            yield options


class ETradeExpireDownloader(ETradeSecurityDownloader, page=ETradeExpirePage):
    def execute(self, symbols, /, expiry=None, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        for symbol in iter(symbols):
            expires = self.download(symbol=symbol, expiry=expiry, **kwargs)
            assert isinstance(expires, list)
            self.console(f"{str(symbol)}[{len(expires):.0f}]")
            if not bool(expires): return
            yield expires

    def download(self, /, **kwargs):
        expires = self.page(**kwargs)
        assert isinstance(expires, list)
        expires.sort()
        return expires



