# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2023
@name:   ETrade Market Objects
@author: Jack Kirby Cook

"""

import pytz
import logging
import regex as re
import numpy as np
import pandas as pd
from abc import ABC
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone

from finance.variables import Variables, Querys
from webscraping.webpages import WebJsonPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Pipelining, Emptying, Sizing, Logging, Sourcing
from support.meta import RegistryMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeProductDownloader", "ETradeSecurityDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"
__logger__ = logging.getLogger(__name__)


class ETradeMarketParsers(object):
    timestamp = lambda x: Datetime.fromtimestamp(int(x), Timezone.utc).astimezone(pytz.timezone("US/Central"))
    datetime = lambda x: np.datetime64(ETradeMarketParsers.timestamp(x))
    date = lambda x: np.datetime64(ETradeMarketParsers.timestamp(x).date(), "D")
    quote = lambda x: Datetime.strptime(re.findall("(?<=:)[0-9:]+(?=:CALL|:PUT)", x)[0], "%Y:%m:%d")
    expires = lambda x: np.datetime64(ETradeMarketParsers.quote(x).date(), "D")
    strike = lambda x: np.round(x, 2).astype(np.float32)


class ETradeSecurityURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"

class ETradeStockURL(ETradeSecurityURL):
    def path(cls, *args, ticker=None, tickers=[], **kwargs):
        tickers = ([ticker] if bool(ticker) else []) + tickers
        assert bool(tickers)
        tickers = ",".join(tickers)
        return f"/v1/market/quote/{tickers}.json"

class ETradeExpireURL(ETradeSecurityURL):
    def path(cls, *args, **kwargs): return "/v1/market/optionexpiredate.json"
    def parms(cls, *args, ticker, **kwargs): return {"symbol": str(ticker), "expiryType": "ALL"}

class ETradeOptionURL(ETradeSecurityURL):
    def path(cls, *args, **kwargs): return "/v1/market/optionchains.json"
    def parms(cls, *args, ticker, **kwargs):
        options = cls.options(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        strikes = cls.strikes(*args, **kwargs)
        return {"symbol": str(ticker), **options, **expires, **strikes}

    @staticmethod
    def expires(*args, expire, **kwargs): return {"expiryYear": f"{expire.year:04.0f}", "expiryMonth": f"{expire.month:02.0f}", "expiryDay": f"{expire.day:02.0f}", "expiryType": "ALL"}
    @staticmethod
    def strikes(*args, strike, **kwargs): return {"strikePriceNear": str(int(strike)), "noOfStrikes": "1000", "priceType": "ALL"}
    @staticmethod
    def options(*args, **kwargs): return {"optionCategory": "STANDARD", "chainType": "CALLPUT", "skipAdjusted": "true"}


class ETradeStockData(WebJSON, locator="//QuoteResponse/QuoteData[]", collection=True):
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Current(WebJSON.Text, locator="//dateTimeUTC", key="current", parser=ETradeMarketParsers.datetime): pass
    class Bid(WebJSON.Text, locator="//All/bid", key="bid", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class Ask(WebJSON.Text, locator="//All/ask", key="ask", parser=np.float32): pass
    class Supply(WebJSON.Text, locator="//All/askSize", key="supply", parser=np.int32): pass
    class Volume(WebJSON.Text, locator="//All/totalVolume", key="volume", parser=np.int64): pass

class ETradeExpireData(WebJSON, locator="//OptionExpireDateResponse/ExpirationDate[]", collection=True, optional=True):
    class Year(WebJSON.Text, locator="//year", key="year", parser=np.int16): pass
    class Month(WebJSON.Text, locator="//month", key="month", parser=np.int16): pass
    class Day(WebJSON.Text, locator="//day", key="day", parser=np.int16): pass

    def execute(self, *args, **kwargs):
        return Date(year=self["year"].data, month=self["month"].data, day=self["day"].data)


class ETradeOptionData(WebJSON, locator="//OptionChainResponse/OptionPair[]", collection=True, optional=True):
    class Call(WebJSON, locator="//Call", key="call"):
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
        class Current(WebJSON.Text, locator="//timeStamp", key="current", parser=ETradeMarketParsers.datetime): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=ETradeMarketParsers.expires): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=ETradeMarketParsers.strike): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass

    class Put(WebJSON, locator="//Put", key="put"):
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
        class Current(WebJSON.Text, locator="//timeStamp", key="current", parser=ETradeMarketParsers.datetime): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=ETradeMarketParsers.expires): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=ETradeMarketParsers.strike): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass


class ETradeExpirePage(WebJsonPage):
    def __call__(self, *args, ticker, expires, **kwargs):
        curl = ETradeExpireURL(ticker=ticker)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeExpireData(self.source)
        expires = [expire for expire in self.expires(contents) if expire in expires]
        return expires

    @staticmethod
    def expires(contents, *args, **kwargs):
        for content in contents:
            yield content(*args, **kwargs)


class ETradeSecurityPage(WebJsonPage, metaclass=RegistryMeta): pass
class ETradeStockPage(ETradeSecurityPage, register=Variables.Instruments.STOCK):
    def __call__(self, *args, ticker, **kwargs):
        curl = ETradeStockURL(ticker=ticker)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeStockData(self.source)
        stocks = self.stocks(contents, *args, instrument=Variables.Instruments.STOCK, **kwargs)
        return stocks

    @staticmethod
    def stocks(contents, *args, instrument, **kwargs):
        stocks = [{key: value(*args, **kwargs) for key, value in iter(content)} for content in iter(contents)]
        stocks = pd.DataFrame.from_records(stocks)
        long = stocks.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = Variables.Positions.LONG
        short = stocks.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = Variables.Positions.SHORT
        stocks = pd.concat([long, short], axis=0).reset_index(drop=True, inplace=False)
        stocks["instrument"] = instrument
        return stocks


class ETradeOptionPage(ETradeSecurityPage, register=Variables.Instruments.OPTION):
    def __call__(self, *args, ticker, expire, strike, **kwargs):
        curl = ETradeOptionURL(ticker=ticker, expire=expire, strike=strike)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeOptionData(self.source)
        puts = self.options(contents, *args, option=Variables.Options.PUT, **kwargs)
        calls = self.options(contents, *args, option=Variables.Options.CALL, **kwargs)
        options = pd.concat([puts, calls], axis=0)
        return options

    @staticmethod
    def options(contents, *args, option, **kwargs):
        string = str(option.name).lower()
        options = [{key: value(*args, **kwargs) for key, value in iter(content[string])} for content in iter(contents)]
        options = pd.DataFrame.from_records(options)
        long = options.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = Variables.Positions.LONG
        short = options.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = Variables.Positions.SHORT
        options = pd.concat([long, short], axis=0).reset_index(drop=True, inplace=False)
        options["instrument"] = Variables.Instruments.OPTION
        options["option"] = option
        return options


class ETradeProductDownloader(Pipelining, Sourcing, Logging, Sizing, Emptying):
    def __init_subclass__(cls, *args, **kwargs): pass
    def __init__(self, *args, **kwargs):
        Logging.__init__(self, *args, **kwargs)
        Pipelining.__init__(self, *args, **kwargs)
        self.__page = ETradeExpirePage(*args, **kwargs)

    def execute(self, stocks, *args, expires, **kwargs):
        assert isinstance(stocks, pd.DataFrame)
        if self.empty(stocks): return
        for symbol, dataframe in self.source(stocks, keys=list(Querys.Symbol)):
            symbol = Querys.Symbol(symbol)
            if self.empty(dataframe): continue
            parameters = dict(ticker=symbol.ticker, expires=expires)
            products = self.download(dataframe, *args, **parameters, **kwargs)
            string = f"Downloaded: {repr(self)}|{str(symbol)}[{len(products):.0f}]"
            self.logger.info(string)
            if not bool(products): continue
            yield products

    def download(self, stocks, *args, ticker, expires, **kwargs):
        assert isinstance(stocks, pd.DataFrame)
        assert len(set(stocks["ticker"].values)) == 1
        underlying = stocks.where(stocks["ticker"] == ticker).dropna(how="all", inplace=False)
        underlying = round(underlying["price"].mean(), 2)
        expires = self.page(*args, ticker=ticker, expires=expires, **kwargs)
        products = [Querys.Product([ticker, expire, underlying]) for expire in expires]
        return products

    @property
    def page(self): return self.__page


class ETradeSecurityDownloader(Pipelining, Logging, Sizing, Emptying, ABC, metaclass=RegistryMeta):
    def __init_subclass__(cls, *args, **kwargs): pass
    def __new__(cls, *args, **kwargs):
        if issubclass(cls, ETradeSecurityDownloader) and cls is not ETradeSecurityDownloader:
            return Pipelining.__new__(cls, *args, **kwargs)
        instrument = kwargs.get("instrument", None)
        return ETradeSecurityDownloader[instrument](*args, **kwargs)

    def __init__(self, *args, instrument, **kwargs):
        Logging.__init__(self, *args, **kwargs)
        Pipelining.__init__(self, *args, **kwargs)
        self.__page = ETradeSecurityPage[instrument](*args, **kwargs)

    @property
    def page(self): return self.__page


class ETradeStockDownloader(ETradeSecurityDownloader, register=Variables.Instruments.STOCK):
    def __init__(self, *args, instrument=Variables.Instruments.STOCK, **kwargs):
        assert instrument == Variables.Instruments.STOCK
        ETradeSecurityDownloader.__init__(self, *args, instrument=instrument, **kwargs)

    def execute(self, symbols, *args, **kwargs):
        assert isinstance(symbols, list) or isinstance(symbols, Querys.Symbol)
        symbols = symbols if isinstance(symbols, list) else [symbols]
        assert all([isinstance(symbol, Querys.Symbol) for symbol in symbols])
        if not bool(symbols): return
        for symbol in list(symbols):
            parameters = dict(ticker=symbol.ticker)
            stocks = self.download(*args, **parameters, **kwargs)
            size = self.size(stocks)
            string = f"Downloaded: {repr(self)}|{str(symbol)}[{size:.0f}]"
            self.logger.info(string)
            if self.empty(stocks): continue
            yield stocks

    def download(self, *args, ticker, **kwargs):
        stocks = self.page(*args, ticker=ticker, **kwargs)
        assert isinstance(stocks, pd.DataFrame)
        return stocks


class ETradeOptionDownloader(ETradeSecurityDownloader, register=Variables.Instruments.OPTION):
    def __init__(self, *args, instrument=Variables.Instruments.OPTION, **kwargs):
        assert instrument == Variables.Instruments.OPTION
        ETradeSecurityDownloader.__init__(self, *args, instrument=Variables.Instruments.OPTION, **kwargs)

    def execute(self, products, *args, **kwargs):
        assert isinstance(products, list) or isinstance(products, Querys.Product)
        products = products if isinstance(products, list) else [products]
        assert all([isinstance(product, Querys.Product) for product in products])
        if not bool(products): return
        for product in list(products):
            parameters = dict(ticker=product.ticker, expire=product.expire, underlying=product.strike, strike=product.strike)
            options = self.download(*args, **parameters, **kwargs)
            size = self.size(options)
            string = f"Downloaded: {repr(self)}|{str(product)}[{size:.0f}]"
            self.logger.info(string)
            if self.empty(options): continue
            yield options

    def download(self, *args, ticker, expire, strike, underlying, **kwargs):
        options = self.page(*args, ticker=ticker, expire=expire, strike=strike, **kwargs)
        assert isinstance(options, pd.DataFrame)
        options["underlying"] = underlying
        return options



