# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2023
@name:   ETrade Market Objects
@author: Jack Kirby Cook

"""

import pytz
import itertools
import regex as re
import numpy as np
import pandas as pd
from abc import ABC
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone

from finance.variables import Variables, Querys
from webscraping.webpages import WebJsonPage
from webscraping.webdatas import WebJSONs
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Logging, Separating

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeProductDownloader", "ETradeStockDownloader", "ETradeOptionDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ETradeMarketParsers(object):
    timestamp = lambda x: Datetime.fromtimestamp(int(x), Timezone.utc).astimezone(pytz.timezone("US/Central"))
    datetime = lambda x: np.datetime64(ETradeMarketParsers.timestamp(x))
    date = lambda x: np.datetime64(ETradeMarketParsers.timestamp(x).date(), "D")
    quote = lambda x: Datetime.strptime(re.findall("(?<=:)[0-9:]+(?=:CALL|:PUT)", x)[0], "%Y:%m:%d")
    expires = lambda x: np.datetime64(ETradeMarketParsers.quote(x).date(), "D")
    strike = lambda x: np.round(x, 2).astype(np.float32)


class ETradeSecurityURL(WebURL, domain="https://api.etrade.com"): pass
class ETradeExpireURL(ETradeSecurityURL):
    @staticmethod
    def path(*args, **kwargs): return ["v1", "market", "optionexpiredate.json"]
    @staticmethod
    def parms(args, ticker, **kwargs): return {"symbol": str(ticker), "expiryType": "ALL"}

class ETradeStockURL(ETradeSecurityURL):
    @staticmethod
    def path(*args, ticker, **kwargs): return ["v1", "market", "quote", f"{ticker}.json"]

class ETradeOptionURL(ETradeSecurityURL):
    @staticmethod
    def path(*args, **kwargs): return ["v1", "market", "optionchains.json"]
    @classmethod
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


class ETradeExpireData(WebJSONs.JSON, locator="//OptionExpireDateResponse/ExpirationDate[]", key="expire", multiple=True, optional=True):
    class Year(WebJSONs.Text, locator="//year", key="year", parser=np.int16): pass
    class Month(WebJSONs.Text, locator="//month", key="month", parser=np.int16): pass
    class Day(WebJSONs.Text, locator="//day", key="day", parser=np.int16): pass

    @staticmethod
    def execute(contents, *args, **kwargs):
        assert isinstance(contents, list) and all([isinstance(content, dict) for content in contents])
        expires = [Date(**{key: value(*args, **kwargs) for key, value in content.items()}) for content in contents]
        return expires

class ETradeStockData(WebJSONs.JSON, locator="//QuoteResponse/QuoteData[]", key="stock", multiple=True, optional=True):
    class Ticker(WebJSONs.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Current(WebJSONs.Text, locator="//dateTimeUTC", key="current", parser=ETradeMarketParsers.datetime): pass
    class Bid(WebJSONs.Text, locator="//All/bid", key="bid", parser=np.float32): pass
    class Demand(WebJSONs.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class Ask(WebJSONs.Text, locator="//All/ask", key="ask", parser=np.float32): pass
    class Supply(WebJSONs.Text, locator="//All/askSize", key="supply", parser=np.int32): pass
    class Volume(WebJSONs.Text, locator="//All/totalVolume", key="volume"): pass

    @staticmethod
    def execute(contents, *args, **kwargs):
        assert isinstance(contents, list) and all([isinstance(content, dict) for content in contents])
        contents = [{key: value(*args, **kwargs) for key, value in content.items()} for content in contents]
        stocks = pd.DataFrame.from_records(contents)
        long = stocks.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = Variables.Positions.LONG
        short = stocks.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = Variables.Positions.SHORT
        stocks = pd.concat([long, short], axis=0).reset_index(drop=True, inplace=False)
        stocks["instrument"] = Variables.Instruments.STOCK
        return stocks

class ETradeOptionData(WebJSONs.JSON, multiple=True, optional=True):
    class Ticker(WebJSONs.Text, locator="//symbol", key="ticker", parser=str): pass
    class Current(WebJSONs.Text, locator="//timeStamp", key="current", parser=ETradeMarketParsers.datetime): pass
    class Expire(WebJSONs.Text, locator="//quoteDetail", key="expire", parser=ETradeMarketParsers.expires): pass
    class Strike(WebJSONs.Text, locator="//strikePrice", key="strike", parser=ETradeMarketParsers.strike): pass
    class Bid(WebJSONs.Text, locator="//bid", key="bid", parser=np.float32): pass
    class Ask(WebJSONs.Text, locator="//ask", key="ask", parser=np.float32): pass
    class Demand(WebJSONs.Text, locator="//bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSONs.Text, locator="//askSize", key="supply", parser=np.int32): pass
    class Volume(WebJSONs.Text, locator="//volume", key="volume", parser=np.int64): pass
    class Interest(WebJSONs.Text, locator="//openInterest", key="interest", parser=np.int32): pass

    @staticmethod
    def execute(contents, *args, option, **kwargs):
        assert isinstance(contents, list) and all([isinstance(content, dict) for content in contents])
        contents = [{key: value(*args, **kwargs) for key, value in content.items()} for content in contents]
        options = pd.DataFrame.from_records(contents)
        long = options.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = Variables.Positions.LONG
        short = options.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = Variables.Positions.SHORT
        options = pd.concat([long, short], axis=0).reset_index(drop=True, inplace=False)
        options["instrument"] = Variables.Instruments.OPTION
        options["option"] = option
        return options

class ETradeOptionsData(WebJSONs.JSON, locator="//OptionChainResponse/OptionPair[]", key="option"):
    class Call(ETradeOptionData, locator="//Call", key="call"): pass
    class Put(ETradeOptionData, locator="//Put", key="put"): pass

    @staticmethod
    def execute(contents, *args, **kwargs):
        assert isinstance(contents, list) and all([isinstance(content, dict) for content in contents])
        options = dict(put=Variables.Options.PUT, call=Variables.Options.CALL).items()
        options = itertools.product(options, contents)
        options = [content[key](*args, option=option, **kwargs) for (key, option), content in options]
        options = pd.concat(options, axis=0)
        return options


class ETradeExpirePage(WebJsonPage, data=ETradeExpireData):
    def execute(self, *args, ticker, **kwargs):
        parameters = dict(ticker=ticker)
        url = ETradeExpireURL(**parameters)
        self.load(url)
        expires = self["expire"](*args, **kwargs)
        expires = [expire for expire in expires if expire in kwargs.get("expires", expires)]
        return expires

class ETradeSecurityPage(WebJsonPage, ABC): pass
class ETradeStockPage(ETradeSecurityPage, data=ETradeStockData):
    def execute(self, *args, ticker, **kwargs):
        parameters = dict(ticker=ticker)
        url = ETradeStockURL(**parameters)
        self.load(url)
        stocks = self["stock"](*args, **kwargs)
        return stocks

class ETradeOptionPage(ETradeSecurityPage, data=ETradeOptionData):
    def execute(self, *args, ticker, expire, strike, **kwargs):
        parameters = dict(ticker=ticker, expire=expire, strike=strike)
        url = ETradeOptionURL(**parameters)
        self.load(url)
        options = self["options"](*args, **kwargs)
        return options


class ETradeProductDownloader(Logging, Sizing, Emptying, Separating):
    def __init_subclass__(cls, *args, **kwargs): pass
    def __init__(self, *args, **kwargs):
        try: super().__init__(*args, **kwargs)
        except TypeError: super().__init__()
        self.__page = ETradeExpirePage(*args, **kwargs)
        self.__query = Querys.Symbol

    def execute(self, stocks, *args, expires, **kwargs):
        assert isinstance(stocks, pd.DataFrame)
        if self.empty(stocks): return
        for parameters, dataframe in self.separate(stocks, *args, fields=self.fields, **kwargs):
            symbol = self.query(parameters)
            parameters = dict(ticker=symbol.ticker, expires=expires)
            products = self.download(dataframe, *args, **parameters, **kwargs)
            string = f"Downloaded: {repr(self)}|{str(symbol)}[{len(products):.0f}]"
            self.logger.info(string)
            if not bool(products): continue
            yield from iter(products)

    def download(self, stocks, *args, ticker, **kwargs):
        assert isinstance(stocks, pd.DataFrame)
        assert len(set(stocks["ticker"].values)) == 1
        underlying = stocks.where(stocks["ticker"] == ticker).dropna(how="all", inplace=False)
        underlying = round(underlying["price"].mean(), 2)
        expires = self.page(*args, ticker=ticker, **kwargs)
        products = [Querys.Product([ticker, expire, underlying]) for expire in expires]
        return products

    @property
    def fields(self): return list(self.__query)
    @property
    def query(self): return self.__query
    @property
    def page(self): return self.__page

class ETradeSecurityDownloader(Logging, Sizing, Emptying, ABC):
    def __init__(self, *args, instrument, query, **kwargs):
        try: super().__init__(*args, **kwargs)
        except TypeError: super().__init__()
        self.__page = ETradeSecurityPage[instrument](*args, **kwargs)
        self.__instrument = instrument
        self.__query = query

    @property
    def instrument(self): return self.__instrument
    @property
    def query(self): return self.__query
    @property
    def page(self): return self.__page

class ETradeStockDownloader(ETradeSecurityDownloader):
    def __init__(self, *args, **kwargs):
        parameters = dict(instrument=Variables.Instruments.STOCK, query=Querys.Symbol)
        super().__init__(*args, **parameters, **kwargs)

    def execute(self, symbol, *args, **kwargs):
        if symbol is None: return
        symbol = self.query(symbol)
        parameters = dict(ticker=symbol.ticker)
        stocks = self.download(*args, **parameters, **kwargs)
        size = self.size(stocks)
        string = f"Downloaded: {repr(self)}|{str(symbol)}[{int(size):.0f}]"
        self.logger.info(string)
        if self.empty(stocks): return
        return stocks

    def download(self, *args, **kwargs):
        stocks = self.page(*args, **kwargs)
        assert isinstance(stocks, pd.DataFrame)
        return stocks

class ETradeOptionDownloader(ETradeSecurityDownloader):
    def __init__(self, *args, **kwargs):
        parameters = dict(instrument=Variables.Instruments.OPTION, query=Querys.Product)
        super().__init__(*args, **parameters, **kwargs)

    def execute(self, product, *args, **kwargs):
        if product is None: return
        product = self.query(product)
        parameters = dict(ticker=product.ticker, expire=product.expire, underlying=product.strike, strike=product.strike)
        options = self.download(*args, **parameters, **kwargs)
        size = self.size(options)
        string = f"Downloaded: {repr(self)}|{str(product)}[{int(size):.0f}]"
        self.logger.info(string)
        if self.empty(options): return
        return options

    def download(self, *args, underlying, **kwargs):
        options = self.page(*args, **kwargs)
        assert isinstance(options, pd.DataFrame)
        options["underlying"] = underlying
        return options



