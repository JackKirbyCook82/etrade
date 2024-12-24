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
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Logging, Separating
from support.meta import RegistryMeta

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


class ETradeExpireData(WebJSON, locator="//OptionExpireDateResponse/ExpirationDate[]", key="expire", multiple=True, optional=True):
    class Year(WebJSON.Text, locator="//year", key="year", parser=np.int16): pass
    class Month(WebJSON.Text, locator="//month", key="month", parser=np.int16): pass
    class Day(WebJSON.Text, locator="//day", key="day", parser=np.int16): pass


class ETradeStockData(WebJSON, locator="//QuoteResponse/QuoteData[]", key="stock", multiple=True, optional=True):
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Current(WebJSON.Text, locator="//dateTimeUTC", key="current", parser=ETradeMarketParsers.datetime): pass
    class Bid(WebJSON.Text, locator="//All/bid", key="bid", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class Ask(WebJSON.Text, locator="//All/ask", key="ask", parser=np.float32): pass
    class Supply(WebJSON.Text, locator="//All/askSize", key="supply", parser=np.int32): pass
    class Volume(WebJSON.Text, locator="//All/totalVolume", key="volume"): pass


class ETradeOptionData(WebJSON):
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

class ETradeOptionsData(WebJSON, locator="//OptionChainResponse/OptionPair[]", key="option", multiple=True, optional=True):
    class Call(ETradeOptionData, locator="//Call", key="call"): pass
    class Put(ETradeOptionData, locator="//Put", key="put"): pass


class ETradeExpirePage(WebJsonPage):
    def execute(self, *args, ticker, **kwargs):
        parameters = dict(ticker=ticker)
        url = ETradeExpireURL(**parameters)
        self.load(url)
        jsondata = ETradeExpireData(self.source.json)
        contents = jsondata(**parameters)
        expires = self.parse(contents, **parameters)
        included = lambda expire: expire in kwargs.get("expires", expires)
        expires = [expire for expire in expires if included(expire)]
        return expires

    @staticmethod
    def parse(contents, *args, **kwargs):
        assert isinstance(contents, list) and all([isinstance(content, dict) for content in contents])
        expires = [Date(**{key: value(*args, **kwargs) for key, value in content.items()}) for content in contents]
        return expires


class ETradeSecurityPage(WebJsonPage, ABC, metaclass=RegistryMeta): pass
class ETradeStockPage(ETradeSecurityPage, register=Variables.Instruments.STOCK):
    def execute(self, *args, ticker, **kwargs):
        parameters = dict(ticker=ticker)
        url = ETradeStockURL(**parameters)
        self.load(url)
        jsondata = ETradeStockData(self.source.json)
        contents = jsondata(**parameters)
        stocks = self.parse(contents, **parameters)
        return stocks

    @staticmethod
    def parse(contents, *args, **kwargs):
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


class ETradeOptionPage(ETradeSecurityPage, register=Variables.Instruments.OPTION):
    def execute(self, *args, ticker, expire, strike, **kwargs):
        parameters = dict(ticker=ticker, expire=expire, strike=strike)
        url = ETradeOptionURL(**parameters)
        self.load(url)
        jsondata = ETradeOptionData(self.source.json)
        contents = jsondata(**parameters)
        options = dict(put=Variables.Options.PUT, call=Variables.Options.CALL).items()
        options = itertools.product(options, contents)
        options = [self.parse(content[key], option=option, **parameters) for (key, option), content in options]
        options = pd.concat(options, axis=0)
        return options

    @staticmethod
    def parse(contents, *args, option, **kwargs):
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


class ETradeProductDownloader(Logging, Sizing, Emptying, Separating):
    def __init_subclass__(cls, *args, **kwargs): pass
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        super().__init__(*args, **kwargs)
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



