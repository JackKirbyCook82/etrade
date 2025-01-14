# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2023
@name:   ETrade Market Objects
@author: Jack Kirby Cook

"""

import pytz
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
from support.mixins import Emptying, Sizing, Logging, Segregating
from support.meta import RegistryMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeProductDownloader", "ETradeStockDownloader", "ETradeOptionDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


timestamp_parser = lambda content: Datetime.fromtimestamp(int(content), Timezone.utc).astimezone(pytz.timezone("US/Central"))
datetime_parser = lambda content: np.datetime64(timestamp_parser(content))
date_parser = lambda content: np.datetime64(timestamp_parser(content).date(), "D")
quote_parser = lambda content: Datetime.strptime(re.findall("(?<=:)[0-9:]+(?=:CALL|:PUT)", content)[0], "%Y:%m:%d")
expire_parser = lambda content: np.datetime64(quote_parser(content).date(), "D")
strike_parser = lambda content: np.round(content, 2).astype(np.float32)


class ETradeSecurityURL(WebURL, domain="https://api.etrade.com"): pass
class ETradeExpireURL(ETradeSecurityURL):
    @staticmethod
    def path(*args, **kwargs): return ["v1", "market", "optionexpiredate.json"]
    @staticmethod
    def parms(*args, ticker, **kwargs): return {"symbol": str(ticker), "expiryType": "ALL"}


class ETradeStockURL(ETradeSecurityURL):
    @staticmethod
    def path(*args, ticker, **kwargs): return ["v1", "market", "quote", f"{ticker}.json"]


class ETradeOptionsURL(ETradeSecurityURL):
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


class ETradeExpireData(WebJSON, locator="//OptionExpireDateResponse/ExpirationDate[]", multiple=True, optional=True):
    class Year(WebJSON.Text, locator="//year", key="year", parser=np.int16): pass
    class Month(WebJSON.Text, locator="//month", key="month", parser=np.int16): pass
    class Day(WebJSON.Text, locator="//day", key="day", parser=np.int16): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        return Date(**contents)


class ETradeStockData(WebJSON, locator="//QuoteResponse/QuoteData[]"):
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Current(WebJSON.Text, locator="//dateTimeUTC", key="current", parser=datetime_parser): pass
    class Bid(WebJSON.Text, locator="//All/bid", key="bid", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class Ask(WebJSON.Text, locator="//All/ask", key="ask", parser=np.float32): pass
    class Supply(WebJSON.Text, locator="//All/askSize", key="supply", parser=np.int32): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        stocks = pd.DataFrame.from_records([contents])
        long = stocks.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = Variables.Positions.LONG
        short = stocks.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = Variables.Positions.SHORT
        stocks = pd.concat([long, short], axis=0).reset_index(drop=True, inplace=False)
        stocks["instrument"] = Variables.Instruments.STOCK
        return stocks


class ETradeOptionData(WebJSON, ABC):
    class Option(WebJSON.Text, locator="//optionType", key="option", parser=Variables.Options): pass
    class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
    class Current(WebJSON.Text, locator="//timeStamp", key="current", parser=datetime_parser): pass
    class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=expire_parser): pass
    class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=np.float32): pass
    class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        options = pd.DataFrame.from_records([contents])
        long = options.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = Variables.Positions.LONG
        short = options.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = Variables.Positions.SHORT
        options = pd.concat([long, short], axis=0).reset_index(drop=True, inplace=False)
        options["instrument"] = Variables.Instruments.OPTION
        options["strike"] = options["strike"].apply(strike_parser)
        return options


class ETradeOptionsData(WebJSON, locator="//OptionChainResponse/OptionPair[]", multiple=True, optional=True):
    class Call(ETradeOptionData, locator="//Call", key="call"): pass
    class Put(ETradeOptionData, locator="//Put", key="put"): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        options = list(contents.values())
        options = pd.concat(options, axis=0)
        return options


class ETradeExpirePage(WebJsonPage):
    def execute(self, *args, ticker, **kwargs):
        parameters = dict(ticker=ticker)
        url = ETradeExpireURL(**parameters)
        self.load(url)
        jsondatas = ETradeExpireData(self.json, *args, **kwargs)
        expires = [jsondata(**parameters) for jsondata in jsondatas]
        included = lambda expire: expire in kwargs.get("expires", expires)
        expires = [expire for expire in expires if included(expire)]
        return expires


class ETradeSecurityPage(WebJsonPage, ABC, metaclass=RegistryMeta): pass
class ETradeStockPage(ETradeSecurityPage, register=Variables.Instruments.STOCK):
    def execute(self, *args, ticker, **kwargs):
        parameters = dict(ticker=ticker)
        url = ETradeStockURL(**parameters)
        self.load(url)
        jsondata = ETradeStockData(self.json, *args, **kwargs)
        stocks = jsondata(**parameters)
        return stocks


class ETradeOptionPage(ETradeSecurityPage, register=Variables.Instruments.OPTION):
    def execute(self, *args, ticker, expire, strike, **kwargs):
        parameters = dict(ticker=ticker, expire=expire, strike=strike)
        url = ETradeOptionsURL(**parameters)
        self.load(url)
        jsondatas = ETradeOptionsData(self.json, *args, **kwargs)
        options = [jsondata(**parameters) for jsondata in jsondatas]
        options = pd.concat(options, axis=0)
        return options


class ETradeProductDownloader(Segregating, Sizing, Emptying, Logging):
    def __init_subclass__(cls, *args, **kwargs): pass
    def __init__(self, *args, **kwargs):
        super().__init__(*args, query=Querys.Symbol, **kwargs)
        self.__page = ETradeExpirePage(*args, **kwargs)

    def execute(self, stocks, *args, **kwargs):
        assert isinstance(stocks, pd.DataFrame)
        if self.empty(stocks): return
        for query, dataframe in self.segregate(stocks, *args, **kwargs):
            products = self.download(dataframe, *args, **kwargs)
            string = f"Downloaded: {repr(self)}|{str(query)}[{len(products):.0f}]"
            self.logger.info(string)
            if not bool(products): continue
            yield from iter(products)

    def download(self, stocks, *args, expires, **kwargs):
        assert isinstance(stocks, pd.DataFrame)
        assert len(set(stocks["ticker"].values)) == 1
        ticker = list(set(stocks["ticker"].values))[0]
        underlying = round(stocks["price"].mean(), 2)
        parameters = dict(ticker=ticker, expires=expires)
        expires = self.page(*args, **parameters, **kwargs)
        products = [Querys.Product([ticker, expire, underlying]) for expire in expires]
        return products

    @property
    def page(self): return self.__page


class ETradeSecurityDownloader(Logging, Sizing, Emptying, ABC):
    def __init__(self, *args, instrument, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = ETradeSecurityPage[instrument](*args, **kwargs)
        self.__instrument = instrument

    @property
    def instrument(self): return self.__instrument
    @property
    def page(self): return self.__page


class ETradeStockDownloader(ETradeSecurityDownloader):
    def __init__(self, *args, **kwargs):
        parameters = dict(instrument=Variables.Instruments.STOCK)
        super().__init__(*args, **parameters, **kwargs)

    def execute(self, symbol, *args, **kwargs):
        if symbol is None: return
        stocks = self.download(symbol, *args, **kwargs)
        size = self.size(stocks)
        string = f"Downloaded: {repr(self)}|{str(symbol)}[{int(size):.0f}]"
        self.logger.info(string)
        if self.empty(stocks): return
        return stocks

    def download(self, symbol, *args, **kwargs):
        parameters = dict(ticker=symbol.ticker)
        stocks = self.page(*args, **parameters, **kwargs)
        assert isinstance(stocks, pd.DataFrame)
        return stocks


class ETradeOptionDownloader(ETradeSecurityDownloader):
    def __init__(self, *args, **kwargs):
        parameters = dict(instrument=Variables.Instruments.OPTION)
        super().__init__(*args, **parameters, **kwargs)

    def execute(self, product, *args, **kwargs):
        if product is None: return
        options = self.download(product, *args, **kwargs)
        size = self.size(options)
        string = f"Downloaded: {repr(self)}|{str(product)}[{int(size):.0f}]"
        self.logger.info(string)
        if self.empty(options): return
        return options

    def download(self, product, *args, underlying, **kwargs):
        parameters = dict(ticker=product.ticker, expire=product.expire, underlying=product.strike, strike=product.strike)
        options = self.page(*args, **parameters, **kwargs)
        assert isinstance(options, pd.DataFrame)
        options["underlying"] = underlying
        return options



