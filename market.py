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
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone
from collections import OrderedDict as ODict

from finance.variables import Pipelines, Variables, Symbol, Contract
from webscraping.weburl import WebURL
from webscraping.webdatas import WebJSON
from webscraping.webpages import WebJsonPage

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeContractDownloader", "ETradeMarketDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"
__logger__ = logging.getLogger(__name__)


class Parsers:
    timestamp = lambda x: Datetime.fromtimestamp(int(x), Timezone.utc).astimezone(pytz.timezone("US/Central"))
    datetime = lambda x: np.datetime64(Parsers.timestamp(x))
    date = lambda x: np.datetime64(Parsers.timestamp(x).date(), "D")
    quote = lambda x: Datetime.strptime(re.findall("(?<=:)[0-9:]+(?=:CALL|:PUT)", x)[0], "%Y:%m:%d")
    expires = lambda x: np.datetime64(Parsers.quote(x).date(), "D")
    strike = lambda x: np.round(x, 2).astype(np.float32)


class ETradeMarketsURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"

class ETradeStockURL(ETradeMarketsURL):
    def path(cls, *args, ticker=None, tickers=[], **kwargs):
        tickers = ([ticker] if bool(ticker) else []) + tickers
        assert bool(tickers)
        tickers = ",".join(tickers)
        return f"/v1/market/quote/{tickers}.json"

class ETradeExpireURL(ETradeMarketsURL):
    def path(cls, *args, **kwargs): return "/v1/market/optionexpiredate.json"
    def parms(cls, *args, ticker, **kwargs): return {"symbol": str(ticker), "expiryType": "ALL"}

class ETradeOptionURL(ETradeMarketsURL):
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
    class Current(WebJSON.Text, locator="//dateTimeUTC", key="current", parser=Parsers.datetime): pass
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
        class Current(WebJSON.Text, locator="//timeStamp", key="current", parser=Parsers.datetime): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=Parsers.expires): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=Parsers.strike): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass

    class Put(WebJSON, locator="//Put", key="put"):
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
        class Current(WebJSON.Text, locator="//timeStamp", key="current", parser=Parsers.datetime): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=Parsers.expires): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=Parsers.strike): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass


class ETradeStockPage(WebJsonPage):
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


class ETradeExpirePage(WebJsonPage):
    def __call__(self, *args, ticker, **kwargs):
        curl = ETradeExpireURL(ticker=ticker)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeExpireData(self.source)
        return list(self.expires(contents))

    @staticmethod
    def expires(contents, *args, **kwargs):
        for content in contents:
            yield content(*args, **kwargs)


class ETradeOptionPage(WebJsonPage):
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


class ETradeContractDownloader(Pipelines.Processor, title="Downloaded"):
    def __init__(self, *args, feed, name=None, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        pages = {Variables.Querys.CONTRACT: ETradeExpirePage}
        self.__pages = {variable: page(*args, feed=feed, **kwargs) for variable, page in pages.items()}

    def processor(self, contents, *args, expires=[], **kwargs):
        symbol = contents[Variables.Querys.SYMBOL]
        assert isinstance(symbol, Symbol)
        parameters = dict(ticker=symbol.ticker, expires=expires)
        contracts = list(self.download(*args, **parameters, **kwargs))
        if not bool(contracts): return
        for contract in contracts:
            contract = {Variables.Querys.CONTRACT: contract}
            yield contents | dict(contract)

    def download(self, *args, ticker, expires, **kwargs):
        generator = self.pages[Variables.Querys.CONTRACT](*args, ticker=ticker, **kwargs)
        expires = [expire for expire in iter(generator) if expire in expires]
        return [Contract(ticker, expire) for expire in expires]

    @property
    def pages(self): return self.__pages


class ETradeMarketDownloader(Pipelines.Processor, title="Downloaded"):
    def __init__(self, *args, feed, name=None, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        pages = {Variables.Instruments.STOCK: ETradeStockPage, Variables.Instruments.OPTION: ETradeOptionPage}
        self.__pages = {variable: page(*args, feed=feed, **kwargs) for variable, page in pages.items()}

    def processor(self, contents, *args, **kwargs):
        contract = contents[Variables.Querys.CONTRACT]
        assert isinstance(contract, Contract)
        parameters = dict(ticker=contract.ticker, expire=contract.expire)
        securities = list(self.download(*args, **parameters, **kwargs))
        if not bool(securities): return
        yield contents | ODict(securities)

    def download(self, *args, ticker, expire, **kwargs):
        variable = Variables.Instruments.STOCK
        stocks = self.pages[variable](*args, ticker=ticker, **kwargs)
        underlying = stocks["price"].mean()
        variable = Variables.Instruments.OPTION
        options = self.pages[variable](*args, ticker=ticker, expire=expire, strike=underlying, **kwargs)
        options["underlying"] = underlying
        yield variable, options

    @property
    def pages(self): return self.__pages



