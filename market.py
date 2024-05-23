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
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone

from finance.variables import Contract, Instruments, Positions
from webscraping.weburl import WebURL
from webscraping.webdatas import WebJSON
from webscraping.webpages import WebJsonPage
from support.query import Header, Query
from support.pipelines import Processor

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeContractDownloader", "ETradeMarketDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


stocks_index = {"instrument": str, "position": str, "ticker": str, "date": np.datetime64}
stocks_columns = {"price": np.float32, "size": np.float32, "volume": np.float32}
stocks_header = Header(pd.DataFrame, index=list(stocks_index.keys()), columns=list(stocks_columns.keys()))
options_index = {"instrument": str, "position": str, "strike": np.float32, "ticker": str, "expire": np.datetime64, "date": np.datetime64}
options_columns = {"price": np.float32, "underlying": np.float32, "size": np.float32, "volume": np.float32, "interest": np.float32}
options_header = Header(pd.DataFrame, index=list(options_index.keys()), columns=list(options_columns.keys()))
securities_headers = dict(stocks=stocks_header, options=options_header)
timestamp_parser = lambda x: Datetime.fromtimestamp(int(x), Timezone.utc).astimezone(pytz.timezone("US/Central"))
quote_parser = lambda x: Datetime.strptime(re.findall("(?<=:)[0-9:]+(?=:CALL|:PUT)", x)[0], "%Y:%m:%d")
datetime_parser = lambda x: np.datetime64(timestamp_parser(x))
date_parser = lambda x: np.datetime64(timestamp_parser(x).date(), "D")
expire_parser = lambda x: np.datetime64(quote_parser(x).date(), "D")
strike_parser = lambda x: np.round(x, 2).astype(np.float32)


class ETradeMarketsURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"

class ETradeStockURL(ETradeMarketsURL):
    def path(cls, *args, ticker=None, tickers=[], **kwargs):
        tickers = ",".join([ticker] if bool(ticker) else [] + tickers)
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
    class Date(WebJSON.Text, locator="//dateTimeUTC", key="date", parser=date_parser): pass
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
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
        class Date(WebJSON.Text, locator="//timeStamp", key="date", parser=date_parser): pass
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=expire_parser): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=strike_parser): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass

    class Put(WebJSON, locator="//Put", key="put"):
        class Date(WebJSON.Text, locator="//timeStamp", key="date", parser=date_parser): pass
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=expire_parser): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=strike_parser): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass


class ETradeStockPage(WebJsonPage):
    def __call__(self, ticker, *args, **kwargs):
        curl = ETradeStockURL(ticker=ticker)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeStockData(self.source)
        stocks = self.stocks(contents, *args, instrument=Instruments.STOCK, **kwargs)
        return stocks

    @staticmethod
    def stocks(contents, *args, instrument, **kwargs):
        stocks = [{key: value(*args, **kwargs) for key, value in iter(content)} for content in iter(contents)]
        stocks = pd.DataFrame.from_records(stocks)
        long = stocks.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = str(Positions.LONG.name).lower()
        short = stocks.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = str(Positions.SHORT.name).lower()
        stocks = pd.concat([long, short], axis=0).reset_index(drop=True, inplace=False)
        stocks["instrument"] = str(instrument.name).lower()
        return stocks


class ETradeExpirePage(WebJsonPage):
    def __call__(self, ticker, *args, **kwargs):
        curl = ETradeExpireURL(ticker=ticker)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeExpireData(self.source)
        return list(self.expires(contents))

    @staticmethod
    def expires(contents, *args, **kwargs):
        for content in contents:
            yield content(*args, **kwargs)


class ETradeOptionPage(WebJsonPage):
    def __call__(self, ticker, *args, expire, strike, **kwargs):
        curl = ETradeOptionURL(ticker=ticker, expire=expire, strike=strike)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeOptionData(self.source)
        puts = self.options(contents, *args, instrument=Instruments.PUT, **kwargs)
        calls = self.options(contents, *args, instrument=Instruments.CALL, **kwargs)
        options = pd.concat([puts, calls], axis=0)
        return options

    @staticmethod
    def options(contents, *args, instrument, **kwargs):
        string = str(instrument.name).lower()
        options = [{key: value(*args, **kwargs) for key, value in iter(content[string])} for content in iter(contents)]
        options = pd.DataFrame.from_records(options)
        long = options.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = str(Positions.LONG.name).lower()
        short = options.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = str(Positions.SHORT.name).lower()
        options = pd.concat([long, short], axis=0).reset_index(drop=True, inplace=False)
        options["instrument"] = str(instrument.name).lower()
        return options


class ETradeContractDownloader(Processor):
    def __init__(self, *args, feed, name=None, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        self.__expire = ETradeExpirePage(*args, feed=feed, **kwargs)

    @Query(arguments=["ticker"])
    def execute(self, *args, ticker, expires=[], **kwargs):
        for expire in self.expire(ticker, *args, **kwargs):
            if expire not in expires:
                continue
            contract = Contract(ticker, expire)
            yield dict(contract=contract)

    @property
    def expire(self): return self.__expire


class ETradeMarketDownloader(Processor, title="Downloaded"):
    def __init__(self, *args, feed, name=None, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        self.__stock = ETradeStockPage(*args, feed=feed, **kwargs)
        self.__option = ETradeOptionPage(*args, feed=feed, **kwargs)

    @Query(arguments=["contract"], headers=securities_headers)
    def execute(self, *args, contract, **kwargs):
        stocks = self.stock(contract.ticker, *args, **kwargs)
        underlying = stocks["price"].mean()
        options = self.option(contract.ticker, *args, expire=contract.expire, strike=underlying, **kwargs)
        options["underlying"] = underlying
        yield dict(stocks=stocks, options=options)

    @property
    def stock(self): return self.__stock
    @property
    def option(self): return self.__option



