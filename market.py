# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2023
@name:   ETrade Market Objects
@author: Jack Kirby Cook

"""

import pytz
import regex as re
import numpy as np
import xarray as xr
import pandas as pd
from enum import IntEnum
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone
from collections import namedtuple as ntuple

from webscraping.weburl import WebURL
from webscraping.webnodes import WebJSON
from webscraping.webpages import WebJsonPage
from support.pipelines import Downloader
from support.dispatchers import kwargsdispatcher

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeStockDownloader", "ETradeOptionDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


Securities = IntEnum("Security", ["STOCK", "PUT", "CALL"], start=1)
Positions = IntEnum("Position", ["LONG", "SHORT"], start=1)

timestamp_parser = lambda x: Datetime.fromtimestamp(int(x), Timezone.utc).astimezone(pytz.timezone("US/Central"))
quote_parser = lambda x: Datetime.strptime(re.findall("(?<=:)[0-9:]+(?=:CALL|:PUT)", x)[0], "%Y:%m:%d")
datetime_parser = lambda x: np.datetime64(timestamp_parser(x))
date_parser = lambda x: np.datetime64(timestamp_parser(x).date(), "D")
expire_parser = lambda x: np.datetime64(quote_parser(x).date(), "D")
security_parser = lambda x: int(Securities[str(x).upper()])
position_parser = lambda x: int(Positions[str(x).upper()])


class ETradeStockQuote(ntuple("Queue", "ticker datetime price bid ask demand supply volume")):
    def __new__(cls, *args, **kwargs):
        contents = [kwargs.get(field, None) for field in cls._fields]
        return super().__new__(cls, *contents)


class ETradeMarketsURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"

    @kwargsdispatcher("dataset")
    def path(cls, *args, dataset, **kwargs): raise KeyError(dataset)
    @path.register.value("stock")
    def path_stocks(cls, *args, ticker, **kwargs): return "/v1/market/quote/{ticker}.json".format(ticker=str(ticker))
    @path.register.value("expire")
    def path_expires(cls, *args, **kwargs): return "/v1/market/optionexpiredate.json"
    @path.register.value("option")
    def path_options(cls, *args, **kwargs): return "/v1/market/optionchains.json"

    @kwargsdispatcher("dataset")
    def parms(cls, *args, dataset, **kwargs): raise KeyError(dataset)
    @parms.register.value("stock")
    def parms_stocks(cls, *args, **kwargs): return {}
    @parms.register.value("expire")
    def parms_expires(cls, *args, ticker, **kwargs): return {"symbol": str(ticker), "expiryType": "ALL"}

    @parms.register.value("option")
    def parms_options(cls, *args, ticker, expire, strike, count=1000, **kwargs):
        expires = {"expiryYear": "{:04.0f}".format(expire.year), "expiryMonth": "{:02.0f}".format(expire.month), "expiryDay": "{:02.0f}".format(expire.day), "expiryType": "ALL", "includeWeekly": "true"}
        strikes = {"strikePriceNear": str(int(strike)), "noOfStrikes": str(int(count)), "priceType": "ALL"}
        return {"symbol": str(ticker), **expires, **strikes}


class ETradeStockData(WebJSON, locator="//QuoteResponse/QuoteData", collection=True):
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", value=str): pass
    class Date(WebJSON.Text, locator="//dateTimeUTC", key="date", value=date_parser): pass
    class DateTime(WebJSON.Text, locator="//dateTimeUTC", key="datetime", value=datetime_parser): pass
    class LastTrade(WebJSON.Text, locator="//All/lastTrade", key="price", value=np.float16): pass
    class Open(WebJSON.Text, locator="//All/open", key="open", value=np.float16): pass
    class High(WebJSON.Text, locator="//All/high52", key="high", value=np.float16): pass
    class Low(WebJSON.Text, locator="//All/low52", key="low", value=np.float16): pass
    class BidPrice(WebJSON.Text, locator="//All/bid", key="bid", value=np.float16): pass
    class BidSize(WebJSON.Text, locator="//All/bidSize", key="demand", value=np.int32): pass
    class AskPrice(WebJSON.Text, locator="//All/ask", key="ask", value=np.float16): pass
    class AskSize(WebJSON.Text, locator="//All/askSize", key="supply", value=np.int32): pass
    class Volume(WebJSON.Text, locator="//All/totalVolume", key="volume", value=np.int64): pass

    @staticmethod
    def parser(contents, *args, **kwargs):
        quote = ETradeStockQuote(**{key: value(*args, **kwargs) for key, value in contents[0].items()})
        return quote


class ETradeExpireData(WebJSON, locator="//OptionExpireDateResponse/ExpirationDate", collection=True, optional=True):
    class Year(WebJSON.Text, locator="//year", key="year", value=np.int16): pass
    class Month(WebJSON.Text, locator="//month", key="month", value=np.int16): pass
    class Day(WebJSON.Text, locator="//day", key="day", value=np.int16): pass

    @staticmethod
    def parser(contents, *args, **kwargs):
        expire = lambda content: Date(year=content["year"](*args, **kwargs), month=content["month"](*args, **kwargs), day=content["day"](*args, **kwargs))
        return [expire(content) for content in iter(contents)]


class ETradeOptionData(WebJSON, locator="//OptionChainResponse/OptionPair", collection=True, optional=True):
    class Call(WebJSON, locator="//Call", key="call"):
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", value=str): pass
        class Security(WebJSON.Text, locator="//optionType", key="security", value=security_parser): pass
        class Date(WebJSON.Text, locator="//timeStamp", key="date", value=date_parser): pass
        class DateTime(WebJSON.Text, locator="//timeStamp", key="datetime", value=datetime_parser): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", value=expire_parser): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", value=np.float32): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", value=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", value=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", value=np.float32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", value=np.float32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", value=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", value=np.int32): pass

    class Put(WebJSON, locator="//Put", key="put"):
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", value=str): pass
        class Security(WebJSON.Text, locator="//optionType", key="security", value=security_parser): pass
        class Date(WebJSON.Text, locator="//timeStamp", key="date", value=date_parser): pass
        class DateTime(WebJSON.Text, locator="//timeStamp", key="datetime", value=datetime_parser): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", value=expire_parser): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", value=np.float32): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", value=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", value=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", value=np.float32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", value=np.float32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", value=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", value=np.int32): pass

    @staticmethod
    def parser(contents, *args, **kwargs):
        puts = [{key: value(*args, **kwargs) for key, value in iter(content["put"])} for content in iter(contents)]
        calls = [{key: value(*args, **kwargs) for key, value in iter(content["call"])} for content in iter(contents)]
        dataframe = pd.DataFrame.from_records(puts + calls)
        long = dataframe.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = int(Positions.LONG)
        short = dataframe.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = int(Positions.SHORT)
        dataframe = pd.concat([long, short], axis=0)
        dataframe = dataframe.set_index(["ticker", "security", "position", "date", "expire", "strike"], inplace=False, drop=True)
        dataset = xr.Dataset.from_dataframe(dataframe)
        dataset = dataset.squeeze("ticker").squeeze("date").squeeze("expire")
        return dataset


class ETradeStocksPage(WebJsonPage): pass
class ETradeExpiresPage(WebJsonPage): pass
class ETradeOptionsPage(WebJsonPage): pass


stock_pages = {"stock": ETradeStocksPage}
class ETradeStockDownloader(Downloader, pages=stock_pages):
    def execute(self, ticker, *args, **kwargs):
        curl = ETradeMarketsURL(dataset="stocks", ticker=str(ticker))
        self["stock"].load(str(curl.address), params=dict(curl.query))
        source = self.pages["stock"].source
        stocks = ETradeStockData(source)(*args, **kwargs)
        yield stocks


option_pages = {"stock": ETradeStocksPage, "expire": ETradeExpiresPage, "option": ETradeOptionsPage}
class ETradeOptionDownloader(Downloader, pages=option_pages):
    def execute(self, ticker, *args, expires, **kwargs):
        curl = ETradeMarketsURL(dataset="stock", ticker=ticker)
        self["stock"].load(str(curl.address), params=dict(curl.query))
        source = self.pages["stock"].source
        stocks = ETradeStockData(source)(*args, **kwargs)
        curl = ETradeMarketsURL(dataset="expire", ticker=ticker)
        self["expire"].load(str(curl.address), params=dict(curl.query))
        source = self.pages["expire"].source
        for expire in ETradeExpireData(source)(*args, **kwargs):
            if expire not in expires:
                continue
            curl = ETradeMarketsURL(dataset="option", ticker=ticker, expire=expire, strike=stocks.price)
            self["option"].load(str(curl.address), params=dict(curl.query))
            source = self.pages["option"].source
            options = ETradeOptionData(source)(*args, ticker=ticker, expire=expire, **kwargs)
            yield ticker, expire, options


