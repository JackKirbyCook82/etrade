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
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone

from webscraping.weburl import WebURL
from webscraping.webdatas import WebJSON
from webscraping.webpages import WebJsonPage
from support.pipelines import Downloader
from support.dispatchers import kwargsdispatcher
from finance.securities import Securities, Positions

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeSecurityDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


timestamp_parser = lambda x: Datetime.fromtimestamp(int(x), Timezone.utc).astimezone(pytz.timezone("US/Central"))
quote_parser = lambda x: Datetime.strptime(re.findall("(?<=:)[0-9:]+(?=:CALL|:PUT)", x)[0], "%Y:%m:%d")
datetime_parser = lambda x: np.datetime64(timestamp_parser(x))
date_parser = lambda x: np.datetime64(timestamp_parser(x).date(), "D")
expire_parser = lambda x: np.datetime64(quote_parser(x).date(), "D")
security_parser = lambda x: int(Securities[str(x).upper()])
position_parser = lambda x: int(Positions[str(x).upper()])


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
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Date(WebJSON.Text, locator="//dateTimeUTC", key="date", parser=date_parser): pass
    class DateTime(WebJSON.Text, locator="//dateTimeUTC", key="datetime", parser=datetime_parser): pass
    class BidPrice(WebJSON.Text, locator="//All/bid", key="bid", parser=np.float16): pass
    class BidSize(WebJSON.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class AskPrice(WebJSON.Text, locator="//All/ask", key="ask", parser=np.float16): pass
    class AskSize(WebJSON.Text, locator="//All/askSize", key="supply", parser=np.int32): pass
    class Volume(WebJSON.Text, locator="//All/totalVolume", key="volume", parser=np.int64): pass

    @staticmethod
    def execute(contents, *args, **kwargs):
        stocks = [{key: value(*args, **kwargs) for key, value in iter(content)} for content in iter(contents)]
        dataframe = pd.DataFrame.from_records(stocks)
        dataframe["security"] = int(Securities.STOCK)
        long = dataframe.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        long["position"] = int(Positions.LONG)
        short = dataframe.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        short["position"] = int(Positions.SHORT)
        dataframe = pd.concat([long, short], axis=0)
        dataframe = dataframe.set_index(["ticker", "security", "position", "date"], inplace=False, drop=True)
        dataset = xr.Dataset.from_dataframe(dataframe)
        dataset = dataset.squeeze("ticker").squeeze("date")
        return dataset


class ETradeExpireData(WebJSON, locator="//OptionExpireDateResponse/ExpirationDate", collection=True, optional=True):
    class Year(WebJSON.Text, locator="//year", key="year", parser=np.int16): pass
    class Month(WebJSON.Text, locator="//month", key="month", parser=np.int16): pass
    class Day(WebJSON.Text, locator="//day", key="day", parser=np.int16): pass

    @staticmethod
    def execute(contents, *args, **kwargs):
        expire = lambda content: Date(year=content["year"](*args, **kwargs), month=content["month"](*args, **kwargs), day=content["day"](*args, **kwargs))
        return [expire(content) for content in iter(contents)]


class ETradeOptionData(WebJSON, locator="//OptionChainResponse/OptionPair", collection=True, optional=True):
    class Call(WebJSON, locator="//Call", key="call"):
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
        class Security(WebJSON.Text, locator="//optionType", key="security", parser=security_parser): pass
        class Date(WebJSON.Text, locator="//timeStamp", key="date", parser=date_parser): pass
        class DateTime(WebJSON.Text, locator="//timeStamp", key="datetime", parser=datetime_parser): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=expire_parser): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=np.float32): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.float32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass

    class Put(WebJSON, locator="//Put", key="put"):
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
        class Security(WebJSON.Text, locator="//optionType", key="security", parser=security_parser): pass
        class Date(WebJSON.Text, locator="//timeStamp", key="date", parser=date_parser): pass
        class DateTime(WebJSON.Text, locator="//timeStamp", key="datetime", parser=datetime_parser): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=expire_parser): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=np.float32): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.float32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.float32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass

    @staticmethod
    def execute(contents, *args, **kwargs):
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


option_pages = {"stock": ETradeStocksPage, "expire": ETradeExpiresPage, "option": ETradeOptionsPage}
class ETradeSecurityDownloader(Downloader, pages=option_pages):
    def execute(self, ticker, *args, expires, **kwargs):
        curl = ETradeMarketsURL(dataset="stock", ticker=ticker)
        self["stock"].load(str(curl.address), params=dict(curl.query))
        source = self.pages["stock"].source
        stocks = ETradeStockData(source)(*args, **kwargs)
        bid = stocks["price"].sel({"security": int(Securities.STOCK), "position": int(Positions.LONG)})
        ask = stocks["price"].sel({"security": int(Securities.STOCK), "position": int(Positions.SHORT)})
        price = np.average(bid, ask)
        curl = ETradeMarketsURL(dataset="expire", ticker=ticker)
        self["expire"].load(str(curl.address), params=dict(curl.query))
        source = self.pages["expire"].source
        for expire in ETradeExpireData(source)(*args, **kwargs):
            if expire not in expires:
                continue
            curl = ETradeMarketsURL(dataset="option", ticker=ticker, expire=expire, strike=price)
            self["option"].load(str(curl.address), params=dict(curl.query))
            source = self.pages["option"].source
            options = ETradeOptionData(source)(*args, ticker=ticker, expire=expire, **kwargs)
            securities = xr.concat([options, stocks], dim="security")
            yield ticker, expire, securities



