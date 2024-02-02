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
from collections import namedtuple as ntuple

from webscraping.weburl import WebURL
from webscraping.webdatas import WebJSON
from webscraping.webpages import WebJsonPage
from support.pipelines import Producer
from finance.variables import Contract, Securities

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
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass

    class Put(WebJSON, locator="//Put", key="put"):
        class Date(WebJSON.Text, locator="//timeStamp", key="date", parser=date_parser): pass
        class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
        class Expire(WebJSON.Text, locator="//quoteDetail", key="expire", parser=expire_parser): pass
        class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=strike_parser): pass
        class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
        class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
        class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
        class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass
        class Volume(WebJSON.Text, locator="//volume", key="volume", parser=np.int64): pass
        class Interest(WebJSON.Text, locator="//openInterest", key="interest", parser=np.int32): pass


class ETradeStockPage(WebJsonPage):
    def __call__(self, ticker, *args, **kwargs):
        curl = ETradeStockURL(ticker=ticker)
        self.load(str(curl.address), params=dict(curl.query))
        contents = ETradeStockData(self.source)
        return self.stocks(contents, *args, **kwargs)

    @staticmethod
    def stocks(contents, *args, **kwargs):
        columns = ["date", "ticker", "price", "size", "volume"]
        stocks = [{key: value(*args, **kwargs) for key, value in iter(content)} for content in iter(contents)]
        dataframe = pd.DataFrame.from_records(stocks)
        long = dataframe.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        short = dataframe.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        stocks = {Securities.Stock.Long: long[columns], Securities.Stock.Short: short[columns]}
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
        puts = self.puts(contents, *args, **kwargs)
        calls = self.calls(contents, *args, **kwargs)
        return puts | calls

    @staticmethod
    def puts(contents, *args, **kwargs):
        columns = ["date", "ticker", "expire", "strike", "price", "size", "volume", "interest"]
        contents = [{key: value(*args, **kwargs) for key, value in iter(content["put"])} for content in iter(contents)]
        dataframe = pd.DataFrame.from_records(contents)
        long = dataframe.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        short = dataframe.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        return {Securities.Option.Put.Long: long[columns], Securities.Option.Put.Short: short[columns]}

    @staticmethod
    def calls(contents, *args, **kwargs):
        columns = ["date", "ticker", "expire", "strike", "price", "size", "volume", "interest"]
        contents = [{key: value(*args, **kwargs) for key, value in iter(content["call"])} for content in iter(contents)]
        dataframe = pd.DataFrame.from_records(contents)
        long = dataframe.drop(["bid", "demand"], axis=1, inplace=False).rename(columns={"ask": "price", "supply": "size"})
        short = dataframe.drop(["ask", "supply"], axis=1, inplace=False).rename(columns={"bid": "price", "demand": "size"})
        return {Securities.Option.Call.Long: long[columns], Securities.Option.Call.Short: short[columns]}


class ETradeSecurityQuery(ntuple("Query", "inquiry contract stocks options")): pass
class ETradeSecurityDownloader(Producer, title="Downloaded"):
    def __init__(self, *args, name, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        pages = {"stock": ETradeStockPage, "expire": ETradeExpirePage, "option": ETradeOptionPage}
        pages = {key: page(*args, **kwargs) for key, page in pages.items()}
        self.pages = pages

    def prepare(self, *args, tickers, **kwargs):
        strikes = [self.stock(ticker, *args, **kwargs) for ticker in tickers]
        chains = [self.expires(ticker, *args, **kwargs) for ticker in tickers]
        return {"strikes": strikes, "chains": chains}

    def stock(self, ticker, *args, **kwargs):
        underlying = self.pages["stock"](ticker, *args, **kwargs)
        bid = underlying[Securities.Stock.Long].set_index(["date", "ticker"], inplace=False, drop=True)
        ask = underlying[Securities.Stock.Short].set_index(["date", "ticker"], inplace=False, drop=True)
        bid = np.float32(bid["price"].values[0])
        ask = np.float32(ask["price"].values[0])
        return (bid + ask) / 2

    def expires(self, ticker, *args, expires, **kwargs):
        expires = [expire for expire in self.pages["expire"](ticker, *args, **kwargs) if expire in expires]
        return expires

    def execute(self, *args, tickers, strikes, chains, **kwargs):
        assert all([isinstance(values, list) for values in (tickers, strikes, chains)])
        assert len(tickers) == len(strikes) == len(chains)
        for ticker, strike, expires in zip(tickers, strikes, chains):
            for expire in expires:
                inquiry = Datetime.now()
                contract = Contract(ticker, expire)
                stocks = self.pages["stock"](ticker, *args, **kwargs)
                options = self.pages["option"](ticker, *args, expire=expire, strike=strike, **kwargs)
                yield ETradeSecurityQuery(inquiry, contract, stocks, options)




