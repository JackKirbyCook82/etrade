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
from datetime import date as Date
from datetime import datetime as Datetime
from datetime import timezone as Timezone

from finance.variables import Querys, Variables, OSI
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeProductDownloader", "ETradeStockDownloader", "ETradeOptionDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


timestamp_parser = lambda string: Datetime.fromtimestamp(int(string), Timezone.utc).astimezone(pytz.timezone("US/Central"))
current_parser = lambda string: np.datetime64(timestamp_parser(string))
osi_parser = lambda string: OSI(str(string).replace("-", ""))
contract_parser = lambda string: Querys.Contract(list(osi_parser(string)))
strike_parser = lambda content: np.round(float(content), 2).astype(np.float32)
expire_parser = lambda string: contract_parser(string).expire


class ETradeURL(WebURL, domain="https://api.etrade.com"): pass
class ETradeExpireURL(ETradeURL, path=["v1", "market", "optionexpiredate" + ".json"], parameters={"expiryType": "ALL"}):
    @staticmethod
    def parameters(*args, ticker, **kwargs): return {"symbol": f"{str(ticker).upper()}"}


class ETradeStockURL(ETradeURL, path=["v1", "market", "quote"]):
    @staticmethod
    def path(*args, ticker, **kwargs):
        assert isinstance(ticker, (str, set, tuple, list))
        ticker = [ticker] if isinstance(ticker, str) else list(ticker)
        return [",".join(list(ticker)) + ".json"]


class ETradeOptionURL(ETradeURL, path=["v1", "market", "optionchains" + ".json"]):
    @staticmethod
    def expires(*args, expire, **kwargs): return {"expiryYear": f"{expire.year:04.0f}", "expiryMonth": f"{expire.month:02.0f}", "expiryDay": f"{expire.day:02.0f}", "expiryType": "ALL"}
    @staticmethod
    def strikes(*args, price, **kwargs): return {"strikePriceNear": str(int(price)), "noOfStrikes": "1000", "priceType": "ALL"}
    @staticmethod
    def options(*args, **kwargs): return {"optionCategory": "STANDARD", "chainType": "CALLPUT", "skipAdjusted": "true"}

    @classmethod
    def parameters(cls, *args, ticker, **kwargs):
        options = cls.options(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        strikes = cls.strikes(*args, **kwargs)
        return {"symbol": str(ticker).upper(), **options, **expires, **strikes}


class ETradeExpireData(WebJSON, locator="//OptionExpireDateResponse/ExpirationDate[]", multiple=True, optional=True):
    class Year(WebJSON.Text, locator="//year", key="year", parser=np.int16): pass
    class Month(WebJSON.Text, locator="//month", key="month", parser=np.int16): pass
    class Day(WebJSON.Text, locator="//day", key="day", parser=np.int16): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        return Date(**contents)


class ETradeStockData(WebJSON, locator="//QuoteResponse/QuoteData[]", multiple=False, optional=False):
    class Ticker(WebJSON.Text, locator="//Product/symbol", key="ticker", parser=str): pass
    class Current(WebJSON.Text, locator="//dateTimeUTC", key="current", parser=current_parser): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        stocks = pd.DataFrame.from_records([contents])
        return stocks

class ETradeStockTradeData(ETradeStockData):
    class Price(WebJSON.Text, locator="//All/lastTrade", key="price", parser=np.float32): pass

class ETradeStockQuoteData(ETradeStockData):
    class Bid(WebJSON.Text, locator="//All/bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//All/ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//All/bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//All/askSize", key="supply", parser=np.int32): pass


class ETradeOptionData(WebJSON, ABC, multiple=False, optional=False):
    class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
    class Expire(WebJSON.Text, locator="//osiKey", key="expire", parser=expire_parser): pass
    class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=strike_parser): pass
    class Option(WebJSON.Text, locator="//optionType", key="option", parser=Variables.Securities.Option): pass
    class Current(WebJSON.Text, locator="//timeStamp", key="current", parser=current_parser): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        options = pd.DataFrame.from_records([contents])
        function = lambda column: np.round(column, 2)
        options["strike"] = options["strike"].apply(function).astype(np.float32)
        return options

class ETradeOptionTradeData(ETradeOptionData):
    class Price(WebJSON.Text, locator="//lastPrice", key="price", parser=np.float32): pass

class ETradeOptionQuoteData(ETradeOptionData):
    class Bid(WebJSON.Text, locator="//bid", key="bid", parser=np.float32): pass
    class Ask(WebJSON.Text, locator="//ask", key="ask", parser=np.float32): pass
    class Demand(WebJSON.Text, locator="//bidSize", key="demand", parser=np.int32): pass
    class Supply(WebJSON.Text, locator="//askSize", key="supply", parser=np.int32): pass


class ETradeOptionsData(WebJSON, locator="//OptionChainResponse/OptionPair[]", multiple=True, optional=True):
    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        options = list(contents.values())
        options = pd.concat(options, axis=0)
        return options

class ETradeOptionsTradeData(ETradeOptionsData):
    class Call(ETradeOptionTradeData, locator="//Call", key="call"): pass
    class Put(ETradeOptionTradeData, locator="//Put", key="put"): pass

class ETradeOptionsQuoteData(ETradeOptionsData):
    class Call(ETradeOptionQuoteData, locator="//Call", key="call"): pass
    class Put(ETradeOptionQuoteData, locator="//Put", key="put"): pass


class ETradeExpirePage(WebJSONPage, url=ETradeExpireURL, web=ETradeExpireData):
    def execute(self, *args, **kwargs):
        expires = ETradeExpireData(self.json, *args, **kwargs)
        assert isinstance(expires, list)
        expires = [expire(*args, **kwargs) for expire in expires]
        expires = [expire for expire in expires if expire in kwargs.get("expires", expires)]
        return expires

class ETradeStockPage(WebJSONPage, url=ETradeStockURL):
    def execute(self, *args, **kwargs):
        trade = ETradeStockTradeData(self.json, *args, **kwargs)
        quote = ETradeStockQuoteData(self.json, *args, **kwargs)
        trade = trade(*args, **kwargs)
        quote = quote(*args, **kwargs)
        stocks = trade.combine_first(quote)
        return stocks

class ETradeOptionPage(WebJSONPage, url=ETradeOptionURL):
    def execute(self, *args, **kwargs):
        trade = ETradeOptionsTradeData(self.json, *args, **kwargs)
        quote = ETradeOptionsQuoteData(self.json, *args, **kwargs)
        assert isinstance(trade, list) and isinstance(quote, list)
        trade = pd.concat([data(*args, **kwargs) for data in trade], axis=0)
        quote = pd.concat([data(*args, **kwargs) for data in quote], axis=0)
        header = list(trade.columns) + [column for column in list(quote.columns) if column not in list(trade.columns)]
        options = trade.merge(quote, how="outer", on=list(Querys.Contract), sort=False, suffixes=("", "_"))[header]
        return options


class ETradeStockDownloader(Sizing, Emptying, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = ETradeStockPage(*args, **kwargs)

    def execute(self, symbols, *args, **kwargs):
        assert isinstance(symbols, (list, Querys.Symbol))
        assert all([isinstance(symbol, Querys.Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        symbols = list(symbols) if isinstance(symbols, list) else [symbols]
        for symbol in list(symbols):
            stocks = self.download(symbol, *args, **kwargs)
            size = self.size(stocks)
            self.console(f"{str(symbol)}[{int(size):.0f}]")
            if self.empty(stocks): return
            yield stocks.squeeze()

    def download(self, symbol, *args, **kwargs):
        parameters = dict(ticker=symbol.ticker)
        stocks = self.page(*args, **parameters, **kwargs)
        assert isinstance(stocks, pd.DataFrame)
        return stocks

    @property
    def page(self): return self.__page


class ETradeProductDownloader(Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = ETradeExpirePage(*args, **kwargs)

    def execute(self, trades, *args, **kwargs):
        assert isinstance(trades, (list, Querys.Trade))
        assert all([isinstance(trade, Querys.Trade) for trade in trades]) if isinstance(trades, list) else True
        trades = list(trades) if isinstance(trades, list) else [trades]
        for trade in list(trades):
            products = self.download(trade, *args, **kwargs)
            self.console(f"{str(trade)}[{len(products):.0f}]")
            if not bool(products): continue
            yield list(products)

    def download(self, trade, *args, expires, **kwargs):
        parameters = dict(ticker=trade.ticker, expires=expires, price=trade.price)
        expires = self.page(*args, **parameters, **kwargs)
        assert isinstance(expires, list)
        products = [Querys.Product([trade.ticker, expire, trade.price]) for expire in expires]
        return products

    @property
    def page(self): return self.__page


class ETradeOptionDownloader(Sizing, Emptying, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = ETradeOptionPage(*args, **kwargs)

    def execute(self, products, *args, **kwargs):
        assert isinstance(products, (list, Querys.Product))
        assert all([isinstance(product, Querys.Product) for product in products]) if isinstance(products, list) else True
        products = list(products) if isinstance(products, list) else [products]
        for product in list(products):
            options = self.download(product, *args, **kwargs)
            size = self.size(options)
            self.console(f"{str(product)}[{int(size):.0f}]")
            if self.empty(options): return
            yield options

    def download(self, product, *args, **kwargs):
        parameters = dict(ticker=product.ticker, expire=product.expire, price=product.price)
        options = self.page(*args, **parameters, **kwargs)
        options["underlying"] = np.round(product.price, 2).astype(np.float32)
        return options

    @property
    def page(self): return self.__page


