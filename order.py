# -*- coding: utf-8 -*-
"""
Created on Thurs Aug 3 2023
@name:   ETrade Order Objects
@author: Jack Kirby Cook

"""

import numpy as np
from enum import IntEnum, StrEnum
from webscraping.weburl import WebURL
from webscraping.webnodes import WebJSON
from webscraping.webpages import WebJsonPage
from support.pipelines import Uploader
from support.dispatchers import kwargsdispatcher

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


Status = StrEnum("Status", [("PLACED", "OPEN"), ("EXECUTED", "EXECUTED"), ("REVOKED", "CANCEL_REQUESTED"), ("CANCELLED", "CANCELLED"), ("EXPIRED", "EXPIRED"), ("REJECTED", "REJECTED")])
Terms = StrEnum("Terms", [("MARKET", "MARKET"), ("LIMIT", "LIMIT"), ("STOP", "STOP"), ("STOPLIMIT", "STOP_LIMIT"), ("DEBIT", "NET_DEBIT"), ("CREDIT", "NET_CREDIT")])
Tenure = StrEnum("Tenure", [("DAY", "GOOD_FOR_DAY"), ("DATE", "GOOD_TILL_DATE"), ("IMMEDIATE", "IMMEDIATE_OR_CANCEL"), ("FILLKILL", "FILL_OR_KILL")])
Content = StrEnum("Content", [("STOCKS", "EQ"), ("OPTIONS", "OPTN"), ("SPREADS", "SPREADS")])
Session = StrEnum("Session", [("MARKET", "REGULAR"), ("EXTENDED", "EXTENDED")])
Basis = StrEnum("Basis", [("QUANTITY", "QUANTITY"), ("CURRENCY", "DOLLAR")])
Security = StrEnum("Security", [("STOCK", "STOCK"), ("OPTION", "OPTION")])
Option = StrEnum("Option", [("PUT", "PUT"), ("CALL", "CALL")])
Action = StrEnum("Action", [("BUY", "BUY"), ("SELL", "SELL")])
Partial = IntEnum("Partial", ["false", "true"], start=0)


class ETradeOrdersURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"

    @kwargsdispatcher("dataset")
    def path(cls, *args, dataset, **kwargs): raise KeyError(dataset)
    @path.register.value("preview")
    def path_preview(cls, *args, account, **kwargs): return "/v1/accounts/{account}/orders/preview.json".format(account=str(account))
    @path.register.value("place")
    def path_place(cls, *args, account, **kwargs): return "/v1/accounts/{account}/orders/place.json".format(account=str(account))
    @path.register.value("cancel")
    def path_cancel(cls, *args, account, **kwargs): return "/v1/accounts/{account}/orders/cancel.json".format(account=str(account))


class ETradeOrderData(WebJSON, locator="//Order", key="orders", collection=True):
    class Status(WebJSON.Text, locator="//status", key="status", value=Status.__getitem__): pass
    class Cost(WebJSON.Json, locator="//estimatedTotalAmount", key="cost", value=np.float32): pass

    class Price(WebJSON.Json, locator="//priceValue", key="price", value=np.float32): pass
    class Terms(WebJSON.Json, locator="//priceType", key="terms", value=Terms.__getitem__): pass
    class Contents(WebJSON.Json, locator="//orderType", key="strategy", value=Content.__getitem__): pass
    class Tenure(WebJSON.Json, locator="//orderTerm", key="tenure", value=Tenure.__getitem__): pass
    class Session(WebJSON.Json, locator="//marketSession", key="session", value=Session.__getitem__): pass
    class Timing(WebJSON.Json, locator="//allOrNone", key="timing", value=Partial.__getitem__): pass

    class Instruments(WebJSON, locator="//Instrument", key="instruments", collection=True):
        class Action(WebJSON.Json, locator="//orderAction", key="action", value=Action.__getitem__): pass
        class Quantity(WebJSON.Json, locator="//quantity", key="quantity", value=np.int16): pass
        class Basis(WebJSON.Json, locator="//quantityType", key="basis", value=Basis.__getitem__): pass

        class Product(WebJSON, locator="//Product", key="product"):
            class Ticker(WebJSON.Json, locator="//symbol", key="ticker", value=str): pass
            class Security(WebJSON.Json, locator="//securityType", key="type", value=Security.__getitem__): pass
            class Option(WebJSON.Json, locator="//callPut", key="security", value=Option.__getitem__, optional=True): pass
            class Strike(WebJSON.Json, locator="//strikePrice", key="strike", value=np.float32, optional=True): pass
            class Year(WebJSON.Json, locator="//expiryYear", key="year", value=np.int16, optional=True): pass
            class Month(WebJSON.Json, locator="//expiryMonth", key="month", value=np.int16, optional=True): pass
            class Day(WebJSON.Json, locator="//expiryDay", key="day", value=np.int16, optional=True): pass

    @staticmethod
    def extractor(source, *args, **kwargs): pass
    @staticmethod
    def parser(contents, *args, **kwargs): pass


class ETradePreviewPayload(WebJSON, locator="//PreviewOrderRequest"):
    class Orders(ETradeOrderData): pass

    @staticmethod
    def extractor(source, *args, **kwargs): pass


class ETradePreviewData(WebJSON, locator="//PreviewOrderResponse"):
    class Previews(WebJSON.Text, locator="//PreviewIds/previewId", key="previews", value=np.int64, collection=True): pass
    class Orders(ETradeOrderData): pass

    @staticmethod
    def parser(contents, *args, **kwargs): pass


class ETradePlacePayload(WebJSON, locator="//PlaceOrderRequest"):
    class Previews(WebJSON.Text, locator="//PreviewIds/previewId", key="previews", value=np.int64, collection=True): pass
    class Orders(ETradeOrderData): pass

    @staticmethod
    def extractor(source, *args, **kwargs): pass


class ETradePlaceData(WebJSON, locator="//PlaceOrderResponse"):
    class Submits(WebJSON.Text, locator="//OrderIds/orderId", key="submits", value=np.int64, collection=True): pass
    class Orders(ETradeOrderData): pass

    @staticmethod
    def parser(contents, *args, **kwargs): pass


class ETradePreviewPage(WebJsonPage): pass
class ETradePlacePage(WebJsonPage): pass


order_pages = {"preview": ETradePreviewPage, "place": ETradePlacePage}
class ETradeOrderUploader(Uploader, pages=order_pages):
    def execute(self, *args, **kwargs):
        pass





