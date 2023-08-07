# -*- coding: utf-8 -*-
"""
Created on Thurs Aug 3 2023
@name:   ETrade Order Objects
@author: Jack Kirby Cook

"""

import numpy as np
from enum import IntEnum, StrEnum
from webscraping.weburl import WebURL
from webscraping.webpages import WebJsonPage
from utilities.pipelines import Uploader
from utilities.dispatchers import kwargsdispatcher

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


class ETradeOrderData(WebJSON.List, locator="//Order"):
    class Status(WebJSON.Text, locator="//status", key="status", parser=Status.__getitem__): pass
    class Cost(WebJSON.Text, locator="//estimatedTotalAmount", key="cost", parser=np.float32): pass

    class Price(WebJSON.Text, locator="//priceValue", key="price", parser=np.float32): pass
    class Terms(WebJSON.Text, locator="//priceType", key="terms", parser=Terms.__getitem__): pass
    class Contents(WebJSON.Text, locator="//orderType", key="strategy", parser=Content.__getitem__): pass
    class Tenure(WebJSON.Text, locator="//orderTerm", key="tenure", parser=Tenure.__getitem__): pass
    class Session(WebJSON.Text, locator="//marketSession", key="session", parser=Session.__getitem__): pass
    class Timing(WebJSON.Text, locator="//allOrNone", key="timing", parser=Partial.__getitem__): pass

    class Instrument(WebJSON.List, locator="//Instrument", key="instruments"):
        class Action(WebJSON.Text, locator="//orderAction", key="action", parser=Action.__getitem__): pass
        class Quantity(WebJSON.Text, locator="//quantity", key="quantity", parser=np.int16): pass
        class Basis(WebJSON.Text, locator="//quantityType", key="basis", parser=Basis.__getitem__): pass

        class Product(WebJSON.Dict, locator="//Product", key="product"):
            class Ticker(WebJSON.Text, locator="//symbol", key="ticker", parser=str): pass
            class Security(WebJSON.Text, locator="//securityType", key="type", parser=Security.__getitem__): pass
            class Option(WebJSON.Text, locator="//callPut", key="security", parser=Option.__getitem__, optional=True): pass
            class Strike(WebJSON.Text, locator="//strikePrice", key="strike", parser=np.float32, optional=True): pass
            class Year(WebJSON.Text, locator="//expiryYear", key="year", parser=np.int16, optional=True): pass
            class Month(WebJSON.Text, locator="//expiryMonth", key="month", parser=np.int16, optional=True): pass
            class Day(WebJSON.Text, locator="//expiryDay", key="day", parser=np.int16, optional=True): pass


class ETradePreviewPayload(WebJSON.Dict, locator="//PreviewOrderRequest"):
    class Orders(ETradeOrderData, key="orders"): pass


class ETradePreviewData(WebJSON.Dict, locator="//PreviewOrderResponse"):
    class Orders(ETradeOrderData, key="orders"): pass
    class Previews(WebJSON.List, locator="//PreviewIds", key="previews"):
        class ID(WebJSON.Text, locator="//previewId", key="id", parser=np.int64): pass


class ETradePlacePayload(WebJSON.Dict, locator="//PlaceOrderRequest"):
    class Orders(ETradeOrderData, key="orders"): pass
    class Previews(WebJSON.List, locator="//PreviewIds", key="previews"):
        class ID(WebJSON.Text, locator="//previewId", key="id", parser=np.int64): pass


class ETradePlaceData(WebJSON.Dict, locator="//PlaceOrderResponse"):
    class Orders(ETradeOrderData, key="orders"): pass
    class Submits(WebJSON.List, locator="//OrderIds", key="submits"):
        class ID(WebJSON.Text, locator="//orderId", key="id", parser=np.int64): pass


class ETradeCancelPayload(WebJSON.Dict, locator=""):
    class ID(WebJSON.Text, locator="//orderId", key="id", parser=np.int64): pass


class ETradeCancelData(WebJSON.Dict, locator=""):
    class ID(WebJSON.Text, locator="//orderId", key="id", parser=np.int64): pass


class ETradePreviewPage(WebJsonPage, writer=ETradePreviewPayload, reader=ETradePreviewData): pass
class ETradePlacePage(WebJsonPage, writer=ETradePlacePayload, reader=ETradePlaceData): pass
class ETradeCancelPage(WebJsonPage, writier=ETradeCancelPayload, reader=ETradeCancelData): pass


order_pages = {"preview": ETradePreviewPage, "place": ETradePlacePage, "cancel": ETradeCancelPage}
class ETradeOrderUploader(Uploader, pages=order_pages):
    def execute(self, *args, **kwargs):
        pass





