# -*- coding: utf-8 -*-
"""
Created on Thurs Aug 3 2023
@name:   ETrade Order Objects
@author: Jack Kirby Cook

"""

import enum
import numpy as np
import xarray as xr

from webscraping.weburl import WebURL
from webscraping.webdatas import WebJSON
from webscraping.webpayloads import WebPayload
from webscraping.webpages import WebJsonPage
from support.pipelines import Uploader
from support.dispatchers import kwargsdispatcher

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


Status = enum.StrEnum("Status", [("PLACED", "OPEN"), ("EXECUTED", "EXECUTED"), ("REVOKED", "CANCEL_REQUESTED"), ("CANCELLED", "CANCELLED"), ("EXPIRED", "EXPIRED"), ("REJECTED", "REJECTED")])
Contents = enum.StrEnum("ContentType", [("STOCKS", "EQ"), ("OPTIONS", "OPTN"), ("SPREADS", "SPREADS")])
Pricing = enum.StrEnum("PriceType", [("MARKET", "MARKET"), ("LIMIT", "LIMIT"), ("STOP", "STOP"), ("STOPLIMIT", "STOP_LIMIT"), ("DEBIT", "NET_DEBIT"), ("CREDIT", "NET_CREDIT")])
Tenure = enum.StrEnum("OrderType", [("DAY", "GOOD_FOR_DAY"), ("DATE", "GOOD_TILL_DATE"), ("IMMEDIATE", "IMMEDIATE_OR_CANCEL"), ("FILLKILL", "FILL_OR_KILL")])
Session = enum.StrEnum("Session", [("MARKET", "REGULAR"), ("EXTENDED", "EXTENDED")])
Concurrent = enum.StrEnum("Concurrent", [("TRUE", "true"), ("FALSE", "false")])
Action = enum.StrEnum("Action", [("LONG", "BUY"), ("SHORT", "SELL")])
Basis = enum.StrEnum("QuantityType", [("QUANTITY", "QUANTITY"), ("CURRENCY", "DOLLAR")])
SecurityType = enum.StrEnum("SecurityType", [("STOCK", "STOCK"), ("OPTION", "OPTION")])
OptionType = enum.StrEnum("OptionType", [("PUT", "PUT"), ("CALL", "CALL")])


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


class ETradeOrderPayload(WebPayload, locator="//Order", key="orders", collection=True):
    class Price(WebPayload, locator="//priceValue", key="price"): pass
    class Pricing(WebPayload, locator="//priceType", key="terms"): pass
    class Contents(WebPayload, locator="//orderType", key="contents"): pass
    class Tenure(WebPayload, locator="//orderTerm", key="tenure"): pass
    class Session(WebPayload, locator="//marketSession", key="session"): pass
    class Concurrent(WebPayload, locator="//allOrNone", key="concurrent"): pass

    class Instruments(WebPayload, locator="//Instrument", key="instruments", collection=True):
        class Action(WebPayload, locator="//orderAction", key="action"): pass
        class Basis(WebPayload, locator="//quantityType", key="basis"): pass
        class Quantity(WebPayload, locator="//quantity", key="quantity"): pass

        class Product(WebPayload, locator="//Product", key="product"):
            class Ticker(WebPayload, locator="//symbol", key="ticker"): pass
            class SecurityType(WebPayload, locator="//securityType", key="type"): pass
            class OptionType(WebPayload, locator="//callPut", key="option", optional=True): pass
            class Strike(WebPayload, locator="//strikePrice", key="strike", optional=True): pass
            class Year(WebPayload, locator="//expiryYear", key="year", optional=True): pass
            class Month(WebPayload, locator="//expiryMonth", key="month", optional=True): pass
            class Day(WebPayload, locator="//expiryDay", key="day", optional=True): pass

class ETradePreviewPayload(WebPayload, locator="//PreviewOrderRequest"):
    class Orders(ETradeOrderPayload): pass

class ETradePlacePayload(WebPayload, locator="//PlaceOrderRequest"):
    class Previews(WebPayload, locator="//PreviewIds/previewId", key="previews", collection=True): pass
    class Orders(ETradeOrderPayload): pass


class ETradeOrderData(WebJSON, locator="//Order", key="orders", collection=True):
    class Status(WebJSON.Text, locator="//status", key="status", parser=Status.__getitem__): pass
    class Cost(WebJSON.Json, locator="//estimatedTotalAmount", key="cost", parser=np.float32): pass

    class Price(WebJSON.Json, locator="//priceValue", key="price", parser=np.float32): pass
    class Pricing(WebJSON.Json, locator="//priceType", key="terms", parser=Pricing.__getitem__): pass
    class Contents(WebJSON.Json, locator="//orderType", key="contents", parser=Contents.__getitem__): pass
    class Tenure(WebJSON.Json, locator="//orderTerm", key="tenure", parser=Tenure.__getitem__): pass
    class Session(WebJSON.Json, locator="//marketSession", key="session", parser=Session.__getitem__): pass
    class Concurrent(WebJSON.Json, locator="//allOrNone", key="concurrent", parser=Concurrent.__getitem__): pass

    class Messages(WebJSON, locator="//messages/Message", key="messages", collection=True, optional=True):
        class Code(WebJSON.Text, locator="//code", key="code", parsers=np.int32): pass
        class Description(WebJSON.Text, locator="//description", key="message", parser=str): pass

    class Instruments(WebJSON, locator="//Instrument", key="instruments", collection=True):
        class Action(WebJSON.Json, locator="//orderAction", key="action", parser=Action.__getitem__): pass
        class Basis(WebJSON.Json, locator="//quantityType", key="basis", parser=Basis.__getitem__): pass
        class Quantity(WebJSON.Json, locator="//quantity", key="quantity", parser=np.int16): pass

        class Product(WebJSON, locator="//Product", key="product"):
            class Ticker(WebJSON.Json, locator="//symbol", key="ticker", parser=str): pass
            class SecurityType(WebJSON.Json, locator="//securityType", key="type", parser=SecurityType.__getitem__): pass
            class OptionType(WebJSON.Json, locator="//callPut", key="option", parser=OptionType.__getitem__, optional=True): pass
            class Strike(WebJSON.Json, locator="//strikePrice", key="strike", parser=np.float32, optional=True): pass
            class Year(WebJSON.Json, locator="//expiryYear", key="year", parser=np.int16, optional=True): pass
            class Month(WebJSON.Json, locator="//expiryMonth", key="month", parser=np.int16, optional=True): pass
            class Day(WebJSON.Json, locator="//expiryDay", key="day", parser=np.int16, optional=True): pass

class ETradePreviewData(WebJSON, locator="//PreviewOrderResponse"):
    class Previews(WebJSON.Text, locator="//PreviewIds/previewId", key="previews", parser=np.int64, collection=True): pass
    class Orders(ETradeOrderData): pass

class ETradePlaceData(WebJSON, locator="//PlaceOrderResponse"):
    class Places(WebJSON.Text, locator="//OrderIds/orderId", key="places", parser=np.int64, collection=True): pass
    class Orders(ETradeOrderData): pass


class ETradePreviewPage(WebJsonPage): pass
class ETradePlacePage(WebJsonPage): pass


order_pages = {"preview": ETradePreviewPage, "place": ETradePlacePage}
class ETradeOrderUploader(Uploader, pages=order_pages):
    def execute(self, contents, *args, funds, apy=0, **kwargs):
        ticker, expire, strategy, securities, dataset = contents
        assert isinstance(dataset, xr.Dataset)

        dataframe = dataset.to_dask_dataframe() if bool(dataset.chunks) else dataset.to_dataframe()
        dataframe = dataframe.where(dataframe["apy"] >= apy)
        dataframe = dataframe.where(dataframe["cost"] <= funds)
        dataframe = dataframe.dropna(how="all")

        if not hasattr(dataframe, "npartitions"):
            dataframe = dataframe.sort_values("apy", axis=1, ascending=False, ignore_index=True, inplace=False)
            for row in dataframe.to_dict("records"):
                order = ETradeOrderPayload(order)

        else:
            for index in dataframe.npartitions:
                partition = dataframe.get_partition(index).compute()
                partition = partition.sort_values("apy", axis=1, ascending=False, ignore_index=True, inplace=False)
                for row in partition.to_dict("records"):
                    order = ETradeOrderPayload(order)







