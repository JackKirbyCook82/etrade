# -*- coding: utf-8 -*-
"""
Created on Thurs Aug 3 2023
@name:   ETrade Order Objects
@author: Jack Kirby Cook

"""

import enum
import numpy as np

from webscraping.weburl import WebURL
from webscraping.webdatas import WebJSON
from webscraping.webpages import WebJsonPage
from webscraping.webpayloads import WebPayload
from support.pipelines import Uploader
from support.dispatchers import kwargsdispatcher

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


Status = enum.StrEnum("Status", [("PLACED", "OPEN"), ("EXECUTED", "EXECUTED"), ("REVOKED", "CANCEL_REQUESTED"), ("CANCELLED", "CANCELLED"), ("EXPIRED", "EXPIRED"), ("REJECTED", "REJECTED")])
OrderType = enum.StrEnum("OrderType", [("STOCKS", "EQ"), ("OPTIONS", "OPTN"), ("SPREADS", "SPREADS")])
Pricing = enum.StrEnum("PriceType", [("MARKET", "MARKET"), ("LIMIT", "LIMIT"), ("STOP", "STOP"), ("STOPLIMIT", "STOP_LIMIT"), ("DEBIT", "NET_DEBIT"), ("CREDIT", "NET_CREDIT")])
Tenure = enum.StrEnum("Tenure", [("DAY", "GOOD_FOR_DAY"), ("DATE", "GOOD_TILL_DATE"), ("IMMEDIATE", "IMMEDIATE_OR_CANCEL"), ("FILLKILL", "FILL_OR_KILL")])
Session = enum.StrEnum("Session", [("MARKET", "REGULAR"), ("EXTENDED", "EXTENDED")])
Concurrent = enum.StrEnum("Concurrent", [("TRUE", "true"), ("FALSE", "false")])
Action = enum.StrEnum("Action", [("BUY", "BUY"), ("BUY", "SELL")])
Basis = enum.StrEnum("QuantityType", [("QUANTITY", "QUANTITY"), ("CURRENCY", "DOLLAR")])
SecurityType = enum.StrEnum("SecurityType", [("STOCK", "STOCK"), ("OPTION", "OPTION")])
OptionType = enum.StrEnum("OptionType", [("PUT", "PUT"), ("CALL", "CALL")])


class ETradeOrderURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://api.etrade.com"

    @kwargsdispatcher("dataset")
    def path(cls, *args, dataset, **kwargs): raise KeyError(dataset)
    @path.register.value("preview")
    def path_preview(cls, *args, account, **kwargs): return "/v1/accounts/{account}/orders/preview.json".format(account=str(account))
    @path.register.value("place")
    def path_place(cls, *args, account, **kwargs): return "/v1/accounts/{account}/orders/place.json".format(account=str(account))
    @path.register.value("cancel")
    def path_cancel(cls, *args, account, **kwargs): return "/v1/accounts/{account}/orders/cancel.json".format(account=str(account))


stock_fields = dict(ticker="//symbol", securitytype="//securityType")
option_fields = dict(optiontype="//optionType", strike="//strikePrice", year="//expiryYear", month="//expiryMonth", day="//expiryDay")
instrument_fields = dict(action="//orderAction", basis="//quantityType", quantity="//quantity")
order_fields = dict(price="//priceValue", pricing="//priceType", ordertype="//orderType", tenure="//orderTerm", session="//marketSession", concurrent="//allOrNone")
place_fields = dict(previews="//PreviewIds[]/previewId")
order_values = dict(pricing=Pricing.DEBIT, ordertype=OrderType.SPREADS, tenure=Tenure.FILLKILL, session=Session.MARKET, concurrent=Concurrent.TRUE)

class Product(WebPayload, locator="//Product", key="product"): pass
class StockProduct(Product, key="stock", fields=stock_fields, values={"securitytype": SecurityType.STOCK}): pass
class OptionProduct(Product, key="security", fields=option_fields, values={"securitytype": SecurityType.OPTION}): pass
class PutProduct(OptionProduct, key="put", values={"optiontype": OptionType.PUT}): pass
class CallProduct(OptionProduct, key="call", values={"optiontype": OptionType.CALL}): pass

class Instrument(WebPayload, locator="//Instrument[]", key="instruments", fields=instrument_fields, collection=True): pass
class BuyInstrument(Instrument, key="long", values={"action": Action.BUY, "quantity": 1}): pass
class SellInstrument(Instrument, key="short", values={"action": Action.SELL, "quantity": 1}): pass

class BuyPutInstrument(BuyInstrument + PutProduct, key="put|long"): pass
class SellPutInstrument(SellInstrument + PutProduct, key="put|short"): pass
class BuyCallInstrument(BuyInstrument + CallProduct, key="call|long"): pass
class SellCallInstrument(SellInstrument + CallProduct, key="call|short"): pass
class BuyStockInstrument(BuyInstrument + StockProduct, key="stock|long"): pass
class SellStockInstrument(SellInstrument + StockProduct, key="stock|short"): pass

class Order(WebPayload, locator="//Order[]", key="orders", fields=order_fields, values=order_values, collection=True): pass
class StrangleOrder(Order + [BuyPutInstrument, BuyCallInstrument], key="strangle|long"): pass
class CollarLongOrder(Order + [BuyPutInstrument, SellCallInstrument, BuyStockInstrument], key="collar|long"): pass
class CollarShortOrder(Order + [SellPutInstrument, BuyCallInstrument, SellStockInstrument], key="collar|short"): pass
class VerticalPutOrder(Order + [BuyPutInstrument, SellPutInstrument], key="vertical|put"): pass
class VerticalCallOrder(Order + [BuyCallInstrument, SellCallInstrument], key="vertical|call"): pass
class CondorOrder(Order + [BuyPutInstrument, BuyCallInstrument, SellPutInstrument, SellCallInstrument], key="condor"): pass

class ETradePreviewPayload(WebPayload): pass
class ETradePlacePayload(WebPayload, fields=place_fields): pass


class ETradeOrderData(WebJSON, locator="//Order[]", key="orders", collection=True):
    class Status(WebJSON.Text, locator="//status", key="status", parser=Status.__getitem__): pass
    class Cost(WebJSON.Json, locator="//estimatedTotalAmount", key="cost", parser=np.float32): pass

    class Price(WebJSON.Json, locator="//priceValue", key="price", parser=np.float32): pass
    class Pricing(WebJSON.Json, locator="//priceType", key="terms", parser=Pricing.__getitem__): pass
    class Contents(WebJSON.Json, locator="//orderType", key="contents", parser=OrderType.__getitem__): pass
    class Tenure(WebJSON.Json, locator="//orderTerm", key="tenure", parser=Tenure.__getitem__): pass
    class Session(WebJSON.Json, locator="//marketSession", key="session", parser=Session.__getitem__): pass
    class Concurrent(WebJSON.Json, locator="//allOrNone", key="concurrent", parser=Concurrent.__getitem__): pass

    class Messages(WebJSON, locator="//messages/Message[]", key="messages", collection=True, optional=True):
        class Code(WebJSON.Text, locator="//code", key="code", parsers=np.int32): pass
        class Description(WebJSON.Text, locator="//description", key="message", parser=str): pass

    class Instruments(WebJSON, locator="//Instrument[]", key="instruments", collection=True):
        class Action(WebJSON.Json, locator="//orderAction", key="action", parser=Action.__getitem__): pass
        class Basis(WebJSON.Json, locator="//quantityType", key="basis", parser=Basis.__getitem__): pass
        class Quantity(WebJSON.Json, locator="//quantity", key="quantity", parser=np.int16): pass

        class Product(WebJSON, locator="//Product", key="product"):
            class Ticker(WebJSON.Json, locator="//symbol", key="ticker", parser=str): pass
            class SecurityType(WebJSON.Json, locator="//securityType", key="type", parser=SecurityType.__getitem__): pass
            class OptionType(WebJSON.Json, locator="//callPut", key="security", parser=OptionType.__getitem__, optional=True): pass
            class Strike(WebJSON.Json, locator="//strikePrice", key="strike", parser=np.float32, optional=True): pass
            class Year(WebJSON.Json, locator="//expiryYear", key="year", parser=np.int16, optional=True): pass
            class Month(WebJSON.Json, locator="//expiryMonth", key="month", parser=np.int16, optional=True): pass
            class Day(WebJSON.Json, locator="//expiryDay", key="day", parser=np.int16, optional=True): pass

class ETradePreviewData(WebJSON, locator="//PreviewOrderResponse"):
    class Previews(WebJSON.Text, locator="//PreviewIds[]/previewId", key="previews", parser=np.int64, collection=True): pass
    class Orders(ETradeOrderData): pass

class ETradePlaceData(WebJSON, locator="//PlaceOrderResponse"):
    class Places(WebJSON.Text, locator="//OrderIds[]/orderId", key="places", parser=np.int64, collection=True): pass
    class Orders(ETradeOrderData): pass


class ETradePreviewPage(WebJsonPage): pass
class ETradePlacePage(WebJsonPage): pass


pages = {"preview": ETradePreviewPage, "place": ETradePlacePage}
class ETradeOrderUploader(Uploader, pages=pages):
    def execute(self, target, *args, **kwargs):
        instruments = {str(security): security.todict() for security in target.securities}
        order = Order[str(target.strategy)](price=target.valuation.price, instruments=instruments)

    def preview(self, *args, **kwargs):
        pass

    def place(self, *args, **kwargs):
        pass


