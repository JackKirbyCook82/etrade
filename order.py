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
from finance.securities import Securities, Positions

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


stock_contents = dict(ticker="//symbol", securitytype="//securityType")
option_contents = dict(optiontype="//optionType", strike="//strikePrice", year="//expiryYear", month="//expiryMonth", day="//expiryDay")
instrument_contents = dict(action="//orderAction", basis="//quantityType", quantity="//quantity")
order_contents = dict(price="//priceValue", pricing="//priceType", ordertype="//orderType", tenure="//orderTerm", session="//marketSession", concurrent="//allOrNone")
place_contents = dict(previews="//PreviewIds/previewId")

class Product(WebPayload, locator="//Product", key="product"): pass
class StockProduct(Product, contents=stock_contents, securitytype=SecurityType.STOCK): pass
class OptionProduct(Product, contents=option_contents, securitytype=SecurityType.OPTION): pass
class PutProduct(OptionProduct, optiontype=OptionType.PUT): pass
class CallProduct(OptionProduct, optiontype=OptionType.CALL): pass

class Instrument(WebPayload, locator="//Instrument", key="instruments", contents=instrument_contents): pass
class BuyInstrument(Instrument, action=Action.BUY, quantity=1): pass
class SellInstrument(Instrument, action=Action.SELL, quantity=1): pass

BuyPutInstrument = BuyInstrument + PutProduct
SellPutInstrument = SellInstrument + PutProduct
BuyCallInstrument = BuyInstrument + CallProduct
SellCallInstrument = SellInstrument + CallProduct
BuyStockInstrument = BuyInstrument + StockProduct
SellStockInstrument = SellInstrument + StockProduct

stranglelong_payloads = {"put|long": BuyPutInstrument, "call|long": BuyCallInstrument}
collarlong_payloads = {"put|long": BuyPutInstrument, "call|short": SellCallInstrument, "stock|long": BuyStockInstrument}
collarshort_payloads = {"put|short": SellPutInstrument, "call|long": BuyCallInstrument, "stock|short": SellStockInstrument}
verticalput_payloads = {"put|long": BuyPutInstrument, "put|short": SellPutInstrument}
verticalcall_payloads = {"call|long": BuyCallInstrument, "call|short": SellCallInstrument}
condor_payloads = {"put|long": BuyPutInstrument, "call|long": BuyCallInstrument, "put|short": SellPutInstrument, "call|long": SellCallInstrument}

class Order(WebPayload, locator="//Order", key="orders", contents=order_contents): pass
class StrategyOrder(Order, pricing=Pricing.DEBIT, ordertype=OrderType.SPREADS, tenure=Tenure.FILLKILL, session=Session.MARKET, concurrent=Concurrent.TRUE): pass
class StrangleOrder(StrategyOrder, payloads=stranglelong_payloads): pass
class CollarLongOrder(StrategyOrder, payloads=collarlong_payloads): pass
class CollarShortOrder(StrategyOrder, payloads=collarshort_payloads): pass
class VerticalPutOrder(StrategyOrder, payloads=verticalput_payloads): pass
class VerticalCallOrder(StrategyOrder, payloads=verticalcall_payloads): pass
class CondorOrder(StrategyOrder, payloads=condor_payloads): pass

order_payloads = {"strangle|long": StrangleOrder, "collar|long": CollarLongOrder, "collar|short": CollarShortOrder}
order_payloads.update({"vertical|put": VerticalPutOrder, "vertical|call": VerticalCallOrder, "condor": CondorOrder})

class ETradePreviewPayload(WebPayload, payloads=order_payloads): pass
class ETradePlacePayload(WebPayload, contents=place_contents, payloads=order_payloads): pass


class ETradeOrderData(WebJSON, locator="//Order", key="orders", collection=True):
    class Status(WebJSON.Text, locator="//status", key="status", parser=Status.__getitem__): pass
    class Cost(WebJSON.Json, locator="//estimatedTotalAmount", key="cost", parser=np.float32): pass

    class Price(WebJSON.Json, locator="//priceValue", key="price", parser=np.float32): pass
    class Pricing(WebJSON.Json, locator="//priceType", key="terms", parser=Pricing.__getitem__): pass
    class Contents(WebJSON.Json, locator="//orderType", key="contents", parser=OrderType.__getitem__): pass
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
class ETradePreviewUploader(Uploader, pages={"preview": ETradePreviewPage}):
    def execute(self, contents, *args, funds, apy=0, **kwargs):
        ticker, expire, strategy, securities, dataset = contents
        assert isinstance(dataset, xr.Dataset)
        dataframe = dataset.to_dask_dataframe() if bool(dataset.chunks) else dataset.to_dataframe()
        dataframe = dataframe.where(dataframe["apy"] >= apy)
        dataframe = dataframe.where(dataframe["cost"] <= funds)
        dataframe = dataframe.dropna(how="all")
        for preview in self.previews(dataframe, strategy, securities):
            yield

    def preview(self, dataframe, strategy, securities):
        for partition in self.partitions(dataframe):
            for orders in self.orders(partition, strategy, securities):
                preview = ETradePreviewPayload(orders)
                yield preview

    @staticmethod
    def partitions(dataframe):
        if not hasattr(dataframe, "npartitions"):
            yield dataframe
            return
        for index in dataframe.npartitions:
            partition = dataframe.get_partition(index).compute()
            yield partition

    @staticmethod
    def orders(partition, strategy, securities):
        partition = partition.sort_values("apy", axis=1, ascending=False, ignore_index=True, inplace=False)
        for record in partition.to_dict("records"):
            product = {"price": record["spot"]} | {attr: getattr(record["expire"], attr) for attr in ("year", "month", "day")}
            instruments = {str(security): ({"strike": record[str(security)]} if str(security) in record else {}) | product for security in securities}
            orders = {str(strategy): instruments}
            return orders







