# -*- coding: utf-8 -*-
"""
1Created on Sat May 31 2025
@name:   ETrade Order Objects
@author: Jack Kirby Cook

"""

import pandas as pd
from abc import ABC
from collections import namedtuple as ntuple

from finance.variables import Variables
from webscraping.weburl import WebURL, WebPayload
from webscraping.webpages import WebJSONPage
from support.mixins import Emptying, Logging, Naming

__author__ = "Jack Kirby Cook"
__all__ = ["ETradeOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ETradeValuation(Naming): pass
class ETradeSecurity(Naming, ABC): pass
class ETradeStock(ETradeSecurity): pass
class ETradeOption(ETradeSecurity): pass


class ETradeOrderURL(WebURL, domain="https://api.etrade.com", path=["v1", "accounts"]): pass
class ETradePreviewURL(ETradeOrderURL):
    @staticmethod
    def path(*args, account, **kwargs): return [str(account), "orders", "preview"]

class ETradePlaceURL(ETradeOrderURL):
    @staticmethod
    def path(*args, account, **kwargs): return [str(account), "orders", "place"]


class ETradeOrderPayload(WebPayload, key="order", locator="Order", fields={"allOrNone": "true", "marketSession": "REGULAR", "pricing": "NET_DEBIT"}, multiple=False, optional=False):
    limit = lambda order: {"limitPrice": None}
    stop = lambda order: {"stopPrice": None}
    tenure = lambda order: {"orderTerm": None}

    class Instrument(WebPayload, key="instrument", locator="Instrument", multiple=True, optional=False):
        action = lambda security: {"orderAction": None}
        quantity = lambda security: {"quantity": None}

        class Product(WebPayload, key="product", locator="Product", multiple=False, optional=False):
            ticker = lambda security: {"symbol": None}
            expire = lambda security: {"expiryYear": None, "expiryMonth": None, "expiryDay": None}
            instrument = lambda security: {"securityType": None}
            option = lambda security: {"callPut": None}
            strike = lambda security: {"strikePrice": None}


class ETradePreviewPayload(WebPayload, key="preview", locator="PreviewOrderRequest", dependents=[ETradeOrderPayload], fields={"orderType": "SPREADS"}, multiple=False, optional=False):
    identity = lambda order: {"clientOrderId": None}

class ETradePlacePayload(WebPayload, key="place", locator="PlaceOrderRequest", dependents=[ETradeOrderPayload], fields={"orderType": "SPREADS"}, multiple=False, optional=False):
    identity = lambda order: {"clientOrderId": None}
    preview = lambda order: {"PreviewIds": {"previewId": None}}


class ETradeOrderPage(WebJSONPage):
    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.__payload__ = kwargs.get("payload", getattr(cls, "__payload__", None))
        cls.__url__ = kwargs.get("url", getattr(cls, "__url__", None))

    def execute(self, *args, order, **kwargs):
        url = self.url(*args, **kwargs)
        payload = self.payload(order, *args, **kwargs)
        self.load(url, *args, payload=dict(payload), **kwargs)

    @property
    def payload(self): return type(self).__payload__
    @property
    def url(self): return type(self).__url__

class ETradePreviewPage(ETradeOrderPage, url=ETradePreviewURL, payload=ETradePreviewPayload): pass
class ETradePlacePage(ETradeOrderPage, url=ETradePreviewURL, payload=ETradePlacePayload): pass


class ETradeOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, **kwargs):
        Orders = ntuple("Pages", "preview, place")
        preview = ETradePreviewPage(*args, **kwargs)
        place = ETradePlacePage(*args, **kwargs)
        self.__pages = Orders(preview, place)

    def execute(self, prospects, *args, **kwargs):
        assert isinstance(prospects, pd.DataFrame)
        if self.empty(prospects): return
        for order, valuation in self.calculator(prospects, *args, **kwargs):
            self.upload(order, *args, **kwargs)
            securities = ", ".join(list(map(str, order.options + order.stocks)))
            self.console(f"{str(securities)}[{order.quantity:.0f}] @ {str(valuation)}")

    def upload(self, order, *args, **kwargs):
        pass

    @staticmethod
    def calculator(prospects, *args, term, tenure, **kwargs):
        assert term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        for index, prospect in prospects.iterrows():
            pass

    @property
    def pages(self): return self.__pages



