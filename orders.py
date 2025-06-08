# -*- coding: utf-8 -*-
"""
1Created on Sat May 31 2025
@name:   ETrade Order Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from abc import ABC
from collections import namedtuple as ntuple

from finance.variables import Querys, Variables, Securities
from webscraping.weburl import WebURL, WebPayload
from webscraping.webpages import WebJSONPage
from support.mixins import Emptying, Logging, Naming

__author__ = "Jack Kirby Cook"
__all__ = ["ETradeOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


tenure_formatter = lambda order: {Variables.Markets.Tenure.DAY: "GOOD_FOR_DAY", Variables.Markets.Tenure.FILLKILL: "FILL_OR_KILL"}[order.tenure]
action_formatter = lambda instrument: {Variables.Markets.Action.BUY: "BUY", Variables.Markets.Action.SELL: "SELL"}[instrument.action]


class ETradeProduct(Naming, ABC, fields=["ticker", "instrument", "option"]):
    def __new__(cls, security, *args, **kwargs):
        security = dict(instrument=security.instrument, option=security.option, position=security.position)
        return super().__new__(cls, *args, **security, **kwargs)

class ETradeOption(ETradeProduct, fields=["expire", "strike"]): pass
class ETradeStock(ETradeProduct): pass

class ETradeInstrument(Naming, fields=["position", "quantity", "product"]):
    def __new__(cls, security, *args, quantity, **kwargs):
        if security.instrument == Variables.Securities.Instrument.OPTION:
            parameters = dict(instrument=security.instrument, option=security.option, quantity=quantity * 1)
            product = ETradeOption(*args, **parameters, **kwargs)
        elif security.instrument == Variables.Securities.Instrument.STOCK:
            parameters = dict(instrument=security.instrument, quantity=quantity * 100)
            product = ETradeStock(*args, **parameters, **kwargs)
        else: raise ValueError(security.instrument)
        parameters = dict(position=security.position, product=product)
        return super().__new__(cls, *args, **parameters, **kwargs)

class ETradeValuation(Naming, fields=["npv"]):
    def __str__(self): return f"${self.npv.min():.0f} -> ${self.npv.max():.0f}"

class ETradeOrder(Naming, ABC, fields=["term", "tenure", "limit", "stop", "instruments"]):
    pass


class ETradeOrderURL(WebURL, domain="https://api.etrade.com", path=["v1", "accounts"]): pass
class ETradePreviewURL(ETradeOrderURL):
    @staticmethod
    def path(*args, account, **kwargs): return [str(account), "orders", "preview"]

class ETradePlaceURL(ETradeOrderURL):
    @staticmethod
    def path(*args, account, **kwargs): return [str(account), "orders", "place"]


class ETradeOrderPayload(WebPayload, key="order", locator="Order", fields={"allOrNone": "true", "marketSession": "REGULAR", "pricing": "NET_DEBIT"}, multiple=False, optional=False):
    limit = lambda order: {"limitPrice": f"{order.limit:.02f}"} if order.term in (Variables.Markets.Term.LIMIT, Variables.Markets.Term.STOPLIMIT) else {}
    stop = lambda order: {"stopPrice": f"{order.stop:.02f}"} if order.term in (Variables.Markets.Term.STOP, Variables.Markets.Term.STOPLIMIT) else {}
    tenure = lambda order: {"orderTerm": tenure_formatter(order)}

    class Instrument(WebPayload, key="instrument", locator="Instrument", multiple=True, optional=False):
        action = lambda instrument: {"orderAction": action_formatter(instrument)}
        quantity = lambda instrument: {"quantity": str(instrument.quantity)}

        class Product(WebPayload, key="product", locator="Product", multiple=False, optional=False):
            expire = lambda product: {"expiryYear": str(product.expire.year), "expiryMonth": str(product.expire.month), "expiryDay": str(product.expire.day)} if isinstance(product, ETradeOption) else {}
            instrument = lambda product: {"securityType": {Variables.Securities.Instrument.OPTION: "OPTN", Variables.Securities.Instrument.STOCK: "EQ"}[product.instrument]}
            option = lambda product: {"callPut": str(product.option).upper()} if isinstance(product, ETradeOption) else {}
            strike = lambda product: {"strikePrice": product.strike} if isinstance(product, ETradeOption) else {}
            ticker = lambda product: {"symbol": str(product.ticker)}


class ETradePreviewPayload(WebPayload, key="preview", locator="PreviewOrderRequest", dependents=[ETradeOrderPayload], fields={"orderType": "SPREADS"}, multiple=False, optional=False):
    identity = lambda order: {"clientOrderId": None}

class ETradePlacePayload(WebPayload, key="place", locator="PlaceOrderRequest", dependents=[ETradeOrderPayload], fields={"orderType": "SPREADS"}, multiple=False, optional=False):
    preview = lambda order: {"PreviewIds": {"previewId": None}}
    identity = lambda order: {"clientOrderId": None}


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

        print(prospects)
        raise Exception()

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
            strategy, quantity = prospect[["strategy", "quantity"]].droplevel(1).values
            settlement = prospect[list(Querys.Settlement)].droplevel(1).to_dict()
            options = prospect[list(map(str, Securities.Options))].droplevel(1).to_dict()
            options = {Securities.Options[option]: strike for option, strike in options.items() if not np.isnan(strike)}
            stocks = {Securities.Stocks[stock] for stock in strategy.stocks}
            breakeven = prospect[("spot", Variables.Scenario.BREAKEVEN)]
            current = prospect[("spot", Variables.Scenario.CURRENT)]
            assert current >= breakeven and quantity >= 1
            options = [ETradeInstrument(security, strike=strike, **settlement) for security, strike in options.items()]
            stocks = [ETradeInstrument(security, **settlement) for security, strike in stocks]
            valuation = ETradeValuation(npv=prospect.xs("npv", axis=0, level=0, drop_level=True))
            order = ETradeOrder(instruments=options + stocks, term=term, tenure=tenure, limit=-breakeven, stop=None, quantity=1)
            yield order, valuation

    @property
    def pages(self): return self.__pages



