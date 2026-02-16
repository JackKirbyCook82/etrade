# -*- coding: utf-8 -*-
"""
1Created on Sat May 31 2025
@name:   ETrade Order Objects
@author: Jack Kirby Cook

"""

import random
import numpy as np
import pandas as pd
from abc import ABC

from finance.concepts import Querys, Concepts, Securities, OSI
from webscraping.webpages import WebJSONPage, WebHTMLPage
from webscraping.weburl import WebURL, WebPayload
from webscraping.webdatas import WebJSON
from support.mixins import Emptying, Logging, Naming

__author__ = "Jack Kirby Cook"
__all__ = ["ETradeOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


tenure_formatter = lambda order: {Concepts.Markets.Tenure.DAY: "GOOD_FOR_DAY", Concepts.Markets.Tenure.FILLKILL: "FILL_OR_KILL"}[order.tenure]
action_formatter = lambda instrument: {Concepts.Securities.Position.LONG: "BUY", Concepts.Securities.Position.SHORT: "SELL"}[instrument.position]


class ETradeProduct(Naming, ABC, fields=["ticker", "instrument", "option"]): pass
class ETradeOption(ETradeProduct, fields=["expire", "strike"]):
    def __str__(self): return str(OSI([self.ticker, self.expire, self.option, self.strike]))

class ETradeStock(ETradeProduct):
    def __str__(self): return str(self.ticker)

class ETradeInstrument(Naming, fields=["position", "quantity", "product"]):
    def __str__(self): return str(self.product)
    def __new__(cls, security, *args, quantity, **kwargs):
        if security.instrument == Concepts.Securities.Instrument.OPTION:
            quantity = quantity * 1
            parameters = dict(instrument=security.instrument, option=security.option)
            product = ETradeOption(*args, **parameters, **kwargs)
        elif security.instrument == Concepts.Securities.Instrument.STOCK:
            quantity = quantity * 100
            parameters = dict(instrument=security.instrument)
            product = ETradeStock(*args, **parameters, **kwargs)
        else: raise ValueError(security.instrument)
        parameters = dict(position=security.position, quantity=quantity, product=product)
        return super().__new__(cls, *args, **parameters, **kwargs)

class ETradeValuation(Naming, fields=["npv"]):
    def __str__(self): return f"${self.npv.min():.0f}"

class ETradeOrder(Naming, fields=["term", "tenure", "limit", "stop", "instruments"]): pass
class ETradePreview(Naming, fields=["identity", "order"]): pass


class ETradeAccountURL(WebURL, domain="https://api.etrade.com", path=["v1", "accounts", "list" + ".json"]): pass
class ETradeOrderURL(WebURL, domain="https://api.etrade.com", path=["v1", "accounts"]): pass
class ETradePreviewURL(ETradeOrderURL):
    @staticmethod
    def path(*args, account, **kwargs): return [str(account), "orders", "preview" + ".json"]


class ETradeAccountData(WebJSON, locator="//AccountListResponse/Accounts/Account[]", multiple=True, optional=False):
    class Key(WebJSON.Text, locator="accountId", key="key", parser=int): pass
    class Value(WebJSON.Text, locator="accountIdKey", key="value", parser=str): pass

    def execute(self, *args, **kwargs):
        accounts = super().execute(*args, **kwargs)
        assert isinstance(accounts, dict)
        accounts = pd.DataFrame.from_records([accounts])
        return accounts


class ETradeOrderPayload(WebPayload, key="order", locator="Order", fields={"allOrNone": "true", "marketSession": "REGULAR", "pricing": "NET_DEBIT"}, multiple=True, optional=False):
    limit = lambda order: {"limitPrice": f"{order.limit:.02f}"} if order.term in (Concepts.Markets.Term.LIMIT, Concepts.Markets.Term.STOPLIMIT) else {}
    stop = lambda order: {"stopPrice": f"{order.stop:.02f}"} if order.term in (Concepts.Markets.Term.STOP, Concepts.Markets.Term.STOPLIMIT) else {}
    tenure = lambda order: {"orderTerm": tenure_formatter(order)}

    class Instrument(WebPayload, key="instruments", locator="Instrument", multiple=True, optional=False):
        action = lambda instrument: {"orderAction": action_formatter(instrument)}
        quantity = lambda instrument: {"quantity": str(instrument.quantity)}

        class Product(WebPayload, key="product", locator="Product", multiple=False, optional=False):
            expire = lambda product: {"expiryYear": str(product.expire.year), "expiryMonth": str(product.expire.month), "expiryDay": str(product.expire.day)} if isinstance(product, ETradeOption) else {}
            instrument = lambda product: {"securityType": {Concepts.Securities.Instrument.OPTION: "OPTN", Concepts.Securities.Instrument.STOCK: "EQ"}[product.instrument]}
            option = lambda product: {"callPut": str(product.option).upper()} if isinstance(product, ETradeOption) else {}
            strike = lambda product: {"strikePrice": product.strike} if isinstance(product, ETradeOption) else {}
            ticker = lambda product: {"symbol": str(product.ticker)}

class ETradePreviewPayload(WebPayload, key="preview", locator="PreviewOrderRequest", dependents=[ETradeOrderPayload], fields={"orderType": "SPREADS"}, multiple=False, optional=False):
    identity = lambda preview: {"clientOrderId": str(preview.identity)}


class ETradeAccountPage(WebJSONPage):
    def execute(self, *args, **kwargs):
        url = ETradeAccountURL(*args, **kwargs)
        self.load(url, *args, **kwargs)
        accounts = ETradeAccountData(self.json, *args, **kwargs)
        accounts = [data(*args, **kwargs) for data in iter(accounts)]
        accounts = pd.concat(accounts, axis=0).set_index("key", drop=True, inplace=False)
        return accounts

class ETradePreviewPage(WebHTMLPage):
    def execute(self, *args, account, preview, **kwargs):
        url = ETradePreviewURL(*args, account=account, **kwargs)
        payload = ETradePreviewPayload(preview, *args, **kwargs)

        print(url)
        print(payload)
        raise Exception()

        self.load(url, *args, payload=payload.json, **kwargs)


class ETradeOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, source, webapi, **kwargs):
        super().__init__(*args, **kwargs)
        page = ETradeAccountPage(*args, source=source, **kwargs)
        account = page(*args, **kwargs)
        account = account.loc[int(webapi.account), "value"]
        page = ETradePreviewPage(*args, source=source, **kwargs)
        self.__account = str(account)
        self.__page = page

    def execute(self, prospects, /, **kwargs):
        assert isinstance(prospects, pd.DataFrame)
        if self.empty(prospects): return
        for preview, valuation in self.calculator(prospects, **kwargs):
            self.upload(preview, **kwargs)
            securities = ", ".join(list(map(str, preview.order.instruments)))
            self.console(f"{str(securities)}[{preview.order.quantity:.0f}] @ {str(valuation)}")

    def upload(self, preview, /, **kwargs):
        assert preview.order.term in (Concepts.Markets.Term.MARKET, Concepts.Markets.Term.LIMIT)
        parameters = dict(account=self.account, preview=preview)
        self.page(**parameters, **kwargs)

    @staticmethod
    def calculator(prospects, /, term, tenure, **kwargs):
        assert term in (Concepts.Markets.Term.MARKET, Concepts.Markets.Term.LIMIT)
        for index, prospect in prospects.iterrows():
            identity = random.randint(1000000000, 9999999999)
            strategy, quantity = prospect[["strategy", "quantity"]].values
            spot, breakeven = prospect[["spot", "breakeven"]].values
            settlement = prospect[list(Querys.Settlement)].to_dict()
            options = prospect[list(map(str, Securities.Options))].to_dict()
            options = {Securities.Options[option]: strike for option, strike in options.items() if not np.isnan(strike)}
            stocks = [Securities.Stocks(stock) for stock in strategy.stocks]
            assert spot >= breakeven and quantity >= 1
            options = [ETradeInstrument(security, strike=strike, quantity=quantity, **settlement) for security, strike in options.items()]
            stocks = [ETradeInstrument(security, quantity=quantity * 100, **settlement) for security in stocks]
            valuation = ETradeValuation(npv=prospect["npv"])
            order = ETradeOrder(instruments=options + stocks, term=term, tenure=tenure, limit=-breakeven, stop=None)
            preview = ETradePreview(identity=identity, order=order)
            yield preview, valuation

    @property
    def account(self): return self.__account
    @property
    def page(self): return self.__page



