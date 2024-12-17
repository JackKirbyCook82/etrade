# -*- coding: utf-8 -*-
"""
Created on Tues Nov 19 2024
@name:   ETrade PaperTrade Objects
@author: Jack Kirby Cook

"""

from itertools import chain

from finance.variables import Variables
from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebELMTs
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class ETradeTerminalData(WebELMTs.ELMT, locator=r"div[@id='application']", key="terminal"):
    class Ticket(WebELMTs.Button, locator=r"//button[@data-id='ViewPort_Navigation_tradeButton']", key="ticket"): pass

class ETradeTicketData(WebELMTs.ELMT, locator=r"//div[@data-id='OrderTicket']", key="ticket"):
    class Value(WebELMTs.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[1]", key="value"): pass
    class Funds(WebELMTs.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[2]", key="funds"): pass
    class Order(WebELMTs.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[1]", key="order"): pass
    class Analysis(WebELMTs.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[2]", key="analysis"): pass
    class Preview(WebELMTs.Button, locator=r"//button[span[text()='Preview']]", key="preview"): pass

class ETradeOrderData(WebELMTs.ELMT, locator=r"//div[contains(@class, 'OrderEntryContent---root')]", key="order"):
    class Ticker(WebELMTs.Input, locator=r"//input[@data-id='OrderTicketQuote_symbolSearchInput']", key="ticker"): pass
    class Spread(WebELMTs.Dropdown, locator=r"//div[@class='OrderTicketQuote_strategyDropdown']", key="spread"): pass
    class Option(WebELMTs.Button, locator=r"//button[@data-id='OrderTicket_addOptionButton']", key="option"): pass
    class Stock(WebELMTs.Button, locator=r"//button[@data-id='OrderTicket_addEquityButton']", key="stock"): pass
    class Securities(WebELMTs, locator="r//div[@data-id='OrderTicket_Leg']", key="securities", multiple=True, optional=True):
        class Position(WebELMTs.Toggle, locator=r"//button[@data-id='OrderTicket_Leg_actionToggle']", key="position"): pass
        class Quantity(WebELMTs.Input, locator=r"//input[@data-id='OrderTicket_Leg_qtyInput']", key="quantity"): pass
        class Option(WebELMTs.Toggle, locator=r"//button[@data-id='OrderTicket_Leg_typeToggle']", key="option"): pass
        class Expire(WebELMTs.Toggle, locator=r"//div[@data-id='OrderTicket_Leg_expirationDropdown']", key="expire"): pass
        class Strike(WebELMTs.Toggle, locator=r"//div[@data-id='OrderTicket_Leg_strikeDropdown']", key="strike"): pass
        class Remove(WebELMTs.Button, locator=r"//button[@data-id='OrderTicket_Leg_removeButton']", key="remove"): pass
    class Pricing(WebELMTs.Toggle, locator=r"//div[@data-id='OrderTicketSettings_priceTypeDropdown']/button", key="pricing"): pass
    class Price(WebELMTs.Input, locator=r"//input[contains(@class, 'PriceField') and contains(@class, 'NumberInput')]", key="price"): pass

class ETradeAnalysisData(locator=r"//div[contains(@class, 'StrategyAnalysis--root')]", key="analysis"):
    class Profit(WebELMTs.Text, locator=r"//div[/span/div/span[Text()=='Max Profit']]/span[2]", key="profit"): pass
    class Loss(WebELMTs.Text, locator=r"//div[/span/div/span[Text()=='Max Loss']]/span[2]", key="loss"): pass
    class Risk(WebELMTs.Text, locator=r"//div[span[text()='Probability for Any Profit']]/span[2]", key="risk"): pass

class ETradePreviewData(WebELMTs.ELMT, locator=r"//div[@data-id='OrderTicket']/div[contains(@class, 'OrderPreview---root')]", key="preview"):
    class Securities(WebELMTs.Text, locator="r//div[contains(@class, 'LegDescriptions--description')]]", key="securities", multiple=True): pass
    class Debit(WebELMTs.Text, locator=r"//div[div[text()='Estimated Total Debit (you spend)']]/div[2]", key="debit", optional=True): pass
    class Credit(WebELMTs.Text, locator=r"//div[div[text()='Estimated Total Credit (you receive)']]/div[2]", key="credit", optional=True): pass


class ETradeTerminalPage(WebBrowserPage, data=ETradeTerminalData):
    def execute(self, *args, **kwargs):
        self["ticket"].click()

class ETradeTicketPage(WebBrowserPage, data=ETradeTicketData):
    def execute(self, *args, **kwargs):
        self["order"].click()

class ETradeOrderPage(WebBrowserPage, data=ETradeOrderData):
    def execute(self, *args, order, **kwargs):
        self["ticker"].fill(order.ticker)
        self["spread"].select(order.spread)
        securities = chain(order.stocks, order.options)
        for index, security in enumerate(securities):
            self[str(security.instrument)].click()
            self["securities"][index]["position"].select(security.position)
            self["securities"][index]["quantity"].fill(security.quantity)
            if security.instrument is Variables.Instruments.STOCK: continue
            self["securities"][index]["option"].select(security.option)
            self["expire"][index]["expire"].select(security.expire)
            self["strike"][index]["strike"].select(security.strike)
        self["pricing"].select(order.pricing)
        self["price"].select(order.price)

class ETradeAnalysisPage(WebBrowserPage, data=ETradeAnalysisData):
    def execute(self, *args, **kwargs):
        pass

class ETradePreviewPage(WebBrowserPage, data=ETradePreviewData):
    def execute(self, *args, **kwargs):
        pass


class ETradeTerminalWindow(Logging):
    def __init__(self, *args, **kwargs):
        try: super().__init__(*args, **kwargs)
        except TypeError: super().__init__()
        self.__terminal = ETradeTerminalPage(*args, **kwargs)
        self.__ticket = ETradeTicketPage(*args, **kwargs)
        self.__order = ETradeOrderPage(*args, **kwargs)
        self.__analysis = ETradeAnalysisPage(*args, **kwargs)
        self.__preview = ETradePreviewPage(*args, **kwargs)

    def execute(self, order, *args, **kwargs):
        if order is None: return
        self.terminal(*args, **kwargs)
        self.ticker(*args, **kwargs)
        self.order(*args, order=order, **kwargs)
        self.analysis(*args, **kwargs)
        self.preview(*args, **kwargs)
        string = f"Ordered: {repr(self)}|{str(order)}"
        self.logger.info(string)

    @property
    def terminal(self): return self.__terminal
    @property
    def ticket(self): return self.__ticket
    @property
    def order(self): return self.__order
    @property
    def analysis(self): return self.__analysis
    @property
    def preview(self): return self.__preview






