# -*- coding: utf-8 -*-
"""
Created on Tues Nov 19 2024
@name:   ETrade PaperTrade Objects
@author: Jack Kirby Cook

"""

from itertools import chain

from finance.variables import Variables
from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebELMT
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class ETradeTerminalData(WebELMT, locator=r"div[@id='application']", key="terminal"):
    class Ticket(WebELMT.Button, locator=r"//button[@data-id='ViewPort_Navigation_tradeButton']", key="ticket"): pass


class ETradeTicketData(WebELMT, locator=r"//div[@data-id='OrderTicket']", key="ticket"):
    class Value(WebELMT.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[1]", key="value"): pass
    class Funds(WebELMT.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[2]", key="funds"): pass
    class Order(WebELMT.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[1]", key="order"): pass
    class Analysis(WebELMT.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[2]", key="analysis"): pass
    class Preview(WebELMT.Button, locator=r"//button[span[text()='Preview']]", key="preview"): pass


class ETradeOrderData(WebELMT, locator=r"//div[contains(@class, 'OrderEntryContent---root')]", key="order"):
    class Ticker(WebELMT.Input, locator=r"//input[@data-id='OrderTicketQuote_symbolSearchInput']", key="ticker"): pass
    class Spread(WebELMT.Dropdown, locator=r"//div[@class='OrderTicketQuote_strategyDropdown']", key="spread"): pass
    class Option(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_addOptionButton']", key="option"): pass
    class Stock(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_addEquityButton']", key="stock"): pass
    class Securities(WebELMT, locator="r//div[@data-id='OrderTicket_Leg']", key="securities", multiple=True, optional=True):
        class Position(WebELMT.Toggle, locator=r"//button[@data-id='OrderTicket_Leg_actionToggle']", key="position"): pass
        class Quantity(WebELMT.Input, locator=r"//input[@data-id='OrderTicket_Leg_qtyInput']", key="quantity"): pass
        class Option(WebELMT.Toggle, locator=r"//button[@data-id='OrderTicket_Leg_typeToggle']", key="option"): pass
        class Expire(WebELMT.Toggle, locator=r"//div[@data-id='OrderTicket_Leg_expirationDropdown']", key="expire"): pass
        class Strike(WebELMT.Toggle, locator=r"//div[@data-id='OrderTicket_Leg_strikeDropdown']", key="strike"): pass
        class Remove(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_Leg_removeButton']", key="remove"): pass
    class Pricing(WebELMT.Toggle, locator=r"//div[@data-id='OrderTicketSettings_priceTypeDropdown']/button", key="pricing"): pass
    class Price(WebELMT.Input, locator=r"//input[contains(@class, 'PriceField') and contains(@class, 'NumberInput')]", key="price"): pass


class ETradeAnalysisData(WebELMT, locator=r"//div[contains(@class, 'StrategyAnalysis--root')]", key="analysis"):
    class Profit(WebELMT.Text, locator=r"//div[/span/div/span[Text()=='Max Profit']]/span[2]", key="profit"): pass
    class Loss(WebELMT.Text, locator=r"//div[/span/div/span[Text()=='Max Loss']]/span[2]", key="loss"): pass
    class Risk(WebELMT.Text, locator=r"//div[span[text()='Probability for Any Profit']]/span[2]", key="risk"): pass


class ETradePreviewData(WebELMT, locator=r"//div[@data-id='OrderTicket']/div[contains(@class, 'OrderPreview---root')]", key="preview"):
    class Securities(WebELMT.Text, locator="r//div[contains(@class, 'LegDescriptions--description')]]", key="securities", multiple=True): pass
    class Debit(WebELMT.Text, locator=r"//div[div[text()='Estimated Total Debit (you spend)']]/div[2]", key="debit", optional=True): pass
    class Credit(WebELMT.Text, locator=r"//div[div[text()='Estimated Total Credit (you receive)']]/div[2]", key="credit", optional=True): pass


class ETradeTerminalPage(WebBrowserPage):
    def execute(self, *args, **kwargs):
        elements = ETradeTerminalData(self.source.element)
        elements["ticket"].click()


class ETradeTicketPage(WebBrowserPage):
    def execute(self, *args, **kwargs):
        elements = ETradeTicketData(self.source.element)
        elements["order"].click()


class ETradeOrderPage(WebBrowserPage):
    def execute(self, *args, order, **kwargs):
        elements = ETradeOrderData(self.source.element)
        elements["ticker"].fill(order.ticker)
        elements["spread"].select(order.spread)
        securities = chain(order.stocks, order.options)
        for index, security in enumerate(securities):
            elements[str(security.instrument)].click()
            elements["securities"][index]["position"].select(security.position)
            elements["securities"][index]["quantity"].fill(security.quantity)
            if security.instrument is Variables.Instruments.STOCK: continue
            elements["securities"][index]["option"].select(security.option)
            elements["expire"][index]["expire"].select(security.expire)
            elements["strike"][index]["strike"].select(security.strike)
        elements["pricing"].select(order.pricing)
        elements["price"].select(order.price)


class ETradeAnalysisPage(WebBrowserPage):
    def execute(self, *args, **kwargs):
        elements = ETradeAnalysisData(self.source.element)


class ETradePreviewPage(WebBrowserPage):
    def execute(self, *args, **kwargs):
        elements = ETradePreviewData(self.source.element)


class ETradeTerminalWindow(Logging):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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






