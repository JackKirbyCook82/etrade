# -*- coding: utf-8 -*-
"""
Created on Tues Nov 19 2024
@name:   ETrade PaperTrade Objects
@author: Jack Kirby Cook

"""

import regex as re
import numpy as np
from itertools import chain
from datetime import datetime as Datetime

from finance.variables import Variables
from webscraping.webpages import WebBrowserPage
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


# expire_parser = lambda x: Datetime.strptime(x, "%b-%m-%y" if ("-" in str(x)) else "%b%y").date()
# strike_parser = lambda x: np.round(x, 2).astype(np.float32)


class ETradeTerminalData(WebELMT, locator=r"div[@id='application']", key="terminal"):
    class Ticket(WebELMT.Button, locator=r"//button[@data-id='ViewPort_Navigation_tradeButton']", key="ticket"): pass


class ETradeTicketData(WebELMT, locator=r"//div[@data-id='OrderTicket']", key="ticket"):
    class Value(WebELMT.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[1]", key="value", parser=np.float32): pass
    class Funds(WebELMT.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[2]", key="funds", parser=np.float32): pass
    class Order(WebELMT.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[1]", key="order"): pass
    class Analysis(WebELMT.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[2]", key="analysis"): pass
    class Preview(WebELMT.Button, locator=r"//button[span[text()='Preview']]", key="preview"): pass


class ETradeOrderData(WebELMT, locator=r"//div[contains(@class, 'OrderEntryContent')]", key="order"):
    class Ticker(WebELMT.Input, locator=r"//input[@data-id='OrderTicketQuote_symbolSearchInput']", key="ticker", parser=str): pass
    class Option(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_addOptionButton']", key="option"): pass
    class Stock(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_addEquityButton']", key="stock"): pass
    class Securities(WebELMT, locator="r//div[@data-id='OrderTicket_Leg']", key="securities", multiple=True, optional=True):
        class Action(WebELMT.Toggle, locator=r"//button[@data-id='OrderTicket_Leg_actionToggle']", key="action", parser=Variables.ActionTypes): pass
        class Quantity(WebELMT.Input, locator=r"//input[@data-id='OrderTicket_Leg_qtyInput']", key="quantity", parser=np.int32): pass
        class Option(WebELMT.Toggle, locator=r"//button[@data-id='OrderTicket_Leg_typeToggle']", key="option", parser=Variables.Options): pass
        class Expire(WebELMT.Dropbown, locator=(r"//div[@data-id='OrderTicket_Leg_expirationDropdown']", r"//ul/li[contains(@class, 'MenuItem')]"), key="expire", parser=expire_parser): pass
        class Strike(WebELMT.DropDown, locator=(r"//div[@data-id='OrderTicket_Leg_strikeDropdown']", r"//ul/li[contains(@class, 'MenuItem')]"), key="strike", parser=strike_parser): pass
    class Remove(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_Leg_removeButton']", key="remove"): pass
    class Pricing(WebELMT.Dropdown, locator=(r"//div[@data-id='OrderTicketSettings_priceTypeDropdown']/button", r"//ul/li[contains(@class, 'MenuItem')]"), key="pricing", parser=Variables.PriceTypes): pass
    class Price(WebELMT.Input, locator=r"//input[contains(@class, 'PriceField') and contains(@class, 'NumberInput')]", key="price", parser=float): pass

class ETradeAnalysisData(WebELMT, locator=r"//div[contains(@class, 'StrategyAnalysis')]", key="analysis"):
    class Profit(WebELMT.Text, locator=r"//div[/span/div/span[Text()=='Max Profit']]/span[2]", key="profit", parser=np.float32): pass
    class Loss(WebELMT.Text, locator=r"//div[/span/div/span[Text()=='Max Loss']]/span[2]", key="loss", parser=np.float32): pass
    class Risk(WebELMT.Text, locator=r"//div[span[text()='Probability for Any Profit']]/span[2]", key="risk", parser=np.float32): pass


class ETradePreviewData(WebELMT, locator=r"//div[@data-id='OrderTicket']/div[contains(@class, 'OrderPreview')]", key="preview"):
    class Credit(WebELMT.Text, locator=r"//div[div[text()='Estimated Total Credit (you receive)']]/div[2]", key="credit", optional=True, parser=np.float32): pass
    class Debit(WebELMT.Text, locator=r"//div[div[text()='Estimated Total Debit (you spend)']]/div[2]", key="debit", optional=True, parser=np.float32): pass
    class Securities(WebELMT.Text, locator=r"//div[contains(@class, 'LegDescriptions')]//div[contains(@class, 'description')]", key="securities", multiple=True, optional=True):
        pass

#        def execute(self, *args, **kwargs):
#            contents = super().execute(*args, **kwargs)
#            instrument = lambda string: str(Variables.Instruments.STOCK) if ("shares" in str(string)) else str(Variables.Instruments.OPTION)
#            contents = str(contents).lower().replace("to", instrument(contents)).replace("shares", "").upper()
#            pattern = "^(?P<actiontype>BUY|SELL)\s(?P<quantity>\d+)\s(?P<ticker>[A-Z]+)\s((?P<expire>[A-Za-z\d\-]+)\s(?P<strike>\d+)\s(?P<option>PUT|CALL))?\s(?P<instrument>STOCK|OPTION)\s(?P<tradetype>OPEN|CLOSE)$"
#            variables = dict(actiontype=Variables.ActionTypes, quantity=int, ticker=str, expire=expire_parser, strike=strike_parser, option=Variables.Options, instrument=Variables.Instruments, tradetype=Variables.TradeTypes)
#            contents = {key: variables[key](value) for key, value in re.match(pattern, contents).groupdict().items()}
#            return contents


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
        for security in elements["securities"]: security["remove"].click()

        elements["ticker"].fill(str(order.ticker))

        securities = chain(list(order.stocks), list(order.options))
        for index, security in enumerate(securities):
            elements[str(security.instrument)].click()

            # REFRESH
            elements["securities"][index]["action"].select(security.action)
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






