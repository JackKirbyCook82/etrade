# -*- coding: utf-8 -*-
"""
Created on Tues Nov 19 2024
@name:   ETrade PaperTrade Objects
@author: Jack Kirby Cook

"""

import regex as re
import numpy as np
from datetime import datetime as Datetime

from finance.variables import Variables
from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebELMT
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ETradeTerminalWindow"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


expire_parser = lambda content: Datetime.strptime(str(content).replace("-", ""), "%b%m%y" if ("-" in str(content)) else "%b%y").date()
strike_parser = lambda content: np.round(content, 2).astype(np.float32)


class ETradeTerminalData(WebELMT, locator=r"//div[@id='application']", key="terminal"):
    class Ticket(WebELMT.Button, locator=r"//button[@data-id='ViewPort_Navigation_tradeButton']", key="ticket"): pass


class ETradeTicketData(WebELMT, locator=r"//div[@data-id='OrderTicket']", key="ticket"):
    class Value(WebELMT.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[1]", key="value", parser=np.float32): pass
    class Funds(WebELMT.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[2]", key="funds", parser=np.float32): pass
    class Order(WebELMT.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[1]", key="order"): pass
    class Analysis(WebELMT.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[2]", key="analysis"): pass
    class Preview(WebELMT.Button, locator=r"//button[span[text()='Preview']]", key="preview"): pass


class ETradeOrderData(WebELMT, locator=r"//div[contains(@class, 'OrderEntryContent---root')]", key="order"):
    class Ticker(WebELMT.Input, locator=r"//input[@data-id='OrderTicketQuote_symbolSearchInput']", key="ticker", parser=str): pass
    class Option(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_addOptionButton']", key="option"): pass
    class Stock(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_addEquityButton']", key="stock"): pass
    class Securities(WebELMT, locator=r"//div[@data-id='OrderTicket_Leg']", key="securities", multiple=True, optional=False):
        class Remove(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_Leg_removeButton']", key="remove"): pass
        class Action(WebELMT.Toggle, locator=r"//button[@data-id='OrderTicket_Leg_actionToggle']", key="action", parser=Variables.Actions): pass
        class Quantity(WebELMT.Input, locator=r"//input[@data-id='OrderTicket_Leg_qtyInput']", key="quantity", parser=np.int32): pass
        class Option(WebELMT.Toggle, locator=r"//button[@data-id='OrderTicket_Leg_typeToggle']", key="option", parser=Variables.Options): pass
        class Expire(WebELMT.Dropdown, locator=r"//div[@data-id='OrderTicket_Leg_expirationDropdown']", locators={"menu": r"//ul/li[contains(@class, 'MenuItem')]"}, key="expire", parser=expire_parser): pass
        class Strike(WebELMT.Dropdown, locator=r"//div[@data-id='OrderTicket_Leg_strikeDropdown']", locators={"menu": r"//ul/li[contains(@class, 'MenuItem')]"}, key="strike", parser=strike_parser): pass
    class Terms(WebELMT.Dropdown, locator=r"//div[@data-id='OrderTicketSettings_priceTypeDropdown']/button", locators={"menu": r"//ul/li[contains(@class, 'MenuItem')]"}, key="terms", parser=Variables.Terms): pass
    class Price(WebELMT.Input, locator=r"//input[contains(@class, 'PriceField') and contains(@class, 'NumberInput')]", key="price", parser=float): pass


class ETradeAnalysisData(WebELMT, locator=r"//div[contains(@class, 'StrategyAnalysis')]", key="analysis"):
    class Profit(WebELMT.Text, locator=r"//div[/span/div/span[Text()=='Max Profit']]/span[2]", key="profit", parser=np.float32): pass
    class Loss(WebELMT.Text, locator=r"//div[/span/div/span[Text()=='Max Loss']]/span[2]", key="loss", parser=np.float32): pass
    class Risk(WebELMT.Text, locator=r"//div[span[text()='Probability for Any Profit']]/span[2]", key="risk", parser=np.float32): pass


class ETradePreviewData(WebELMT, locator=r"//div[@data-id='OrderTicket']/div[contains(@class, 'OrderPreview')]", key="preview"):
    class Credit(WebELMT.Text, locator=r"//div[div[text()='Estimated Total Credit (you receive)']]/div[2]", key="credit", optional=True, parser=np.float32): pass
    class Debit(WebELMT.Text, locator=r"//div[div[text()='Estimated Total Debit (you spend)']]/div[2]", key="debit", optional=True, parser=np.float32): pass
    class Securities(WebELMT.Text, locator=r"//div[contains(@class, 'LegDescriptions')]//div[contains(@class, 'description')]", key="securities", multiple=True, optional=True):
        def execute(self, *args, **kwargs):
            contents = super().execute(*args, **kwargs)
            instrument = lambda string: str(Variables.Instruments.STOCK) if ("shares" in str(string)) else str(Variables.Instruments.OPTION)
            contents = str(contents).lower().replace("to", instrument(contents)).replace("shares", "").upper()
            pattern = "^(?P<action>BUY|SELL)\s(?P<quantity>\d+)\s(?P<ticker>[A-Z]+)\s((?P<expire>[A-Za-z\d\-]+)\s(?P<strike>\d+)\s(?P<option>PUT|CALL))?\s(?P<instrument>STOCK|OPTION)\s(?P<trade>OPEN|CLOSE)$"
            variables = dict(action=Variables.Actions, quantity=int, ticker=str, expire=expire_parser, strike=strike_parser, option=Variables.Options, instrument=Variables.Instruments, trade=Variables.Trades)
            contents = {key: variables[key](value) for key, value in re.match(pattern, contents).groupdict().items()}
            return contents


class ETradeTerminalPage(WebBrowserPage):
    def execute(self, *args, order, **kwargs):
        self.navigate("Power E*TRADE | Trading")
        ETradeTerminalData(self.elmt, *args, **kwargs)["ticket"].click()
        ETradeTicketData(self.elmt, *args, **kwargs)["order"].click()
        elements = ETradeOrderData(self.elmt, *args, **kwargs)
        for security in elements["securities"]: security["remove"].click()
        elements["ticker"].fill(order.contract.ticker)
        for index, security in enumerate(order.securities):
            elements[str(security.instrument)].click()
            elements["securities"][index]["action"].select(security.action)
            elements["securities"][index]["quantity"].fill(int(security.quantity))
            if security.instrument is Variables.Instruments.STOCK: continue
            elements["securities"][index]["option"].select(security.option)
            elements["expire"][index]["expire"].select(order.contract.expire)
            elements["strike"][index]["strike"].select(security.strike)
        elements["terms"].select(order.transaction.terms)
        elements["price"].select(order.transaction.price)


class ETradeTerminalWindow(Logging):
    def __init_subclass__(cls, *args, **kwargs): pass
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = ETradeTerminalPage(*args, **kwargs)

    def execute(self, order, *args, **kwargs):
        if order is None: return
        self.page(*args, order=order, **kwargs)
        string = f"Ordered: {repr(self)}|{str(order)}"
        self.logger.info(string)

    @property
    def page(self): return self.__page







