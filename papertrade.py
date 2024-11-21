# -*- coding: utf-8 -*-
"""
Created on Tues Nov 19 2024
@name:   ETrade PaperTrade Objects
@author: Jack Kirby Cook

"""

from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebELMT

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class ETradeTradingData(WebELMT.Button, locator=r"//button[@data-id='ViewPort_Navigation_tradeButton']", key="trading"): pass
class ETradeTicketData(WebELMT, locator=r"//div[@data-id='OrderTicket']", key="ticket"):
    class Value(WebELMT.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[1]", key="value"): pass
    class Funds(WebELMT.Text, locator=r"//div[contains(@class, 'OrderEntryHeader') and contains(@class, 'details')]/span[2]", key="funds"): pass
    class Order(WebELMT.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[1]", key="order"): pass
    class Analysis(WebELMT.Clickable, locator=r"//ul[contains(@class, 'OrderEntryNavigation')]/li[2]", key="analysis"): pass
    class Preview(WebELMT.Button, locator=r"//button[span[text()='Preview']]", key="preview"): pass


class ETradeOrderData(WebELMT, locator=r"//div[contains(@class, 'OrderEntryContent--root')]", key="order"):
    class Ticker(WebELMT.Input, locator=r"//input[@data-id='OrderTicketQuote_symbolSearchInput']", key="ticker"): pass
    class Option(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_addOptionButton']", key="option"): pass
    class Stock(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_addEquityButton']", key="stock"): pass
    class Securities(WebELMT, locator="r//div[@data-id='OrderTicket_Leg']", key="securities", multiple=True, optional=True):
        class Position(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_Leg_actionToggle']", key="position"): pass
        class Quantity(WebELMT.Input, locator=r"//input[@data-id='OrderTicket_Leg_qtyInput']", key="quantity"): pass
        class Expire(WebELMT.Button, locator=r"//div[@data-id='OrderTicket_Leg_expirationDropdown']/button", key="expire"): pass
        class Expires(WebELMT.Clickable, locator=r"//div[@data-id='OrderTicket_Leg_expirationDropdown']/ul/li/div/a", key="expires", multiple=True, optional=True): pass
        class Strike(WebELMT.Button, locator=r"//div[@data-id='OrderTicket_Leg_strikeDropdown']/button", key="strike"): pass
        class Strikes(WebELMT.Clickable, locator=r"//div[@data-id='OrderTicket_Leg_strikeDropdown']/ul/li/div/a", key="strikes", multiple=True, optional=True): pass
        class Option(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_Leg_typeToggle']", key="option"): pass
        class Remove(WebELMT.Button, locator=r"//button[@data-id='OrderTicket_Leg_removeButton']", key="remove"): pass
    class Pricing(WebELMT.Button, locator=r"//div[@data-id='OrderTicketSettings_priceTypeDropdown']/button", key="pricing"): pass
    class Pricings(WebELMT.Clickable, locator=r"//div[@data-id='OrderTicketSettings_priceTypeDropdown']/ul/li/div/a", key="pricings"): pass
    class Price(WebELMT.Input, locator=r"//input[contains(@class, 'PriceField') and contains(@class, 'NumberInput')]", key="price"): pass


class ETradeAnalysisData(WebELMT, locator=r"//div[contains(@class, 'StrategyAnalysis--root')]", key="analysis"):
    class MaxProfit(WebELMT.Text, locator=r"//div[/span/div/span[Text()=='Max Profit']]/span[2]", key="maxprofit"): pass
    class MaxLoss(WebELMT.Text, locator=r"//div[/span/div/span[Text()=='Max Loss']]/span[2]", key="maxloss"): pass
    class Risk(WebELMT.Text, locator=r"//div[span[text()='Probability for Any Profit']]/span[2]", key="risk"): pass


class ETradePreviewData(WebELMT, locator=r"//div[@data-id='OrderTicket']/div[contains(@class, 'OrderPreview---root')]", key="preview"):
    class Securities(WebELMT.Text, locator="r//div[contains(@class, 'LegDescriptions--description')]]", key="securities", multiple=True): pass
    class Debit(WebELMT.Text, locator=r"//div[div[text()='Estimated Total Debit (you spend)']]/div[2]", key="debit", optional=True): pass
    class Credit(WebELMT.Text, locator=r"//div[div[text()='Estimated Total Credit (you receive)']]/div[2]", key="credit", optional=True): pass


class ETradeTradingPage(WebBrowserPage):
    def __call__(self, *args, **kwargs):
        pass







