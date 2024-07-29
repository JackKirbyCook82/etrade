# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2024
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import numpy as np
import tkinter as tk

from finance.variables import Variables, Contract
from support.windows import Application, Stencils, Layouts

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["PaperTradeApplication"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


contract_parser = lambda contract: f"{str(contract.ticker)}\n{str(contract.expire.strftime('%Y%m%d'))}"
contract_locator = lambda row: Contract(row[("ticker", "")], row[("expire", "")])
security_parser = lambda securities: "\n".join([f"{str(security)}={int(strike):.02}" for security, strike in dict(securities).items()])
security_locator = lambda row: {(str(security), ""): row[str(security)] for security in list(Variables.Securities) if not np.isnan(row[str(security)])}


class Product(Stencils.Frame):
    strategy = Layouts.Widget(Stencils.Label, text="strategy", font="Arial 10 bold", justify=tk.LEFT, locator=(0, 0))
    contract = Layouts.Widget(Stencils.Label, text="contract", font="Arial 10", justify=tk.LEFT, locator=(1, 0))
    security = Layouts.Widget(Stencils.Label, text="security", font="Arial 10", justify=tk.LEFT, locator=(2, 0))

class Appraisal(Stencils.Frame):
    valuation = Layouts.Widget(Stencils.Label, text="valuation", font="Arial 10 bold", justify=tk.LEFT, locator=(0, 0))
    earnings = Layouts.Widget(Stencils.Label, text="earnings", font="Arial 10", justify=tk.LEFT, locator=(1, 0))
    cashflow = Layouts.Widget(Stencils.Label, text="cashflow", font="Arial 10", justify=tk.LEFT, locator=(2, 0))
    size = Layouts.Widget(Stencils.Label, text="size", font="Arial 10", justify=tk.LEFT, locator=(3, 0))


class HoldingsScroll(Stencils.Scroll): pass
class HoldingsTable(Stencils.Table):
    tag = Layouts.Column(text="tag", width=50, parser=lambda tag: f"{int(tag):.0f}", locator=lambda row: row[("tag", "")])
    valuation = Layouts.Column(text="valuation", width=200, parser=lambda valuation: str(valuation), locator=lambda row: row[("valuation", "")])
    strategy = Layouts.Column(text="strategy", width=200, parser=lambda strategy: str(strategy), locator=lambda row: row[("strategy", "")])
    contract = Layouts.Column(text="contract", width=200, parser=contract_parser, locator=contract_locator)
    security = Layouts.Column(text="security", width=200, parser=security_parser, locator=security_locator)
    apy = Layouts.Column(text="apy", width=100, parser=lambda apy: f"{apy * 100:.0f}% / YR", locator=lambda row: row[("apy", "minimum")])
    tau = Layouts.Column(text="tau", width=100, parser=lambda tau: f"{tau:.0f} DY", locator=lambda row: row[("tau", "")])
    npv = Layouts.Column(text="npv", width=100, parser=lambda npv: f"${npv:,.0f}", locator=lambda row: row[("npv", "minimum")])
    cost = Layouts.Column(text="cost", width=100, parser=lambda cost: f"${cost:,.0f}", locator=lambda row: row[("cost", "minimum")])
    size = Layouts.Column(text="size", width=100, parser=lambda size: f"{size:,.0f} CT", locator=lambda row: row[("size", "")])

    def select(self, event):
        pass


class Holdings(Stencils.Frame):
    table = Layouts.Widget(HoldingsTable, locator=(0, 0))
    vertical = Layouts.Widget(HoldingsScroll, orientation=tk.VERTICAL, locator=(0, 1))

class Acquisitions(Holdings): pass
class Divestitures(Holdings): pass


class Pursue(Stencils.Button):
    def click(self, event):
        pass

class Abandon(Stencils.Button):
    def click(self, event):
        pass

class Success(Stencils.Button):
    def click(self, event):
        pass

class Failure(Stencils.Button):
    def click(self, event):
        pass


class StatusWindow(Stencils.Window):
    product = Layouts.Widget(Product, locator=(0, 0))
    appraisal = Layouts.Widget(Appraisal, locator=(0, 1))

class AcceptedWindow(StatusWindow): pass
class RejectedWindow(StatusWindow): pass

class ProspectWindow(StatusWindow):
    pursue = Layouts.Widget(Pursue, text="pursue", font="Arial 10", justify=tk.CENTER, locator=(1, 0))
    abandon = Layouts.Widget(Abandon, text="abandon", font="Arial 10", justify=tk.CENTER, locator=(1, 1))

class PendingWindow(StatusWindow):
    success = Layouts.Widget(Success, text="success", font="Arial 10", justify=tk.CENTER, locator=(1, 0))
    failure = Layouts.Widget(Failure, text="failure", font="Arial 10", justify=tk.CENTER, locator=(1, 1))

class PaperTradingWindow(Stencils.Notebook):
    acquisitions = Layouts.Widget(Acquisitions, locator=(0, 0))
    divestitures = Layouts.Widget(Divestitures, locator=(0, 0))


class PaperTradeApplication(Application, window=PaperTradingWindow, heading="PaperTrading"):
    def __init__(self, *args, acquisitions, divestitures, **kwargs):
        super().__init__(*args, **kwargs)
        self.acquisitions = acquisitions
        self.divestitures = divestitures






