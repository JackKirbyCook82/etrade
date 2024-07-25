# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2024
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import numpy as np
import tkinter as tk

from finance.variables import Variables, Contract
from support.windows import Application, Stencils, Content, Column

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
    strategy = Content(Stencils.Label, text="strategy", font="Arial 10 bold", justify=tk.LEFT, locator=(0, 0))
    contract = Content(Stencils.Label, text="contract", font="Arial 10", justify=tk.LEFT, locator=(1, 0))
    security = Content(Stencils.Label, text="security", font="Arial 10", justify=tk.LEFT, locator=(2, 0))

class Appraisal(Stencils.Frame):
    valuation = Content(Stencils.Label, text="valuation", font="Arial 10 bold", justify=tk.LEFT, locator=(0, 0))
    earnings = Content(Stencils.Label, text="earnings", font="Arial 10", justify=tk.LEFT, locator=(1, 0))
    cashflow = Content(Stencils.Label, text="cashflow", font="Arial 10", justify=tk.LEFT, locator=(2, 0))
    size = Content(Stencils.Label, text="size", font="Arial 10", justify=tk.LEFT, locator=(3, 0))


class HoldingsScroll(Stencils.Scroll): pass
class HoldingsTable(Stencils.Table):
    tag = Column(text="tag", width=5, parser=lambda tag: f"{tag:.0f}", locator=lambda row: row[("tag", "")])
    valuation = Column(text="valuation", width=10, parser=lambda valuation: str(valuation), locator=lambda row: row[("valuation", "")])
    strategy = Column(text="strategy", width=10, parser=lambda strategy: str(strategy), locator=lambda row: row[("strategy", "")])
    contract = Column(text="contract", width=10, parser=contract_parser, locator=contract_locator)
    security = Column(text="security", width=20, parser=security_parser, locator=security_locator)
    apy = Column(text="apy", width=10, parser=lambda apy: f"{apy * 100:.0f}% / YR", locator=lambda row: row[("apy", "minimum")])
    tau = Column(text="tau", width=10, parser=lambda tau: f"{tau:.0f} DY", locator=lambda row: row[("tau", "")])
    npv = Column(text="npv", width=10, parser=lambda npv: f"${npv:,.0f}", locator=lambda row: row[("npv", "minimum")])
    cost = Column(text="cost", width=10, parser=lambda cost: f"${cost:,.0f}", locator=lambda row: row[("cost", "minimum")])
    size = Column(text="size", width=10, parser=lambda size: f"{size:,.0f} CT", locator=lambda row: row[("size", "")])


class Holdings(Stencils.Frame):
    table = Content(HoldingsTable, locator=(0, 0))
    vertical = Content(HoldingsScroll, orientation=tk.VERTICAL, locator=(0, 1))

class Acquisitions(Holdings): pass
class Divestitures(Holdings): pass


class Pursue(Stencils.Button): pass
class Abandon(Stencils.Button): pass
class Success(Stencils.Button): pass
class Failure(Stencils.Button): pass


class StatusWindow(Stencils.Window):
    product = Content(Product, locator=(0, 0))
    appraisal = Content(Appraisal, locator=(0, 1))

class AcceptedWindow(StatusWindow): pass
class RejectedWindow(StatusWindow): pass

class ProspectWindow(StatusWindow):
    pursue = Content(Pursue, text="pursue", font="Arial 10", justify=tk.CENTER, locator=(1, 0))
    abandon = Content(Abandon, text="abandon", font="Arial 10", justify=tk.CENTER, locator=(1, 1))

class PendingWindow(StatusWindow):
    success = Content(Success, text="success", font="Arial 10", justify=tk.CENTER, locator=(1, 0))
    failure = Content(Failure, text="failure", font="Arial 10", justify=tk.CENTER, locator=(1, 1))

class PaperTradingWindow(Stencils.Notebook):
    acquisitions = Content(Acquisitions, sticky=tk.NSEW, locator=(0, 0))
    divestitures = Content(Divestitures, sticky=tk.NSEW, locator=(0, 0))


class PaperTradeApplication(Application):
    def __init__(self, *args, acquisitions, divestitures, **kwargs):
        super().__init__(*args, **kwargs)
        self.__acquisitions = acquisitions
        self.__divestitures = divestitures

    @property
    def acquisitions(self): return self.__acquisitions
    @property
    def divestitures(self): return self.__divestitures





