# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2024
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""
import multiprocessing
import numpy as np
import tkinter as tk
from collections import namedtuple as ntuple
from collections import OrderedDict as ODict

from finance.variables import Variables, Contract
from support.windows import Application, Stencils, Widget, Events

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["PaperTradeApplication"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


table_valuation_parser = lambda row: str(row[("valuation", "")])
table_strategy_parser = lambda row: str(row[("strategy", "")])
table_contract_parser = lambda row: f"{row[('ticker', '')].ticker}|{row[('expire', '')].strftime('%Y-%m-%d')}"
table_security_iterator = lambda row: {security: row[(str(security), '')] for security in list(Variables.Securities) if not np.isnan(row[(str(security), '')])}
table_security_parser = lambda row: "\n".join([f"{str(security)}|{strike:.02f}" for security, strike in table_security_iterator(row)])
table_earning_parser = lambda row: f"{row[('apy', Variables.Scenarios.MINIMUM)]:.02f} %/YR -> {row[('apy', Variables.Scenarios.MAXIMUM)]:.02f} %/YR @ {row[('tau', Variables.Scenarios.MINIMUM)]:.02f} DY"
table_cashflow_parser = lambda row: f"${row[('npv', Variables.Scenarios.MINIMUM)]:,.02f} -> ${row[('npv', Variables.Scenarios.MAXIMUM)]:,.02f}"
table_size_parser = lambda row: f"{row[('size', '')]:,.0f} CT"

selection_status_parser = lambda selection: str(selection.status)
selection_valuation_parser = lambda selection: str(selection.valuation)
selection_strategy_parser = lambda selection: str(selection.strategy)
selection_contract_parser = lambda selection: f"{selection.contract.ticker}\n{selection.contract.expire.strftime('%Y-%m-%d')}"
selection_security_parser = lambda selection: "\n".join([f"{str(security)} @ {strike:.02f}" for security, strike in selection.securities.items()])
selection_earning_parser = lambda selection: f"{selection.earnings.apy.minimum:.02f} %/YR -> {selection.earnings.apy.maximum:.02f} %/YR @ {selection.earnings.tau:.02f} DY"
selection_cashflow_parser = lambda selection: f"${selection.cashflow.npv.minimum:,.02f} -> ${selection.cashflow.npv.maximum:,.02f}"
selection_size_parser = lambda selection: f"{selection.size:,.0f} CT"

Scenario = ntuple("Scenario", "minimum maximum")
Earnings = ntuple("Earnings", "apy tau")
Cashflow = ntuple("Cashflow", "npv cost")


class Selections(ODict):
    def __init__(self, dataset, **contents):
        selections = [(index, Selection(dataset, index, series)) for index, series in contents.items()]
        super().__init__(selections)
        self.mutex = multiprocessing.RLock()
        self.dataset = dataset

    def __setitem__(self, index, series):
        selection = Selection(self.dataset, index, series)
        super().__setitem__(index, selection)


class Selection(ntuple("Selection", "dataset index series")):
    @property
    def status(self): return self.contents[("status", "")]
    @property
    def valuation(self): return self.contents[("valuation", "")]
    @property
    def strategy(self): return self.contents[("strategy", "")]
    @property
    def contract(self): return Contract(self.contents[("ticker", "")], self.contents[("expire", "")])
    @property
    def securities(self): return {security: self.contents[(str(security), "")] for security in list(Variables.Securities) if not np.isnan(self.contents[(str(security), "")])}
    @property
    def earnings(self): return Earnings(self.apy, self.tau)
    @property
    def cashflow(self): return Cashflow(self.npy, self.cost)

    @property
    def apy(self): return Scenario(self.contents[("apy", Variables.Scenarios.MINIMUM)], self.contents[("apy", Variables.Scenarios.MAXIMUM)])
    @property
    def tau(self): return self.contents[("tau", "")]
    @property
    def npv(self): return Scenario(self.contents[("npv", Variables.Scenarios.MINIMUM)], self.contents[("npv", Variables.Scenarios.MAXIMUM)])
    @property
    def cost(self): return Scenario(self.contents[("cost", Variables.Scenarios.MINIMUM)], self.contents[("cost", Variables.Scenarios.MAXIMUM)])
    @property
    def size(self): return self.contents[("size", "")]


class SelectionsButton(Stencils.Button):
    def __init_subclass__(cls, *args, reverse, **kwargs): cls.reverse = reverse

    @Events.Handler(Events.Mouse.LEFT, Events.Mouse.LEFT)
    def click(self, controller, **parameters):
        pass

class ForwardButton(SelectionsButton, reverse=False): pass
class BackwardButton(SelectionsButton, reverse=True): pass


class SelectionButton(Stencils.Button):
    def __init_subclass__(cls, *args, status, **kwargs): cls.status = status

    @Events.Handler(Events.Mouse.LEFT)
    def click(self, controller, **parameters):
        pass


class PursueButton(SelectionButton, status=Variables.Status.PENDING): pass
class AbandonButton(SelectionButton, status=Variables.Status.REJECTED): pass
class AcceptButton(SelectionButton, status=Variables.Status.ACCEPTED): pass
class RejectButton(SelectionButton, status=Variables.Status.REJECTED): pass


class SelectionsFrame(Stencils.Frame):
    identity = Widget(element=Stencils.Variable, font="Arial 10 bold", justify=tk.LEFT, locator=(0, 0))
    status = Widget(element=Stencils.Variable, font="Arial 10 bold", justify=tk.LEFT, locator=(1, 0))
    valuation = Widget(element=Stencils.Variable, font="Arial 10 bold", justify=tk.LEFT, locator=(2, 0))
    strategy = Widget(element=Stencils.Variable, font="Arial 10 bold", justify=tk.LEFT, locator=(3, 0))
    contract = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(4, 0))
    security = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(5, 0))
    earnings = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(6, 0))
    cashflow = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(7, 0))
    size = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(8, 0))

    pursue = Widget(element=PursueButton, text="Pursue", font="Arial 8", justify=tk.CENTER, locator=(9, 0))
    abandon = Widget(element=AbandonButton, text="Abandon", font="Arial 8", justify=tk.CENTER, locator=(9, 1))
    accepted = Widget(element=AcceptButton, text="Accepted", font="Arial 8", justify=tk.CENTER, locator=(10, 0))
    rejected = Widget(element=RejectButton, text="Rejected", font="Arial 8", justify=tk.CENTER, locator=(10, 1))
    backward = Widget(element=BackwardButton, text="Preview", font="Arial 8", justify=tk.CENTER, locator=(11, 0))
    forward = Widget(element=ForwardButton, text="Next", font="Arial 8", justify=tk.CENTER, locator=(11, 1))


class HoldingsScroll(Stencils.Scroll): pass
class HoldingsTable(Stencils.Table):
    valuation = Widget(element=Stencils.Column, text="valuation", width=200, parser=table_valuation_parser)
    strategy = Widget(element=Stencils.Column, text="strategy", width=200, parser=table_strategy_parser)
    contract = Widget(element=Stencils.Column, text="contract", width=200, parser=table_contract_parser)
    security = Widget(element=Stencils.Column, text="security", width=200, parser=table_security_parser)
    earning = Widget(element=Stencils.Column, text="apy", width=100, parser=table_earning_parser)
    cashflow = Widget(element=Stencils.Column, text="tau", width=100, parser=table_cashflow_parser)
    size = Widget(element=Stencils.Column, text="size", width=100, parser=table_size_parser)

    @Events.Handler(Events.Virtual.SELECT)
    def select(self, controller, **parameters):
        pass


class HoldingsFrame(Stencils.Frame):
    table = Widget(HoldingsTable, locator=(0, 0), vertical=True, horizontal=False)
    vertical = Widget(HoldingsScroll, orientation=tk.VERTICAL, locator=(0, 1))
    selections = Widget(SelectionsFrame, locator=(0, 2))

class AcquisitionFrame(HoldingsFrame): pass
class DivestitureFrame(HoldingsFrame): pass

class PaperTradingFrame(Stencils.Notebook):
    acquisitions = Widget(AcquisitionFrame, locator=(0, 0))
    divestitures = Widget(DivestitureFrame, locator=(0, 0))

class PaperTradingWindow(Stencils.Window):
    holdings = Widget(PaperTradingFrame, locator=(0, 0))


class PaperTradeApplication(Application, window=PaperTradingWindow, heading="PaperTrading"):
    def create(self, window, *args, acquisitions, divestitures, **kwargs):
        pass

    def execute(self, *args, **kwargs): self.update()
    def update(self):
        pass



