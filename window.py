# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2024
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import numpy as np
import tkinter as tk
from collections import namedtuple as ntuple
from collections import OrderedDict as ODict

from finance.variables import Variables, Contract
from support.windows import MVC, Application, Stencils, Widget, Events

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


class HoldingsModel(MVC.Model):
    def __init__(self, table, *args, **kwargs):
        variables = ["status", "valuation", "strategy", "contract", "securities", "earnings", "cashflow", "apy", "npy", "cost", "tau", "size"]
        super().__init__(*args, **kwargs)
        self.__variables = variables
        self.__table = table

    def status(self, index): return self.table[index, ("status", "")]
    def valuation(self, index): return self.table[index, ("valuation", "")]
    def strategy(self, index): return self.table[index, ("strategy", "")]
    def contract(self, index): return Contract(self.table[index, ("ticker", "")], self.table[index, ("expire", "")])
    def securities(self, index): return {security: self.table[index, (str(security), "")] for security in list(Variables.Securities) if not np.isnan(self.table[index, (str(security), "")])}
    def earnings(self, index): return Earnings(self.apy(index), self.tau(index))
    def cashflow(self, index): return Cashflow(self.npy(index), self.cost(index))

    def apy(self, index): return Scenario(self.table[index, ("apy", Variables.Scenarios.MINIMUM)], self.table[index, ("apy", Variables.Scenarios.MAXIMUM)])
    def npv(self, index): return Scenario(self.table[index, ("npv", Variables.Scenarios.MINIMUM)], self.table[index, ("npv", Variables.Scenarios.MAXIMUM)])
    def cost(self, index): return Scenario(self.table[index, ("cost", Variables.Scenarios.MINIMUM)], self.table[index, ("cost", Variables.Scenarios.MAXIMUM)])
    def tau(self, index): return self.table[index, ("tau", "")]
    def size(self, index): return self.table[index, ("size", "")]

    def contents(self, index):
        contents = [(variable, getattr(self, variable)) for variable in self.variables]
        contents = [(variable, content(index)) for variable, content in contents.items()]
        return ODict(contents)

    @property
    def variables(self): return self.__variables
    @property
    def table(self): return self.__table


class SelectionButton(Stencils.Button):
    def __init_subclass__(cls, *args, activity, status, **kwargs):
        cls.activity = activity
        cls.status = status

    @Events.Handler(Events.Mouse.Click.LEFT)
    def click(self, controller):
        status = type(self).status
        controller.change(status)


class PursueButton(SelectionButton, activity=Variables.Status.PROSPECT, status=Variables.Status.PURSUING): pass
class AbandonButton(SelectionButton, activity=Variables.Status.PROSPECT, status=Variables.Status.ABANDONED): pass
class AcceptButton(SelectionButton, activity=Variables.Status.PENDING, status=Variables.Status.ACCEPTED): pass
class RejectButton(SelectionButton, activity=Variables.Status.PENDING, status=Variables.Status.REJECTED): pass


class SelectionFrame(Stencils.Frame):
    status = Widget(element=Stencils.Variable, font="Arial 10 bold", justify=tk.LEFT, locator=(0, 0), parser=selection_status_parser)
    valuation = Widget(element=Stencils.Variable, font="Arial 10 bold", justify=tk.LEFT, locator=(1, 0), parser=selection_valuation_parser)
    strategy = Widget(element=Stencils.Variable, font="Arial 10 bold", justify=tk.LEFT, locator=(2, 0), parser=selection_strategy_parser)
    contract = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(3, 0), parser=selection_contract_parser)
    security = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(4, 0), parser=selection_security_parser)
    earnings = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(5, 0), parser=selection_size_parser)
    cashflow = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(6, 0), parser=selection_cashflow_parser)
    size = Widget(element=Stencils.Variable, font="Arial 10", justify=tk.LEFT, locator=(7, 0), parser=selection_size_parser)

    pursue = Widget(element=PursueButton, text="Pursue", font="Arial 8", justify=tk.CENTER, locator=(8, 0))
    abandon = Widget(element=AbandonButton, text="Abandon", font="Arial 8", justify=tk.CENTER, locator=(8, 1))
    accepted = Widget(element=AcceptButton, text="Accepted", font="Arial 8", justify=tk.CENTER, locator=(9, 0))
    rejected = Widget(element=RejectButton, text="Rejected", font="Arial 8", justify=tk.CENTER, locator=(9, 1))

    def populate(self, contents):
        for identity, element in iter(self):
            if isinstance(element, Stencils.Variable):
                element.value = contents.get(identity, None)
            elif isinstance(element, Stencils.Button):
                state = bool(contents.get("status", False) == element.activity)
                element.state = state

    def unpopulate(self):
        for identity, element in iter(self):
            if isinstance(element, Stencils.Variable):
                element.value = None
            elif isinstance(element, Stencils.Button):
                element.state = False


class HoldingsScroll(Stencils.Scroll): pass
class HoldingsTable(Stencils.Table):
    valuation = Widget(element=Stencils.Column, text="valuation", width=200, parser=table_valuation_parser)
    strategy = Widget(element=Stencils.Column, text="strategy", width=200, parser=table_strategy_parser)
    contract = Widget(element=Stencils.Column, text="contract", width=200, parser=table_contract_parser)
    security = Widget(element=Stencils.Column, text="security", width=200, parser=table_security_parser)
    earning = Widget(element=Stencils.Column, text="apy", width=100, parser=table_earning_parser)
    cashflow = Widget(element=Stencils.Column, text="tau", width=100, parser=table_cashflow_parser)
    size = Widget(element=Stencils.Column, text="size", width=100, parser=table_size_parser)

    @Events.Handler(Events.Table.SELECT)
    def select(self, controller): controller.select()


class HoldingsView(MVC.View, Stencils.Frame):
    table = Widget(HoldingsTable, locator=(0, 0), vertical=True, horizontal=False)
    vertical = Widget(HoldingsScroll, orientation=tk.VERTICAL, locator=(0, 1))
    selection = Widget(SelectionFrame, locator=(0, 2))

class PaperTradingFrame(Stencils.Notebook):
    acquisitions = Widget(HoldingsView, locator=(0, 0))
    divestitures = Widget(HoldingsView, locator=(0, 0))

class PaperTradingWindow(Stencils.Window):
    holdings = Widget(PaperTradingFrame, locator=(0, 0))


class HoldingsController(MVC.Controller, model=HoldingsModel, view=HoldingsView):
    def execute(self, *args, **kwargs):
        self.update()

    def update(self, *args, **kwargs):
        dataframe = self.model.table.dataframe
        self.view.table.erase()
        self.view.table.draw(dataframe)
        self.deselect()

    def select(self, *args, **kwargs):
        index = self.view.table.selected[0]
        contents = self.model.contents(index)
        self.view.selection.populate(contents)

    def deselect(self, *args, **kwargs):
        self.view.selection.unpopulate()

    def change(self, status, *args, **kwargs):
        assert status in Variables.Status
        index = self.view.table.selected[0]
        self.model.table[index, "status"] = status
        self.update()


class PaperTradeApplication(Application, window=PaperTradingWindow, heading="PaperTrading"):
    def __init__(self, *args, acquisitions, divestitures, **kwargs):
        super().__init__(*args, **kwargs)
        acquisitions = HoldingsModel(acquisitions, *args, **kwargs), self.root.holdings.acquisitions
        divestitures = HoldingsModel(divestitures, *args, **kwargs), self.root.holdings.divestitures
        acquisitions = HoldingsController(*acquisitions, *args, **kwargs)
        divestitures = HoldingsController(*divestitures, *args, **kwargs)
        self["acquisitions"] = acquisitions
        self["divestitures"] = divestitures




