# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2024
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import numpy as np
import tkinter as tk

from support.windows import Application, Stencils
from support.meta import RegistryMeta
from finance.variables import Variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["PaperTradeApplication"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class Product(Stencils.Layout):
    strategy = Stencils.Text("strategy", font="Arial 10 bold", justify=tk.LEFT)
    contract = Stencils.Text("contract", font="Arial 10", justify=tk.LEFT)
    security = Stencils.Text("security", font="Arial 10", justify=tk.LEFT)

class Appraisal(Stencils.Layout):
    valuation = Stencils.Text("valuation", font="Arial 10 bold", justify=tk.LEFT)
    earnings = Stencils.Text("earnings", font="Arial 10", justify=tk.LEFT)
    cashflow = Stencils.Text("cashflow", font="Arial 10", justify=tk.LEFT)
    size = Stencils.Text("size", font="Arial 10", justify=tk.LEFT)


class Pursue(Stencils.Button):
    @staticmethod
    def click(*args, **kwargs):
        application[table]["table"][column, "status"] = Variables.Status.PENDING
        window.destroy()

class Abandon(Stencils.Button):
    @staticmethod
    def click(*args, **kwargs):
        application[table]["table"][column, "status"] = Variables.Status.REJECTED
        window.destroy()

class Success(Stencils.Button):
    @staticmethod
    def click(*args, **kwargs):
        application[table]["table"][column, "status"] = Variables.Status.ACCEPTED
        window.destroy()

class Failure(Stencils.Button):
    @staticmethod
    def click(*args, **kwargs):
        application[table]["table"][column, "status"] = Variables.Status.REJECTED
        window.destroy()


class HoldingsTable(Stencils.Table):
    tag = Stencils.Column("tag", width=5, parser=lambda row: f"{row.tag:.0f}")
    valuation = Stencils.Column("valuation", width=10, parser=lambda row: str(row.valuation))
    strategy = Stencils.Column("strategy", width=10, parser=lambda row: str(row.strategy))
    contract = Stencils.Column("contract", width=10, parser=lambda row: f"{str(row.contract.ticker)}\n{str(row.expire.strftime('%Y%m%d'))}")
    security = Stencils.Column("security", width=20, parser=lambda row: "\n".join([f"{str(key)}={int(value):.02}" for key, value in iter(row) if key in list(Variables.Securities) and not np.isnan(value)]))
    apy = Stencils.Column("apy", width=10, parser=lambda target: f"{target.profit.apy * 100:.0f}% / YR")
    tau = Stencils.Column("tau", width=10, parser=lambda target: f"{target.profit.tau:.0f} DYS")
    npv = Stencils.Column("npv", width=10, parser=lambda target: f"${target.value.profit:,.0f}")
    cost = Stencils.Column("cost", width=10, parser=lambda target: f"${target.value.cost:,.0f}")
    size = Stencils.Column("size", width=10, parser=lambda target: f"{target.size:,.0f} CNT")

    @staticmethod
    def click(*args, **kwargs):
        row = application[table]["table"][row, :].to_dict()
        StatusWindow[status](window, *args, row=row, **kwargs)


class StatusWindow(Stencils.Window, elements={"product": Product, "appraisal": Appraisal}, metaclass=RegistryMeta): pass
class ProspectWindow(StatusWindow, title="Prospect", elements={"pursue": Pursue, "abandon": Abandon}, register=Variables.Status.PROSPECT): pass
class PendingWindow(StatusWindow, title="Pending", elements={"success": Success, "failure": Failure}, register=Variables.Status.PENDING): pass
class AcceptedWindow(StatusWindow, title="Accepted", register=Variables.Status.ACCEPTED): pass
class RejectedWindow(StatusWindow, title="Rejected", register=Variables.Status.REJECTED): pass
class HoldingsWindow(Stencils.Window, elements={"table": HoldingsTable}, metaclass=RegistryMeta): pass
class AcquisitionsWindow(Stencils.Window, title="Acquisitions", register="acquisitions"): pass
class DivestituresWindow(Stencils.Window, title="Divestitures", register="divestitures"): pass
class PaperTradeWindow(Stencils.Window, title="PaperTrading", elements={}): pass


class PaperTradeApplication(Application, window=PaperTradeWindow):
    def __init__(self, *args, acquisitions, divestitures, **kwargs):
        super().__init__(*args, **kwargs)
        self.__acquisitions = acquisitions
        self.__divestitures = divestitures

#    def execute(self, *args, **kwargs):
#        self["acquisitions"] = self.HoldingsWindow["acquisitions"](self.window, *args, table=self.acquisitions, **kwargs)
#        self["divestitures"] = self.HoldingsWindow["divestitures"](self.window, *args, table=self.divestitures, **kwargs)

    @property
    def acquisitions(self): return self.__acquisitions
    @property
    def divestitures(self): return self.__divestitures





