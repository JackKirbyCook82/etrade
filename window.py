# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2024
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import numpy as np
import tkinter as tk

from finance.variables import Variables
from support.windows import Stencils

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["PaperTradeApplication"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class Product(Stencils.Frame):
    strategy = Stencils.Text("strategy", font="Arial 10 bold", justify=tk.LEFT)
    contract = Stencils.Text("contract", font="Arial 10", justify=tk.LEFT)
    security = Stencils.Text("security", font="Arial 10", justify=tk.LEFT)

class Appraisal(Stencils.Frame):
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
        application[identity] = HoldingsWindow[status](window, application, *args, row=row, **kwargs)


class HoldingsWindow(Stencils.Window):
    def __init__(self, *args, row, **kwargs):



class ProspectWindow(HoldingsWindow, elements={"product": Product, "appraisal": Appraisal, "pursue": Pursue, "abandon": Abandon}): pass
class PendingWindow(HoldingsWindow, elements={"product": Product, "appraisal": Appraisal, "success": Success, "failure": Failure}): pass
class AcceptedWindow(HoldingsWindow, elements={"product": Product, "appraisal": Appraisal}): pass
class RejectedWindow(HoldingsWindow, elements={"product": Product, "appraisal": Appraisal}): pass
class AcquisitionWindow(Stencils.Window, elements={"table": HoldingsTable}): pass
class DivestitureWindow(Stencils.Window, elements={"table": HoldingsTable}): pass
class PaperTradeWindow(Stencils.Window, elements={}): pass


class PaperTradeApplication(Stencils.Application, window=PaperTradeWindow):
    def __init__(self, *args, acquisitions, divestitures, **kwargs):
        super().__init__(*args, **kwargs)
        self["acquisitions"] = AcquisitionWindow(self.window, self, *args, table=acquisitions, **kwargs)
        self["divestitures"] = DivestitureWindow(self.window, self, *args, table=divestitures, **kwargs)








