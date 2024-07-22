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
    def click(application, window, *args, **kwargs):
        Variables.Status.PENDING
        window.destroy()

class Abandon(Stencils.Button):
    @staticmethod
    def click(application, window, *args, **kwargs):
        Variables.Status.REJECTED
        window.destroy()

class Success(Stencils.Button):
    @staticmethod
    def click(application, window, *args, **kwargs):
        Variables.Status.ACCEPTED
        window.destroy()

class Failure(Stencils.Button):
    @staticmethod
    def click(application, window, *args, **kwargs):
        Variables.Status.REJECTED
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


class ProspectWindow(Stencils.Window, elements={"product": Product, "appraisal": Appraisal, "pursue": Pursue, "abandon": Abandon}): pass
class PendingWindow(Stencils.Window, elements={"product": Product, "appraisal": Appraisal, "success": Success, "failure": Failure}): pass
class AcceptedWindow(Stencils.Window, elements={"product": Product, "appraisal": Appraisal}): pass
class RejectedWindow(Stencils.Window, elements={"product": Product, "appraisal": Appraisal}): pass
class AcquisitionWindow(Stencils.Window, elements={"acquisitions": HoldingsTable}): pass
class DivestitureWindow(Stencils.Window, elements={"divestitures": HoldingsTable}): pass
class PaperTradeWindow(Stencils.Window, elements={}): pass


windows = {"acquisitions": AcquisitionWindow, "divestitures": DivestitureWindow, "prospect": ProspectWindow, "pending": PendingWindow, "accepted": AcceptedWindow, "rejected": RejectedWindow}
class PaperTradeApplication(Stencils.Application, window=PaperTradeWindow, windows=windows):
    def __init__(self, *args, acquisitions, divestitures, **kwargs):
        super().__init__(*args, **kwargs)





