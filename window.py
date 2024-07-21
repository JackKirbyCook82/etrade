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


class Pursue(Stencils.Button, title="Pursue", action=): pass
class Abandon(Stencils.Button, title="Abandon", action=): pass
class Success(Stencils.Button, title="Success", action=): pass
class Failure(Stencils.Button, title="Failure", action=): pass


class Prospect(Stencils.Window, title="Prospect", layout=[[Product, Appraisal], [Pursue, Abandon]]): pass
class Pending(Stencils.Window, title="Pending", layout=[[Product, Appraisal], [Success, Failure]]): pass
class Accepted(Stencils.Window, title="Accepted", layout=[[Product, Appraisal], []]): pass
class Rejected(Stencils.Window, title="Rejected", layout=[[Product, Appraisal], []]): pass


class HoldingsTable(Stencils.Table, action=):
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


class HoldingsWindow(Stencils.Window, layout=[[HoldingsTable]]): pass
class AcquisitionWindow(HoldingsWindow, title="Acquisitions"): pass
class DivestitureWindow(HoldingsWindow, title="Divestitures"): pass


class PaperTradeWindow(Stencils.Window, title="PaperTrading"):
    pass


class PaperTradeApplication(Stencils.Application, window=PaperTradeWindow):
    pass

