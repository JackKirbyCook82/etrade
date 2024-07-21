# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2024
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import numpy as np
import tkinter as tk

from support.windows import Application, Window, Frame, Table, Button, Column, Text
from finance.variables import Variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["PaperTradeApplication"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


class Product(Frame):
    strategy = Text("strategy", font="Arial 10 bold", justify=tk.LEFT)
    contract = Text("contract", font="Arial 10", justify=tk.LEFT)
    security = Text("security", font="Arial 10", justify=tk.LEFT)


class Appraisal(Frame):
    valuation = Text("valuation", font="Arial 10 bold", justify=tk.LEFT)
    earnings = Text("earnings", font="Arial 10", justify=tk.LEFT)
    cashflow = Text("cashflow", font="Arial 10", justify=tk.LEFT)
    size = Text("size", font="Arial 10", justify=tk.LEFT)


class Pursue(Button, title="Pursue"): pass
class Abandon(Button, title="Abandon"): pass
class Success(Button, title="Success"): pass
class Failure(Button, title="Failure"): pass


class HoldingsWindow(Window): pass
class AcquisitionWindow(HoldingsWindow, title="Acquisitions"): pass
class DivestitureWindow(HoldingsWindow, title="Divestitures"): pass


class HoldingsTable(Table):
    tag = Column("tag", width=5, parser=lambda row: f"{row.tag:.0f}")
    valuation = Column("valuation", width=10, parser=lambda row: str(row.valuation))
    strategy = Column("strategy", width=10, parser=lambda row: str(row.strategy))
    contract = Column("contract", width=10, parser=lambda row: f"{str(row.contract.ticker)}\n{str(row.expire.strftime('%Y%m%d'))}")
    security = Column("security", width=20, parser=lambda row: "\n".join([f"{str(key)}={int(value):.02}" for key, value in iter(row) if key in list(Variables.Securities) and not np.isnan(value)]))
    apy = Column("apy", width=10, parser=lambda target: f"{target.profit.apy * 100:.0f}% / YR")
    tau = Column("tau", width=10, parser=lambda target: f"{target.profit.tau:.0f} DYS")
    npv = Column("npv", width=10, parser=lambda target: f"${target.value.profit:,.0f}")
    cost = Column("cost", width=10, parser=lambda target: f"${target.value.cost:,.0f}")
    size = Column("size", width=10, parser=lambda target: f"{target.size:,.0f} CNT")


class PaperTradeWindow(Window, title="PaperTrading"):
    pass


class PaperTradeApplication(Application, window=PaperTradeWindow):
    pass

