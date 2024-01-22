# -*- coding: utf-8 -*-
"""
Created on Sun Dec 21 2023
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import PySimpleGUI as gui
from abc import ABC
from support.windows import Window, Table, Frame, Button, Text, Column, Justify

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["TargetsWindow"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


class ContentsTable(Table, ABC, justify=Justify.LEFT, height=40, events=True):
    identity = Column("ID", 5, lambda target: f"{str(target.identity):.0f}")
    strategy = Column("strategy", 15, lambda target: str(target.strategy))
    ticker = Column("ticker", 10, lambda target: str(target.product.ticker).upper())
    expire = Column("expire", 10, lambda target: target.product.expire.strftime("%Y-%m-%d"))
    options = Column("options", 20, lambda target: "\n".join(list(map(str, target.options))))
    profit = Column("profit", 20, lambda target: f"{target.valuation.profit * 100:.0f}% / YR @ {target.valuation.tau:.0f} DAYS")
    value = Column("value", 10, lambda target: f"${target.valuation.value:,.0f}")
    cost = Column("cost", 10, lambda target: f"${target.valuation.cost:,.0f}")
    size = Column("size", 10, lambda target: f"{target.size:,.0f} CNT")

class ProspectTable(ContentsTable): pass
class PendingTable(ContentsTable): pass


class DetailFrame(Frame):
    strategy = Text("strategy", "Arial 12 bold", lambda target: str(target.strategy))
    product = Text("product", "Arial 10", lambda target: str(target.product))
    options = Text("options", "Arial 10", lambda target: list(map(str, target.options)))

    @staticmethod
    def layout(*args, strategy, product, options, **kwargs):
        options = [[option] for option in options]
        return [[strategy], [product], *options]

class ValueFrame(Frame):
    size = Text("size", "Arial 12 bold", lambda target: f"{target.size:,.0f} CNT")
    valuations = Text("valuations", "Arial 10", lambda target: list(str(target.valuation).split(", ")))

    @staticmethod
    def layout(*args, valuations, size, **kwargs):
        valuations = [[valuation] for valuation in valuations]
        return [[size], *valuations, [gui.Text("")]]


class AdoptButton(Button): pass
class AbandonButton(Button): pass
class SuccessButton(Button): pass
class FailureButton(Button): pass


class TargetWindow(Window, ABC):
    def __init__(self, *args, name, target, **kwargs):
        identity = gui.Text(f"#{str(target.identity):.0f}")
        detail = DetailFrame(name="Detail", index=target.identity, content=target)
        value = ValueFrame(name="Value", index=target.identity, content=target)
        elements = dict(identity=identity, detail=detail.element, value=value.element)
        super().__init__(*args, name=name, index=target.identity, **elements, **kwargs)
        self.__target = target

    @staticmethod
    def layout(*args, identity, detail, value, positive, negative, **kwargs):
        return [[detail, gui.VerticalSeparator(), value], [identity, gui.Push(), positive, negative]]

    @property
    def target(self): return self.__target

class ProspectWindow(TargetWindow):
    def __init__(self, *args, name, target, **kwargs):
        adopt = AdoptButton(name="Adopt", index=target.identitys)
        abandon = AbandonButton(name="Abandon", index=target.identity)
        elements = dict(positive=adopt.element, negative=abandon.element)
        super().__init__(*args, name=name, target=target, **elements, **kwargs)

class PendingWindow(TargetWindow):
    def __init__(self, *args, name, target, **kwargs):
        success = SuccessButton(name="Success", index=target.identity)
        failure = FailureButton(name="Failure", index=target.identity)
        elements = dict(positive=success.element, negative=failure.element)
        super().__init__(*args, name=name, target=target, **elements, **kwargs)


class TargetsWindow(Window):
    def __init__(self, *args, name, targets=[], **kwargs):
        prospect = ProspectTable(name="Prospect", index=0, content=[])
        pending = PendingTable(name="Pending", index=0, content=[])
        elements = dict(prospect=prospect.element, pending=pending.element)
        super().__init__(*args, name=name, index=0, **elements, **kwargs)
        self.__prospect = prospect
        self.__pending = pending
        self.__targets = targets

    def __call__(self, *args, **kwargs):
        with self:
            while True:
                window, event, values = gui.read_all_windows()
                if event == gui.WINDOW_CLOSED:
                    break

    @staticmethod
    def layout(*args, prospect, pending, **kwargs):
        tables = dict(prospect=prospect, pending=pending)
        tabs = [gui.Tab(name, [[table]]) for name, table in tables.items()]
        return [[gui.TabGroup([tabs])]]

    @property
    def targets(self): return self.__targets
    @property
    def prospect(self): return self.__prospect
    @property
    def pending(self): return self.__pending




