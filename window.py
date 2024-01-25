# -*- coding: utf-8 -*-
"""
Created on Sun Dec 21 2023
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import PySimpleGUI as gui
from abc import ABC
from support.windows import Terminal, Window, Table, Frame, Button, Text, Column, Justify

from finance.targets import TargetStatus

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["TargetsWindow"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


class ContentsTable(Table, ABC, justify=Justify.LEFT, height=40, events=True):
    index = Column("ID", 5, lambda target: f"{target.index:.0f}")
    strategy = Column("strategy", 15, lambda target: str(target.strategy))
    ticker = Column("ticker", 10, lambda target: str(target.product.ticker).upper())
    expire = Column("expire", 10, lambda target: target.product.expire.strftime("%Y-%m-%d"))
    options = Column("options", 20, lambda target: "\n".join(list(map(str, target.options))))
    apy = Column("apy", 10, lambda target: f"{target.profitability.apy * 100:.0f}% / YR")
    tau = Column("tau", 10, lambda target: f"{target.profitability.tau:.0f} DAYS")
    value = Column("value", 10, lambda target: f"${target.valuation.profit:,.0f}")
    cost = Column("cost", 10, lambda target: f"${target.valuation.cost:,.0f}")
    size = Column("size", 10, lambda target: f"{target.size:,.0f} CNT")

    def __init__(self, *args, contents=[], **kwargs):
        super().__init__(*args, contents=contents, **kwargs)
        self.__targets = contents

    def refresh(self, contents=[]):
        super().refresh(contents)
        self.targets = contents

    @property
    def targets(self): return self.__targets
    @targets.setter
    def targets(self, targets): self.__targets = targets


class ProspectTable(ContentsTable):
    def click(self, indexes, *args, **kwargs):
        for index in indexes:
            target = self.targets[index]
            window = ProspectWindow(name="Prospect", content=target, parent=self.window)
            window.start()

class PendingTable(ContentsTable):
    def click(self, indexes, *args, **kwargs):
        for index in indexes:
            target = self.targets[index]
            window = PendingWindow(name="Pending", content=target, parent=self.window)
            window.start()

class PurchasedTable(ContentsTable):
    pass


class StrategyFrame(Frame):
    strategy = Text("strategy", "Arial 10 bold", lambda target: str(target.strategy))
    size = Text("size", "Arial 10 bold", lambda target: f"{target.size:,.0f} CNT")

    @staticmethod
    def layout(*args, strategy, size, **kwargs): return [[strategy, gui.Push(), size]]

class SecurityFrame(Frame):
    product = Text("product", "Arial 10", lambda target: str(target.product))
    options = Text("options", "Arial 10", lambda target: list(map(str, target.options)))

    @staticmethod
    def layout(*args, product, options, **kwargs): return [[product], *[[text] for text in options]]

class ProfitabilityFrame(Frame):
    profitability = Text("profitability", "Arial 10", lambda target: list(str(target.profitability).split(", ")))

    @staticmethod
    def layout(*args, profitability, **kwargs): return [[text] for text in profitability]

class ValuationFrame(Frame):
    valuation = Text("valuation", "Arial 10", lambda target: list(str(target.valuation).split(", ")))

    @staticmethod
    def layout(*args, valuation, **kwargs): return [[text] for text in valuation]


class AdoptButton(Button):
    def click(self, *args, **kwargs):
        indx, col = (hash(self.window.target), "status")
        self.window.parent.feed[indx, col] = TargetStatus.PENDING
        self.window.parent.execute(*args, **kwargs)
        self.window.stop()

class AbandonButton(Button):
    def click(self, *args, **kwargs):
        indx, col = (hash(self.window.target), "status")
        self.window.parent.feed[indx, col] = TargetStatus.ABANDONED
        self.window.parent.execute(*args, **kwargs)
        self.window.stop()

class SuccessButton(Button):
    def click(self, *args, **kwargs):
        indx, col = (hash(self.window.target), "status")
        self.window.parent.feed[indx, col] = TargetStatus.PURCHASED
        self.window.parent.execute(*args, **kwargs)
        self.window.stop()

class FailureButton(Button):
    def click(self, *args, **kwargs):
        indx, col = (hash(self.window.target), "status")
        self.window.parent.feed[indx, col] = TargetStatus.ABANDONED
        self.window.parent.execute(*args, **kwargs)
        self.window.stop()


class TargetWindow(Window, ABC):
    def __init__(self, *args, content, **kwargs):
        index = gui.Text(f"#{content.index:.0f}", font="Arial 10")
        strategy = StrategyFrame(name="Strategy", tag=hash(content), content=content, window=self)
        security = SecurityFrame(name="Security", tag=hash(content), content=content, window=self)
        profitability = ProfitabilityFrame(name="Profitability", tag=hash(content), content=content, window=self)
        valuation = ValuationFrame(name="Valuation", tag=hash(content), content=content, window=self)
        elements = dict(index=index, strategy=strategy.element, security=security.element, profitability=profitability.element, valuation=valuation.element)
        super().__init__(*args, **elements, **kwargs)
        self.__target = content

    @staticmethod
    def layout(*args, index, strategy, security, profitability, valuation, positive, negative, **kwargs):
        return [[strategy], [gui.HorizontalSeparator()], [security, profitability, valuation], [gui.HorizontalSeparator()], [index, gui.Push(), positive, negative]]

    @property
    def target(self): return self.__target


class ProspectWindow(TargetWindow):
    def __init__(self, *args, name, content, **kwargs):
        adopt = AdoptButton(name="Adopt", tag=hash(content), window=self)
        abandon = AbandonButton(name="Abandon", tag=hash(content), window=self)
        elements = dict(positive=adopt.element, negative=abandon.element)
        super().__init__(*args, name=name, tag=hash(content), content=content, **elements, **kwargs)

class PendingWindow(TargetWindow):
    def __init__(self, *args, name, content, **kwargs):
        success = SuccessButton(name="Success", tag=hash(content), window=self)
        failure = FailureButton(name="Failure", tag=hash(content), window=self)
        elements = dict(positive=success.element, negative=failure.element)
        super().__init__(*args, name=name, tag=hash(content), content=content, **elements, **kwargs)


class TargetsWindow(Terminal):
    def __init__(self, *args, name, feed, **kwargs):
        prospect = ProspectTable(name="Prospect", tag=0, contents=[], window=self)
        pending = PendingTable(name="Pending", tag=0, contents=[], window=self)
        purchased = PurchasedTable(name="Purchased", tag=0, contents=[], window=self)
        elements = dict(prospect=prospect.element, pending=pending.element, purchased=purchased.element)
        super().__init__(*args, name=name, tag=0, **elements, **kwargs)
        self.__prospect = prospect
        self.__pending = pending
        self.__purchased = purchased
        self.__feed = feed

    def execute(self, *args, **kwargs):
        prospect = [target for target in self.feed.read() if target.status == TargetStatus.PROSPECT]
        pending = [target for target in self.feed.read() if target.status == TargetStatus.PENDING]
        purchased = [target for target in self.feed.read() if target.status == TargetStatus.PURCHASED]
        self.prospect.refresh(contents=prospect)
        self.pending.refresh(contents=pending)
        self.purchased.refresh(contents=purchased)

    @staticmethod
    def layout(*args, prospect, pending, purchased, **kwargs):
        tables = dict(prospect=prospect, pending=pending, purchased=purchased)
        tabs = [gui.Tab(name, [[table]]) for name, table in tables.items()]
        return [[gui.TabGroup([tabs])]]

    @property
    def prospect(self): return self.__prospect
    @property
    def pending(self): return self.__pending
    @property
    def purchased(self): return self.__purchased
    @property
    def feed(self): return self.__feed



