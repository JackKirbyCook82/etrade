# -*- coding: utf-8 -*-
"""
Created on Sun Dec 21 2023
@name:   ETrade Window Objects
@author: Jack Kirby Cook

"""

import PySimpleGUI as gui
from abc import ABC
from support.windows import Terminal, Window, Table, Frame, Button, Text, Column, Justify

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class ETradeContentsTable(Table, ABC, justify=Justify.LEFT, height=40, events=True):
    index = Column("ID", 5, lambda target: f"{target.index:.0f}")
    strategy = Column("strategy", 15, lambda target: str(target.strategy))
    contract = Column("contract", 10, lambda target: "\n".join(list(map(str, target.contract))))
    options = Column("options", 20, lambda target: "\n".join(list(map(str, target.options))))
    apy = Column("apy", 10, lambda target: f"{target.profit.apy * 100:.0f}% / YR")
    tau = Column("tau", 10, lambda target: f"{target.profit.tau:.0f} DAYS")
    npv = Column("npv", 10, lambda target: f"${target.value.profit:,.0f}")
    cost = Column("cost", 10, lambda target: f"${target.value.cost:,.0f}")
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


class ETradeProspectTable(ETradeContentsTable):
    def click(self, indexes, *args, **kwargs):
        for index in indexes:
            target = self.targets[index]
            window = ETradeProspectWindow(name="Prospect", content=target, parent=self.window)
            window.start()


class ETradeAcquiringTable(ETradeContentsTable):
    def click(self, indexes, *args, **kwargs):
        for index in indexes:
            target = self.targets[index]
            window = ETradePendingWindow(name="Pending", content=target, parent=self.window)
            window.start()


class ETradePortfolioTable(ETradeContentsTable):
    def click(self, indexes, *args, **kwargs):
        for index in indexes:
            target = self.targets[index]
            window = ETradePurchasedWindow(name="Purchased", content=target, parent=self.window)
            window.start()


class ETradeStrategyFrame(Frame):
    strategy = Text("strategy", "Arial 10 bold", lambda target: str(target.strategy))
    size = Text("size", "Arial 10 bold", lambda target: f"{target.size:,.0f} CNT")

    @staticmethod
    def layout(*args, strategy, size, **kwargs): return [[strategy, gui.Push(), size]]

class ETradeSecurityFrame(Frame):
    contract = Text("product", "Arial 10", lambda target: str(target.contract))
    options = Text("options", "Arial 10", lambda target: list(map(str, target.options)))

    @staticmethod
    def layout(*args, product, options, **kwargs): return [[product], *[[text] for text in options]]

class ETradeProfitabilityFrame(Frame):
    profitability = Text("profitability", "Arial 10", lambda target: list(str(target.profitability).split(", ")))

    @staticmethod
    def layout(*args, profitability, **kwargs): return [[text] for text in profitability]

class ETradeValuationFrame(Frame):
    valuation = Text("valuation", "Arial 10", lambda target: list(str(target.valuation).split(", ")))

    @staticmethod
    def layout(*args, valuation, **kwargs): return [[text] for text in valuation]


class ETradeAdoptButton(Button):
    def click(self, *args, **kwargs): pass
#        indx, col = (hash(self.window.target), "status")
#        self.window.parent.feed[indx, col] = Status.PENDING
#        self.window.parent.execute(*args, **kwargs)
#        self.window.stop()

class ETradeAbandonButton(Button):
    def click(self, *args, **kwargs): pass
#        indx, col = (hash(self.window.target), "status")
#        self.window.parent.feed[indx, col] = Status.ABANDONED
#        self.window.parent.execute(*args, **kwargs)
#        self.window.stop()

class ETradeSuccessButton(Button):
    def click(self, *args, **kwargs): pass
#        indx, col = (hash(self.window.target), "status")
#        self.window.parent.feed[indx, col] = Status.PURCHASED
#        self.window.parent.execute(*args, **kwargs)
#        self.window.stop()

class ETradeFailureButton(Button):
    def click(self, *args, **kwargs): pass
#        indx, col = (hash(self.window.target), "status")
#        self.window.parent.feed[indx, col] = Status.ABANDONED
#        self.window.parent.execute(*args, **kwargs)
#        self.window.stop()


class ETradeTargetWindow(Window, ABC):
    def __init__(self, *args, content, **kwargs):
        index = gui.Text(f"#{content.index:.0f}", font="Arial 10")
        strategy = ETradeStrategyFrame(name="Strategy", tag=hash(content), content=content, window=self)
        security = ETradeSecurityFrame(name="Security", tag=hash(content), content=content, window=self)
        profitability = ETradeProfitabilityFrame(name="Profitability", tag=hash(content), content=content, window=self)
        valuation = ETradeValuationFrame(name="Valuation", tag=hash(content), content=content, window=self)
        elements = dict(index=index, strategy=strategy.element, security=security.element, profitability=profitability.element, valuation=valuation.element)
        super().__init__(*args, **elements, **kwargs)
        self.__target = content

    @staticmethod
    def layout(*args, index, strategy, security, profitability, valuation, positive, negative, **kwargs):
        return [[strategy], [gui.HorizontalSeparator()], [security, profitability, valuation], [gui.HorizontalSeparator()], [index, gui.Push(), positive, negative]]

    @property
    def target(self): return self.__target


class ETradeProspectWindow(ETradeTargetWindow):
    def __init__(self, *args, name, content, **kwargs):
        adopt = ETradeAdoptButton(name="Adopt", tag=hash(content), window=self)
        abandon = ETradeAbandonButton(name="Abandon", tag=hash(content), window=self)
        elements = dict(positive=adopt.element, negative=abandon.element)
        super().__init__(*args, name=name, tag=hash(content), content=content, **elements, **kwargs)

class ETradePendingWindow(ETradeTargetWindow):
    def __init__(self, *args, name, content, **kwargs):
        success = ETradeSuccessButton(name="Success", tag=hash(content), window=self)
        failure = ETradeFailureButton(name="Failure", tag=hash(content), window=self)
        elements = dict(positive=success.element, negative=failure.element)
        super().__init__(*args, name=name, tag=hash(content), content=content, **elements, **kwargs)

class ETradePurchasedWindow(ETradeTargetWindow):
    def __init__(self, *args, name, content, **kwargs):
        elements = dict(positive=gui.Text(""), negative=gui.Text(""))
        super().__init__(*args, name=name, tag=hash(content), content=content, **elements, **kwargs)


class ETradeTargetsWindow(Terminal):
    def __init__(self, *args, name, feed, **kwargs):
        prospect = ETradeProspectTable(name="Prospect", tag=0, contents=[], window=self)
        pending = ETradePendingTable(name="Pending", tag=0, contents=[], window=self)
        purchased = ETradePurchasedTable(name="Purchased", tag=0, contents=[], window=self)
        elements = dict(prospect=prospect.element, pending=pending.element, purchased=purchased.element)
        super().__init__(*args, name=name, tag=0, **elements, **kwargs)
        self.__prospect = prospect
        self.__pending = pending
        self.__purchased = purchased
        self.__feed = feed

    def execute(self, *args, **kwargs):
        prospect = [target for target in iter(self.feed) if target.status == Status.PROSPECT]
        pending = [target for target in iter(self.feed) if target.status == Status.PENDING]
        purchased = [target for target in iter(self.feed) if target.status == Status.PURCHASED]
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



