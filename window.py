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
__all__ = ["TargetsWindow"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


class ContentsTable(Table, ABC, justify=Justify.LEFT, height=40, events=True):
    identity = Column("ID", 5, lambda target: f"{target.identity:.0f}")
    strategy = Column("strategy", 15, lambda target: str(target.strategy))
    ticker = Column("ticker", 10, lambda target: str(target.product.ticker).upper())
    expire = Column("expire", 10, lambda target: target.product.expire.strftime("%Y-%m-%d"))
    options = Column("options", 20, lambda target: "\n".join(list(map(str, target.options))))
    profit = Column("profit", 20, lambda target: f"{target.valuation.profit * 100:.0f}% / YR @ {target.valuation.tau:.0f} DAYS")
    value = Column("value", 10, lambda target: f"${target.valuation.value:,.0f}")
    cost = Column("cost", 10, lambda target: f"${target.valuation.cost:,.0f}")
    size = Column("size", 10, lambda target: f"{target.size:,.0f} CNT")

    def __init__(self, *args, contents=[], **kwargs):
        super().__init__(*args, contents=contents, **kwargs)
        self.__targets = contents

    def append(self, target):
        targets = list(set(self.targets) | {target})
        targets = sorted(targets, reverse=True, key=lambda x: x.valuation)
        self.targets = targets
        self.refresh(targets)

    def remove(self, target):
        targets = list(set(self.targets) - {target})
        targets = sorted(targets, reverse=True, key=lambda x: x.valuation)
        self.targets = targets
        self.refresh(targets)

    def extend(self, targets):
        assert isinstance(targets, list)
        targets = list(set(self.targets) | set(targets))
        targets = sorted(targets, reverse=True, key=lambda x: x.valuation)
        self.targets = targets
        self.refresh(targets)

    @property
    def targets(self): return self.__targets
    @targets.setter
    def targets(self, targets): self.__targets = targets


class ProspectTable(ContentsTable):
    def click(self, *args, indexes=[], **kwargs):
        for index in indexes:
            target = self.targets[index]
            window = ProspectWindow(name="Prospect", content=target)
            window.start()

class PendingTable(ContentsTable):
    def click(self, *args, indexes=[], **kwargs):
        for index in indexes:
            target = self.targets[index]
            window = PendingWindow(name="Pending", content=target)
            window.start()


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


class ActionButton(Button):
    def __init__(self, *args, content, **kwargs):
        super().__init__(*args, **kwargs)
        self.__target = content

    @property
    def target(self): return self.__target

class AdoptButton(ActionButton):
    def click(self, *args, **kwargs):
#        windows[str(TargetsWindow)].prospect.remove(self.target)
#        windows[str(TargetsWindow)].pending.append(self.target)
        self.window.stop()

class AbandonButton(ActionButton):
    def click(self, *args, **kwargs):
#        windows[str(TargetsWindow)].prospect.remove(self.target)
        self.window.stop()

class SuccessButton(ActionButton):
    def click(self, *args, **kwargs):
#        windows[str(TargetsWindow)].pending.remove(self.target)
        self.window.stop()

class FailureButton(ActionButton):
    def click(self, *args, **kwargs):
#        windows[str(TargetsWindow)].pending.remove(self.target)
        self.window.stop()


class TargetWindow(Window, ABC):
    def __init__(self, *args, content, **kwargs):
        identity = gui.Text(f"#{content.identity:.0f}")
        detail = DetailFrame(name="Detail", tag=hash(content), content=content, window=self)
        value = ValueFrame(name="Value", tag=hash(content), content=content, window=self)
        elements = dict(identity=identity, detail=detail.element, value=value.element)
        super().__init__(*args, **elements, **kwargs)
        self.__target = content

    @staticmethod
    def layout(*args, identity, detail, value, positive, negative, **kwargs):
        top = [detail, gui.VerticalSeparator(), value]
        bottom = [identity, gui.Push(), positive, negative]
        return [top, bottom]

    @property
    def target(self): return self.__target


class ProspectWindow(TargetWindow):
    def __init__(self, *args, name, content, **kwargs):
        adopt = AdoptButton(name="Adopt", tag=hash(content), content=content, window=self)
        abandon = AbandonButton(name="Abandon", tag=hash(content), content=content, window=self)
        elements = dict(positive=adopt.element, negative=abandon.element)
        super().__init__(*args, name=name, tag=hash(content), content=content, **elements, **kwargs)

class PendingWindow(TargetWindow):
    def __init__(self, *args, name, content, **kwargs):
        success = SuccessButton(name="Success", tag=hash(content), content=content, window=self)
        failure = FailureButton(name="Failure", tag=hash(content), content=content, window=self)
        elements = dict(positive=success.element, negative=failure.element)
        super().__init__(*args, name=name, tag=hash(content), content=content, **elements, **kwargs)


class TargetsWindow(Terminal):
    def __init__(self, *args, name, feed, **kwargs):
        prospect = ProspectTable(name="Prospect", tag=0, contents=[], window=self)
        pending = PendingTable(name="Pending", tag=0, contents=[], window=self)
        elements = dict(prospect=prospect.element, pending=pending.element)
        super().__init__(*args, name=name, tag=0, **elements, **kwargs)
        self.__prospect = prospect
        self.__pending = pending
        self.__feed = feed

#    def execute(self, *args, **kwargs):
#        targets = list(iter(self.feed))
#        self.prospect.update(targets)

    @staticmethod
    def layout(*args, prospect, pending, **kwargs):
        tables = dict(prospect=prospect, pending=pending)
        tabs = [gui.Tab(name, [[table]]) for name, table in tables.items()]
        return [[gui.TabGroup([tabs])]]

    @property
    def prospect(self): return self.__prospect
    @property
    def pending(self): return self.__pending
    @property
    def feed(self): return self.__feed



