# -*- coding: utf-8 -*-
"""
Created on Thurs Dec 21 2023
@name:   ETrade Paper Trading
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import xarray as xr
import pandas as pd
import PySimpleGUI as gui
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
if ROOT not in sys.path:
    sys.path.append(ROOT)
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
API = os.path.join(ROOT, "Library", "api.csv")

from finance.securities import DateRange, Securities, SecurityLoader, SecurityFilter, SecurityCalculator
from finance.strategies import Strategies, StrategyCalculator
from finance.valuations import Valuations, ValuationFilter, ValuationCalculator
from finance.targets import TargetCalculator
from webscraping.webreaders import WebAuthorizer, WebReader
from support.synchronize import Routine

from market import ETradeSecurityDownloader

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 20)
pd.set_option("display.max_columns", 25)
gui.theme("DarkGrey11")


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"


class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeReader(WebReader, delay=10): pass


def terminal():
    frame = lambda title: gui.Frame(title, [[]])
    layout = [[frame("Prospects"), frame("Pending"), frame("Portfolio")], [gui.Exit()]]
    window = gui.Window("Title", layout)
    while True:
        event, values = window.read()
        if event in (gui.WINDOW_CLOSED, "Exit"):
            break


def main(*args, tickers, expires, parameters, **kwargs):
    api = pd.read_csv(API, header=0, index_col="website").loc["etrade"].to_dict()
    authorizer = ETradeAuthorizer(name="ETradeAuthorizer", apikey=api["key"], apicode=api["code"])
    reader = ETradeReader(authorizer=authorizer, name="ETradeReader")
    security_downloader = ETradeSecurityDownloader(name="SecurityDownloader", source=reader)
    security_filter = SecurityFilter(name="SecurityFilter")
    downloader_pipeline = security_downloader + security_filter

    security_loader = SecurityLoader(name="SecurityLoader", repository=REPOSITORY)
    security_filter = SecurityFilter(name="SecurityFilter")
    loader_pipeline = security_loader + security_filter

    security_calculator = SecurityCalculator(name="SecurityCalculator", calculations=list(Securities))
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", calculations=list(Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", calculations=[Valuations.Arbitrage.Minimum])
    valuation_filter = ValuationFilter(name="ValuationFilter")
    target_calculator = TargetCalculator(name="TargetCalculator")
    calculator_pipeline = security_calculator + strategy_calculator + valuation_calculator + valuation_filter + target_calculator

    pipeline = loader_pipeline + calculator_pipeline
    routine = Routine(pipeline, name="ETradeRoutine")
    routine.setup(tickers=tickers, expires=expires, **parameters)
    routine.start()
    routine.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysParameters = {"volume": 100, "interest": 100, "size": 25, "apy": 0.25, "funds": None, "fees": 5.0, "discount": 0.0}
    main(tickers=sysTickers, expires=sysExpires, parameters=sysParameters)





