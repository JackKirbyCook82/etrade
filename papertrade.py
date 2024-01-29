# -*- coding: utf-8 -*-
"""
Created on Thurs Dec 21 2023
@name:   ETrade Paper Trading Application
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
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
API = os.path.join(ROOT, "Library", "api.csv")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from webscraping.webreaders import WebAuthorizer, WebReader
from support.synchronize import MainThread, SideThread
from support.pipelines import CycleBreaker
from finance.securities import SecurityFilter, SecurityCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter
from finance.targets import TargetsCalculator, TargetsSaver, TargetsFile, TargetsTable
from finance.variables import DateRange

from market import ETradeSecurityDownloader
from window import ETradeTargetsWindow

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
class ETradeBreaker(CycleBreaker): pass


def main(*args, tickers, expires, parameters, **kwargs):
    api = pd.read_csv(API, header=0, index_col="website").loc["etrade"].to_dict()
    file = TargetsFile(name="TargetFile", repository=REPOSITORY, timeout=None)
    table = TargetsTable(name="TargetTable", timeout=None)
    authorizer = ETradeAuthorizer(name="ETradeAuthorizer", apikey=api["key"], apicode=api["code"])
    breaker = ETradeBreaker(name="ETradeBreaker")
    with ETradeReader(authorizer=authorizer, name="ETradeReader") as reader:
        security_downloader = ETradeSecurityDownloader(name="SecurityDownloader", feed=reader, breaker=breaker)
        security_filter = SecurityFilter(name="SecurityFilter")
        security_calculator = SecurityCalculator(name="SecurityCalculator")
        strategy_calculator = StrategyCalculator(name="StrategyCalculator")
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        valuation_filter = ValuationFilter(name="ValuationFilter")
        target_calculator = TargetsCalculator(name="TargetCalculator")
        target_writer = TargetsSaver(name="TargetWriter", table=table, file=file)
        feed_pipeline = security_downloader + security_filter + security_calculator + strategy_calculator
        feed_pipeline = feed_pipeline + valuation_calculator + valuation_filter + target_calculator + target_writer
        target_window = ETradeTargetsWindow(name="TargetsWindow", feed=table)
        feed_writer = SideThread(feed_pipeline, name="TargetFeedThread")
        window_thread = MainThread(target_window, name="TargetWindowThread")
        feed_writer.setup(tickers=tickers, expires=expires, **parameters)
        window_thread.setup()
        feed_writer.start()
        window_thread.run()
        feed_writer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysSecurity, sysValuation = {"volume": 100, "interest": 100, "size": 10}, {"apy": 0.25, "discount": 0.0}
    sysMarket, sysPortfolio = {"liquidity": 0.1, "tenure": None, "fees": 5.0}, {"funds": None}
    main(tickers=logging, expires=sysExpires, parameters=sysSecurity | sysValuation | sysMarket | sysPortfolio)





