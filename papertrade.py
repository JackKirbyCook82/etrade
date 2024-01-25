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

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
API = os.path.join(ROOT, "Library", "api.csv")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from webscraping.webreaders import WebAuthorizer, WebReader
from support.synchronize import MainThread, SideThread
from finance.securities import Securities, SecurityFilter, SecurityParser, SecurityCalculator
from finance.strategies import Strategies, StrategyCalculator
from finance.valuations import Valuations, ValuationCalculator, ValuationFilter
from finance.targets import TargetsCalculator, TargetsWriter, TargetsTable

from market import ETradeSecurityDownloader
from window import TargetsWindow

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


def main(*args, tickers, expires, parameters, **kwargs):
    table = TargetsTable(name="TargetTable", timeout=None)
    api = pd.read_csv(API, header=0, index_col="website").loc["etrade"].to_dict()
    authorizer = ETradeAuthorizer(name="ETradeAuthorizer", apikey=api["key"], apicode=api["code"])
    with ETradeReader(authorizer=authorizer, name="ETradeReader") as reader:
        security_downloader = ETradeSecurityDownloader(name="SecurityDownloader", feed=reader)
        security_filter = SecurityFilter(name="SecurityFilter")
        security_parser = SecurityParser(name="SecurityParser")
        security_calculator = SecurityCalculator(name="SecurityCalculator", calculations=list(Securities))
        strategy_calculator = StrategyCalculator(name="StrategyCalculator", calculations=list(Strategies))
        valuation_calculator = ValuationCalculator(name="ValuationCalculator", calculations=[Valuations.Arbitrage.Minimum])
        valuation_filter = ValuationFilter(name="ValuationFilter")
        target_calculator = TargetsCalculator(name="TargetCalculator")
        target_writer = TargetsWriter(name="TargetWriter", destination=table)
        pipeline = security_downloader + security_filter + security_parser + security_calculator + strategy_calculator + valuation_calculator
        pipeline = pipeline + valuation_filter + target_calculator + target_writer
        window = TargetsWindow(name="TargetsWindow", feed=table)
        writer = SideThread(pipeline, name="TargetWriterThread")
        terminal = MainThread(window, name="TargetWindowThread")
        writer.setup(tickers=tickers, expires=expires, **parameters)
        terminal.setup()
        writer.start()
        window.run()
        writer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"volume": 25, "interest": 25, "size": 5, "liquidity": 0.1, "tenure": None, "apy": 0.5, "funds": 100000, "fees": 5.0, "discount": 0.0}
    main(tickers=None, expires=None, parameters=sysParameters)





