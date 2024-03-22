# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Valuation
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
MARKET = os.path.join(REPOSITORY, "market")
ETRADE = os.path.join(ROOT, "Library", "etrade.txt")
TICKERS = os.path.join(ROOT, "Library", "tickers.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from support.synchronize import SideThread
from support.processes import Filtering
from finance.variables import Scenarios, Valuations
from finance.securities import SecurityFile, SecurityFilter, SecurityLoader
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter, ValuationSaver
from finance.variables import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"
__date__ = Datetime.today().date()


warnings.filterwarnings("ignore")
gui.theme("DarkGrey11")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option("display.width", 1000)
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", 25)


def main(*args, tickers, expires, parameters, **kwargs):
    market_file = SecurityFile(name="MarketFile", repository=MARKET, timeout=None)
    security_scheduler = SecurityScheduler(name="MarketSecurityScheduler")  # SINGLE READING, NO CYCLING
    security_loader = SecurityLoader(name="MarketSecurityLoader", file=market_file)
    security_filter = SecurityFilter(name="SecurityFilter", filtering={Filtering.FLOOR: ["volume", "interest", "size"]})
    strategy_calculator = StrategyCalculator(name="StrategyCalculator")
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", scenario=Scenarios.MINIMUM, filtering={Filtering.FLOOR: ["apy", "size"]})
    valuation_saver = ValuationSaver(name="ValuationSaver", file=market_file)
    valuation_pipeline = security_scheduler + security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="ValuationThread")
    valuation_thread.setup(tickers=tickers, expires=expires, **parameters)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
    sysExpires = DateRange([(__date__ + Timedelta(days=1)).date(), (__date__ + Timedelta(weeks=52)).date()])
    sysParameters = {"volume": 25, "interest": 25, "size": 10, "apy": 0.01, "discount": 0.0, "fees": 0.0}
    main(tickers=sysTickers, expires=None, parameters=sysParameters)



