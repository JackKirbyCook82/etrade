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

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
MARKET = os.path.join(REPOSITORY, "market")
ETRADE = os.path.join(ROOT, "Library", "etrade.txt")
TICKERS = os.path.join(ROOT, "Library", "tickers.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from support.files import Archive, FileTiming, FileTyping
from support.synchronize import SideThread
from support.processes import Filtering
from finance.variables import Scenarios, Valuations
from finance.securities import SecurityFilter, SecurityLoader, SecuritySaver, StockFile, OptionFile
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFile

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


gui.theme("DarkGrey11")
warnings.filterwarnings("ignore")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option("display.width", 1000)
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", 25)


def valuation(archive, *args, parameters, **kwargs):
    security_filtering = {Filtering.FLOOR: {"volume": 25, "interest": 25, "size": 10}, Filtering.NULL: ["volume", "interest", "size"]}
    valuation_filtering = {Filtering.FLOOR: {"apy": 0.0, "size": 10}, Filtering.NULL: ["apy", "size"]}
    security_loader = SecurityLoader(name="MarketSecurityLoader", source=archive, mode="r")
    security_filter = SecurityFilter(name="MarketSecurityFilter", filtering=security_filtering)
    strategy_calculator = StrategyCalculator(name="MarketStrategyCalculator")
    valuation_calculator = ValuationCalculator(name="MarketValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", scenario=Scenarios.MINIMUM, filtering=valuation_filtering)
    valuation_saver = SecuritySaver(name="MarketValuationSaver", destination=archive, mode="a")
    valuation_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="MarketValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def main(*args, **kwargs):
    stock_file = StockFile(name="MarketStockFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    option_file = OptionFile(name="MarketOptionFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    valuation_file = ValuationFile(name="MarketValuationFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    market_archive = Archive(name="MarketArchive", repository=MARKET, load=[stock_file, option_file], save=[valuation_file])
    valuation_thread = valuation(market_archive, *args, **kwargs)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"discount": 0.0, "fees": 0.0}
    main(parameters=sysParameters)



