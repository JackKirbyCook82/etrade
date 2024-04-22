# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Valuation
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
REPOSITORY = os.path.join(ROOT, "Library", "repository")
MARKET = os.path.join(REPOSITORY, "market")
TICKERS = os.path.join(ROOT, "AlgoTrading", "tickers.txt")
ETRADE = os.path.join(ROOT, "AlgoTrading", "etrade.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.valuations import ValuationCalculator, ValuationFilter, ValuationFile
from finance.variables import Scenarios, Valuations, Contract
from finance.securities import SecurityFilter, SecurityFile
from finance.strategies import StrategyCalculator
from support.files import FileTiming, FileTyping
from support.synchronize import SideThread
from support.processes import Criterion

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


def valuation(source, destination, *args, parameters, **kwargs):
    security_query = lambda folder: Contract.fromstring(folder, delimiter="_")
    valuation_folder = lambda contents: str(contents["contract"].tostring(delimiter="_"))
    security_criterion = {Criterion.FLOOR: {"volume": 25, "interest": 25, "size": 10}, Criterion.NULL: ["volume", "interest", "size"]}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    security_loader = Loader(name="SecurityLoader", source=source, query=security_query, mode="r")
    security_filter = SecurityFilter(name="SecurityFilter", criterion=security_criterion)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator")
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", scenario=Scenarios.MINIMUM, criterion=valuation_criterion)
    valuation_saver = Saver(name="ValuationSaver", destination=destination, folder=valuation_folder, mode="a")
    valuation_pipeline = security_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + valuation_saver
    valuation_thread = SideThread(valuation_pipeline, name="ValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def main(*args, **kwargs):
    security_file = SecurityFile(name="SecurityFile", repository=MARKET, typing=FileTyping.CSV, timing=FileTiming.EAGER, duplicates=False)
    valuation_file = ValuationFile(name="ValuationFile", repository=MARKET, typing=FileTyping.CSV, timing=FileTiming.EAGER, duplicates=False)
    valuation_thread = valuation(security_file, valuation_file, *args, **kwargs)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"discount": 0.0, "fees": 0.0}
    main(parameters=sysParameters)



