# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade PaperTrading Simulation
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import numpy as np
import xarray as xr
import pandas as pd
import PySimpleGUI as gui
from time import sleep

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
MARKET = os.path.join(REPOSITORY, "market")
PORTFOLIO = os.path.join(REPOSITORY, "portfolio")
ETRADE = os.path.join(ROOT, "Library", "etrade.txt")
TICKERS = os.path.join(ROOT, "Library", "tickers.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from support.files import Archive, FileTiming, FileTyping
from support.tables import DataframeOptions
from support.synchronize import SideThread
from support.processes import Filtering
from support.pipelines import Breaker
from finance.variables import Scenarios, Valuations
from finance.securities import SecurityLoader, SecuritySaver
from finance.valuations import ValuationFile, ValuationFilter
from finance.acquisitions import AcquisitionTable, AcquisitionWriter, AcquisitionReader
from finance.targets import HoldingFile, TargetStatus

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


warnings.filterwarnings("ignore")
gui.theme("DarkGrey11")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option("display.width", 1000)
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", 25)


def valuation(archive, table, *args, parameters, **kwargs):
    valuation_filtering = {Filtering.FLOOR: {"apy": 0.0, "size": 10}, Filtering.NULL: ["apy", "size"]}
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Scenarios.MINIMUM.name).lower())]
    valuation_functions = dict(liquidity=liquidity_function, priority=priority_function)
    valuation_loader = SecurityLoader(name="MarketValuationLoader", source=archive, mode="r")
    valuation_filter = ValuationFilter(name="MarketValuationFilter", scenario=Scenarios.MINIMUM, filtering=valuation_filtering)
    acquisition_writer = AcquisitionWriter(name="MarketAcquisitionWriter", destination=table, valuation=Valuations.ARBITRAGE, **valuation_functions)
    valuation_pipeline = valuation_loader + valuation_filter + acquisition_writer
    valuation_thread = SideThread(valuation_pipeline, name="MarketValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def acquisition(table, archive, breaker, *args, parameters, **kwargs):
    acquisition_reader = AcquisitionReader(name="PortfolioAcquisitionReader", source=table, breaker=breaker, wait=10)
    acquisition_saver = SecuritySaver(name="PortfolioAcquisitionSaver", destination=archive, mode="a")
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = SideThread(acquisition_pipeline, name="PortfolioAcquisitionThread")
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def main(*args, **kwargs):
    valuation_file = ValuationFile(name="MarketValuationFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    holding_file = HoldingFile(name="PortfolioHoldingFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    market_archive = Archive(name="MarketArchive", repository=MARKET, loading=valuation_file)
    portfolio_archive = Archive(name="PortfolioArchive", repository=PORTFOLIO, saving=holding_file)
    acquisition_breaker = Breaker(name="AcquisitionBreaker")
    table_options = DataframeOptions(rows=20, columns=25, width=1000, format=lambda num: f"{num:.02f}")
    acquisition_table = AcquisitionTable(name="AcquisitionTable", options=table_options)
    valuation_thread = valuation(market_archive, acquisition_table, *args, **kwargs)
    acquisition_thread = acquisition(acquisition_table, portfolio_archive, acquisition_breaker, *args, **kwargs)
    acquisition_thread.start()
    valuation_thread.start()
    valuation_thread.join()
    while True:
        sleep(10)
        break
    acquisition_breaker.stop()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"discount": 0.0, "fees": 0.0}
    main(parameters=sysParameters)



