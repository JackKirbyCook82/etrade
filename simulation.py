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
from support.tables import Tabulation, Options
from support.synchronize import SideThread
from support.processes import Filtering
from support.pipelines import Breaker
from finance.variables import Scenarios, Valuations
from finance.securities import SecurityLoader
from finance.valuations import ValuationFilter, ValuationFile
from finance.holdings import HoldingReader, HoldingSaver, HoldingFile
from finance.acquisitions import AcquisitionWriter, AcquisitionTable
from finance.divestitures import DivestitureWriter, DivestitureTable

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


def valuation(archive, tables, *args, parameters, **kwargs):
    valuation_filtering = {Filtering.FLOOR: {"apy": 0.0, "size": 10}, Filtering.NULL: ["apy", "size"]}
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Scenarios.MINIMUM.name).lower())]
    valuation_functions = dict(liquidity=liquidity_function, priority=priority_function)
    valuation_loader = SecurityLoader(name="MarketValuationLoader", source=archive, mode="r")
    valuation_filter = ValuationFilter(name="MarketValuationFilter", scenario=Scenarios.MINIMUM, filtering=valuation_filtering)
    acquisition_writer = AcquisitionWriter(name="MarketAcquisitionWriter", destination=tables, valuation=Valuations.ARBITRAGE, **valuation_functions)
    valuation_pipeline = valuation_loader + valuation_filter + acquisition_writer
    valuation_thread = SideThread(valuation_pipeline, name="MarketValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def acquisition(tables, archive, breaker, *args, parameters, **kwargs):
    acquisition_reader = TargetReader(name="PortfolioAcquisitionReader", source=tables, breaker=breaker, wait=5)
    acquisition_saver = TargetSaver(name="PortfolioAcquisitionSaver", destination=archive, mode="a")
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = SideThread(acquisition_pipeline, name="PortfolioAcquisitionThread")
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def divestiture(tables, archive, breaker, *args, parameters, **kwargs):
    divestiture_reader = TargetReader(name="PortfolioDivestitureReader", source=tables, breaker=breaker, wait=5)
    divestiture_saver = TargetSaver(name="PortfolioDivestitureSaver", destination=archive, mode="a")
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = SideThread(divestiture_pipeline, name="PortfolioDivestitureThread")
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, **kwargs):
    table_options = Options.Dataframe(rows=20, columns=25, width=1000, format=lambda num: f"{num:.02f}")
    acquisition_table = AcquisitionTable(name="AcquisitionTable", options=table_options)
    divestiture_table = DivestitureTable(name="DivestitureTable", options=table_options)
    target_tabulation = Tabulation(name="TargetTables", tables=[acquisition_table, divestiture_table])
    valuation_file = ValuationFile(name="MarketValuationFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    target_file = TargetFile(name="PortfolioTargetFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    market_archive = Archive(name="MarketArchive", repository=MARKET, files=[valuation_file])
    portfolio_archive = Archive(name="PortfolioArchive", repository=PORTFOLIO, files=[target_file])
    acquisition_breaker = Breaker(name="AcquisitionBreaker")
    divestiture_breaker = Breaker(name="DivestitureBreaker")
    valuation_thread = valuation(market_archive, target_tabulation, *args, **kwargs)
    acquisition_thread = acquisition(target_tabulation, portfolio_archive, acquisition_breaker, *args, **kwargs)
    divestiture_thread = divestiture(target_tabulation, portfolio_archive, divestiture_breaker, *args, **kwargs)

#    acquisition_thread.start()
#    divestiture_thread.start()
#    valuation_thread.start()
#    valuation_thread.join()
#    print(acquisition_table)
#    acquisition_table[0:10, "status"] = TargetStatus.PURCHASED
#    acquisition_breaker.stop()
#    divestiture_breaker.stop()
#    acquisition_thread.join()
#    divestiture_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"discount": 0.0, "fees": 0.0}
    main(parameters=sysParameters)



