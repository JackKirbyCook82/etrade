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
from support.tables import Tabulation
from support.synchronize import SideThread
from support.processes import Filtering
from support.pipelines import Breaker
from finance.variables import Scenarios, Valuations
from finance.securities import SecurityLoader
from finance.valuations import ValuationFilter, ValuationFile
from finance.holdings import HoldingLoader, HoldingSaver, HoldingFile
from finance.acquisitions import AcquisitionReader, AcquisitionWriter, AcquisitionTable
from finance.divestitures import DivestitureReader, DivestitureWriter, DivestitureTable

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


def valuation(archive, tabulation, *args, parameters, **kwargs):
    valuations_filtering = {Filtering.FLOOR: {"apy": 0.0, "size": 10}, Filtering.NULL: ["apy", "size"]}
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Scenarios.MINIMUM.name).lower())]
    valuations_functions = dict(liquidity=liquidity_function, priority=priority_function)
    valuations_loader = SecurityLoader(name="MarketValuationLoader", source=archive, mode="r")
    valuations_filter = ValuationFilter(name="MarketValuationFilter", scenario=Scenarios.MINIMUM, filtering=valuations_filtering)
    acquisitions_writer = AcquisitionWriter(name="MarketAcquisitionWriter", destination=tabulation, valuation=Valuations.ARBITRAGE, **valuations_functions)
    valuations_pipeline = valuations_loader + valuations_filter + acquisitions_writer
    valuations_thread = SideThread(valuations_pipeline, name="MarketValuationThread")
    valuations_thread.setup(**parameters)
    return valuations_thread


def acquisition(tabulation, archive, breaker, *args, parameters, **kwargs):
    acquisitions_reader = AcquisitionReader(name="PortfolioAcquisitionReader", source=tabulation, breaker=breaker, wait=5)
    acquisitions_saver = HoldingSaver(name="PortfolioAcquisitionSaver", destination=archive, mode="a")
    acquisitions_pipeline = acquisitions_reader + acquisitions_saver
    acquisitions_thread = SideThread(acquisitions_pipeline, name="PortfolioAcquisitionThread")
    acquisitions_thread.setup(**parameters)
    return acquisitions_thread


def divestiture(tabulation, archive, breaker, *args, parameters, **kwargs):
    divestitures_reader = DivestitureReader(name="PortfolioDivestitureReader", source=tabulation, breaker=breaker, wait=5)
    divestitures_saver = HoldingSaver(name="PortfolioDivestitureSaver", destination=archive, mode="a")
    divestitures_pipeline = divestitures_reader + divestitures_saver
    divestitures_thread = SideThread(divestitures_pipeline, name="PortfolioDivestitureThread")
    divestitures_thread.setup(**parameters)
    return divestitures_thread


def main(*args, **kwargs):
    acquisitions_table = AcquisitionTable(name="AcquisitionTable")
    divestitures_table = DivestitureTable(name="DivestitureTable")
    holdings_tabulation = Tabulation(name="HoldingsTables", tables=[acquisitions_table, divestitures_table])
    valuations_file = ValuationFile(name="MarketValuationFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    holdings_file = HoldingFile(name="PortfolioTargetFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    market_archive = Archive(name="MarketArchive", repository=MARKET, load=[valuations_file])
    portfolio_archive = Archive(name="PortfolioArchive", repository=PORTFOLIO, save=[holdings_file])
    acquisition_breaker = Breaker(name="AcquisitionBreaker")
    divestiture_breaker = Breaker(name="DivestitureBreaker")
    valuation_thread = valuation(market_archive, holdings_tabulation, *args, **kwargs)
    acquisition_thread = acquisition(holdings_tabulation, portfolio_archive, acquisition_breaker, *args, **kwargs)
    divestiture_thread = divestiture(holdings_tabulation, portfolio_archive, divestiture_breaker, *args, **kwargs)

    valuation_thread.start()
    valuation_thread.join()
    print(holdings_tabulation["acquisitions"])

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



