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

from support.files import FileTiming, FileTyping
from support.synchronize import SideThread
from support.processes import Filtering
from finance.variables import Scenarios, Valuations
from finance.securities import SecurityArchive, SecurityFile, HoldingFile, SecurityLoader, SecuritySaver
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationArchive, ValuationFile, ValuationCalculator, ValuationFilter, ValuationLoader
from finance.acquisitions import AcquisitionTable, AcquisitionWriter, AcquisitionReader
from finance.divestitures import DivestitureTable, DivestitureWriter, DivestitureReader

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


def valuation(file, table, *args, parameters, **kwargs):
    valuation_filtering = {Filtering.FLOOR: {"apy": 0.0, "size": 10}, Filtering.NULL: ["apy", "size"]}
    acquisition_priority = lambda cols: cols[(Scenarios.MINIMUM, "apy")].astype(np.float32)
    valuation_loader = ValuationLoader(name="MarketSecurityLoader", source=file)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", scenario=Scenarios.MINIMUM, filtering=valuation_filtering)
    acquisition_writer = AcquisitionWriter(name="MarketAcquisitionWriter", destination=table, valuation=Valuations.ARBITRAGE, priority=acquisition_priority)
    valuation_pipeline = valuation_loader + valuation_filter + acquisition_writer
    valuation_thread = SideThread(valuation_pipeline, name="MarketValuationThread")
    valuation_thread.setup(**parameters)
    return valuation_thread


def holding(source, destination, *args, parameters, **kwargs):
    holding_loader = SecurityLoader(name="PortfolioHoldingLoader", source=source)
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator")
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", scenario=Scenarios.MINIMUM, filtering={Filtering.FLOOR: ["apy", "size"]})
    market_simulator = MarketSimulatior(name="PortfolioSecuritySimulator")
    acquisition_writer = DivestitureWriter(name="PortfolioDivestitureWriter", destination=destination, valuation=Valuations.ARBITRAGE, priority=priority)
    security_pipeline = holding_loader + strategy_calculator + valuation_calculator + valuation_filter + market_simulator + acquisition_writer
    holding_thread = SideThread(security_pipeline, name="MarketValuationThread")
    holding_thread.setup(**parameters)
    return holding_thread


def acquisition(source, destination, *args, parameters, breaker, wait, **kwargs):
    acquisition_reader = AcquisitionReader(name="PortfolioAcquisitionReader", source=source, breaker=breaker, wait=wait)
    acquisition_saver = SecuritySaver(name="PortfolioAcquisitionSaver", destination=destination)
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = SideThread(acquisition_pipeline, name="PortfolioAcquisitionThread")
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def divestiture(source, destination, *args, parameters, breaker, wait, **kwargs):
    divestiture_reader = DivestitureReader(name="PortfolioDivestitureReader", source=source, breaker=breaker, wait=wait)
    divestiture_saver = SecuritySaver(name="PortfolioDivestitureSaver", destination=destination)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = SideThread(divestiture_pipeline, name="PortfolioDivestitureThread")
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, **kwargs):
    security_file = SecurityFile(typing=FileTyping.CSV, timing=FileTiming.EAGER)
    holding_file = HoldingFile(typing=FileTyping.CSV, timing=FileTiming.EAGER)
    valuation_file = ValuationFile(typing=FileTyping.CSV, timing=FileTiming.EAGER)
    market_archive = ValuationArchive(repository=MARKET, files=[])
    portfolio_archive = SecurityArchive(repository=PORTFOLIO, files=[])
    acquisition_table = AcquisitionTable()
    divestiture_table = DivestitureTable()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"liquidity": 1.0, "discount": 0.0, "fees": 0.0}
    main(parameters=sysParameters)



