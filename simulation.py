# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade PaperTrading Simulation
@author: Jack Kirby Cook

"""

import os
import sys
import time
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
from finance.securities import SecurityFilter
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter, ValuationLoader, ValuationFile
from finance.holdings import HoldingCalculator, HoldingLoader, HoldingSaver, HoldingFile, HoldingStatus
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


def market(archive, tabulation, *args, parameters, **kwargs):
    valuations_filtering = {Filtering.FLOOR: {"apy": 0.0, "size": 10}, Filtering.NULL: ["apy", "size"]}
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Scenarios.MINIMUM.name).lower())]
    valuations_functions = dict(liquidity=liquidity_function, priority=priority_function)
    valuation_loader = ValuationLoader(name="MarketValuationLoader", source=archive, mode="r")
    valuation_filter = ValuationFilter(name="MarketValuationFilter", scenario=Scenarios.MINIMUM, filtering=valuations_filtering)
    acquisition_writer = AcquisitionWriter(name="MarketAcquisitionWriter", destination=tabulation, valuation=Valuations.ARBITRAGE, **valuations_functions)
    market_pipeline = valuation_loader + valuation_filter + acquisition_writer
    market_thread = SideThread(market_pipeline, name="MarketValuationThread")
    market_thread.setup(**parameters)
    return market_thread


def portfolio(archive, tabulation, *args, breaker, parameters, **kwargs):
    security_filtering = {Filtering.FLOOR: {"volume": 25, "interest": 25, "size": 10}, Filtering.NULL: ["volume", "interest", "size"]}
    valuation_filtering = {Filtering.FLOOR: {"apy": 0.0, "size": 10}, Filtering.NULL: ["apy", "size"]}
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Scenarios.MINIMUM.name).lower())]
    valuations_functions = dict(liquidity=liquidity_function, priority=priority_function)
    holding_loader = HoldingLoader(name="PortfolioHoldingLoader", source=archive, breaker=breaker, mode="r")
    holding_calculator = HoldingCalculator(name="PortfolioHoldingCalculator")
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", filtering=security_filtering)
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator")
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", scenario=Scenarios.MINIMUM, filtering=valuation_filtering)
    acquisition_writer = DivestitureWriter(name="PortfolioDivestitureWriter", destination=tabulation, valuation=Valuations.ARBITRAGE, **valuations_functions)
    portfolio_pipeline = holding_loader + holding_calculator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + acquisition_writer
    portfolio_thread = SideThread(portfolio_pipeline, name="MarketValuationThread")
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def acquisition(tabulation, archive, *args, breaker, parameters, **kwargs):
    acquisition_reader = AcquisitionReader(name="PortfolioAcquisitionReader", source=tabulation, breaker=breaker, wait=5)
    acquisition_saver = HoldingSaver(name="PortfolioAcquisitionSaver", destination=archive, mode="a")
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = SideThread(acquisition_pipeline, name="PortfolioAcquisitionThread")
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def divestiture(tabulation, archive, *args, breaker, parameters, **kwargs):
    divestiture_reader = DivestitureReader(name="PortfolioDivestitureReader", source=tabulation, breaker=breaker, wait=5)
    divestiture_saver = HoldingSaver(name="PortfolioDivestitureSaver", destination=archive, mode="a")
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = SideThread(divestiture_pipeline, name="PortfolioDivestitureThread")
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, **kwargs):
    acquisition_table = AcquisitionTable(name="AcquisitionTable")
    divestiture_table = DivestitureTable(name="DivestitureTable")
    holding_tabulation = Tabulation(name="HoldingTables", tables=[acquisition_table, divestiture_table])
    valuation_file = ValuationFile(name="MarketValuationFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    holding_file = HoldingFile(name="PortfolioHoldingFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    market_archive = Archive(name="MarketArchive", repository=MARKET, load=[valuation_file])
    portfolio_archive = Archive(name="PortfolioArchive", repository=PORTFOLIO, load=[holding_file], save=[holding_file])
    acquisition_breaker = Breaker(name="AcquisitionBreaker")
    divestiture_breaker = Breaker(name="DivestitureBreaker")
    portfolio_breaker = Breaker(name="PortfolioBreaker")

    market_thread = market(market_archive, holding_tabulation, *args, **kwargs)
    portfolio_thread = portfolio(portfolio_archive, holding_tabulation, *args, breaker=portfolio_breaker, **kwargs)
    acquisition_thread = acquisition(holding_tabulation, portfolio_archive, *args, breaker=acquisition_breaker, **kwargs)
    divestiture_thread = divestiture(holding_tabulation, portfolio_archive, *args, breaker=divestiture_breaker, **kwargs)

    portfolio_thread.start()
    time.sleep(5)
    portfolio_breaker.stop()
    portfolio_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"discount": 0.0, "fees": 0.0}
    main(parameters=sysParameters)



