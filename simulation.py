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

from support.synchronize import SideThread
from support.pipelines import Breaker
from support.processes import Filtering
from finance.variables import Scenarios, Valuations
from finance.securities import SecurityLoader, SecuritySaver, SecurityFile
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter, ValuationLoader, ValuationFile
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


def security(file, table, *args, parameters, priority, **kwargs):
    market_loader = ValuationLoader(name="MarketSecurityLoader", file=file)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", scenario=Scenarios.MINIMUM, filtering={Filtering.FLOOR: ["apy", "size"]})
    acquisition_writer = AcquisitionWriter(name="MarketAcquisitionWriter", table=table, valuation=Valuations.ARBITRAGE, priority=priority)
    security_pipeline = market_loader + valuation_filter + acquisition_writer
    security_thread = SideThread(security_pipeline, name="MarketValuationThread")
    security_thread.setup(**parameters)
    return security_thread


def holding(file, table, *args, parameters, priority, **kwargs):
    holding_loader = SecurityLoader(name="PortfolioHoldingLoader", file=file)
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator")
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", scenario=Scenarios.MINIMUM, filtering={Filtering.FLOOR: ["apy", "size"]})
    market_simulator = MarketSimulatior(name="PortfolioSecuritySimulator")
    acquisition_writer = DivestitureWriter(name="PortfolioDivestitureWriter", table=table, valuation=Valuations.ARBITRAGE, priority=priority)
    security_pipeline = holding_loader + strategy_calculator + valuation_calculator + valuation_filter + market_simulator + acquisition_writer
    holding_thread = SideThread(security_pipeline, name="MarketValuationThread")
    holding_thread.setup(**parameters)
    return holding_thread


def acquisition(table, file, *args, parameters, breaker, **kwargs):
    acquisition_reader = AcquisitionReader(name="PortfolioAcquisitionReader", table=table, breaker=breaker)
    acquisition_saver = SecuritySaver(name="PortfolioAcquisitionSaver", file=file)
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = SideThread(acquisition_pipeline, name="PortfolioAcquisitionThread")
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def divestiture(table, file, *args, parameters, breaker, **kwargs):
    divestiture_reader = DivestitureReader(name="PortfolioDivestitureReader", table=table, breaker=breaker)
    divestiture_saver = SecuritySaver(name="PortfolioDivestitureSaver", file=file)
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = SideThread(divestiture_pipeline, name="PortfolioDivestitureThread")
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, **kwargs):
    security_priority = lambda cols: cols[(Scenarios.MINIMUM, "apy")].astype(np.float32)
    holding_priority = lambda cols: cols[(Scenarios.MINIMUM, "apy")].astype(np.float32)
    acquisition_breaker = Breaker(name="AcquisitionBreaker")
    divestiture_breaker = Breaker(name="DivestitureBreaker")
    market_file = ValuationFile(name="MarketFile", repository=MARKET, timeout=None)
    portfolio_file = SecurityFile(name="PortfolioFile", repository=PORTFOLIO, timeout=None)
    acquisition_table = AcquisitionTable(name="AcquisitionTable", timeout=None)
    divestiture_table = DivestitureTable(name="DivestitureTable", timeout=None)
    security_thread = security(market_file, acquisition_table, *args, priority=security_priority, **kwargs)
    holding_thread = holding(portfolio_file, divestiture_table, *args, priority=holding_priority, **kwargs)
    acquisition_thread = acquisition(acquisition_table, portfolio_file, *args, breaker=acquisition_breaker, **kwargs)
    divestiture_thread = divestiture(divestiture_table, portfolio_file, *args, breaker=divestiture_breaker, **kwargs)
    security_thread.start()
    acquisition_thread.start()
    security_thread.join()
    acquisition_breaker.filp()
    acquisition_thread.join()
    holding_thread.start()
    divestiture_thread.start()
    divestiture_breaker.flip()
    holding_thread.join()
    divestiture_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"volume": 25, "interest": 25, "size": 10, "apy": 0.01, "liquidity": 0.1, "discount": 0.0, "fees": 0.0}
    main(parameters=sysParameters)



