# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Simulation
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
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

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

from support.synchronize import MainThread, SideThread
from support.processes import Filtering
from finance.variables import Scenarios, Valuations
from finance.securities import SecurityFile, SecurityFilter, SecurityLoader, SecuritySaver
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationCalculator, ValuationFilter
from finance.acquisitions import AcquisitionTable, AcquisitionWriter, AcquisitionReader
from finance.divestitures import DivestitureTable, DivestitureWriter, DivestitureReader
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
    priority = lambda cols: cols[(Scenarios.MINIMUM, "apy")].astype(np.float32)
    market_file = SecurityFile(name="MarketFile", repository=MARKET, timeout=None)
    portfolio_file = SecurityFile(name="PortfolioFile", repository=PORTFOLIO, timeout=None)
    acquisitions_table = AcquisitionTable(name="AcquisitionTable", timeout=None)
    divestitures_table = DivestitureTable(name="DivestitureTable", timeout=None)
    market_scheduler = SecurityScheduler(name="MarketSecurityScheduler")  # SINGLE READING, NO CYCLING
    portfolio_scheduler = SecurityScheduler(name="PortfolioSecurityScheduler")  # CONTINUOUS READING, WITH CYCLING, BREAKER = ???
    market_loader = SecurityLoader(name="MarketSecurityLoader", file=market_file)
    portfolio_loader = SecurityLoader(name="PortfolioSecurityLoader", file=portfolio_file)
    security_simulator = SecuritySimulator(name="SecuritySimulator")
    security_filter = SecurityFilter(name="SecurityFilter", filtering={Filtering.FLOOR: ["volume", "interest", "size"]})
    strategy_calculator = StrategyCalculator(name="StrategyCalculator")
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", scenario=Scenarios.MINIMUM, filtering={Filtering.FLOOR: ["apy", "size"]})
    acquisitions_writer = AcquisitionWriter(name="AcquisitionWriter", table=acquisitions_table, valuation=Valuations.ARBITRAGE, priority=priority)
    divestitures_writer = DivestitureWriter(name="DivestitureWriter", table=divestitures_table, valuation=Valuations.ARBITRAGE, priority=priority)
    acquisitions_scheduler = AcquisitionScheduler(name="AcquisitionScheduler")  # CONTINUOUS READING, WITH CYCLING, BREAKER = ???
    acquisitions_reader = AcquisitionReader(name="AcquisitionReader", table=acquisitions_table)
    acquisitions_saver = SecuritySaver(name="PortfolioSecuritySaver", file=portfolio_file)
    divestitures_scheduler = DivestitureScheduler(name="DivestitureScheduler")  # CONTINUOUS READING, WITH CYCLING, BREAKER = ???
    divestitures_reader = DivestitureReader(name="DivestitureReader", table=divestitures_table)
    divestitures_saver = SecuritySaver(name="PortfolioSecuritySaver", file=portfolio_file)
    acquisitions_window = AcquisitionWindow(name="AcquisitionWindow", table=acquisitions_table)
    divestitures_window = DivestitureWindow(name="DivestitureWindow", table=divestitures_table)
    market_pipeline = market_scheduler + market_loader + security_filter + strategy_calculator + valuation_calculator + valuation_filter + acquisitions_writer
    portfolio_pipeline = portfolio_scheduler + portfolio_loader + security_simulator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + divestitures_writer
    acquisition_pipeline = acquisitions_scheduler + acquisitions_reader + acquisitions_saver
    divestiture_pipeline = divestitures_scheduler + divestitures_reader + divestitures_saver
    window_terminal = WindowTerminal(name="WindowTerminal", windows=[acquisitions_window, divestitures_window])
    window_thread = MainThread(window_terminal, name="WindowThread")
    market_thread = SideThread(market_pipeline, name="MarketThread")
    portfolio_thread = SideThread(portfolio_pipeline, name="PortfolioThread")
    acquisition_thread = SideThread(acquisition_pipeline, name="AcquisitionThread")
    divestiture_thread = SideThread(divestiture_pipeline, name="DivestitureThread")
    threads = [acquisition_thread, divestiture_thread, market_thread, portfolio_thread]
    for thread in list(threads):
        thread.setup(tickers=tickers, expires=expires, **parameters)
    window_thread.run()
    for thread in list(threads):
        thread.start()
    for thread in reversed(threads):
        thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
    sysExpires = DateRange([(__date__ + Timedelta(days=1)).date(), (__date__ + Timedelta(weeks=52)).date()])
    sysParameters = {"volume": 25, "interest": 25, "size": 10, "apy": 0.01, "liquidity": 0.1, "discount": 0.0, "fees": 0.0}
    main(tickers=sysTickers, expires=sysExpires, parameters=sysParameters)



