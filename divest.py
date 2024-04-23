# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Divestitures
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
REPOSITORY = os.path.join(ROOT, "Library", "repository")
MARKET = os.path.join(REPOSITORY, "market")
PORTFOLIO = os.path.join(REPOSITORY, "portfolio")
TICKERS = os.path.join(ROOT, "AlgoTrading", "tickers.txt")
ETRADE = os.path.join(ROOT, "AlgoTrading", "etrade.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.divestitures import DivestitureReader, DivestitureWriter
from finance.holdings import HoldingTable
from finance.valuations import ValuationCalculator, ValuationFilter
from finance.variables import Scenarios, Valuations, Contract
from finance.holdings import HoldingCalculator, HoldingFile
from finance.securities import SecurityFilter
from finance.strategies import StrategyCalculator
from support.files import Loader, Saver, FileTiming, FileTyping
from support.synchronize import CycleThread
from support.processes import Criterion
from support.pipelines import Processor

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


class HoldingSimulator(Processor):
    def execute(self, query, *args, **kwargs): pass


def portfolio(holdings, divestitures, *args, parameters, **kwargs):
    security_criterion = {Criterion.FLOOR: {"volume": 25, "interest": 25, "size": 10}, Criterion.NULL: ["volume", "interest", "size"]}
    valuation_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Scenarios.MINIMUM.name).lower())]
    holding_query = lambda folder: Contract.fromstring(folder, delimiter="_")
    valuations_functions = dict(liquidity=liquidity_function, priority=priority_function)
    holding_loader = Loader(name="PortfolioHoldingLoader", source=holdings, query=holding_query, mode="r")
    holding_calculator = HoldingCalculator(name="PortfolioHoldingCalculator")
    holding_simulator = HoldingSimulator(name="PortfolioHoldingSimulator")
    security_filter = SecurityFilter(name="PortfolioSecurityFilter", criterion=security_criterion)
    strategy_calculator = StrategyCalculator(name="PortfolioStrategyCalculator")
    valuation_calculator = ValuationCalculator(name="PortfolioValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="PortfolioValuationFilter", scenario=Scenarios.MINIMUM, criterion=valuation_criterion)
    divestiture_writer = DivestitureWriter(name="PortfolioDivestitureWriter", destination=divestitures, valuation=Valuations.ARBITRAGE, capacity=None, **valuations_functions)
    portfolio_pipeline = holding_loader + holding_calculator + holding_simulator + security_filter + strategy_calculator + valuation_calculator + valuation_filter + divestiture_writer
    portfolio_thread = CycleThread(portfolio_pipeline, name="MarketValuationThread", wait=10)
    portfolio_thread.setup(**parameters)
    return portfolio_thread


def divestiture(divestitures, holdings, *args, parameters, **kwargs):
    divestiture_folder = lambda contents: str(contents["contract"].tostring(delimiter="_"))
    divestiture_reader = DivestitureReader(name="PortfolioDivestitureReader", source=divestitures)
    divestiture_saver = Saver(name="PortfolioDivestitureSaver", destination=holdings, folder=divestiture_folder, mode="a")
    divestiture_pipeline = divestiture_reader + divestiture_saver
    divestiture_thread = CycleThread(divestiture_pipeline, name="PortfolioDivestitureThread", wait=10)
    divestiture_thread.setup(**parameters)
    return divestiture_thread


def main(*args, **kwargs):
    holding_file = HoldingFile(name="HoldingFile", repository=PORTFOLIO, typing=FileTyping.CSV, timing=FileTiming.EAGER, duplicates=True)
    divestiture_table = HoldingTable(name="DivestitureTable")
    portfolio_thread = portfolio(holding_file, divestiture_table, *args, **kwargs)
    divestiture_thread = divestiture(divestiture_table, holding_file, *args, **kwargs)
    portfolio_thread.start()
    portfolio_thread.cease()
    portfolio_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    main(parameters={})



