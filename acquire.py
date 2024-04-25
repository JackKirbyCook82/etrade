# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Acquisitions
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
VALUATIONS = os.path.join(REPOSITORY, "market", "valuations")
HOLDINGS = os.path.join(REPOSITORY, "portfolio", "holdings")
TICKERS = os.path.join(ROOT, "AlgoTrading", "tickers.txt")
ETRADE = os.path.join(ROOT, "AlgoTrading", "etrade.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.acquisitions import AcquisitionReader, AcquisitionWriter
from finance.holdings import HoldingTable
from finance.variables import Contract, Scenarios, Valuations
from finance.valuations import ValuationFilter, ValuationFile
from finance.holdings import HoldingFile, HoldingStatus
from support.synchronize import SideThread, CycleThread
from support.files import Loader, Saver, Directory, Timing, Typing
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


def market(source, destination, *args, parameters, **kwargs):
    valuations_criterion = {Criterion.FLOOR: {"apy": 0.0, "size": 10}, Criterion.NULL: ["apy", "size"]}
    liquidity_function = lambda cols: np.floor(cols["size"] * 0.1).astype(np.int32)
    priority_function = lambda cols: cols[("apy", str(Scenarios.MINIMUM.name).lower())]
    valuations_functions = dict(liquidity=liquidity_function, priority=priority_function)
    contract_directory = Directory("contract", lambda folder: Contract.fromstring(folder, delimiter="_"), repository=VALUATIONS)
    valuation_loader = Loader(name="MarketValuationLoader", source=source, directory=contract_directory)
    valuation_filter = ValuationFilter(name="MarketValuationFilter", scenario=Scenarios.MINIMUM, criterion=valuations_criterion)
    acquisition_writer = AcquisitionWriter(name="MarketAcquisitionWriter", destination=destination, valuation=Valuations.ARBITRAGE, capacity=None, **valuations_functions)
    market_pipeline = valuation_loader + valuation_filter + acquisition_writer
    market_thread = SideThread(market_pipeline, name="MarketValuationThread")
    market_thread.setup(**parameters)
    return market_thread


def acquisition(source, destination, *args, parameters, **kwargs):
    acquisition_reader = AcquisitionReader(name="PortfolioAcquisitionReader", source=source)
    acquisition_saver = Saver(name="PortfolioAcquisitionSaver", destination=destination, query="contract")
    acquisition_pipeline = acquisition_reader + acquisition_saver
    acquisition_thread = CycleThread(acquisition_pipeline, name="PortfolioAcquisitionThread", wait=10)
    acquisition_thread.setup(**parameters)
    return acquisition_thread


def main(*args, **kwargs):
    contract_query = lambda contract: contract.tostring(delimiter="_")
    valuations_file = ValuationFile(name="ValuationFile", repository=VALUATIONS, query=contract_query, typing=Typing.CSV, timing=Timing.EAGER, duplicates=False)
    holdings_file = HoldingFile(name="HoldingFile", repository=HOLDINGS, query=contract_query, typing=Typing.CSV, timing=Timing.EAGER, duplicates=True)
    acquisitions_table = HoldingTable(name="AcquisitionTable")
    market_thread = market({valuations_file: "r"}, acquisitions_table, *args, **kwargs)
    acquisition_thread = acquisition(acquisitions_table, {holdings_file: "a"}, *args, **kwargs)
    market_thread.start()
    market_thread.join()
    acquisition_thread.start()
    acquisitions_table[:, "status"] = HoldingStatus.PURCHASED
    acquisition_thread.cease()
    acquisition_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    main(parameters={})



