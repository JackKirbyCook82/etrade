# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Strategies Calculation
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import xarray as xr
import pandas as pd

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Valuations, Scenarios
from support.processes import Parsing, Filtering
from support.synchronize import SideThread
from finance.securities import SecurityFile, SecurityLoader, SecurityFilter, SecurityCleaner, SecurityParser, SecurityPivoter
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationFile, ValuationCalculator, ValuationFilter, ValuationCleaner, ValuationParser, ValuationSaver

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


warnings.filterwarnings("ignore")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option("display.width", 1000)
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 25)


def main(*args, parameters, **kwargs):
    security_file = SecurityFile(name="SecurityFile", repository=REPOSITORY, timeout=None)
    valuation_file = ValuationFile(name="ValuationFile", repository=REPOSITORY, timeout=None)
    security_reader = SecurityLoader(name="SecurityReader", file=security_file)
    security_filter = SecurityFilter(name="SecurityFilter", filtering={Filtering.FLOOR: ["volume", "interest", "size"]})
    security_cleaner = SecurityCleaner(name="SecurityCleaner")
    security_parser = SecurityParser(name="SecurityParser", parsing=Parsing.UNFLATTEN)
    security_pivoter = SecurityPivoter(name="SecurityPivoter")
    strategy_calculator = StrategyCalculator(name="StrategyCalculator")
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", valuation=Valuations.ARBITRAGE)
    valuation_filter = ValuationFilter(name="ValuationFilter", filtering={Filtering.FLOOR: ["apy", "size"]}, scenario=Scenarios.MINIMUM)
    valuation_parser = ValuationParser(name="ValuationParser", parsing=Parsing.FLATTEN)
    valuation_cleaner = ValuationCleaner(name="ValuationCleaner")
    valuation_writer = ValuationSaver(name="ValuationWriter", file=valuation_file)
    valuation_pipeline = security_reader + security_filter + security_cleaner + security_parser + security_pivoter
    valuation_pipeline = valuation_pipeline + strategy_calculator + valuation_calculator
    valuation_pipeline = valuation_pipeline + valuation_filter + valuation_parser + valuation_cleaner + valuation_writer
    valuation_thread = SideThread(valuation_pipeline, name="ValuationThread")
    valuation_thread.setup(**parameters)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysSecurity, sysValuation, sysMarket = {"volume": 50, "interest": 50, "size": 10}, {"apy": 0.001, "discount": 0.0}, {"fees": 0.0}
    sysParameters = sysSecurity | sysValuation | sysMarket
    main(parameters=sysParameters)



