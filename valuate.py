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
SECURITY = os.path.join(REPOSITORY, "security")
VALUATION = os.path.join(REPOSITORY, "valuation")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import Actions
from support.pipelines import Parsing, Filtering
from support.synchronize import SideThread
from finance.securities import SecurityFile, SecurityLoader, SecurityFilter, SecurityParser
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationFile, ValuationCalculator, ValuationFilter, ValuationParser, ValuationSaver

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


warnings.filterwarnings("ignore")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 20)
pd.set_option("display.max_columns", 25)


def main(*args, parameters, **kwargs):
    security_file = SecurityFile(name="SecurityFile", repository=SECURITY, timeout=None)
    valuation_file = ValuationFile(name="ValuationFile", repository=VALUATION, timeout=None)
    security_reader = SecurityLoader(name="SecurityReader", file=security_file)
    security_filter = SecurityFilter(name="SecurityFilter", filtering={Filtering.LOWER: ["volume", "interest", "size"]})
    security_parser = SecurityParser(name="SecurityParser", parsing=Parsing.UNFLATTEN)
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", action=Actions.OPEN)
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", action=Actions.OPEN)
    valuation_filter = ValuationFilter(name="ValuationFilter", lower={Filtering.LOWER: ["apy", "size"]})
    valuation_parser = ValuationParser(name="ValuationParser", parsing=Parsing.FLATTEN)
    valuation_writer = ValuationSaver(name="ValuationWriter", file=valuation_file)
    valuation_pipeline = security_reader + security_filter + security_parser + strategy_calculator + valuation_calculator + valuation_filter + valuation_parser + valuation_writer
    valuation_thread = SideThread(valuation_pipeline, name="ValuationThread")
    valuation_thread.setup(**parameters)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysSecurity, sysValuation, sysMarket = {"volume": 50, "interest": 50, "size": 10}, {"apy": 0.0}, {"fees": 0.0}
    sysParameters = sysSecurity | sysValuation | sysMarket
    main(parameters=sysParameters)



