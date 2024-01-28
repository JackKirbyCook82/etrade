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
API = os.path.join(ROOT, "Library", "api.csv")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from support.synchronize import SideThread
from finance.securities import SecurityFile, SecurityReader, SecurityFilter, SecurityCalculator
from finance.strategies import StrategyCalculator
from finance.valuations import ValuationFile, ValuationCalculator, ValuationFilter, ValuationWriter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 20)
pd.set_option("display.max_columns", 25)


def main(*args, tickers, expires, parameters, **kwargs):
    security_file = SecurityFile(name="SecurityFile", repository=os.path.join(REPOSITORY, "security"), timeout=None)
    valuation_file = ValuationFile(name="ValuationFile", repository=os.path.join(REPOSITORY, "valuation"), timeout=None)
    security_reader = SecurityReader(name="SecurityReader", file=security_file)
    security_filter = SecurityFilter(name="SecurityFilter")
    security_calculator = SecurityCalculator(name="SecurityCalculator")
    strategy_calculator = StrategyCalculator(name="StrategyCalculator")
    valuation_calculator = ValuationCalculator(name="ValuationCalculator")
    valuation_filter = ValuationFilter(name="ValuationFilter")
    valuation_writer = ValuationWriter(name="ValuationWriter", file=valuation_file)
    valuation_pipeline = security_reader + security_filter + security_calculator + strategy_calculator
    valuation_pipeline = valuation_pipeline + valuation_calculator + valuation_filter + valuation_writer
    valuation_thread = SideThread(valuation_pipeline, name="ValuationThread")
    valuation_thread.setup(tickers=tickers, expires=expires, **parameters)
    valuation_thread.start()
    valuation_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysSecurity, sysValuation, sysMarket = {"volume": 50, "interest": 50, "size": 5}, {"apy": 0.0, "discount": 0.0}, {"fees": 1.0}
    main(tickers=None, expires=None, parameters=sysSecurity | sysValuation | sysMarket)



