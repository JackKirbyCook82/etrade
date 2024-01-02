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

from support.files import Files
from support.synchronize import Routine
from finance.securities import Securities, SecurityReader, SecurityFilter, SecurityParser, SecurityCalculator
from finance.strategies import Strategies, StrategyCalculator
from finance.valuations import Valuations, ValuationCalculator, ValuationFilter, ValuationWriter

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
    files = Files(name="ETradeFiles", repository=REPOSITORY, timeout=None)
    security_reader = SecurityReader(name="SecurityReader", source=files)
    security_filter = SecurityFilter(name="SecurityFilter")
    security_parser = SecurityParser(name="SecurityParser")
    security_calculator = SecurityCalculator(name="SecurityCalculator", calculations=list(Securities))
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", calculations=list(Strategies))
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", calculations=[Valuations.Arbitrage.Minimum])
    valuation_filter = ValuationFilter(name="ValuationFilter")
    valuation_writer = ValuationWriter(name="ValuationWriter", destination=files)
    pipeline = security_reader + security_filter + security_parser + security_calculator
    pipeline = pipeline + strategy_calculator + valuation_calculator + valuation_filter + valuation_writer
    routine = Routine(pipeline, name="ValuationThread")
    routine.setup(tickers=tickers, expires=expires, **parameters)
    routine.start()
    routine.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"volume": 100, "interest": 100, "size": 10, "apy": 0.0, "fees": 0.0, "discount": 0.0}
    main(tickers=None, expires=None, parameters=sysParameters)



