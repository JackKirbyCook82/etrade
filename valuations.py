# -*- coding: utf-8 -*-
"""
Created on Weds Dec 13 2023
@name:   ETrade Valuation Calculation
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
if ROOT not in sys.path:
    sys.path.append(ROOT)
REPOSITORY = os.path.join(ROOT, "Library", "repository")
API = os.path.join(ROOT, "Library", "api.csv")

from support.synchronize import Routine
from finance.valuations import ValuationFilter, ValuationLoader
from finance.markets import MarketCalculator

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
    valuation_loader = ValuationLoader(name="ValuationLoader", repository=REPOSITORY)
    valuation_filter = ValuationFilter(name="ValuationFilter")
    market_calculator = MarketCalculator(name="MarketCalculator")
    pipeline = valuation_loader + valuation_filter + market_calculator
    consumer = Routine(pipeline, name="ETradeValuation")
    consumer.setup(tickers=tickers, expires=expires, **parameters)
    consumer.start()
    consumer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"apy": 0.05}
    main(tickers=None, expires=None, parameters=sysParameters)
