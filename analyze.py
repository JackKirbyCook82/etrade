# -*- coding: utf-8 -*-
"""
Created on Weds Dec 13 2023
@name:   ETrade Valuation Analysis
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
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
API = os.path.join(ROOT, "Library", "api.csv")

from support.synchronize import Routine
from finance.analysis import SupplyDemandLoader, EquilibriumCalculator, EquilibriumAnalysis

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
pd.set_option('display.max_rows', 35)
pd.set_option("display.max_columns", 25)


def main(*args, tickers, expires, parameters, **kwargs):
    market_loader = SupplyDemandLoader(name="MarketLoader", repository=REPOSITORY)
    market_calculator = EquilibriumCalculator(name="MarketCalculator")
    market_analysis = EquilibriumAnalysis(name="MarketAnalysis")
    pipeline = market_loader + market_calculator + market_analysis
    routine = Routine(pipeline, name="ETradeMarketCalculation")
    routine.setup(tickers=tickers, expires=expires, **parameters)
    routine.start()
    routine.join()
    results = market_analysis.results
    print(results.markets)
    print(results)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"apy": 1.0}
    main(tickers=None, expires=None, parameters=sysParameters)
