# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Target Calculation
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

from support.synchronize import Routine, Locks
from finance.valuations import Valuations, ValuationLoader, ValuationFilter
from finance.targets import TargetCalculator, TargetTable

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
    locks = Locks(name="TargetLocks", timeout=None)
    valuation_loader = ValuationLoader(name="ValuationLoader", repository=REPOSITORY, locks=locks)
    valuation_filter = ValuationFilter(name="ValuationFilter")
    target_calculator = TargetCalculator(name="TargetCalculator", valuation=Valuations.Arbitrage.Minimum)
    target_table = TargetTable(name="TargetTable")
    pipeline = valuation_loader + valuation_filter + target_calculator + target_table
    routine = Routine(pipeline, name="TargetThread")
    routine.setup(tickers=tickers, expires=expires, **parameters)
    routine.start()
    routine.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"size": 25, "apy": 0.10, "funds": None, "liquidity": 0.10, "limit": None, "tenure": None}
    main(tickers=None, expires=None, parameters=sysParameters)


