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

from support.synchronize import SideThread, MainThread
from finance.valuations import ValuationFile, ValuationLoader, ValuationFilter
from finance.targets import TargetsFile, TargetsCalculator, TargetsSaver, TargetsTable

from window import ETradeTargetsWindow

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


def main(*args, parameters, **kwargs):
    valuations = ValuationFile(name="ValuationFile", repository=os.path.join(REPOSITORY, "valuation"), timeout=None)
    file = TargetsFile(name="TargetFile", repository=REPOSITORY, timeout=None)
    table = TargetsTable(name="TargetTable", timeout=None)
    valuation_reader = ValuationLoader(name="ValuationReader", file=valuations)
    valuation_filter = ValuationFilter(name="ValuationFilter")
    target_calculator = TargetsCalculator(name="TargetCalculator")
    target_writer = TargetsSaver(name="TargetWriter", table=table, file=file)
    target_feed = valuation_reader + valuation_filter + target_calculator + target_writer
    target_window = ETradeTargetsWindow(name="TargetsWindow", feed=table)
    feed_thread = SideThread(target_feed, name="TargetFeedThread")
    window_thread = MainThread(target_window, name="TargetWindowThread")
    feed_thread.setup(**parameters)
    window_thread.setup()
    feed_thread.start()
    window_thread.run()
    feed_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    security, valuation, market, portfolio = {"size": 5}, {"apy": 0.15}, {"liquidity": 0.1, "tenure": None}, {"funds": None}
    main(parameters=security | valuation | market | portfolio)



