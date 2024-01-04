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

from support.synchronize import Routine
from finance.valuations import ValuationFile, ValuationReader, ValuationFilter
from finance.targets import TargetCalculator, TargetWriter, TargetTable

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
    files = ValuationFile(name="ETradeFiles", repository=REPOSITORY, timeout=None)
    table = TargetTable(name="TargetTable", capacity=None, timeout=None)
    valuation_reader = ValuationReader(name="ValuationReader", source=files)
    valuation_filter = ValuationFilter(name="ValuationFilter")
    target_calculator = TargetCalculator(name="TargetCalculator")
    target_writer = TargetWriter(name="TargetWriter", destination=table)
    pipeline = valuation_reader + valuation_filter + target_calculator + target_writer
    routine = Routine(pipeline, name="TargetThread")
    routine.setup(tickers=tickers, expires=expires, **parameters)
    routine.start()
    routine.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"size": 25, "liquidity": 0.10, "apy": 0.10, "funds": 250000, "limit": None, "tenure": None}
    main(tickers=None, expires=None, parameters=sysParameters)



