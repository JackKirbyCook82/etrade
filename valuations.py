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
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
if ROOT not in sys.path:
    sys.path.append(ROOT)
REPOSITORY = os.path.join(ROOT, "Library", "repository")
API = os.path.join(ROOT, "Library", "api.csv")

from support.synchronize import Routine
from finance.securities import DateRange
from finance.valuations import Valuations, ValuationLoader, ValuationFilter, ValuationMarket

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
    valuation_loader = ValuationLoader(name="ValuationLoader", valuations=[Valuations.Arbitrage.Minimum], repository=REPOSITORY)
    valuation_filter = ValuationFilter(name="ValuationFilter")
    valuation_market = ValuationMarket(name="ValuationAnalysis")
    pipeline = valuation_loader + valuation_filter + valuation_market
    consumer = Routine(pipeline, name="ETradeValuation")
    consumer.setup(tickers=tickers, expires=expires, **parameters)
    consumer.start()
    consumer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=26)).date()])
    sysParameters = {}
    main(tickers=sysTickers, expires=sysExpires, parameters=sysParameters)



