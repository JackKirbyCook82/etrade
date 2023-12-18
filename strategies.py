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
from finance.securities import DateRange, Securities, SecurityLoader, SecurityFilter, SecurityCalculator
from finance.strategies import Strategies, StrategyCalculator
from finance.valuations import Valuations, ValuationCalculator, ValuationFilter, ValuationSaver

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
    security_loader = SecurityLoader(name="SecurityLoader", repository=REPOSITORY)
    security_filter = SecurityFilter(name="SecurityFilter")
    security_calculator = SecurityCalculator(name="SecurityCalculator", calculations=list(Securities))
    strategy_calculator = StrategyCalculator(name="StrategyCalculator", calculations=[Strategies.Vertical.Put, Strategies.Vertical.Call])
    valuation_calculator = ValuationCalculator(name="ValuationCalculator", calculations=[Valuations.Arbitrage.Minimum])
    valuation_filter = ValuationFilter(name="ValuationFilter")
    valuation_saver = ValuationSaver(name="ValuationSaver", repository=REPOSITORY)
    pipeline = security_loader + security_filter
    pipeline = pipeline + security_calculator + strategy_calculator + valuation_calculator
    pipeline = pipeline + valuation_filter + valuation_saver
    consumer = Routine(pipeline, name="ETradeStrategies")
    consumer.setup(tickers=tickers, expires=expires, **parameters)
    consumer.start()
    consumer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=26)).date()])
    sysParameters = {"volume": 100, "interest": 100, "size": 25, "apy": 0.0, "fees": 0.0, "discount": 0.0}
    main(tickers=sysTickers, expires=sysExpires, parameters=sysParameters)



