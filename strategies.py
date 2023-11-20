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
API = os.path.join(ROOT, "Library", "api.csv")
LOAD = os.path.join(ROOT, "Library", "repository", "security")
SAVE = os.path.join(ROOT, "Library", "repository", "strategy")

from support.synchronize import Consumer, FIFOQueue
from finance.securities import DateRange, Securities, SecurityLoader, SecurityProcessor, SecurityCalculator
from finance.strategies import Strategies, StrategyCalculator
from finance.valuations import Valuations, ValuationCalculator, ValuationSaver

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


def main(tickers, *args, expires, parameters, **kwargs):
    source = FIFOQueue(tickers, size=None, name="TickerQueue")
    loader = SecurityLoader(repository=LOAD, name="SecurityLoader")
    processor = SecurityProcessor(name="SecurityProcessor")
    securities = SecurityCalculator(calculations=Securities, name="SecurityCalculator")
    strategies = StrategyCalculator(calculations=Strategies, name="StrategyCalculator")
    valuations = ValuationCalculator(calculations=Valuations, name="ValuationCalculator")
    saver = ValuationSaver(repository=SAVE, name="ValuationSaver")
    pipeline = loader + processor + securities + strategies + valuations + saver
    consumer = Consumer(pipeline, source=source, name="ETradeStrategies")
    consumer.setup(expires=expires, **parameters)
    consumer.start()
    consumer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=26)).date()])
    sysParameters = {"size": None, "interest": None, "volume": None, "partition": None, "fees": 0, "discount": 0}
    main(sysTickers, expires=sysExpires, parameters=sysParameters)



