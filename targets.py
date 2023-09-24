# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Targets Calculation
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
API = os.path.join(ROOT, "Library", "api.csv")
LOAD = os.path.join(ROOT, "Library", "repository", "strategies")

from support.synchronize import Consumer, FIFOQueue
from finance.targets import TargetLoader, TargetAnalysis

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


def main(tickers, *args, parameters, **kwargs):
    source = FIFOQueue(tickers, size=None, name="TickerQueue")
    loader = TargetLoader(repository=LOAD, name="TargetLoader")
    analysis = TargetAnalysis(name="TargetAnalysis")
    pipeline = loader + analysis
    consumer = Consumer(pipeline, source=source, name="ETradeTargets")
    consumer.setup(**parameters)
    consumer.start()
    consumer.join()
    figure = visualize(analysis)
    figure()

def visualize(analysis):
    variables = {"x": "tau", "y": "cost", "z": "apy"}
    strings = {"x": "{:.0f}|{:.0f}", "y": "${:.0f}K|${:.0f}K", "z": "{:.0f}%|{:.0f}%"}
    scales = {"x": 1, "y": 0.001, "z": 100}
    return analysis.figure(variables=variables, strings=strings, scales=scales, size=25)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysParameters = {"apy": 0, "funds": None}
    main(sysTickers, parameters=sysParameters)
