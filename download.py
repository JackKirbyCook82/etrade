
# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Securities Downloader
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import xarray as xr
import pandas as pd
import PySimpleGUI as gui
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
if ROOT not in sys.path:
    sys.path.append(ROOT)
API = os.path.join(ROOT, "Library", "api.csv")
SAVE = os.path.join(ROOT, "Library", "repository", "security")

from webscraping.webreaders import WebAuthorizer, WebReader
from support.synchronize import Queue, Consumer
from finance.securities import DateRange, SecuritySaver

from market import ETradeSecurityDownloader

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
gui.theme("DarkGrey11")


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"


class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeReader(WebReader, delay=10): pass
class ETradeConsumer(Consumer):
    def terminate(self, *args, funds, **kwargs): return not bool(self.source) or not bool(funds)
    def execute(self, content, *args, **kwargs): pass


def main(tickers, *args, expires, **kwargs):
    api = pd.read_csv(API, header=0, index_col="website").loc["etrade"].to_dict()
    source = Queue(tickers, size=None, name="TickerQueue")
    authorizer = ETradeAuthorizer(apikey=api["key"], apicode=api["code"], name="ETradeAuthorizer")
    with ETradeReader(authorizer=authorizer, name="ETradeReader") as reader:
        downloader = ETradeSecurityDownloader(source=reader, name="SecurityDownloader")
        saver = SecuritySaver(repository=SAVE, name="SecuritySaver")
        pipeline = downloader + saver
        consumer = ETradeConsumer(pipeline, source=source, name="ETradeDownloader")
        consumer.setup(expires=expires)
        consumer.start()
        consumer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=26)).date()])
    main(sysTickers, expires=sysExpires)





