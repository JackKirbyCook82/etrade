# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Option Downloader
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
MODULE = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(MODULE, os.pardir))
if ROOT not in sys.path:
    sys.path.append(ROOT)
USER = os.path.join(ROOT, "resources", "api.csv")
SAVE = os.path.join(ROOT, "save", "options")

from webscraping.webreaders import WebAuthorizer, WebReader
from utilities.synchronize import Queue, Consumer

from finance.etrade.market import ETradeOptionDownloader
from finance.calculations.securities import DateRange, OptionSaver

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


def main(tickers, *args, expires, **kwargs):
    api = pd.read_csv(USER, header=0, index_col="website").loc["etrade"].to_dict()
    source = Queue(tickers, size=None, name="ETradeTickerQueue")
    authorizer = ETradeAuthorizer(apikey=api["key"], apicode=api["code"], name="ETradeAuthorizer")
    with ETradeReader(authorizer=authorizer, name="ETradeReader") as reader:
        downloader = ETradeOptionDownloader(source=reader, name="ETradeOptionDownloader")
        saver = OptionSaver(repository=SAVE, name="ETradeOptionSaver")
        pipeline = downloader + saver
        consumer = Consumer(pipeline, source=source, name="ETradeDownloader")
        consumer.setup(expires=expires)
        consumer.start()
        consumer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=26)).date()])
    main(sysTickers[5:], expires=sysExpires)





