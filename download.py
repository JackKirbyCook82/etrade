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
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
API = os.path.join(ROOT, "Library", "api.csv")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from support.synchronize import SideThread
from webscraping.webreaders import WebAuthorizer, WebReader
from finance.securities import SecurityFile, SecurityFilter, SecuritySaver
from finance.variables import DateRange

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


def main(*args, tickers, expires, parameters, **kwargs):
    api = pd.read_csv(API, header=0, index_col="website").loc["etrade"].to_dict()
    file = SecurityFile(name="SecurityFile", repository=os.path.join(REPOSITORY, "security"), timeout=None)
    authorizer = ETradeAuthorizer(name="ETradeAuthorizer", apikey=api["key"], apicode=api["code"])
    with ETradeReader(authorizer=authorizer, name="ETradeReader") as reader:
        security_downloader = ETradeSecurityDownloader(name="SecurityDownloader", feed=reader)
        security_filter = SecurityFilter(name="SecurityFilter")
        security_writer = SecuritySaver(name="SecurityWriter", file=file)
        security_pipeline = security_downloader + security_filter + security_writer
        security_thread = SideThread(security_pipeline, name="SecurityThread")
        security_thread.setup(tickers=tickers, expires=expires, **parameters)
        security_thread.start()
        security_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysTickers = ["NVDA", "AMD", "AMC", "TSLA", "AAPL", "IWM", "AMZN", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=26)).date()])
    sysSecurity = {"volume": None, "interest": None, "size": None}
    main(tickers=sysTickers, expires=sysExpires, parameters=sysSecurity)



