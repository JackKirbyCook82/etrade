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
SECURITY = os.path.join(REPOSITORY, "security")
ETRADE = os.path.join(ROOT, "Library", "etrade.txt")
TICKERS = os.path.join(ROOT, "Library", "tickers.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from support.synchronize import SideThread
from support.pipelines import Filtering
from webscraping.webreaders import WebAuthorizer, WebReader
from finance.securities import SecurityFile, SecurityFilter, SecuritySaver
from finance.variables import DateRange

from market import ETradeMarketDownloader

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


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


def main(*args, apikey, apicode, tickers, expires, parameters, **kwargs):
    security_file = SecurityFile(name="SecurityFile", repository=SECURITY, timeout=None)
    authorizer = ETradeAuthorizer(name="ETradeAuthorizer", apikey=apikey, apicode=apicode)
    with ETradeReader(authorizer=authorizer, name="ETradeReader") as reader:
        security_downloader = ETradeMarketDownloader(name="SecurityDownloader", feed=reader)
        security_filter = SecurityFilter(name="SecurityFilter", lower={Filtering.LOWER: ["volume", "interest", "size"]})
        security_writer = SecuritySaver(name="SecurityWriter", file=security_file)
        security_pipeline = security_downloader + security_filter + security_writer
        security_thread = SideThread(security_pipeline, name="SecurityThread")
        security_thread.setup(tickers=tickers, expires=expires, **parameters)
        security_thread.start()
        security_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    with open(ETRADE, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:1]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysSecurity = {"volume": None, "interest": None, "size": None}
    main(apikey=sysApiKey, apicode=sysApiCode, tickers=sysTickers, expires=sysExpires, parameters=sysSecurity)



