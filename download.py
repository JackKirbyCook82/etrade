# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Downloader
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
MARKET = os.path.join(REPOSITORY, "market")
API = os.path.join(ROOT, "Library", "etrade.txt")
TICKERS = os.path.join(ROOT, "Library", "tickers.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from support.synchronize import SideThread
from support.processes import Filtering
from webscraping.webreaders import WebAuthorizer, WebReader
from finance.securities import SecuritySchedule, SecurityFile, SecurityFilter, SecurityQueue, SecurityDequeue, SecuritySaver
from finance.variables import DateRange

from market import ETradeExpireDownloader, ETradeSecurityDownloader

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"
__date__ = Datetime.today().date()


warnings.filterwarnings("ignore")
gui.theme("DarkGrey11")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option("display.width", 1000)
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", 25)


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"


class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeReader(WebReader, delay=10): pass


def expire(reader, destination, *args, tickers, expires, **kwargs):
    expire_downloader = ETradeExpireDownloader(name="ETradeExpireDownloader", feed=reader)
    expire_queue = SecurityQueue(name="MarketExpireQueue", destination=destination)
    expire_pipeline = expire_downloader + expire_queue
    expire_thread = SideThread(expire_pipeline, name="MarketExpireThread")
    expire_thread.setup(tickers=tickers, expires=expires)
    return expire_thread


def security(source, reader, file, *args, parameters, **kwargs):
    security_dequeue = SecurityDequeue(name="MarketSecurityDequeue", source=source)
    security_downloader = ETradeSecurityDownloader(name="ETradeSecurityDownloader", feed=reader)
    security_filter = SecurityFilter(name="MarketSecurityFilter", filtering={Filtering.FLOOR: ["volume", "interest", "size"]})
    security_saver = SecuritySaver(name="MarketSecuritySaver", file=file)
    security_pipeline = security_dequeue + security_downloader + security_downloader + security_filter + security_saver
    security_thread = SideThread(security_pipeline, name="MarketSecurityThread")
    security_thread.setup(**parameters)
    return security_thread


def main(*args, apikey, apicode, **kwargs):
    market_file = SecurityFile(name="MarketFile", repository=MARKET, timeout=None)
    market_schedule = SecuritySchedule(name="MarketSchedule", timeout=None)
    authorizer = ETradeAuthorizer(name="ETradeAuthorizer", apikey=apikey, apicode=apicode)
    with ETradeReader(name="ETradeReader", authorizer=authorizer) as market_reader:
        expire_thread = expire(market_reader, market_schedule, *args, **kwargs)
        security_thread = security(market_schedule, market_reader, market_file, *args, **kwargs)
        expire_thread.start()
        expire_thread.join()
        security_thread.start()
        security_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
    sysExpires = DateRange([(__date__ + Timedelta(days=1)).date(), (__date__ + Timedelta(weeks=52)).date()])
    sysParameters = {"volume": None, "interest": None, "size": None}
    main(apikey=sysApiKey, apicode=sysApiCode, tickers=sysTickers, expires=sysExpires, parameters=sysParameters)



