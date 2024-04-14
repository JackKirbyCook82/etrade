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

from support.files import Archive, FileTiming, FileTyping
from support.synchronize import SideThread
from support.processes import Filtering
from webscraping.webreaders import WebAuthorizer, WebReader
from finance.securities import SecurityFilter, SecuritySaver, SecurityStockFile, SecurityOptionFile
from finance.variables import DateRange

from market import ETradeContractDownloader, ETradeMarketDownloader

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


gui.theme("DarkGrey11")
warnings.filterwarnings("ignore")
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


def security(reader, archive, *args, tickers, expires, **kwargs):
    security_filtering = {Filtering.NULL: ["price", "underlying", "volume", "interest", "size"]}
    contract_downloader = ETradeContractDownloader(feed=reader, name="MarketContractDownloader")
    security_downloader = ETradeMarketDownloader(feed=reader, name="MarketSecurityDownloader")
    security_filter = SecurityFilter(filtering=security_filtering, name="MarketSecurityFilter")
    security_saver = SecuritySaver(destination=archive, name="MarketSecuritySaver", mode="w")
    security_pipeline = contract_downloader + security_downloader + security_filter + security_saver
    security_thread = SideThread(security_pipeline, name="MarketSecurityThread")
    security_thread.setup(tickers=tickers, expires=expires)
    return security_thread


def main(*args, apikey, apicode, **kwargs):
    stock_file = SecurityStockFile(name="MarketStockFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    option_file = SecurityOptionFile(name="MarketOptionFile", typing=FileTyping.CSV, timing=FileTiming.EAGER)
    market_archive = Archive(name="MarketArchive", repository=MARKET, save=[stock_file, option_file])
    market_authorizer = ETradeAuthorizer(name="MarketAuthorizer", apikey=apikey, apicode=apicode)
    with ETradeReader(name="MarketReader", authorizer=market_authorizer) as market_reader:
        security_thread = security(market_reader, market_archive, *args, **kwargs)
        security_thread.start()
        security_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    main(apikey=sysApiKey, apicode=sysApiCode, tickers=sysTickers, expires=sysExpires)



