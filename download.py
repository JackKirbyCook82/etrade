# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Downloader
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
REPOSITORY = os.path.join(ROOT, "Library", "repository")
MARKET = os.path.join(REPOSITORY, "market")
TICKERS = os.path.join(ROOT, "AlgoTrading", "tickers.txt")
API = os.path.join(ROOT, "AlgoTrading", "etrade.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.securities import SecurityFilter, SecurityFile
from finance.variables import DateRange
from webscraping.webreaders import WebAuthorizer, WebReader
from support.files import Saver, FileTiming, FileTyping
from support.queues import Schedule, Scheduler, Queues
from support.synchronize import SideThread
from support.processes import Criterion

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
class TickerQueue(Queues.FIFO, variable="ticker"): pass
class ContractQueue(Queues.FIFO, variable="contract"): pass


def contract(tickers, reader, contracts, *args, expires, parameters, **kwargs):
    contract_schedule = Schedule(name="ContractSchedule", source=tickers)
    contract_downloader = ETradeContractDownloader(name="ContractDownloader", feed=reader)
    contract_scheduler = Scheduler(name="ContractScheduler", destination=contracts)
    contract_pipeline = contract_schedule + contract_downloader + contract_scheduler
    contract_thread = SideThread(contract_pipeline, name="ContractThread")
    contract_thread.setup(expires=expires, **parameters)
    return contract_thread


def security(contracts, reader, securities, *args, parameters, **kwargs):
    security_folder = lambda contents: str(contents["contract"].tostring(delimiter="_"))
    security_criterion = {Criterion.NULL: ["price", "underlying", "volume", "interest", "size"]}
    security_schedule = Schedule(name="HistorySchedule", source=contracts)
    security_downloader = ETradeMarketDownloader(name="HistoryDownloader", feed=reader)
    security_filter = SecurityFilter(name="HistoryFilter", criterion=security_criterion)
    security_saver = Saver(name="HistorySaver", destination=securities, folder=security_folder, mode="w")
    security_pipeline = security_schedule + security_downloader + security_filter + security_saver
    security_thread = SideThread(security_pipeline, name="HistoryThread")
    security_thread.setup(**parameters)
    return security_thread


def main(*args, apikey, apicode, tickers, **kwargs):
    ticker_queue = TickerQueue(name="TickerQueue", contents=list(tickers), capacity=None)
    contract_queue = ContractQueue(name="ContractQueue", contents=[], capacity=None)
    security_file = SecurityFile(name="SecurityFile", repository=MARKET, typing=FileTyping.CSV, timing=FileTiming.EAGER, duplicates=False)
    security_authorizer = ETradeAuthorizer(name="SecurityAuthorizer", apikey=apikey, apicode=apicode)
    with ETradeReader(name="SecurityReader", authorizer=security_authorizer) as security_reader:
        contract_thread = contract(ticker_queue, security_reader, contract_queue, *args, **kwargs)
        security_thread = security(contract_queue, security_reader, security_file, *args, **kwargs)
        contract_thread.start()
        contract_thread.join()
        security_thread.start()
        security_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:2]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=60)).date()])
    main(apikey=sysApiKey, apicode=sysApiCode, tickers=sysTickers, expires=sysExpires, parameters={})



