# -*- coding: utf-8 -*-
"""
Created on Thurs Dec 21 2023
@name:   ETrade Paper Trading Application
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
PORTFOLIO = os.path.join(REPOSITORY, "portfolio")
ETRADE = os.path.join(ROOT, "Library", "etrade.txt")
TICKERS = os.path.join(ROOT, "Library", "tickers.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from webscraping.webreaders import WebAuthorizer, WebReader
from finance.variables import DateRange

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


def main(*args, apikey, apicode, tickers, expires, parameters, **kwargs):
    authorizer = ETradeAuthorizer(name="ETradeAuthorizer", apikey=apikey, apicode=apicode)
    with ETradeReader(authorizer=authorizer, name="ETradeReader") as reader:
        pass


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    with open(ETRADE, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52)).date()])
    sysSecurity, sysValuation = {"volume": 100, "interest": 100, "size": 10}, {"apy": 0.25, "discount": 0.0}
    sysMarket, sysPortfolio = {"liquidity": 0.1, "tenure": None, "fees": 5.0}, {"funds": None}
    sysParameters = sysSecurity | sysValuation | sysMarket | sysPortfolio
    main(apikey=sysApiKey, apicode=sysApiCode, tickers=sysTickers, expires=sysExpires, parameters=sysParameters)



