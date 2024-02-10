# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Target Calculation
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
REPOSITORY = os.path.join(ROOT, "Library", "repository", "etrade")
SECURITY = os.path.join(REPOSITORY, "security")
PORTFOLIO = os.path.join(REPOSITORY, "portfolio")
if ROOT not in sys.path:
    sys.path.append(ROOT)

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


def main(*args, parameters, **kwargs):
    pass


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysSecurity, sysValuation, sysMarket, sysPortfolio = {"size": 5}, {"apy": 0.15}, {"liquidity": 0.1, "tenure": None}, {"funds": None}
    sysParameters = sysSecurity | sysValuation | sysMarket | sysPortfolio
    main(parameters=sysParameters)



