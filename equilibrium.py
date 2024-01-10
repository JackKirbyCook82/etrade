# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Equilibrium Calculation
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
API = os.path.join(ROOT, "Library", "api.csv")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from support.synchronize import Routine
from finance.equilibriums import SupplyDemandFile, SupplyDemandReader, SupplyDemandFilter, EquilibriumCalculator, EquilibriumWriter, EquilibriumTable

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


def main(*args, tickers, expires, parameters, **kwargs):
    files = SupplyDemandFile(name="SupplyDemandFiles", repository=REPOSITORY, timeout=None)
    table = EquilibriumTable(name="EquilibriumTable", timeout=None)
    equilibrium_reader = SupplyDemandReader(name="SupplyDemandReader", source=files)
    equilibrium_filter = SupplyDemandFilter(name="SupplyDemandFilter")
    equilibrium_calculator = EquilibriumCalculator(name="EquilibriumCalculator")
    equilibrium_writer = EquilibriumWriter(name="EquilibriumWriter", destination=table)
    equilibrium_pipeline = equilibrium_reader + equilibrium_filter + equilibrium_calculator + equilibrium_writer
    equilibrium_thread = Routine(equilibrium_pipeline, name="EquilibriumThread")
    equilibrium_thread.setup(tickers=tickers, expires=expires, **parameters)
    equilibrium_thread.start()
    equilibrium_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    sysParameters = {"size": 10, "liquidity": 0.1, "apy": 0.1}
    main(tickers=None, expires=None, parameters=sysParameters)



