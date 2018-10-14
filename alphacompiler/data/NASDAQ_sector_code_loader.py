
"""
Downloads sector codes from NASDAQ.  For exchanges: NASDAQ, NYSE, AMEX

Created by Peter Harrington (pbharrin) on 10/21/17.
The download also provides industries within sectors.
"""
import numpy as np
import pandas as pd
import sys
from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
import os
import requests

# this gets all the data for the three exchanges 6000+ tickers
BASE_URL = "http://www.nasdaq.com/screening/companies-by-industry.aspx?&render=download"

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
RAW_FILE = "NASDAQ_table.csv"
SID_FILE = "NASDAQ_sids.npy"  # persisted np.array where

# NASDAQ sectors, not the same as Morningstar
SECTOR_CODING = {"Basic Industries": 0,
           "Capital Goods": 1,
           "Consumer Durables": 2,
           "Consumer Non-Durables": 3,
           "Consumer Services": 4,
           "Energy": 5,
           "Finance": 6,
           "Health Care": 7,
           "Miscellaneous": 8,
           "Public Utilities": 9,
           "Technology": 10,
           "Transportation": 11,
           "n/a": -1}

def create_sid_table_from_file(filepath):
    """reads the raw file, maps tickers -> SIDS,
    then maps sector strings to integers, and saves
    to the file: SID_FILE"""
    df = pd.read_csv(filepath, index_col="Symbol")
    df = df.drop_duplicates()

    coded_sectors_for_ticker = df["Sector"].map(SECTOR_CODING)

    ae_d = get_ticker_sid_dict_from_bundle('quantopian-quandl')
    N = max(ae_d.values()) + 1

    # create empty 1-D array to hold data where index = SID
    sectors = np.full(N, -1, np.dtype('int64'))

    # iterate over Assets in the bundle, and fill in sectors
    for ticker, sid in ae_d.items():
        sectors[sid] = coded_sectors_for_ticker.get(ticker, -1)

    np.save(os.path.join(BASE_PATH , SID_FILE), sectors)



if __name__ == '__main__':
    INPUT_FILE = os.path.join(BASE_PATH , RAW_FILE)
    r = requests.get(BASE_URL, allow_redirects=True)
    open(INPUT_FILE, 'wb').write(r.content)
    create_sid_table_from_file(INPUT_FILE)
    print("all done boss")