
"""
This file is for loading the sector codes from Sharadar.  The sector codes come in a
'ticker' file.
The sectors are coded as an integer and stored in a .npy file for fast loading during
a Zipline algorithm run.

Make sure you get the ticker file from:
https://www.quandl.com/tables/SHARADAR-TICKERS/export?api_key=your_api_key
not from the Quandl web GUI.

Created by Peter Harrington (pbharrin) on 8/5/19.
"""

import pandas as pd
import numpy as np
from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from zipline.data.bundles.core import register

TICKER_FILE = '/Users/peterharrington/Downloads/SHARADAR_TICKERS_6cc728d11002ab9cb99aa8654a6b9f4e.csv'
BUNDLE_NAME = 'sep'

ZIPLINE_DATA_DIR = '/Users/peterharrington/.zipline/data/'  # TODO: get this from Zipline api
SID_FILE = "SHARDAR_sectors.npy"  # persisted np.array
STATIC_FILE = "SHARDAR_static.npy"  # persisted np.array


SECTOR_CODING = {'Technology': 0,
                 'Industrials': 1,
                 'Energy': 2,
                 'Utilities': 3,
                 'Consumer Cyclical': 4,
                 'Healthcare': 5,
                 'Financial Services': 6,
                 'Basic Materials': 7,
                 'Consumer Defensive': 8,
                 'Real Estate': 9,
                 'Communication Services': 10,
                 np.nan: -1}  # a few tickers are missing sectors, these should be ignored

EXCHANGE_CODING = {'NYSE': 0,
                   'NASDAQ': 1,
                   'NYSEMKT': 2,  # previously AMEX
                   'OTC': 3,
                   'NYSEARCA': 4,
                   'BATS': 5}

# this is organized so that we can filter out the tradeable stuff with one less than operation
CATEGORY_CODING = {'Domestic': 0,
                   'Canadian': 1,
                   'Domestic Primary': 2,
                   'Domestic Secondary': 3,
                   'Canadian Primary': 4,
                   'Domestic Preferred': 5,
                   'Domestic Warrant': 6,
                   'Canadian Warrant': 7,
                   'Canadian Preferred': 8,
                   'ADR': 9,
                   'ADR Primary': 10,
                   'ADR Warrant': 11,
                   'ADR Preferred': 12,
                   'ADR Secondary': 13, }


def create_sid_table_from_file(filepath):
    """reads the raw file, maps tickers -> SIDS,
    then maps sector strings to integers, and saves
    to the file: SID_FILE"""
    register(BUNDLE_NAME, int, )

    df = pd.read_csv(filepath, index_col="ticker")
    assert df.shape[0] > 10001  # there should be more than 10k tickers
    df = df[df.exchange != 'None']
    df = df[df.exchange != 'INDEX']
    df = df[df.table == 'SEP']

    coded_sectors_for_ticker = df["sector"].map(SECTOR_CODING)

    ae_d = get_ticker_sid_dict_from_bundle(BUNDLE_NAME)
    N = max(ae_d.values()) + 1

    # create empty 1-D array to hold data where index = SID
    sectors = np.full(N, -1, np.dtype('int64'))

    # iterate over Assets in the bundle, and fill in sectors
    for ticker, sid in ae_d.items():
        sector_coded = coded_sectors_for_ticker.get(ticker, -1)
        print(ticker, sid, sector_coded)
        sectors[sid] = sector_coded
    print(sectors)

    # finally save the file to disk
    np.save(ZIPLINE_DATA_DIR + SID_FILE, sectors)


def create_static_table_from_file(filepath):
    """Stores static items to a persisted np array.
    The following static fields are currently persisted.
    -Sector
    -exchange
    -category
    """
    register(BUNDLE_NAME, int, )

    df = pd.read_csv(filepath, index_col="ticker")
    assert df.shape[0] > 10001  # there should be more than 10k tickers
    df = df[df.exchange != 'None']
    df = df[df.exchange != 'INDEX']
    df = df[df.table == 'SEP']

    coded_sectors_for_ticker = df['sector'].map(SECTOR_CODING)
    coded_exchange_for_ticker = df['exchange'].map(EXCHANGE_CODING)
    coded_category_for_ticker = df['category'].map(CATEGORY_CODING)

    ae_d = get_ticker_sid_dict_from_bundle(BUNDLE_NAME)
    N = max(ae_d.values()) + 1

    # create 2-D array to hold data where index = SID
    sectors = np.full((3, N), -1, np.dtype('int64'))
    # sectors = np.full(N, -1, np.dtype('int64'))

    # iterate over Assets in the bundle, and fill in static fields
    for ticker, sid in ae_d.items():
        print(ticker, sid, coded_sectors_for_ticker.get(ticker, -1))
        sectors[0, sid] = coded_sectors_for_ticker.get(ticker, -1)
        sectors[1, sid] = coded_exchange_for_ticker.get(ticker, -1)
        sectors[2, sid] = coded_category_for_ticker.get(ticker, -1)


    print(sectors)
    print(sectors[:, -10:])

    # finally save the file to disk
    np.save(ZIPLINE_DATA_DIR + STATIC_FILE, sectors)


if __name__ == '__main__':

    create_static_table_from_file(TICKER_FILE)
    create_sid_table_from_file(TICKER_FILE)  # only SID sectors
