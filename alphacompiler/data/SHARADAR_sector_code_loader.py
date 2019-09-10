
"""
This file is for loading the sector codes from Sharadar.  The sector codes come in a
'ticker' file.
The sectors are coded as an integer and stored in a .npy file for fast loading during
a Zipline algorithm run.

Created by Peter Harrington (pbharrin) on 8/5/19.
"""

import pandas as pd
import numpy as np
from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from zipline.data.bundles.core import register

TICKER_FILE = '/Users/peterharrington/Downloads/SHARADAR_TICKERS_fc1f55188fbc034eadec5932a08d626f.csv'
BUNDLE_NAME = 'sep'

ZIPLINE_DATA_DIR = '/Users/peterharrington/.zipline/data/'  # TODO: get this from Zipline api
SID_FILE = "SHARDAR_sectors.npy"  # persisted np.array where


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

def create_sid_table_from_file(filepath):
    """reads the raw file, maps tickers -> SIDS,
    then maps sector strings to integers, and saves
    to the file: SID_FILE"""
    register(BUNDLE_NAME, int, )

    df = pd.read_csv(filepath, index_col="ticker")

    coded_sectors_for_ticker = df["sector"].map(SECTOR_CODING)

    ae_d = get_ticker_sid_dict_from_bundle(BUNDLE_NAME)
    N = max(ae_d.values()) + 1

    # create empty 1-D array to hold data where index = SID
    sectors = np.full(N, -1, np.dtype('int64'))

    # iterate over Assets in the bundle, and fill in sectors
    for ticker, sid in ae_d.items():
        sectors[sid] = coded_sectors_for_ticker.get(ticker, -1)
    print(sectors)

    # finally save the file to disk
    np.save(ZIPLINE_DATA_DIR + SID_FILE, sectors)


if __name__ == '__main__':

    create_sid_table_from_file(TICKER_FILE)
