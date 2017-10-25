
# Code to load raw data from Quandl/SF1
# https://www.quandl.com/data/SF1-Core-US-Fundamentals-Data/
# requires the python Quandl package, and the 
# Quandl API key to be set as an ENV variable QUANDL_API_KEY.

import quandl

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from util import quandl_tools
from logbook import Logger
import datetime
from os import listdir
import numpy as np
import pandas as pd

DS_NAME = "SF1"   # quandl DataSet code
RAW_FLDR = "raw/"  # folder to store the raw text file
VAL_COL_NAME = "Value"
START_DATE = '2010-01-01'
END_DATE = datetime.datetime.today().strftime('%Y-%m-%d')

BASE = "/Users/peterharrington/Documents/GitHub/alpha-compiler/alphacompiler/data/"
FN = "SF1.npy"


log = Logger('load_quandl_sf1.py')

def populate_raw_data(tickers, fields):
    """tickers is a dict with the ticker string as the key and the SID
    as the value.  """
    quandl_tools.set_api_key()

    existing = listdir(RAW_FLDR)

    for ticker, sid in tickers.items():
        if "%d.csv" % sid in existing:
            continue
        all_fields = None

        try:

            for field in fields:

                query_str = "%s/%s_%s" % (DS_NAME, ticker, field)
                print("fetching data for: {}".format(query_str))

                df = quandl.get(query_str, start_date=START_DATE, end_date=END_DATE)

                #  Change column name to field
                df = df.rename(columns={VAL_COL_NAME: field})

                if all_fields is None:
                    all_fields = df
                else:
                    all_fields = all_fields.join(df)  # join data of all fields

            # write to file: raw/
            all_fields.to_csv("{}/{}.csv".format(RAW_FLDR, sid))
        except quandl.errors.quandl_error.NotFoundError:
            print("error with ticker: {}".format(ticker))


def demo():
    # demo works on free data
    tickers = ["WMT", "HD", "CSCO"]
    fields = ["GP", "CAPEX", "EBIT", "ASSETS"]
    populate_raw_data(tickers, fields)


def all_tickers_for_bundle(fields):
    tickers = get_ticker_sid_dict_from_bundle('quantopian-quandl')
    #tickers = {"DOGGY":69, "WMT":3173, "HD":2912, "CSCO":2809}

    populate_raw_data(tickers, fields)

def pack_recarray(N, rawpath, fields, filename):
    """pack data into np.recarray and persists it to a file."""


    # create buffer to hold data for all tickers
    dfs = [None] * N

    max_len = -1
    for fn in listdir(rawpath):
        if not fn.endswith(".csv"):
            continue
        df = pd.read_csv(rawpath + fn, index_col="Date", parse_dates=True)
        sid = int(fn.split('.')[0])
        dfs[sid] = df

        # width is max number of rows in any file
        max_len = max(max_len, df.shape[0])

    # pack up data as buffer
    num_fundamentals = len(fields)
    buff = np.full((num_fundamentals + 1, N, max_len), np.nan)
    # pack self.data as np.recarray
    data = np.recarray(shape=(N, max_len),
                            buf=buff,
                            dtype=[('date', '<f8'),
                                   ("ROE_ART", '<f8'),
                                   ("BVPS_ARQ", '<f8'),
                                   ("SPS_ART", '<f8'),
                                   ("FCFPS_ARQ", '<f8'),
                                   ("PRICE", '<f8')])

    # iterate over loaded data and populate self.data
    for i, df in enumerate(dfs):
        if df is None:
            continue
        ind_len = df.index.shape[0]
        data.date[i, :ind_len] = df.index
        for field in fields:
            data[field][i, :ind_len] = df[field]

    data.dump(filename)  # can be read back with np.load()


if __name__ == '__main__':

    #demo()
    fields = ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]
    #all_tickers_for_bundle()
    pack_recarray(3193,
                  BASE + RAW_FLDR,
                  fields,
                  BASE + FN)

    print("this worked boss")
