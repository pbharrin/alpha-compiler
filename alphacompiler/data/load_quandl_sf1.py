
# Code to load raw data from Quandl/SF1
# https://www.quandl.com/data/SF1-Core-US-Fundamentals-Data/
# requires the python Quandl package, and the 
# Quandl API key to be set as an ENV variable QUANDL_API_KEY.

import quandl

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from alphacompiler.util.sparse_data import pack_sparse_data
from util import quandl_tools
from logbook import Logger
import datetime
from os import listdir


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


if __name__ == '__main__':

    #demo()
    fields = ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]
    #all_tickers_for_bundle()
    pack_sparse_data(3193,
                     BASE + RAW_FLDR,
                     fields,
                     BASE + FN)

    print("this worked boss")
