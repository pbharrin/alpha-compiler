
# Code to load raw data from Quandl/SF1
# https://www.quandl.com/data/SF1-Core-US-Fundamentals-Data/
# requires the python Quandl package, and the 
# Quandl API key to be set as an ENV variable QUANDL_API_KEY.

import quandl

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from alphacompiler.util.sparse_data import pack_sparse_data
from alphacompiler.util import quandl_tools
from logbook import Logger
import datetime
from os import listdir
import os
import pandas as pd


BASE = os.path.dirname(os.path.realpath(__file__))
DS_NAME = 'SHARADAR/SF1'   # quandl DataSet code
RAW_FLDR = "raw"  # folder to store the raw text file
VAL_COL_NAME = "Value"
START_DATE = '2013-01-01'
END_DATE = datetime.datetime.today().strftime('%Y-%m-%d')


FN = "SF1.npy"

log = Logger('load_quandl_sf1.py')


def populate_raw_data(tickers, fields, dimensions, raw_path):
    """tickers is a dict with the ticker string as the key and the SID
    as the value.
    For each field a dimension is required, so dimensions should be a list
    of dimensions for each field.
    """
    assert len(fields) == len(dimensions)
    quandl_tools.set_api_key()

    # existing = listdir(RAW_FLDR)

    for ticker, sid in tickers.items():
        # if "%d.csv" % sid in existing:
        #     continue
        try:
            query_str = "%s %s" % (DS_NAME, ticker)
            print("fetching data for: {}".format(query_str))

            # df = quandl.get_table(query_str, start_date=START_DATE, end_date=END_DATE)
            df = quandl.get_table(DS_NAME,
                                  calendardate={'gte': START_DATE, 'lte': END_DATE},
                                  ticker=ticker,
                                  qopts={'columns': ['dimension', 'datekey'] + fields})
            df = df.rename(columns={'datekey': 'Date'}).set_index('Date')

            # loop over the fields and dimensions
            series = []
            for i, field in enumerate(fields):
                s = df[df.dimension == dimensions[i]][field]
                series.append(s)
            df = pd.concat(series, axis=1)
            print(df)

            # write raw file: raw/
            df.to_csv(os.path.join(raw_path, "{}.csv".format(sid)))
        except quandl.errors.quandl_error.NotFoundError:
            print("error with ticker: {}".format(ticker))


def populate_raw_data_aqr(tickers, fields, raw_path):
    """tickers is a dict with the ticker string as the key and the SID
    as the value.
    Assumes that all fields desired are AQR.
    """
    quandl_tools.set_api_key()

    # existing = listdir(RAW_FLDR)

    for ticker, sid in tickers.items():
        # if "%d.csv" % sid in existing:
        #     continue
        try:
            query_str = "%s %s" % (DS_NAME, ticker)
            print("fetching data for: {}".format(query_str))

            # df = quandl.get_table(query_str, start_date=START_DATE, end_date=END_DATE)
            df = quandl.get_table(DS_NAME,
                                  calendardate={'gte': START_DATE, 'lte': END_DATE},
                                  ticker=ticker,
                                  qopts={'columns': ['dimension', 'datekey'] + fields})
            print(df)
            df = df[df.dimension == "ARQ"]  # only use As-Reported numbers

            #  Change column name to field
            df = df.rename(columns={"datekey": "Date"})
            df = df.drop(["dimension"], axis=1)

            # write raw file: raw/
            df.to_csv(os.path.join(raw_path, "{}.csv".format(sid)))
        except quandl.errors.quandl_error.NotFoundError:
            print("error with ticker: {}".format(ticker))

def demo():
    # demo works on free data

    tickers = {"WMT":3173, "HD":2912, "DOGGY":69, "CSCO":2809}
    fields = ["GP", "CAPEX", "EBIT", "ASSETS"]
    populate_raw_data(tickers, fields, os.path.join(BASE, RAW_FLDR))


def all_tickers_for_bundle(fields, dims, bundle_name, raw_path=os.path.join(BASE,RAW_FLDR)):
    tickers = get_ticker_sid_dict_from_bundle(bundle_name)
    populate_raw_data(tickers, fields, dims, raw_path)


def num_tkrs_in_bundle(bundle_name):
    return len(get_ticker_sid_dict_from_bundle(bundle_name))


if __name__ == '__main__':

    # demo()
    # fields = ["marketcap", "pb"]  # minimum fundamentals for risk
    # fields = ["ROE", "BVPS", "SPS", "FCFPS", "PRICE"]
    fields0 = ['netinc', 'equity', 'bvps', 'sps', 'fcfps', 'price']  # basic QV
    dimensions0 = ['ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ']

    # magic formula
    fields1 = ['ebit', 'workingcapital', 'assets', 'assetsc', 'intangibles', 'ev', 'marketcap']
    dimensions1 = ['ART', 'ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ']
    fields = fields0 + fields1
    dimensions = dimensions0 + dimensions1

    from zipline.data.bundles.core import register
    from alphacompiler.data.loaders.sep_quandl import from_sep_dump

    BUNDLE_NAME = 'sep'
    register(BUNDLE_NAME, from_sep_dump('.'), )
    num_tickers = num_tkrs_in_bundle(BUNDLE_NAME)
    print('number of tickers: ', num_tickers)

    # all_tickers_for_bundle(fields, dimensions, 'sep')
    pack_sparse_data(num_tickers + 1,  # number of tickers in buldle + 1
                    os.path.join(BASE, RAW_FLDR),
                    fields,
                    os.path.join(BASE, FN))


    print("this worked boss")
