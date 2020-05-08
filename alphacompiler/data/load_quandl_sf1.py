
# Code to load raw data from Quandl/SF1
# https://www.quandl.com/data/SF1-Core-US-Fundamentals-Data/
# requires the python Quandl package, and the 
# Quandl API key to be set as an ENV variable QUANDL_API_KEY.

import quandl

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from alphacompiler.util.sparse_data import pack_sparse_data
from alphacompiler.util import quandl_tools
import alphacompiler.util.load_extensions  # this simply loads the extensions

from logbook import Logger
import datetime
import os
import pandas as pd

BASE = os.path.dirname(os.path.realpath(__file__))
DS_NAME = 'SHARADAR/SF1'   # quandl DataSet code
RAW_FLDR = "raw"  # folder to store the raw text file
START_DATE = '2009-01-01'  # this is only used for getting data from the API
END_DATE = datetime.datetime.today().strftime('%Y-%m-%d')

ZIPLINE_DATA_DIR = '/Users/peterharrington/.zipline/data/'  # TODO: get this from Zipline api
FN = "SF1.npy"  # the file name to be used when storing this in ~/.zipline/data

DUMP_FILE = '/Users/peterharrington/Downloads/SHARADAR_SF1_2daa4baaad2a300c166b5c0f7e546bd1.csv'

log = Logger('load_quandl_sf1.py')



def populate_raw_data_from_dump(tickers2sid, fields, dimensions, raw_path):
    """
    Populates the raw/ folder based on a single dump download.

    :param tickers2sid: a dict with the ticker string as the key and the SID
    as the value
    :param fields: a list of field names
    :param dimensions: a list with dimensions for each field in fields
    :param raw_path: the path to the folder to write the files.
    """
    assert len(fields) == len(dimensions)

    df = pd.read_csv(DUMP_FILE)  # open dump file

    df = df[['ticker', 'dimension', 'datekey'] + fields]  # remove columns not in fields
    for tkr, df_tkr in df.groupby('ticker'):
        print('processing: ', tkr)

        sid = tickers2sid.get(tkr)
        if sid is None:
            print('no sid found for: {}'.format(tkr))
            continue

        df_tkr = df_tkr.rename(columns={'datekey': 'Date'}).set_index('Date')

        # loop over the fields and dimensions
        series = []
        for i, field in enumerate(fields):
            s = df_tkr[df_tkr.dimension == dimensions[i]][field]
            series.append(s)
        df_tkr = pd.concat(series, axis=1)
        print("AFTER reorganizing")
        print(df_tkr)

        # write raw file: raw/
        df_tkr.to_csv(os.path.join(raw_path, "{}.csv".format(sid)))



def populate_raw_data_from_api(tickers, fields, dimensions, raw_path):
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


def demo():  # demo works on free data
    tickers = {"WMT":3173, "HD":2912, "DOGGY":69, "CSCO":2809}
    fields = ["GP", "CAPEX", "EBIT", "ASSETS"]
    populate_raw_data_from_api(tickers, fields, os.path.join(BASE, RAW_FLDR))


def all_tickers_for_bundle_from_api(fields, dims, bundle_name, raw_path=os.path.join(BASE, RAW_FLDR)):
    tickers = get_ticker_sid_dict_from_bundle(bundle_name)
    populate_raw_data_from_api(tickers, fields, dims, raw_path)


def all_tickers_for_bundle_from_dump(fields, dims, bundle_name, raw_path=os.path.join(BASE, RAW_FLDR)):
    tickers = get_ticker_sid_dict_from_bundle(bundle_name)
    populate_raw_data_from_dump(tickers, fields, dims, raw_path)


def num_tkrs_in_bundle(bundle_name):
    return len(get_ticker_sid_dict_from_bundle(bundle_name))


if __name__ == '__main__':

    # fields = ["marketcap", "pb"]  # minimum fundamentals for risk
    # fields = ["ROE", "BVPS", "SPS", "FCFPS", "PRICE"]
    fields0 = ['netinc', 'equity', 'bvps', 'sps', 'fcfps', 'price']  # basic QV
    dimensions0 = ['ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ']

    # magic formula
    # fields1 = ['ebit', 'workingcapital', 'assets', 'assetsc', 'intangibles', 'ev', 'marketcap']
    # dimensions1 = ['ART', 'ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ']

    # Marc's turntup Quality companies in an uptrend
    # fields2 = ['roe', 'marketcap', 'de', 'debt', 'debtnc']
    # dimensions2 = ['ART', 'ARQ', 'ARQ', 'ARQ', 'ARQ']

    # more value
    fields3 = ['ebitda', 'ev', 'pe', 'pe1', 'marketcap']
    dimensions3 = ['ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ']

    fields = fields0 + fields3
    dimensions = dimensions0 + dimensions3

    BUNDLE_NAME = 'sep'
    num_tickers = num_tkrs_in_bundle(BUNDLE_NAME)
    print('number of tickers: ', num_tickers)

    all_tickers_for_bundle_from_dump(fields, dimensions, 'sep')  # downloads the data to /raw
    # pack_sparse_data(num_tickers + 1,  # number of tickers in buldle + 1
    #                 os.path.join(BASE, RAW_FLDR),
    #                 fields,
    #                 ZIPLINE_DATA_DIR + FN)  # write directly to the zipline data dir


    print("this worked boss")
