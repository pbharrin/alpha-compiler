
# Code to load raw data from Quandl/SF1
# https://www.quandl.com/data/SF1-Core-US-Fundamentals-Data/
# requires the python Quandl package, and the 
# Quandl API key to be set as an ENV variable QUANDL_API_KEY.

import quandl

from util import zipline_data_tools
from util import quandl_tools
from logbook import Logger
import datetime

DS_NAME = "SF1"   # quandl DataSet code
RAW_FLDR = "raw"  # folder to store the raw text file
VAL_COL_NAME = "Value"
START_DATE = '2010-01-01'
END_DATE = datetime.datetime.today().strftime('%Y-%m-%d')


log = Logger('load_quandl_sf1.py')

def populate_raw_data(tickers, fields):
    quandl_tools.set_api_key()

    for ticker in tickers:
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
            all_fields.to_csv("{}/{}.csv".format(RAW_FLDR, ticker))
        except quandl.errors.quandl_error.NotFoundError:
            print("error with ticker: {}".format(ticker))


def demo():
    # demo works on free data
    tickers = ["WMT", "HD", "CSCO"]
    fields = ["GP", "CAPEX", "EBIT", "ASSETS"]
    populate_raw_data(tickers, fields)


def all_tickers_for_bundle():
    #tickers = zipline_data_tools.get_tickers_from_bundle('quantopian-quandl')
    tickers = ["DOGGY", "WMT", "HD", "CSCO"]
    print(tickers[:20])
    fields = ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]
    populate_raw_data(tickers, fields)


if __name__ == '__main__':

    #demo()

    all_tickers_for_bundle()

    print("this worked boss")
