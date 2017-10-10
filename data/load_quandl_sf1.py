
# Code to load raw data from Quandl/SF1
# requires the python Quandl package, and the 
# Quandl API key to be set as an ENV variable QUANDL_API_KEY.

import quandl
from os import environ
import sys

DS_NAME = "SF1"

# TODO: move this function to utils/
def set_api_key():
    """read QUANDL_API_KEY env variable, and set it."""
    try:
        api_key = environ["QUANDL_API_KEY"]
    except KeyError:
        print("could not read the env variable: QUANDL_API_KEY")
        sys.exit()
    quandl.ApiConfig.api_key = api_key


if __name__ == '__main__':

    set_api_key()

    # returns Pandas DataFrame with Date as index and Value
    #jj = quandl.get('SF1/WMT_GP_MRQ', start_date='2017-07-31', end_date='2017-07-31')

    tickers = ["WMT"]
    fields = ["GP", "CAPEX", "EBIT", "ASSETS"]
    at_time_of = False  # if this is true append "", else append "_MRQ"

    if at_time_of:
        suffix = ""
    else:
        suffix = "MRQ"
    # for every ticker in ticker_list get
    fields_a = map(lambda s:"%s_%s" % (s, suffix), fields)

    for ticker in tickers:
        for field in fields_a:
            query_str =  "%s/%s_%s" % (DS_NAME, ticker, field)
            df = quandl.get(query_str)
    #   fetch each field in fields
    #   join data of all fields and write to file: raw/

    #print(type(jj))
    #print(jj)

    print("this worked boss")
