
# Code to load raw data from Quandl/SF1
# requires the python Quandl package, and the 
# Quandl API key to be set as an ENV variable QUANDL_API_KEY.

import quandl
from os import environ
import sys


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

    jj = quandl.get('SF1/WMT_GP_MRQ')
    print(type(jj))
    print(jj)

    print("this worked boss")