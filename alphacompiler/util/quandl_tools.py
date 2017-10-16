
"""

Created by Peter Harrington (pbharrin) on 10/10/17.
"""

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