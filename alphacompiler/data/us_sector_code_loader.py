
"""
Downloads sector codes from NASDAQ.  For exchanges: NASDAQ, NYSE, AMEX

Created by Peter Harrington (pbharrin) on 10/21/17.
The download also provides industries within sectors.
"""

# this gets all the data for the three exchanges 6000+ tickers
BASE_URL = "http://www.nasdaq.com/screening/companies-by-industry.aspx?&render=download"


# NASDAQ sectors, not the same as Morningstar
SECTORS = {"Basic Industries": 0,
           "Capital Goods": 1,
           "Consumer Durables": 2,
           "Consumer Non-Durables": 3,
           "Consumer Services": 4,
           "Energy": 5,
           "Finance": 6,
           "Health Care": 7,
           "Miscellaneous": 8,
           "Public Utilities": 9,
           "Technology": 10,
           "Transportation": 11,
           "n/a": -1}

def download_list_for_sector(sector_name):
    """Downloads a list from nasdaq.com, and saves it do disk."""
    pass

if __name__ == '__main__':
    print("all done boss")