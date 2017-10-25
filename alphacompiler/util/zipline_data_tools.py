
"""

Created by Peter Harrington (pbharrin) on 10/10/17.
"""

import os
from zipline.data.bundles.core import load
import numpy as np

def get_tickers_from_bundle(bundle_name):
    """Gets a list of tickers from a given bundle"""
    bundle_data = load(bundle_name, os.environ, None)

    # get a list of all sids
    lifetimes = bundle_data.asset_finder._compute_asset_lifetimes()
    all_sids = lifetimes.sid

    # retreive all assets in the bundle
    all_assets = bundle_data.asset_finder.retrieve_all(all_sids)

    # return only tickers
    return map(lambda x: (x.symbol, x.sid), all_assets)


def get_ticker_sid_dict_from_bundle(bundle_name):
    """Packs the (ticker,sid) tuples into a dict."""
    all_equities = get_tickers_from_bundle('quantopian-quandl')
    return dict(all_equities)

if __name__ == '__main__':

    ae_d = get_ticker_sid_dict_from_bundle('quantopian-quandl')
    print "max sid: ", max(ae_d.values())
    print "min sid: ", min(ae_d.values())

    print "WMT sid:",ae_d["WMT"]
    print "HD sid:",ae_d["HD"]
    print "CSCO sid:",ae_d["CSCO"]