
"""

Created by Peter Harrington (pbharrin) on 10/10/17.
"""

import os
from zipline.data.bundles.core import load

def get_tickers_from_bundle(bundle_name):
    """Gets a list of tickers from a given bundle"""
    bundle_data = load(bundle_name, os.environ, None)

    # get a list of all sids
    lifetimes = bundle_data.asset_finder._compute_asset_lifetimes()
    all_sids = lifetimes.sid

    # retreive all assets in the bundle
    all_assets = bundle_data.asset_finder.retrieve_all(all_sids)
    # return only tickers
    return map(lambda x: x.symbol, all_assets)


if __name__ == '__main__':
    get_tickers_from_bundle('quantopian-quandl')