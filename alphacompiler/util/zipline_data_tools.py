
"""

Created by Peter Harrington (pbharrin) on 10/10/17.
"""

import os
from zipline.data.bundles.core import load
import numpy as np
from zipline.utils.math_utils import nanmean, nanstd

def fast_cov(m0, m1):
    """Improving the speed of cov()"""
    nan = np.nan
    isnan = np.isnan
    N, M = m0.shape
    #out = np.full(M, nan)
    allowed_missing_count = int(0.25 * N)

    independent = np.where(  # shape: (N, M)
        isnan(m0),
        nan,
        m1,
    )
    ind_residual = independent - nanmean(independent, axis=0)  # shape: (N, M)
    covariances = nanmean(ind_residual * m0, axis=0)           # shape: (M,)

    nanlocs = isnan(independent).sum(axis=0) > allowed_missing_count
    covariances[nanlocs] = nan
    return covariances


def fast_corr(m0, m1):
    """Improving the speed of correlation"""
    nan = np.nan
    isnan = np.isnan
    N, M = m0.shape
    out = np.full(M, nan)
    allowed_missing_count = int(0.25 * N)

    independent = np.where(  # shape: (N, M)
        isnan(m0),
        nan,
        m1,
    )
    ind_residual = independent - nanmean(independent, axis=0)  # shape: (N, M)
    covariances = nanmean(ind_residual * m0, axis=0)  # shape: (M,)

    # corr(x,y) = cov(x,y)/std(x)/std(y)
    std_v = nanstd(m0, axis=0)  # std(X)  could reuse ind_residual for possible speedup
    np.divide(covariances, std_v, out=out)
    std_v = nanstd(m1, axis=0)  # std(Y)
    np.divide(out, std_v, out=out)

    # handle NaNs
    nanlocs = isnan(independent).sum(axis=0) > allowed_missing_count
    out[nanlocs] = nan
    return out


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
    all_equities = get_tickers_from_bundle(bundle_name)
    return dict(all_equities)

if __name__ == '__main__':

    ae_d = get_ticker_sid_dict_from_bundle('quantopian-quandl')
    print("max sid: ", max(ae_d.values()))
    print("min sid: ", min(ae_d.values()))

    print("WMT sid:",ae_d["WMT"])
    print("HD sid:",ae_d["HD"])
    print("CSCO sid:",ae_d["CSCO"])
