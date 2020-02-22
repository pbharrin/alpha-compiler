
"""
Module to calculate risk factor exposures.

Created by Peter Harrington (pbharrin) on 2/22/18.
"""
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import CustomFactor, Returns, VWAP, SimpleMovingAverage, AverageDollarVolume, RSI
from zipline.utils.paths import zipline_root

# from alphacompiler.data.compustat_fundamentals import Fundamentals
from alphacompiler.data.sf1_fundamentals import Fundamentals
# from eclongshort.alphafactory.ASTtoQFactor import make_pipeline_code  # TODO: remove this
# from eclongshort.alphafactory.AlphaTree import parse_AST  # TODO: remove this
from alphacompiler.util.zipline_data_tools import make_pipeline_engine

import pandas as pd
import numpy as np
import pickle

from scipy import stats
import os
from dateutil.tz import tzutc
from dateutil.parser import parse
import statsmodels.api as sm
from alphacompiler.util.zipline_data_tools import fast_corr, fast_cov

SECTOR_ETF_RETURNS_FILE = '../data/sector_etf_returns.csv'  # todo: put this in constants file
MAPPING_FILE = '../data/sector_mappings.pkl'
NUM_SEC = 20         # Number of securities per side (long or short)
SPY_PATH = "%s/data/SPY.csv" % zipline_root()

GICSNAME2ETF = {'Energy': 'XLE',
                'Materials': 'XLB',
                'Industrials': 'XLI',
                'Consumer Discretionary': 'XLY',
                'Consumer Staples': 'XLP',
                'Health Care': 'XLV',
                'Financials': 'XLF',
                'Information Technology': 'XLK',
                'Communication Services': 'IYZ',
                'Utilities': 'XLU',
                'Real Estate': 'IYR',
                'Unmappable': 'SPY'}


# SECTOR_MAPPING = pickle.load(open(MAPPING_FILE, 'rb'))

def date_utc(s):
    """For setting the timezone while parsing dates."""
    return parse(s, tzinfos=tzutc)


class Momentum(CustomFactor):
    """delay(close, 19)/delay(close, 251)"""
    inputs = [USEquityPricing.close]
    window_length = 252

    def compute(self, today, assets, out, close):
        out[:] = close[-20] / close[0]


class Volatility(CustomFactor):
    inputs = [USEquityPricing.close]
    window_length = 252

    def compute(self, today, assets, out, close):
        close = pd.DataFrame(data=close, columns=assets)

        # Since we are going to rank largest is best we need to invert the sdev.
        out[:] = 1 / np.log(close).diff().std()


def make_pipeline():
    """Sets up the pipeline"""
    dollar_volume = AverageDollarVolume(window_length=20)
    adv1000 = dollar_volume.top(1000)
    fd = Fundamentals(mask=adv1000)
    market_cap = fd.cshoq * fd.prccq  # this is how to calculate market cap with Computstat fields
    book_equity = fd.seqq - fd.PS     # this is a quick way to calculate book_equity
    book_to_price = book_equity/market_cap
    biggest = market_cap.top(500, mask=adv1000)
    smallest = market_cap.bottom(500, mask=adv1000)

    highpb = book_to_price.top(500, mask=adv1000)
    lowpb = book_to_price.bottom(500, mask=adv1000)

    momentum = Momentum(mask=adv1000)  # momentum
    high_momentum = momentum.top(500, mask=adv1000)
    low_momentum = momentum.bottom(500, mask=adv1000)

    volatility = Volatility(mask=adv1000)
    highvol = volatility.top(500, mask=adv1000)
    lowvol = volatility.bottom(500, mask=adv1000)

    streversal = RSI(window_length=14, mask=adv1000)
    high_streversal = streversal.top(500, mask=adv1000)
    low_streversal = streversal.bottom(500, mask=adv1000)


    universe = biggest | smallest | highpb | lowpb | low_momentum | high_momentum

    return Pipeline(
        columns={'returns': Returns(window_length=2),
                 # 'market_cap': market_cap,  # not needed
                 # 'book_to_price': book_to_price,  # not needed
                 'biggest': biggest,
                 'smallest': smallest,
                 'highpb': highpb,
                 'lowpb': lowpb,
                 # 'momentum': momentum,  # not needed
                 'low_momentum': low_momentum,
                 'high_momentum': high_momentum,
                 # 'volatility': volatility, # not needed
                 'highvol': highvol,
                 'lowvol': lowvol,
                 # 'streversal': streversal,  # not needed
                 'high_streversal': high_streversal,
                 'low_streversal': low_streversal},
        screen=universe
    )


def get_factor_returns(alpha_str, bundle, data_dates, run_dates):
    """Calculates factor returns."""

    # create the pipeline engine
    spe = make_pipeline_engine(bundle, data_dates)
    my_pipeline = make_pipeline()

    results = spe.run_pipeline(my_pipeline, run_dates[0], run_dates[1])
    print results.head()

    # mean returns of the biggest/smallest by market cap
    R_biggest = results[results.biggest]['returns'].groupby(level=0).mean()
    R_smallest = results[results.smallest]['returns'].groupby(level=0).mean()

    R_highpb = results[results.highpb]['returns'].groupby(level=0).mean()
    R_lowpb = results[results.lowpb]['returns'].groupby(level=0).mean()

    R_low_momentum = results[results.low_momentum]['returns'].groupby(level=0).mean()
    R_high_momentum = results[results.high_momentum]['returns'].groupby(level=0).mean()

    R_lowvol = results[results.lowvol]['returns'].groupby(level=0).mean()
    R_highvol = results[results.highvol]['returns'].groupby(level=0).mean()

    R_low_streversal = results[results.low_streversal]['returns'].groupby(level=0).mean()
    R_high_streversal = results[results.high_streversal]['returns'].groupby(level=0).mean()

    R_longs = results[results.my_longs]['returns'].groupby(level=0).mean()
    R_shorts = results[results.my_shorts]['returns'].groupby(level=0).mean()


    SMB = R_smallest - R_biggest
    HML = R_highpb - R_lowpb
    MOMENTUM = R_high_momentum - R_low_momentum
    VOL = R_highvol - R_lowvol
    STREVERSAL = R_high_streversal - R_low_streversal
    MyReturns = R_longs - R_shorts         #

    smb_n_hml = pd.DataFrame({
        'SMB': SMB,                # company size
        'HML': HML,                # company PB ratio  value
        'MOMENTUM': MOMENTUM,
        'VOL': VOL,
        'STREVERSAL': STREVERSAL,  # short term reversal
        'MyReturns': MyReturns
    }, columns=["SMB", "HML", "MOMENTUM", "VOL", "STREVERSAL", "MyReturns"]).shift(periods=-1).dropna()

    # # get SPY data (not included in quantopian-quandl bundle)
    # spy_series = pd.read_csv(SPY_PATH, index_col=0, parse_dates=True, usecols=[0, 4], date_parser=date_utc)
    # spy_series = spy_series[spy_series.index >= run_dates[0]]  # added to only use same dates as data from the Pipeline
    #
    # assert "close" in spy_series.columns
    # MKT = spy_series.pct_change()[1:].rename(columns={"close": "MKT"})  # market returns
    #
    # return pd.concat([MKT, smb_n_hml], axis=1).dropna()
    return smb_n_hml


def calc_exposures_to_factor(alpha_str, bundle, data_dates, run_dates):
    """Calculates the factor exposures to a given alpha on a given day, and returns them."""

    # get data
    F = get_factor_returns(alpha_str, bundle, data_dates, run_dates)

    # calculate exposures
    y = F["MyReturns"]
    x = sm.add_constant(F.drop(['MyReturns'], axis=1))  # add a column of 1s to F

    # remove outliers
    y_inlier = y[np.abs(y - y.mean()) <= (3 * y.std())]
    x_inlier = x[np.abs(y - y.mean()) <= (3 * y.std())]

    # fit OLS model
    result = sm.OLS(y_inlier, x_inlier).fit()
    print "result.rsquared: ", result.rsquared

    return result.params, result.rsquared


def get_equity_returns(bundle, data_dates, run_dates):
    """Gets the close price for all assets over all trading days in run_dates."""

    pipe = Pipeline(columns={'Close': USEquityPricing.close.latest},)

    # create the pipeline engine
    spe = make_pipeline_engine(bundle, data_dates)
    # stocks = spe.run_pipeline(pipe, run_dates[0], run_dates[1])

    stocks = spe.run_pipeline(pipe, run_dates[0], run_dates[1])

    unstacked_results = stocks.unstack() # what does this do?
    prices = (unstacked_results['Close'].fillna(method='ffill').fillna(method='bfill')
              .dropna(axis=1, how='any').shift(periods=-1).dropna())
    return prices.pct_change()#[1:]


def get_style_cov(bundle, data_dates, run_dates):
    """Calculates the style covariance matrix."""

    my_pipeline = make_pipeline(Momentum)  # Momentum is just placeholder to reuse code
    spe = make_pipeline_engine(bundle, data_dates)

    results = spe.run_pipeline(my_pipeline,
                               pd.to_datetime(run_dates[0], utc=True),
                               pd.to_datetime(run_dates[1], utc=True))
    results.drop(['my_factor', 'my_longs', 'my_shorts'], axis=1)
    # print results.head()

    # mean returns of the biggest/smallest by market cap
    R_biggest = results[results.biggest]['returns'].groupby(level=0).mean()
    R_smallest = results[results.smallest]['returns'].groupby(level=0).mean()

    R_highpb = results[results.highpb]['returns'].groupby(level=0).mean()
    R_lowpb = results[results.lowpb]['returns'].groupby(level=0).mean()

    R_low_momentum = results[results.low_momentum]['returns'].groupby(level=0).mean()
    R_high_momentum = results[results.high_momentum]['returns'].groupby(level=0).mean()

    R_lowvol = results[results.lowvol]['returns'].groupby(level=0).mean()
    R_highvol = results[results.highvol]['returns'].groupby(level=0).mean()

    R_low_streversal = results[results.low_streversal]['returns'].groupby(level=0).mean()
    R_high_streversal = results[results.high_streversal]['returns'].groupby(level=0).mean()

    SMB = R_smallest - R_biggest
    HML = R_highpb - R_lowpb
    MOMENTUM = R_high_momentum - R_low_momentum
    VOL = R_highvol - R_lowvol
    STREVERSAL = R_high_streversal - R_low_streversal

    smb_n_hml = pd.DataFrame({
        'SMB': SMB,                # company size
        'HML': HML,                # company PB ratio  value
        'MOMENTUM': MOMENTUM,
        'VOL': VOL,
        'STREVERSAL': STREVERSAL  # short term reversal
    }, columns=["SMB", "HML", "MOMENTUM", "VOL", "STREVERSAL"]).shift(periods=-1).dropna()

    # get SPY data (not included in bundle)
    spy_series = pd.read_csv(SPY_PATH, index_col=0, parse_dates=True, usecols=[0, 4], date_parser=date_utc)
    assert "close" in spy_series.columns

    MKT = spy_series.pct_change()[1:].rename(columns={"close": "MKT"})  # market returns

    F = pd.concat([MKT, smb_n_hml], axis=1).dropna()
    print(F)

    # calculate cov
    return F.cov()


def remove_sector_returns(asset, y, sector_returns):
    """Computes the beta to the sector to which the asset belongs, over the last two years."""

    # choose the correct sector (use sector map)
    # print SECTOR_MAPPING[asset]

    # use sector mapping (to etf) to get data for regression
    r_sect = sector_returns[GICSNAME2ETF[SECTOR_MAPPING[asset]]]  # TODO: get asset to sector map
    x = sm.add_constant(r_sect)

    # compute Beta to the relevant sector returns (r_sect) using OLS
    # remove outliers
    outlier_mask = (np.abs(y - y.mean()) <= (3 * y.std()))
    y_inlier = y[outlier_mask]
    x_inlier = x[outlier_mask]

    result = sm.OLS(y_inlier, x_inlier).fit()
    return y - np.dot(x, result.params) # return residual


def calc_exposures_to_equities(equities_of_interest, bundle, data_dates, run_dates):
    """Calculates the every stock's exposure to the five style factors, using my
    implementation of the Quantopian Risk model.
    https://media.quantopian.com/quantopian_risk_model_whitepaper.pdf
    (Mostly the same, except there are no complimentary stocks)

    equities_of_interest is a set of Zipline Assets for equites we want factor loadings
    bundle is string denoting the Zipline bundle to be used.
    data_dates and run_dates should be a tuple of Pandas datetime.
    """

    # get factor data, used dummy factor
    F = get_factor_returns("close", bundle, data_dates, run_dates)
    # R is DF with stocks as columns, dates as rows
    print 'The number of timestamps in F is {} from {} to {}.'.format(F.shape[0], run_dates[0], run_dates[1])

    R = get_equity_returns(bundle, data_dates, run_dates)
    # R is DF with stocks as columns, dates as rows

    print "The universe we define includes {} assets.".format(R.shape[1])
    print 'The number of timestamps in R is {} from {} to {}.'.format(R.shape[0], run_dates[0], run_dates[1])
    assets = R.columns

    x = sm.add_constant(F.drop(['MyReturns'], axis=1))  # add a column of 1s to F, and drop dummy column

    # create DF for packing exposures
    packed_exposures = pd.DataFrame(index=equities_of_interest, #index=assets, # use index=assets for full universe
                                    columns=["const", "SMB", "HML", "MOMENTUM", "VOL", "STREVERSAL", "rsquared"])

    # load sector returns
    sector_returns = pd.read_csv(SECTOR_ETF_RETURNS_FILE, index_col='date',
                                 parse_dates=['date'], na_values=['NA'], date_parser=pd.to_datetime)
    sector_returns.index = sector_returns.index.tz_localize('UTC')

    # mask = (sector_returns.index >= R.index[0]) & (sector_returns.index <= R.index[-1])
    sector_returns = sector_returns.loc[R.index]
    print 'The number of timestamps in sector_returns is {} from {} to {}'.format(sector_returns.shape[0],
                                                                                  sector_returns.index[0],
                                                                                  sector_returns.index[-1])

    for i in assets:  # iterate over all the assets
        print('calculating exposures to: {}'.format(i))
        if i not in equities_of_interest:
            print('skipping')
            continue

        y = R.loc[:, i]

        # calculate beta to sector and subtract off beta * sector_returns
        eps_sector = remove_sector_returns(i, y, sector_returns)

        # remove outliers
        outlier_mask = (np.abs(eps_sector - eps_sector.mean()) <= (3 * eps_sector.std()))
        y_inlier = eps_sector[outlier_mask]
        x_inlier = x[outlier_mask]

        result = sm.OLS(y_inlier, x_inlier).fit()

        # pack betas in data structure
        packed_exposures.loc[i, :] = result.params
        packed_exposures.loc[i, "rsquared"] = result.rsquared

    return packed_exposures, R[1:]


if __name__ == '__main__':

    print("ISYMFS")