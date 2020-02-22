
"""
Utilities to format data properly for risk attribution.

Created by Peter Harrington (pbharrin) on 11/15/18.
"""
import pandas as pd
import pyfolio as pf
from risk_factors import calc_exposures_to_equities, SECTOR_ETF_RETURNS_FILE, GICSNAME2ETF
from alphacompiler.util.zipline_data_tools import get_all_assets_for_bundle
from zipline.data.bundles.core import load, register
import os


# sector raw data file
SECTOR_RAW_DATA = '/Users/peterharrington/Downloads/sector_etf_data.csv'
EQUITIES_OF_INTEREST_FILE = 'equities_of_interest.txt'
FACTOR_LOADINGS_FILE = 'factor_loadings.h5'
RISK_FACTORS = ['SMB', 'HML', 'MOMENTUM', 'VOL', 'STREVERSAL']


def create_equities_of_interest(backtest_fn, outputfn_fn):
    """Finds equities used in a backtest and writes them to a file.
    This is done to speed up risk attribution calculations.
    Returns the number of equities saved. (for testing)

    There may be better ways of doing this.  """
    results = pd.read_pickle(backtest_fn)
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(results)
    s = set(positions.columns.tolist())
    s.remove('cash')
    print('saving {} assets to {}'.format(len(s), outputfn_fn))
    fw = open(EQUITIES_OF_INTEREST_FILE, 'w')
    for asset in s:
        fw.write('{}\n'.format(asset.symbol))

    fw.close()
    return len(s)

def create_sector_returns_file_yahoo(datadir):
    """Creates a file of returns for sector ETFs, assumes the data came from a Yahoo finance.
    Assumes the data is stored in a common folder and the files are named TICKER.csv.
    (That's how they come from yahoo.)"""
    for fn in os.listdir(datadir):
        print "opening: ", fn

    # df_all = pd.read_csv(SECTOR_RAW_DATA, index_col='date', parse_dates=['date'], na_values=['NA'])
    # desired_columns = ['date', 'TSYMBOL', 'PRC']
    # df_all = df_all.drop([col for col in df_all.columns if col not in desired_columns], axis=1)
    # print df_all.index
    # df_all.index = df_all.index.tz_localize('UTC')
    # print df_all.index
    #
    # tickers = df_all.TSYMBOL.unique()
    # print(tickers)
    # data_f = {}
    # for ticker in tickers:
    #     data_f[ticker] = df_all[df_all.TSYMBOL == ticker]['PRC']
    #
    # df_stacked = pd.DataFrame(data=data_f)
    # print 'df_stacked: ', df_stacked.pct_change()[1:]
    # returns = df_stacked.pct_change()[1:]
    # # save to file
    # returns.to_csv(SECTOR_ETF_RETURNS_FILE)


def create_sector_returns_file_CRSP():
    """Creates a file of returns for sector ETFs, assumes the data came from a CRSP dump.  """
    df_all = pd.read_csv(SECTOR_RAW_DATA, index_col='date', parse_dates=['date'], na_values=['NA'])
    desired_columns = ['date', 'TSYMBOL', 'PRC']
    df_all = df_all.drop([col for col in df_all.columns if col not in desired_columns], axis=1)
    print df_all.index
    df_all.index = df_all.index.tz_localize('UTC')
    print df_all.index

    tickers = df_all.TSYMBOL.unique()
    print(tickers)
    data_f = {}
    for ticker in tickers:
        data_f[ticker] = df_all[df_all.TSYMBOL == ticker]['PRC']

    df_stacked = pd.DataFrame(data=data_f)
    print 'df_stacked: ', df_stacked.pct_change()[1:]
    returns = df_stacked.pct_change()[1:]
    # save to file
    returns.to_csv(SECTOR_ETF_RETURNS_FILE)


def create_equities_of_interest_file_and_test():
    """Test for create_equities_of_interest file"""
    n = create_equities_of_interest('../backtest/perf_full.pkl', EQUITIES_OF_INTEREST_FILE)

    # now make sure we have the same number of assets
    asset_symbols = [line.strip() for line in open(EQUITIES_OF_INTEREST_FILE).readlines()]
    assert len(asset_symbols) == n


def get_trading_days(bundle, start_date, end_date):
    """Gets the trading days between start_date and end_date inclusive, for a given bundle.
    Will also return the trading calendar."""
    bundle_data = load(bundle, os.environ, None)
    cal = bundle_data.equity_daily_bar_reader.trading_calendar.all_sessions
    return cal[(cal >= start_date) & (cal <= end_date)], cal


def create_factor_loadings_files():
    """Creates a dataframe for each risk factor, the dataframes are persisted to files.
    Each dataframe has dates as the index, and Assets as the columns, with values being the factor
    loadings of each equity of each day.  """

    data_dates = ('2008-01-01', '2016-12-31')
    backtest_dates = ('2012-01-04', '2016-12-31')
    pd_data_dates = (pd.to_datetime(data_dates[0], utc=True), pd.to_datetime(data_dates[1], utc=True))

    register('crsp', int)  # dummy register

    # get Assets for all symbols stored
    all_assets = get_all_assets_for_bundle('crsp')
    symbol2asset_map = dict(map(lambda x: (x.symbol, x), all_assets))

    asset_symbols = [line.strip() for line in open(EQUITIES_OF_INTEREST_FILE).readlines()]
    equities_of_interest = map(lambda x: symbol2asset_map[x], asset_symbols)
    print equities_of_interest

    # create continer (DataFrame for factor loadings)
    # columns=equities_of_interest, rows=dates
    trading_days_in_bt, cal = get_trading_days('crsp', backtest_dates[0], backtest_dates[1])  # index for dates
    factor_loadings = {}  # dictonary of DataFrames
    for factor in RISK_FACTORS:
        factor_loadings[factor] = pd.DataFrame(index=trading_days_in_bt, columns=equities_of_interest)

    # loop to go over all days in backtest, calculate loadings over two years and pack in Pandas Panel
    cal_list = cal.tolist()
    for day in trading_days_in_bt:
        i = cal_list.index(day)
        print cal[i - 504], day
        factor_loading_calc_period = (cal[i - 504], day)

        one_day_exposures = calc_exposures_to_equities(equities_of_interest, 'crsp', pd_data_dates, factor_loading_calc_period)
        one_day_t = one_day_exposures.transpose()
        print one_day_t  # this contains all risk factors

        for factor in RISK_FACTORS:  # pack loadings by factor and date
            factor_loadings[factor].loc[factor_loading_calc_period[1]] = one_day_t.loc[factor]

    # create Panel of factor_loadings and save to HDF5 file
    pd.Panel(factor_loadings).to_hdf(FACTOR_LOADINGS_FILE, 'key', mode='w')


def verify_factor_loadins_file():
    p = pd.read_hdf(FACTOR_LOADINGS_FILE, 'key')
    # print p['HML']
    print p.size
    print p.shape
    for item in p.items:
        assert item in RISK_FACTORS
        print(item)


if __name__ == '__main__':
    # create_equities_of_interest_file_and_test()
    create_sector_returns_file_yahoo('/Users/peterharrington/Downloads/sector_etf')
    # create_factor_loadings_files()
    # verify_factor_loadins_file()

    print("ISYMFS")
