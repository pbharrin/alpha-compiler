
"""
Tools to work with the limitations of Zipline's benchmark tools.

Created by Peter Harrington (pbharrin) on 3/20/19.
"""

from trading_calendars import get_calendar
from zipline.data.loader import ensure_treasury_data
import pandas as pd

BENCHMARK_DATA_FILE = '/Users/peterharrington/Downloads/HistoricalQuotes.csv'


def ensure_benchmark_data_from_file():
    """
    Loads a file of benchmark data and calculates returns.
    :return: pd.Series of returns
    """
    df = pd.read_csv(BENCHMARK_DATA_FILE)
    df.index = pd.DatetimeIndex(df['date'])
    df = df.sort_index().tz_localize('UTC')

    # drop date, volume, "open","high","low"
    df = df.drop(["date", "volume", "open", "high", "low"], axis=1)

    # compute the delta
    df = df / df.shift(1) - 1.0

    # drop first row
    df = df.iloc[1:]

    return df.close  # return a pd.Series


def load_market_data_from_file(trading_day=None, trading_days=None, bm_symbol='SPY',
                     environ=None):
    """
    This is a drop in replacement for zipline.data.loader.load_market_data

    """
    if trading_day is None:
        trading_day = get_calendar('NYSE').trading_day
    if trading_days is None:
        trading_days = get_calendar('NYSE').all_sessions

    first_date = trading_days[0]
    now = pd.Timestamp.utcnow()

    # we will fill missing benchmark data through latest trading date
    last_date = trading_days[trading_days.get_loc(now, method='ffill')]

    # load the benchmark returns from a file not a the API
    br = ensure_benchmark_data_from_file()

    # below is copied verbatim from zipline.data.loader.load_market_data
    tc = ensure_treasury_data(
        bm_symbol,
        first_date,
        last_date,
        now,
        environ,
    )

    # combine dt indices and reindex using ffill then bfill
    all_dt = br.index.union(tc.index)
    br = br.reindex(all_dt, method='ffill').fillna(method='bfill')
    tc = tc.reindex(all_dt, method='ffill').fillna(method='bfill')

    benchmark_returns = br[br.index.slice_indexer(first_date, last_date)]
    treasury_curves = tc[tc.index.slice_indexer(first_date, last_date)]
    return benchmark_returns, treasury_curves


if __name__ == '__main__':
    ensure_benchmark_data_from_file()
    print('all done boss')