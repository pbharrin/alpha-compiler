"""
Custom Bundle for Loading the SEP daily stock dataset from Sharadar, from a dump.

Created by Peter Harrington (pbharrin) on 3/8/18.
"""


import pandas as pd
from zipline.utils.calendars import get_calendar
import sys


METADATA_HEADERS = ['start_date', 'end_date', 'auto_close_date',
                    'symbol', 'exchange', 'asset_name']


def from_sep_dump(file_name, start=None, end=None):
    """
    ticker,date,open,high,low,close,volume,dividends,lastupdated
    A,2008-01-02,36.67,36.8,36.12,36.3,1858900.0,0.0,2017-11-01

    To use this make your ~/.zipline/extension.py look similar this:

    from zipline.data.bundles import register
    from alphacompiler.data.loaders.sep_quandl import from_sep_dump

    register("sep",
         from_sep_dump("/path/to/your/SEP/dump/SHARADAR_SEP_69.csv"),)

    """
    us_calendar = get_calendar("NYSE").all_sessions

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):

        print("starting ingesting data from: {}".format(file_name))

        # read in the whole dump (will require ~7GB of RAM)
        df = pd.read_csv(file_name, index_col='date',
                         parse_dates=['date'], na_values=['NA'])

        # drop unused columns, dividends will be used later
        df = df.drop(['lastupdated', 'dividends'], axis=1)

        # drop row with NaNs or the loader will turn all columns to NaNs
        # df = df.dropna()

        uv = df.ticker.unique()  # get unique m_tickers (Zacks primary key)

        # counter of valid securites, this will be our primary key
        sec_counter = 0
        data_list = []  # list to send to daily_bar_writer
        metadata_list = []  # list to send to asset_db_writer (metadata)
        missing_counter = 0

        # iterate over all the unique securities and pack data, and metadata
        # for writing
        for tkr in uv:
            df_tkr = df[df['ticker'] == tkr]

            row0 = df_tkr.ix[0]  # get metadata from row

            print(" preparing {}".format(row0["ticker"]))

            # check to see if there are missing dates in the middle
            this_cal = us_calendar[(us_calendar >= df_tkr.index[0]) & (us_calendar <= df_tkr.index[-1])]
            if len(this_cal) != df_tkr.shape[0]:
                print "MISSING interstitial dates for: %s using forward fill" % row0["ticker"]
                df_desired = pd.DataFrame(index=this_cal.tz_localize(None))
                df_desired = df_desired.join(df_tkr)
                df_tkr = df_desired.fillna(method='ffill')

            # update metadata; 'start_date', 'end_date', 'auto_close_date',
            # 'symbol', 'exchange', 'asset_name'
            metadata_list.append((df_tkr.index[0],
                                  df_tkr.index[-1],
                                  df_tkr.index[-1] + pd.Timedelta(days=1),
                                  row0["ticker"],
                                  "SEP",  # all have exchange = SEP
                                  row0["ticker"]  # TODO: can we delete this?
                                  )
                                 )

            # drop metadata columns
            df_tkr = df_tkr.drop(['ticker'], axis=1)

            # pack data to be written by daily_bar_writer
            data_list.append((sec_counter, df_tkr))
            sec_counter += 1

        print("writing data for {} securities".format(len(metadata_list)))
        daily_bar_writer.write(data_list, show_progress=False)

        # write metadata
        asset_db_writer.write(equities=pd.DataFrame(metadata_list,
                                                    columns=METADATA_HEADERS))
        print("a total of {} securities were loaded into this bundle".format(
            sec_counter))


        # read in Dividend History # TODO: similar to CRSP
        adjustment_writer.write()  # TODO: delte this when dividends loaded properly


        # dfd = pd.read_csv(dvdend_file, index_col='div_ex_date',
        #                       parse_dates=['div_ex_date', 'per_end_date'],
        #                       na_values=['NA'])
        #
        # dfd = dfd.ix[START_DATE:]  # drop old data
        # # format dfd to have sid
        # adjustment_writer.write(dividends=dfd)

    return ingest
