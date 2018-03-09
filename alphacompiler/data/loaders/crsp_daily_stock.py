
"""
Custom Bundle for Loading the CRSP daily stock dataset.

Created by Peter Harrington (pbharrin) on 3/6/18.
"""


import pandas as pd
from zipline.utils.calendars import get_calendar
import sys


METADATA_HEADERS = ['start_date', 'end_date', 'auto_close_date',
                    'symbol', 'exchange', 'asset_name']


def from_crsp_dump(file_name, start=None, end=None):
    """
    Load data from a CRSP daily stock dump, the following fields are assumed to
    be in the dump: date, PERMCO SHRCD TSYMBOL PRIMEXCH PRC VOL OPENPRC ASKHI BIDLO DIVAMT FACPR DCLRDT RCRDDT PAYDT
    The dump is assumed to reside in a .csv file located at: file_name.

    To use this make your ~/.zipline/extension.py look similar this:

    from zipline.data.bundles import register
    from alphacompiler.data.loaders.crsp_daily_stock import from_crsp_dump

    register("crsp",
         from_crsp_dump("/path/to/your/CRSP/dump/7112bc373f6a4ba8.csv"),)

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

        # read in the whole dump
        df = pd.read_csv(file_name, index_col='date',
                         parse_dates=['date'], na_values=['NA'])

        uv = df.PERMNO.unique()  # get unique PERMNO (CRSP primary key, doesn't change)

        # counter of valid securites
        sec_counter = 0
        data_list = []  # list to send to daily_bar_writer
        metadata_list = []  # list to send to asset_db_writer (metadata)

        # iterate over all the unique securities (CRSP permno) and pack data,
        for permno in uv:
            df_tkr = df[df['PERMNO'] == permno]
            df_tkr = df_tkr[~df_tkr.index.duplicated(keep='first')]

            row0 = df_tkr.ix[0]  # get metadata from row

            if row0["PRIMEXCH"] == "X":  # skip exchange X
                continue

            print(" preparing {} / {} ".format(row0["TSYMBOL"],
                                               row0["PRIMEXCH"]))

            if pd.isnull(row0["TSYMBOL"]):
                print "no ticker skipping"
                continue

            # check to see if there are missing dates in the middle
            this_cal = us_calendar[(us_calendar >= df_tkr.index[0]) & (us_calendar <= df_tkr.index[-1])]
            if len(this_cal) != df_tkr.shape[0]:
                print "MISSING interstitial dates for: %s please fix" % row0["TSYMBOL"]
                sys.exit()

            # update metadata; 'start_date', 'end_date', 'auto_close_date',
            # 'symbol', 'exchange', 'asset_name'
            metadata_list.append((df_tkr.index[0],
                                  df_tkr.index[-1],
                                  df_tkr.index[-1] + pd.Timedelta(days=1),
                                  row0["TSYMBOL"],
                                  row0["PRIMEXCH"],
                                  permno,    # store CRSP permno for company name
                                  )
                                 )

            # drop metadata columns
            df_tkr.loc[:, 'close'] = abs(df_tkr['PRC'])  # take abs(close) for esitmated values

            df_tkr = df_tkr.drop(['TSYMBOL', 'PERMNO',
                                  'PERMCO', 'PRIMEXCH', 'PRC'], axis=1)

            # rename volume, high, low, open
            df_tkr = df_tkr.rename(columns={"VOL": "volume",
                                            "ASKHI": "high",
                                            "BIDLO": "low",
                                            "OPENPRC": "open"})

            # pack data to be written by daily_bar_writer
            data_list.append((int(permno), df_tkr))
            sec_counter += 1

        print("writing data for {} securities".format(len(metadata_list)))
        daily_bar_writer.write(data_list, show_progress=False)

        # write metadata
        asset_db_writer.write(equities=pd.DataFrame(metadata_list,
                                                    columns=METADATA_HEADERS))
        print("**a total of {} securities were loaded into this bundle **".format(
            sec_counter))

        # read in Dividend History
        # dataframe for dividends and splits
        dfds = pd.read_csv(file_name, index_col='date',
                             parse_dates=['date'], na_values=['NA'])

        # drop rows where FACPR is absent
        dfds = dfds.dropna()
        dfds = dfds.drop(["VOL", "ASKHI", "BIDLO", "OPENPRC",
                              'PERMCO', 'PRIMEXCH', 'PRC'], axis=1)
        dfds = dfds.rename(columns={'PERMNO': 'sid'})

        # if FACPR == 0, then those are dividend, else it is a split
        dfd = dfds[dfds["FACPR"] == 0.0]  # dividend
        dfd.loc[:, 'ex_date'] = dfd.index
        dfd = dfd.rename(columns={'DCLRDT': 'declared_date',
                                  'PAYDT': 'pay_date',
                                  'RCRDDT': 'record_date',
                                  'DIVAMT': 'amount'})
        dfd = dfd.drop(['FACPR', 'SHRCD', 'TSYMBOL'], axis=1)

        # move splits into desired format
        dfs = dfds[dfds["FACPR"] != 0.0]  # splits
        dfs.loc[:, 'effective_date'] = dfs.index
        dfs.loc[:, 'ratio'] = 1 + dfs.loc[:,'FACPR']  # TODO: double check this captures reverse splits properly
        dfs = dfs.drop(['SHRCD', 'TSYMBOL', 'DCLRDT', 'PAYDT', 'RCRDDT', 'DIVAMT', 'FACPR'], axis=1)

        # write
        adjustment_writer.write(dividends=dfd, splits=dfs)

    return ingest
