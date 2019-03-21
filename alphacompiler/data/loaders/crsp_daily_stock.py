
"""
Custom Bundle for Loading the CRSP daily stock dataset.

Created by Peter Harrington (pbharrin) on 3/6/18.
"""


import pandas as pd
import numpy as np
from zipline.utils.calendars import get_calendar
import sys


METADATA_HEADERS = ['start_date', 'end_date', 'auto_close_date',
                    'symbol', 'exchange', 'asset_name']

HURRICANE_SANDY_ER = pd.Timestamp('2012-10-29')  # earnings release
HURRICANE_SANDY_FD = pd.Timestamp('2012-10-31')  # first day back
DIV_COLUMNS = ['DCLRDT', 'PAYDT', 'RCRDDT', 'DIVAMT', 'FACPR']

CRAZY_DATE = pd.Timestamp('2008-09-28')  # TODO: delete me


ALLOWED_MISSING_DATES_RATIO = 0.5   # ticker will be skipped if missing/possible > ratio


def from_crsp_dump(file_name, start=None, end=None):
    """
    Load data from a CRSP daily stock dump, the following fields are assumed to
    be in the dump: date, PERMCO TSYMBOL PRIMEXCH PRC VOL OPENPRC ASKHI BIDLO DIVAMT FACPR DCLRDT RCRDDT PAYDT
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
        permno2sid_map = {}
        skipped_permnos = []

        # iterate over all the unique securities (CRSP permno) and pack data,
        for permno in uv:
            df_tkr = df[df['PERMNO'] == permno]
            df_tkr = df_tkr[~df_tkr.index.duplicated(keep='first')]

            # could drop all rows where the ticker is null
            # df_tkr = df_tkr[~pd.isnull(df_tkr['TSYMBOL'])]

            row0 = df_tkr.ix[0]  # get metadata from row

            # need to find the first row with a ticker.
            if pd.isnull(row0['TSYMBOL']):
                print('no ticker on first line, finding first line with ticker')
                df_non_null_ticker = df_tkr[~pd.isnull(df_tkr['TSYMBOL'])]
                if df_non_null_ticker.shape[0] == 0:
                    print('no ticker, skipping')
                    skipped_permnos.append(permno)
                    continue

                first_index = df_non_null_ticker.index[0]
                print('first index with a ticker: ', first_index)
                df_tkr = df_tkr.loc[first_index:]
                row0 = df_tkr.ix[0]  # reset row0

                # # try 2nd row
                # if df_tkr.shape[0] > 1 and not pd.isnull(df_tkr.ix[1]["TSYMBOL"]):
                #     print('first row missing ticker, dropping first row')
                #     df_tkr = df_tkr.drop(df_tkr.index[0])
                #     row0 = df_tkr.ix[0]
                # else:
                #     print "no ticker skipping"
                #     continue

            if row0["PRIMEXCH"] == "X":  # skip exchange X
                skipped_permnos.append(permno)
                continue

            print(" preparing {} / {} ".format(row0["TSYMBOL"],
                                               row0["PRIMEXCH"]))

            if HURRICANE_SANDY_ER in df_tkr.index:
                print 'contains HURRICANE Sandy'
                # copy dividend dates
                df_tkr.loc[HURRICANE_SANDY_FD, DIV_COLUMNS] = df_tkr.loc[HURRICANE_SANDY_ER, DIV_COLUMNS].values
                # delete extra day
                df_tkr = df_tkr.drop(HURRICANE_SANDY_ER)

            # check to see if there are missing dates in the middle
            this_cal = us_calendar[(us_calendar >= df_tkr.index[0]) & (us_calendar <= df_tkr.index[-1])]

            if df_tkr.shape[0]/float(len(this_cal)) < ALLOWED_MISSING_DATES_RATIO:
                print 'too many missing dates, skipping ticker'
                skipped_permnos.append(permno)
                continue

            if len(this_cal) != df_tkr.shape[0]:
                print len(this_cal), df_tkr.shape[0]  # the ticker has more rows than the calendar (traded on non-trading day?)
                emptyDF = pd.DataFrame(index=this_cal)
                emptyDF.index.tz = None
                print "MISSING interstitial dates for: %s please fix" % row0["TSYMBOL"]
                print df_tkr.join(emptyDF, how='outer')
                missing_dates = set(emptyDF.index) - set(df_tkr.index)
                print "dates missing: ", missing_dates

                # detect delisting pattern and fix
                if set(emptyDF.index[-(len(missing_dates) + 1):-1]) == missing_dates:
                    print "all missing dates lead to end, dropping last two rows"
                    # delete last two rows of df_tkr
                    df_tkr = df_tkr.drop(df_tkr.index[-2:])
                else:
                    sys.exit()

            this_cal_post = us_calendar[(us_calendar >= df_tkr.index[0]) & (us_calendar <= df_tkr.index[-1])]
            if len(this_cal_post) != df_tkr.shape[0]:
                print('calendar not correct, has {} dates, please fix'.format(df_tkr.shape[0]))
                print('This could be dividends released on the weekend.')
                sys.exit()

            cusip_ticker = '{}-{}'.format(row0['CUSIP'], row0['TSYMBOL'])
            # update metadata; 'start_date', 'end_date', 'auto_close_date',
            # 'symbol', 'exchange', 'asset_name'
            metadata_list.append((df_tkr.index[0],
                                  df_tkr.index[-1],
                                  df_tkr.index[-1] + pd.Timedelta(days=1),
                                  cusip_ticker,   # store CUSIP-ticker as ticker, Zipline cannot reuse tickers
                                  row0['PRIMEXCH'],
                                  row0['CUSIP']  #permno,    # store CRSP permno for company name
                                  )
                                 )

            # drop metadata columns
            df_tkr.loc[:, 'close'] = abs(df_tkr['PRC'])  # take abs(close) for esitmated values

            OHLCV_COLUMNS = ['close', 'VOL', 'ASKHI', 'BIDLO', 'OPENPRC']
            df_tkr = df_tkr.drop([col for col in df_tkr.columns if col not in OHLCV_COLUMNS], axis=1)
            # df_tkr = df_tkr.drop(['TSYMBOL', 'PERMNO',
            #                       'PERMCO', 'PRIMEXCH', 'PRC', 'CUSIP',
            #                       u'DCLRDT', u'PAYDT', u'RCRDDT', u'DIVAMT', u'FACPR', u'NWPERM'], axis=1)


            # rename volume, high, low, open
            df_tkr = df_tkr.rename(columns={"VOL": "volume",
                                            "ASKHI": "high",
                                            "BIDLO": "low",
                                            "OPENPRC": "open"})

            # pack data to be written by daily_bar_writer
            data_list.append((sec_counter, df_tkr))  # zipline will only use this permno as ref to match adjustments
            permno2sid_map[permno] = sec_counter  # record the sid for use later
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

        if 'NWPERM' in dfds.columns:  # todo this could be replaced with a whitelist
            dfds = dfds.drop(['NWPERM'], axis=1)
        if 'SHRCD' in dfds.columns:
            dfds = dfds.drop(['SHRCD'], axis=1)
        print(dfds)

        # drop rows where FACPR is absent
        dfds = dfds[pd.notnull(dfds['FACPR'])]
        # the splits that have declare date absent will be dropped as well
        print("number of non-NaN rows: ", dfds.shape[0])

        # drop rows of skipped securities
        dfds = dfds[~dfds['PERMNO'].isin(skipped_permnos)]
        print("number of non-NaN rows, after dropping skipped permnos: ", dfds.shape[0])

        # create sid column by mapping PERMNOs to sids
        dfds.loc[:, 'sid'] = dfds.loc[:, 'PERMNO'].apply(lambda x: permno2sid_map[x])
        dfds = dfds.drop(["VOL", "ASKHI", "BIDLO", "OPENPRC",
                              'PERMCO', 'PRIMEXCH', 'PRC', 'PERMNO', 'CUSIP'], axis=1)

        # if FACPR == 0, then those are dividend, else it is a split
        dfd = dfds[dfds["FACPR"] == 0.0]  # dividend
        dfd.loc[:, 'ex_date'] = dfd.index
        dfd = dfd.rename(columns={'DCLRDT': 'declared_date',
                                  'PAYDT': 'pay_date',
                                  'RCRDDT': 'record_date',
                                  'DIVAMT': 'amount'})
        dfd = dfd.drop(['FACPR', 'TSYMBOL'], axis=1)

        # move splits into desired format
        dfs = dfds[dfds["FACPR"] != 0.0]  # splits
        # drop rows where FACPR is -1.0 (stock going away)
        dfs = dfs[dfs['FACPR'] != -1.0]

        dfs.loc[:, 'effective_date'] = dfs.index
        dfs.loc[:, 'ratio'] = 1.0 / (1 + dfs.loc[:,'FACPR'])

        print('extreme split values')
        print dfs[(dfs['ratio'] > 100) | (dfs['ratio'] < 0.01)]

        dfs = dfs.drop(['TSYMBOL', 'DCLRDT', 'PAYDT', 'RCRDDT', 'DIVAMT', 'FACPR'], axis=1)

        print("number of rows in splits: {}".format(dfs.shape[0]))

        # write adjustments
        adjustment_writer.write(splits=dfs, dividends=dfd)

    return ingest
