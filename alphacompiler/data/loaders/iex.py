"""
Custom Bundle for Loading the IEX cloud data.   

Created by Peter Harrington (pbharrin) on 2/25/2021.
"""


import pandas as pd
import sys
from os import listdir
import trading_calendars as tc


METADATA_HEADERS = ['start_date', 'end_date', 'auto_close_date',
                    'symbol', 'exchange', 'asset_name']

UNUSED_COLUMNS = ['close','high','low','open','volume','id','key','subkey','updated','changeOverTime','marketChangeOverTime','uOpen','uClose','uHigh','uLow','uVolume','label','change','changePercent']

MISSING_DAYS_THRESH = 20 # max allowable number of missing dates (in 15 years)


def from_iex_dir(folder_name, start=None, end=None):
    """
    close,high,low,open,symbol,volume,id,key,subkey,date,updated,changeOverTime,marketChangeOverTime,uOpen,uClose,uHigh,uLow,uVolume,fOpen,fClose,fHigh,fLow,fVolume,label,change,changePercent
    22.04,22.13,21.9,21.96,AES,2527800,HISTORICAL_PRICES,AES,,2006-12-29,1611800225000,0,0,21.96,22.04,22.13,21.9,2527800,16.9385,17.0002,17.0696,16.8922,2527800,"Dec 29, 06",0,0


    To use this make your ~/.zipline/extension.py look similar this:

    from zipline.data.bundles import register
    from alphacompiler.data.loaders.iex import from_iex_dir

    register("iex", from_iex_dir("/path/to/IEX/dir"),)

    """
    ticker2sid_map = {}
    us_calendar = tc.get_calendar("XNYS")

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

        print("starting ingesting data from: {}".format(folder_name))

        # read in the whole dump (will require ~7GB of RAM)
	#df = pd.read_csv(file_name, index_col='date',
	#                 parse_dates=['date'], na_values=['NA'])

        # counter of valid securites, this will be our primary key
        sec_counter = 0
        data_list = []  # list to send to daily_bar_writer
        metadata_list = []  # list to send to asset_db_writer (metadata)

        # iterate over all the unique securities and pack data, and metadata
        # for writing
	#for tkr, df_tkr in df.groupby('ticker'):
        for fn in listdir(folder_name):
            print('preparing {}'.format(fn))
            df_tkr = pd.read_csv(folder_name + fn, index_col='date',
                         parse_dates=['date'], na_values=['NA'])
            #print(" preparing {}".format(df_tkr.ix[0]["symbol"]))
            #print(df_tkr)
            try:
                df_tkr = df_tkr.drop(UNUSED_COLUMNS, axis=1)
            except:
                print('SKIPPING:{}'.format(fn))  # IEX csv bug
                continue
            df_tkr = df_tkr.sort_index()
            df_tkr = df_tkr.rename(columns={'fOpen': 'open',
		                            'fClose': 'close',
					    'fHigh': 'high',
					    'fLow': 'low',
					    'fVolume': 'volume'}) 

            row0 = df_tkr.ix[0]  # get metadata from row

	    # check to see if there are missing interstitial dates
	    #this_cal = us_calendar[(us_calendar >= df_tkr.index[0]) & (us_calendar <= df_tkr.index[-1])]
            this_cal = us_calendar.sessions_in_range(df_tkr.index[0], df_tkr.index[-1])
            # remove extra days (weekends and shit)
            df_tkr['dateUTC'] = df_tkr.index.tz_localize('UTC')
            df_tkr = df_tkr.set_index('dateUTC')
            df_tkr = df_tkr.loc[this_cal]
            """
            number_missing = len(this_cal) - df_tkr.shape[0]
            assert number_missing >= 0
            if number_missing > 0:
                if number_missing < MISSING_DAYS_THRESH:
                    print('MISSING interstitial dates for: %s using forward fill' % row0['symbol'])
                    print('number of dates missing: {}'.format(number_missing))
                    df_desired = pd.DataFrame(index=this_cal.tz_localize(None))
                    df_desired = df_desired.join(df_tkr)
                    df_tkr = df_desired.fillna(method='ffill')
                else:
                    print('{} has too many missing days, skipping'.format(fn))
                    continue
            """

            # update metadata; 'start_date', 'end_date', 'auto_close_date',
            # 'symbol', 'exchange', 'asset_name'
            metadata_list.append((df_tkr.index[0],
                                  df_tkr.index[-1],
                                  df_tkr.index[-1] + pd.Timedelta(days=1),
                                  row0["symbol"],
                                  "IEX",  # all have exchange = IEX 
                                  row0["symbol"]  
                                  )
                                 )

            # drop metadata columns
            tkr = row0['symbol']
            df_tkr = df_tkr.drop(['symbol'], axis=1)

            # pack data to be written by daily_bar_writer
            data_list.append((sec_counter, df_tkr))
            ticker2sid_map[tkr] = sec_counter  # record the sid for use later
            sec_counter += 1

        print("writing data for {} securities".format(len(metadata_list)))
        daily_bar_writer.write(data_list, show_progress=True)

        # write metadata
        asset_db_writer.write(equities=pd.DataFrame(metadata_list,
                                                    columns=METADATA_HEADERS))
        print("a total of {} securities were loaded into this bundle".format(
            sec_counter))

    return ingest