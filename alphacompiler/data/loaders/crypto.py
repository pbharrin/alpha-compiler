"""
Custom Bundle for Loading the Crypto data.
Created by Peter Harrington (pbharrin) on 9/22/2021.
"""


import pandas as pd
from os import listdir
from alphacompiler.util.crypto_trading_calendar import TwentyFourSevenCal

METADATA_HEADERS = ['start_date', 'end_date', 'auto_close_date',
                    'symbol', 'exchange', 'asset_name']

MISSING_DAYS_THRESH = 20  # max allowable number of missing dates (in 15 years)
UINT32_MAX = 4294967295

# Exchange Metadata (for country code mapping)
exchange_d = {'exchange': ['CRYPTO'], 'canonical_name': ['CRYPTO'], 'country_code': ['US']}

TRUE_STARTS = {'BCH': pd.to_datetime('2017-08-09', utc=True),
               'AAVE': pd.to_datetime('2020-10-07', utc=True)}


def scale_df_for_uint32(dfin):
    """
    Dynamically scales the price and volume so that they fit into uint32.
    Preserves market cap.
    """
    sf = 2.0  # scale factor
    if dfin['low'].min() <= 0:  # for debug purposes
        print('adjusting for less or equal to 0')
        print('the min is: ', dfin['low'].idxmin())

    while dfin['low'].min() < 10:
        dfin['volume'] = dfin['volume'] / sf
        for field in ['open', 'high', 'low', 'close']:
            dfin[field] = dfin[field] * sf

    dfin['volume'] = UINT32_MAX - 1
    return dfin


def from_crypto_dir(folder_name, start=None, end=None):
    """
    Crypto data (from Messari) will look like this:
    timestamp,open,high,low,close,volume
    2020-09-22T00:00:00Z,6.481369,10.478451,4.043462,5.310313,114002546.794525
    2020-09-23T00:00:00Z,5.249185,5.348590,3.392705,3.526133,103248071.227028
    2020-09-24T00:00:00Z,3.518865,4.885225,3.419303,4.639547,64396127.646014
    To use this make your ~/.zipline/extension.py look similar this:
    from zipline.data.bundles import register
    from alphacompiler.data.loaders.crypto import from_crypto_dir
    register("crypto", from_crypto_dir("/path/to/crypto/data/dir"),)
    """
    ticker2sid_map = {}
    us_calendar = TwentyFourSevenCal()

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               us_calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):

        print("starting ingesting data from: {}".format(folder_name))

        # counter of valid securites, this will be our primary key
        sec_counter = 0
        data_list = []  # list to send to daily_bar_writer
        metadata_list = []  # list to send to asset_db_writer (metadata)

        # iterate over all the unique securities and pack data, and metadata
        # for writing
        for fn in listdir(folder_name):
            print('preparing {}'.format(fn))
            tkr = fn.split('.')[0]
            try:
                df_tkr = pd.read_csv(folder_name + fn, index_col='timestamp',
                                     parse_dates=['timestamp'], na_values=['NA'])
            except pd.errors.EmptyDataError:
                print('No data in: {}, skipping.'.format(fn))
                continue

            #print(f'the min value of this ticker is: {df_tkr.idxmin()}')
            df_tkr = df_tkr.sort_index()
            if tkr in TRUE_STARTS:
                df_tkr = df_tkr[df_tkr.index >= TRUE_STARTS[tkr]]

            # check to see if there are missing interstitial dates
            this_cal = us_calendar.sessions_in_range(df_tkr.index[0], df_tkr.index[-1])

            num_missing_dates = len(this_cal) - df_tkr.shape[0]
            if num_missing_dates > MISSING_DAYS_THRESH:
                print(f'too many missing dates({num_missing_dates}) for {tkr}, skipping')
                continue
            # fill in missing dates with NaNs
            df_tkr = df_tkr.reindex(this_cal)

            # divide the volume by 1000
            # df_tkr['volume'] = df_tkr['volume']/1000

            df_tkr = scale_df_for_uint32(df_tkr)

            # update metadata; 'start_date', 'end_date', 'auto_close_date',
            # 'symbol', 'exchange', 'asset_name'
            metadata_list.append((df_tkr.index[0],
                                  df_tkr.index[-1],
                                  df_tkr.index[-1] + pd.Timedelta(days=1),
                                  tkr,
                                  "CRYPTO",  # all have exchange = IEX
                                  tkr,
                                  )
                                 )

            # pack data to be written by daily_bar_writer
            data_list.append((sec_counter, df_tkr))
            ticker2sid_map[tkr] = sec_counter  # record the sid for use later
            sec_counter += 1

        print("writing data for {} securities".format(len(metadata_list)))
        daily_bar_writer.write(data_list, show_progress=True)

        # write metadata
        asset_db_writer.write(equities=pd.DataFrame(metadata_list,
                                                    columns=METADATA_HEADERS),
                              exchanges=pd.DataFrame(data=exchange_d))
        adjustment_writer.write()
        print("a total of {} securities were loaded into this bundle".format(
            sec_counter))

    return ingest
