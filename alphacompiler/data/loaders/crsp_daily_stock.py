
"""
Custom Bundle for Loading the CRSP daily stock dataset.

Created by Peter Harrington (pbharrin) on 3/6/18.
"""


import pandas as pd
from zipline.utils.calendars import get_calendar
import sys


METADATA_HEADERS = ['start_date', 'end_date', 'auto_close_date',
                    'symbol', 'exchange', 'asset_name']
# UNWANTED_EXCHANGES = set(["OTC", "OTCBB", "INDX"])  # TODO: is this needed?


def from_crsp_dump(file_name, dvdend_file=None, start=None, end=None):
    """
    Load data from a CRSP daily stock dump, the following fields are assumed to
    be in the dump: PERMNO,date,TSYMBOL,PRIMEXCH,PERMCO,BIDLO,ASKHI,PRC,VOL,OPENPRC
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

        # drop row with NaNs or the loader will turn all columns to NaNs
        #df = df.dropna()

        uv = df.PERMNO.unique()  # get unique PERMNO (CRSP primary key, doesn't change)

        # counter of valid securites, this will be our primary key
        sec_counter = 0
        data_list = []  # list to send to daily_bar_writer
        metadata_list = []  # list to send to asset_db_writer (metadata)

        # iterate over all the unique securities (CRSP permno) and pack data,
        # also pack metadata for writing.
        for permno in uv:
            df_tkr = df[df['PERMNO'] == permno]

            row0 = df_tkr.ix[0]  # get metadata from row

            if row0["PRIMEXCH"] == "X":  # skip exchange X
                continue

            print(" preparing {} / {} ".format(row0["TSYMBOL"],
                                               row0["PRIMEXCH"]))

            if row0["TSYMBOL"] is None:
                print "no ticker please fix"
                sys.exit()

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
            df_tkr['close'] = abs(df_tkr['PRC'])  # take abs(close) for esitmated values

            df_tkr = df_tkr.drop(['TSYMBOL', 'PERMNO',
                                  'PERMCO', 'PRIMEXCH', 'PRC'], axis=1)

            # rename volume, high, low, open
            df_tkr = df_tkr.rename(columns={"VOL": "volume",
                                            "ASKHI": "high",
                                            "BIDLO": "low",
                                            "OPENPRC": "open"})

            # pack data to be written by daily_bar_writer
            data_list.append((sec_counter, df_tkr))  # TODO: use int(PERMNO) for SID
            sec_counter += 1

        print("writing data for {} securities".format(len(metadata_list)))
        daily_bar_writer.write(data_list, show_progress=False)

        # write metadata
        asset_db_writer.write(equities=pd.DataFrame(metadata_list,
                                                    columns=METADATA_HEADERS))
        print("a total of {} securities were loaded into this bundle".format(
            sec_counter))
        # read in Dividend History
        """
        m_ticker,ticker,comp_name,comp_name_2,exchange,currency_code,div_ex_date,div_amt,per_end_date
        Z86Z,0425B,PCA INTL,PCA INTL,,USD,1997-06-09,0.07,1997-07-31

        div_ex_date is the date you are entitled to the dividend
        """
        if dvdend_file is None:
            adjustment_writer.write()
        else:
            dfd = pd.read_csv(dvdend_file, index_col='div_ex_date',
                              parse_dates=['div_ex_date', 'per_end_date'],
                              na_values=['NA'])

            # format dfd to have sid
            adjustment_writer.write(dividends=dfd)

    return ingest
