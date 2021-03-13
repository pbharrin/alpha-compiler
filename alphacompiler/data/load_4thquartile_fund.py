"""
Packs the fundamental data from a single 4th Quartile csv dump.
"""

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from alphacompiler.util.sparse_data import pack_sparse_data
import alphacompiler.util.load_extensions  # this simply loads the extensions
from zipline.utils.paths import zipline_root

from logbook import Logger
import datetime
import os
import pandas as pd
import glob

BASE = os.path.dirname(os.path.realpath(__file__))
RAW_FLDR = "raw"  # folder to store the raw text file
START_DATE = '2009-01-01'  # this is only used for getting data from the API
END_DATE = datetime.datetime.today().strftime('%Y-%m-%d')

ZIPLINE_DATA_DIR = zipline_root() + '/data/'
FN = "4th.npy"  # the file name to be used when storing this in ~/.zipline/data

DUMP_FILE = '/Users/peter/Documents/Bitbucket/qlite-backend/fundamental/data/master.csv'

log = Logger('load_4thquartile_fund.py')


def clear_raw_folder(raw_folder_path):
    # removes all the files in the raw folder
    print('   **   clearing the raw/ folder   **')
    files = glob.glob(raw_folder_path + '/*')
    for f in files:
        os.remove(f)


def populate_raw_data_from_dump(tickers2sid, fields, raw_path):
    """
    Populates the raw/ folder based on a single dump download.

    :param tickers2sid: a dict with the ticker string as the key and the SID
    as the value
    :param fields: a list of field names
    :param raw_path: the path to the folder to write the files.
    """

    df = pd.read_csv(DUMP_FILE)  # open dump file
    clear_raw_folder(RAW_FLDR)

    df = df[['ticker', 'date'] + fields]  # remove columns not in fields
    df = df.loc[:, ~df.columns.duplicated()]  # drop any columns with redundant names

    for tkr, df_tkr in df.groupby('ticker'):
        tkr = tkr.upper()
        print('processing: ', tkr)

        sid = tickers2sid.get(tkr)
        if sid is None:
            print('no sid found for: {}'.format(tkr))
            continue
        print('sid: {}'.format(sid))

        df_tkr = df_tkr.rename(columns={'date': 'Date'}).set_index('Date')

        # loop over the fields and dimensions
        series = []
        for i, field in enumerate(fields):
            print(i, field)
            s = df_tkr[field]
            #new_name = '{}_{}'.format(field, dimensions[i])
            #s = s.rename(new_name)
            series.append(s)
        df_tkr = pd.concat(series, axis=1)
        df_tkr.index.names = ['Date']  # ensure that the index is named Date
        print("AFTER reorganizing")
        print(df_tkr)

        # write raw file: raw/
        df_tkr.to_csv(os.path.join(raw_path, "{}.csv".format(sid)))


def all_tickers_for_bundle_from_dump(fields, bundle_name, raw_path=os.path.join(BASE, RAW_FLDR)):
    tickers = get_ticker_sid_dict_from_bundle(bundle_name)
    print(tickers)
    populate_raw_data_from_dump(tickers, fields, raw_path)


def num_tkrs_in_bundle(bundle_name):
    return len(get_ticker_sid_dict_from_bundle(bundle_name))


if __name__ == '__main__':

    # Marc's turntup Quality companies in an uptrend
    # fields = ['roe', 'marketcap', 'de', 'debt', 'debtnc']
    fields = ['Revenue', 'Net_Income', 'Total_Assets', 'Total_Current_Liabilities']


    BUNDLE_NAME = 'iex'
    num_tickers = num_tkrs_in_bundle(BUNDLE_NAME)
    print('number of tickers: ', num_tickers)

    all_tickers_for_bundle_from_dump(fields, BUNDLE_NAME)  # downloads the data to /raw
    pack_sparse_data(num_tickers + 1,  # number of tickers in buldle + 1
                    os.path.join(BASE, RAW_FLDR),
                    fields,
                    ZIPLINE_DATA_DIR + FN)  # write directly to the zipline data dir

    print("this worked boss")
