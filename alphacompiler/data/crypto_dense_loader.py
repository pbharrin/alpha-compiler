
"""
Loader for formatting and populating data for a dense factor (data for all
time periods).
"""
from os import listdir

import pandas as pd

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from alphacompiler.util.sparse_data import clear_raw_folder
from alphacompiler.util.dense_data import pack_dense_data
from zipline.utils.paths import zipline_root
import alphacompiler.util.load_extensions  # code exectutes when this is imported.

RAW_FLDR = "raw"
BUNDLE_NAME = 'crypto'

ZIPLINE_DATA_DIR = zipline_root() + '/data/'

def num_tkrs_in_bundle(bundle_name):
    return len(get_ticker_sid_dict_from_bundle(bundle_name))


def populate_raw_folder(tkr_data_folder:str, bundle_name:str):
    """
    This basically saves the data to individual files by thier SID.
    :param tkr_data_folder:
    :param bundle_name:
    :return:
    """
    clear_raw_folder(RAW_FLDR)
    tickers2sid = get_ticker_sid_dict_from_bundle(bundle_name)

    #
    for fn in listdir(tkr_data_folder):
        if not fn.endswith(".csv"):
            continue
        tkr = fn.split('.')[0]
        sid = tickers2sid.get(tkr.upper())
        if sid is None:
            continue
        print(f'processing: {tkr}, sid: {sid}')
        # files have the following header timestamp,marketcap_dominance
        df_tkr = pd.read_csv(f'{tkr_data_folder}/{fn}')

        # reformat
        # write to file in /raw
        # write raw file: raw/
        df_tkr.to_csv(f'{RAW_FLDR}/{sid}.csv', index=False)


if __name__ == '__main__':
    data_dir = '/Users/peterharrington/Documents/Bitbucket/cryptoresearch/messari_historical_data/dom'
    # populate_raw_folder(data_dir, BUNDLE_NAME)
    pack_dense_data(RAW_FLDR, ZIPLINE_DATA_DIR + 'mktdom.parquet.gzip', BUNDLE_NAME)

    print('this works')