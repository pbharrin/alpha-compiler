
"""
Loader for formatting and populating data for a dense factor (data for all
time periods).
"""
from os import listdir

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from alphacompiler.util.sparse_data import clear_raw_folder
from zipline.utils.paths import zipline_root
import alphacompiler.util.load_extensions  # code exectutes when this is imported.

RAW_FLDR = "raw"
BUNDLE_NAME = 'crypto'

ZIPLINE_DATA_DIR = zipline_root() + '/data/'

def num_tkrs_in_bundle(bundle_name):
    return len(get_ticker_sid_dict_from_bundle(bundle_name))

def populate_raw_folder(tkr_data_folder:str, bundle_name:str):
    """

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
        print(f'processing: {tkr}, sid: {sid}')
        # files have the following header timestamp,marketcap_dominance

        # reformat
        # write to file in /raw
        # write raw file: raw/
        #df_tkr.to_csv(os.path.join(raw_path, "{}.csv".format(sid)))

# TODO: move to alphacompiler.util when this is ready
def pack_dense_data(bundle_name:str, data_dir:str):
    """
    Packs data stored in data_dir so that it can be attached as a Zipline factor during
    a pipeline operation.
    The bundle needs to exist and already be ingested.

    :param bundle_name:
    :param data_dir:
    :return:
    """
    pass

if __name__ == '__main__':
    data_dir = '/Users/peter/Documents/Bitbucket/cryptoresearch/messari_historical_data/dom'
    populate_raw_folder(data_dir, BUNDLE_NAME)
    # pack_dense_data('crypto', data_dir)

    print('this works')