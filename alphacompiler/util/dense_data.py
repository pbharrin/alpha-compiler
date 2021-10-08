
"""Created by Peter Harrington (pbharrin) on 10/5/21."""
import numpy as np
from zipline.pipeline.factors import CustomFactor
import pandas as pd
from os import listdir
from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle


def pack_dense_data(raw_fldr, final_fn, bundle_name):
    """
    This uses a pd.DataFrame, the index is dates and SIDs.
    Special care is taken to make sure every SID has a value for
    every date.  This simplifies the runtime code.
    Multiple columns of data can be stored, no extra work is required.
    """
    all_dfs = []
    # create a pandas dataframe
    for fn in listdir(raw_fldr):
        df = pd.read_csv(f'{raw_fldr}/{fn}', parse_dates=['timestamp'])
        sid = int(fn.split('.')[0])
        print(f'processing: {sid}')
        df['sid'] = sid
        all_dfs.append(df)

    df_all = pd.concat(all_dfs)
    # set date and sid as indicies
    df_all = df_all.set_index(['timestamp', 'sid'])
    df_all = df_all.sort_index()
    print(df_all)

    # fill in df_all with NaNs for sids that are missing
    # iterate over all the dates

    # get all SIDs in this bundle
    tickers2sid = get_ticker_sid_dict_from_bundle(bundle_name)
    all_possible_sids = set(tickers2sid.values())
    empty_data = []
    for this_day, daydf in df_all.groupby(by='timestamp'):
        print(this_day)
        used_sids = daydf.index.get_level_values(1)
        print('sids on this day: ', used_sids)
        unused_sids = all_possible_sids - set(used_sids)
        print('not used sids: ', unused_sids)
        emptynans = [np.NAN] * daydf.shape[1]
        for unused_sid in unused_sids:
            empty_data.append([this_day, unused_sid] + emptynans)
    unindexed_columns = ['timestamp', 'sid'] + df_all.columns.to_list()

    df_empty = pd.DataFrame(empty_data, columns=unindexed_columns)
    df_empty = df_empty.set_index(['timestamp', 'sid'])

    # merge all and sort
    df_all = pd.concat([df_all, df_empty])
    df_all = df_all.sort_index()

    # save to file
    df_all.to_parquet(final_fn)


class DenseDataFactor(CustomFactor):
    """Abstract Base Class for dense (few missing bars) data."""
    inputs = []
    window_length = 1

    def __init__(self, *args, **kwargs):
        self.data = None
        self.data_path = "please_specify_.npy_file"

    def compute(self, today, assets, out, *arrays):
        """
        This simply loads the data from a parquet and then reads the
        data and outputs it.  See pack_dense_data() above for how to store
        the data.
        """
        if self.data is None:
            self.data = pd.read_parquet(self.data_path)

        for field in self.__class__.outputs:
            out[field][:] = self.data.loc[(today, assets), field].values
