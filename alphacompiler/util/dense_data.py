
"""Created by Peter Harrington (pbharrin) on 10/5/21."""

from zipline.pipeline.factors import CustomFactor
import pandas as pd
from os import listdir


def pack_dense_data(raw_fldr, final_fn):
    """
    use a pd.DataFrame, the index could be dates and SIDs
    the column order is preserved when you create and retreive data.
    """
    all_dfs = []
    # create a pandas dataframe
    for fn in listdir(raw_fldr):
        df = pd.read_csv(f'{raw_fldr}/{fn}')
        sid = int(fn.split('.')[0])
        print(f'processing: {sid}')
        df['sid'] = sid
        all_dfs.append(df)

    df_all = pd.concat(all_dfs)
    # set date and sid as indicies
    df_all = df_all.set_index(['timestamp', 'sid'])
    print(df_all)

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
        # OPTION 1
        # store data as an dict, and a np.ndarray in a pickle.
        # today is a date(time) while the ndarry requires an int
        # I could hold an extra map form date to an index in the array.
        # date_map

        for field in self.__class__.outputs:
            out[field][:] = self.data[field][assets, today_i]