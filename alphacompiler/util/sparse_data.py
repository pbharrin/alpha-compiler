

"""
Created by Peter Harrington (pbharrin) on 10/10/17.

This file contains the CustomFactors that can be used in
Zipline's Pipeline.
"""

from zipline.pipeline.factors import CustomFactor
import numpy as np
from os import listdir
import pandas as pd
import os

class SparseDataFactor(CustomFactor):
    """Abstract Base Class to be used for computing sparse data.
    The data is packed and persisted into a NumPy binary data file
    in a previous step.

    This class must be subclassed with class variable 'outputs' set.  The fields
    in 'outputs' should match those persisted."""
    inputs = []
    window_length = 1

    def __init__(self, *args, **kwargs):
        self.time_index = None
        self.curr_date = None # date for which time_index is accurate
        self.data = None
        self.data_path = "please_specify_.npy_file"


    def bs(self, arr):
        """Binary Search"""
        if len(arr) == 1:
            if self.curr_date < arr[0]:
                return 0
            else: return 1

        mid = int(len(arr) / 2)
        if self.curr_date < arr[mid]:
            return self.bs(arr[:mid])
        else:
            return mid + self.bs(arr[mid:])


    def bs_sparse_time(self, sid):
        """For each security find the best range in the sparse data."""
        dates_for_sid = self.data.date[sid]
        if np.isnan(dates_for_sid[0]):
            return 0

        # do a binary search of the dates array finding the index
        # where self.curr_date will lie.
        non_nan_dates = dates_for_sid[~np.isnan(dates_for_sid)]
        return self.bs(non_nan_dates) - 1


    def cold_start(self, today, assets):
        if self.data is None:
            self.data = np.load(self.data_path)

        self.M = self.data.date.shape[1]

        # for each sid, do binary search of date array to find current index
        # the results can be shared across all factors that inherit from SparseDataFactor
        # this sets an array of ints: time_index
        self.time_index = np.full(self.N, -1, np.dtype('int64'))
        self.curr_date = today.value
        for asset in assets:  # asset is numpy.int64
            self.time_index[asset] = self.bs_sparse_time(asset)


    def update_time_index(self, today, assets):
        """Ratchet update.

        for each asset check if today >= dates[self.time_index]
        if so then increment self.time_index[asset.sid] += 1"""

        ind_p1 = self.time_index.copy()
        np.add.at(ind_p1, ind_p1 != (self.M - 1), 1)
        sids_to_increment = today.value >= self.data.date[np.arange(self.N), ind_p1]
        sids_not_max = self.time_index != (self.M - 1)   # create mask of non-maxed
        self.time_index[sids_to_increment & sids_not_max] += 1

        self.curr_date = today.value


    def compute(self, today, assets, out, *arrays):
        # for each asset in assets determine index from date (today)
        if self.time_index is None:
            self.cold_start(today, assets)
        else:
            self.update_time_index(today, assets)

        ti_used_today = self.time_index[assets]

        for field in self.__class__.outputs:
            out[field][:] = self.data[field][assets, ti_used_today]



def pack_sparse_data(N, rawpath, fields, filename):
    """pack data into np.recarray and persists it to a file to be
    used by SparseDataFactor"""


    # create buffer to hold data for all tickers
    dfs = [None] * N

    max_len = -1
    for fn in listdir(rawpath):
        if not fn.endswith(".csv"):
            continue
        df = pd.read_csv(os.path.join(rawpath,fn), index_col="Date", parse_dates=True)
        df = df.sort_index()
        sid = int(fn.split('.')[0])
        print("packing sid: %d" % sid)
        dfs[sid] = df

        # width is max number of rows in any file
        max_len = max(max_len, df.shape[0])

    # TODO: temp workaround for `Array Index Out of Bound` bug
    max_len = max_len + 1

    # pack up data as buffer
    num_fundamentals = len(fields)
    buff = np.full((num_fundamentals + 1, N, max_len), np.nan)

    dtypes = [('date', '<f8')]
    for field in fields:
        dtypes.append((field, '<f8'))

    # pack self.data as np.recarray
    data = np.recarray(shape=(N, max_len), buf=buff, dtype=dtypes)

    # iterate over loaded data and populate self.data
    for i, df in enumerate(dfs):
        if df is None:
            continue
        ind_len = df.index.shape[0]
        data.date[i, :ind_len] = df.index
        for field in fields:
            data[field][i, :ind_len] = df[field]

    data.dump(filename)  # can be read back with np.load()
