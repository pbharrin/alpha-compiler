
"""
Created by Peter Harrington (pbharrin) on 10/10/17.

This file contains the CustomFactors that can be used in
Zipline's Pipeline.
"""

from zipline.pipeline.factors import CustomFactor
import numpy as np
from os import listdir
import pandas as pd
import numpy as np


class RandomFactor(CustomFactor):
    """Returns a random number, for demo purposes"""
    inputs = []
    window_length = 1

    def compute(self, today, assets, out):
        print "assets.shape: ",assets.shape
        out[:] = np.random.random(assets.shape)

class RandomFactor2(CustomFactor):
    """Returns two random numbers, for demo purposes"""
    inputs = []
    window_length = 1
    outputs = ["rand0", "rand1"]

    def compute(self, today, assets, out):
        # out[:] = np.random.random(assets.shape) # 2 outputs
        out.rand0 = np.random.random(assets.shape)
        out.rand1 = np.random.random(assets.shape)


# TODO: move this class to its own file, perhaps in util
class SparseDataFactor(CustomFactor):
    """Abstract Base Class to be used for computing """
    inputs = []
    window_length = 1
    outputs = ["GP_MRQ", "CAPEX_MRQ"]   # TODO: figure out how to pass this in

    def __init__(self, *args, **kwargs):
        self.time_index = None
        self.curr_date = None # date for which time_index is accurate
        self.data = None

    def load_data_from_disk(self):
        """Populate memory (self.data) with data from disk
        Some of this could be done at the data download time, so the final array
        is loaded from disk.
        """

        # create buffer to hold data for all tickers
        dfs = [None] * self.N

        max_len = -1
        for fn in listdir(self.raw_path):
            if not fn.endswith(".csv"):
                continue
            df = pd.read_csv(self.raw_path + fn, index_col="Date", parse_dates=True)
            sid = int(fn.split('.')[0])
            dfs[sid] = df

            # width is max number of rows in any file
            max_len = max(max_len, df.shape[0])

        # pack up data as buffer
        num_fundamentals = 2          # TODO: get this from the constructor
        buff = np.full((num_fundamentals + 1, self.N, max_len), np.nan)
        # pack self.data as np.recarray
        self.data = np.recarray(shape=(self.N, max_len),
                                buf=buff,
                                dtype=[('date', '<f8'),
                                       ('GP_MRQ', '<f8'),
                                       ('CAPEX_MRQ', '<f8')])

        # iterate over loaded data and populate self.data
        for i, df in enumerate(dfs):
            if df is None:
                continue
            self.data.date[i] = df.index
            self.data['GP_MRQ'][i] = df['GP_MRQ']  # TODO: get these field names from constructor
            self.data['CAPEX_MRQ'][i] = df['CAPEX_MRQ']


    def bs(self, arr):
        """Binary Search"""
        if len(arr) == 1:
            if self.curr_date < arr[0]:
                return 0
            else: return 1

        mid = len(arr) / 2
        if self.curr_date < arr[mid]:
            print "   IT was less than the middle date"
            return self.bs(arr[:mid])
        else:
            print "   IT was AFTER than the middle date"
            return mid + self.bs(arr[mid:])


    def bs_sparse_time(self, sid):
        dates_for_sid = self.data.date[sid]
        if np.isnan(dates_for_sid[0]):
            return 0

        # do a binary search of the dates array finding the index
        # where self.curr_date will lie.
        return self.bs(dates_for_sid) - 1


    def cold_start(self, today, assets):
        if self.data is None:
            self.load_data_from_disk()

        # for each sid, do binary search of date array to find current index
        # the results can be shared across all factors that inherit from SparseDataFactor
        # this sets an array of ints: time_index
        self.time_index = np.full(self.N, -1, np.dtype('int64'))
        self.curr_date = today.value
        for asset in assets:  # asset is numpy.int64
            self.time_index[asset] = self.bs_sparse_time(asset)

    def update_time_index(self, today):
        print "updating time index   ************** updating time index   **************"
        self.curr_date = today.value
        # for each asset check if today >= dates[self.time_index]
        # if so then increment self.time_index[asset.sid] += 1

        # TODO: this needs work

        pass

    def compute(self, today, assets, out, *arrays):
        # for each asset in assets determine index from date (today)
        if self.time_index is None:
            self.cold_start(today, assets)

        if self.curr_date != today:
            self.update_time_index(today)

        print "self.time_index: ", self.time_index

        ti_used_today = self.time_index[assets]
        out.GP_MRQ[:] = self.data.GP_MRQ[assets, ti_used_today]
        out.CAPEX_MRQ[:] = self.data.CAPEX_MRQ[assets, ti_used_today]


class Fundamentals(SparseDataFactor):
    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = 3193  #(max sid +1) get this from the bundle
        self.raw_path = "/Users/peterharrington/Documents/GitHub/alpha-compiler/alphacompiler/data/raw/"
