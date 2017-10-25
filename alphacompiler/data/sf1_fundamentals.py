
"""
Created by Peter Harrington (pbharrin) on 10/10/17.

This file contains the CustomFactors that can be used in
Zipline's Pipeline.
"""

from zipline.pipeline.factors import CustomFactor

from os import listdir
import pandas as pd
import numpy as np


# TODO: move this class to its own file, perhaps in util
class SparseDataFactor(CustomFactor):
    """Abstract Base Class to be used for computing """
    inputs = []
    window_length = 1
    outputs = ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]   # TODO: figure out how to pass this in

    def __init__(self, *args, **kwargs):
        self.time_index = None
        self.curr_date = None # date for which time_index is accurate
        self.data = None
        self.fields = ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]

    # TODO: move this into the loader, and save the file as a numpy binary
    # using np.save()
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
        num_fundamentals = len(self.fields)
        buff = np.full((num_fundamentals + 1, self.N, max_len), np.nan)
        # pack self.data as np.recarray
        self.data = np.recarray(shape=(self.N, max_len),
                                buf=buff,
                                dtype=[('date', '<f8'),
                                       ("ROE_ART", '<f8'),
                                       ("BVPS_ARQ", '<f8'),
                                       ("SPS_ART", '<f8'),
                                       ("FCFPS_ARQ", '<f8'),
                                       ("PRICE", '<f8')])

        # iterate over loaded data and populate self.data
        for i, df in enumerate(dfs):
            if df is None:
                continue
            ind_len = df.index.shape[0]
            self.data.date[i, :ind_len] = df.index
            for field in self.fields:
                self.data[field][i, :ind_len] = df[field]


    def bs(self, arr):
        """Binary Search"""
        if len(arr) == 1:
            if self.curr_date < arr[0]:
                return 0
            else: return 1

        mid = len(arr) / 2
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

    def update_time_index(self, today, assets, ti_today):
        """Ratchet update."""

        # for each asset check if today >= dates[self.time_index]
        # if so then increment self.time_index[asset.sid] += 1
        sids_to_increment = today.value >= self.data.date[np.arange(self.N), self.time_index]
        self.time_index[sids_to_increment] += 1

        self.curr_date = today.value


    def compute(self, today, assets, out, *arrays):
        # for each asset in assets determine index from date (today)
        if self.time_index is None:
            self.cold_start(today, assets)

        ti_used_today = self.time_index[assets]

        if self.curr_date != today:
            self.update_time_index(today, assets, ti_used_today)

        # ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]
        #out.GP_MRQ[:] = self.data.GP_MRQ[assets, ti_used_today] # original
        for field in self.fields:
            out[field][:] = self.data[field][assets, ti_used_today]


class Fundamentals(SparseDataFactor):
    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = 3193  #(max sid +1) get this from the bundle
        self.raw_path = "/Users/peterharrington/Documents/GitHub/alpha-compiler/alphacompiler/data/raw/"
