
"""
Created by Peter Harrington (pbharrin) on 10/10/17.

This file contains the CustomFactors that can be used in
Zipline's Pipeline.
"""

from zipline.pipeline.factors import CustomFactor

from os import listdir
import pandas as pd
import numpy as np

DATA_PATH = "/Users/peterharrington/Documents/GitHub/alpha-compiler/alphacompiler/data/SF1.npy"

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
        non_nan_dates = dates_for_sid[~np.isnan(dates_for_sid)]
        return self.bs(non_nan_dates) - 1


    def cold_start(self, today, assets):
        if self.data is None:
            self.data = np.load(DATA_PATH)

        # for each sid, do binary search of date array to find current index
        # the results can be shared across all factors that inherit from SparseDataFactor
        # this sets an array of ints: time_index
        self.time_index = np.full(self.N, -1, np.dtype('int64'))
        self.curr_date = today.value
        for asset in assets:  # asset is numpy.int64
            self.time_index[asset] = self.bs_sparse_time(asset)


    def update_time_index(self, today, assets):
        """Ratchet update."""

        # for each asset check if today >= dates[self.time_index]
        # if so then increment self.time_index[asset.sid] += 1
        sids_to_increment = today.value >= self.data.date[np.arange(self.N), self.time_index + 1]
        self.time_index[sids_to_increment] += 1

        self.curr_date = today.value


    def compute(self, today, assets, out, *arrays):
        # for each asset in assets determine index from date (today)
        if self.time_index is None:
            self.cold_start(today, assets)
        else:
            self.update_time_index(today, assets)

        ti_used_today = self.time_index[assets]

        for field in self.fields:
            out[field][:] = self.data[field][assets, ti_used_today]


class Fundamentals(SparseDataFactor):
    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = 3193  #(max sid +1) get this from the bundle
        self.raw_path = "/Users/peterharrington/Documents/GitHub/alpha-compiler/alphacompiler/data/raw/"
