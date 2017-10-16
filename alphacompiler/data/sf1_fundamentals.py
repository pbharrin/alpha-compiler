
"""
Created by Peter Harrington (pbharrin) on 10/10/17.

This file contains the CustomFactors that can be used in
Zipline's Pipeline.
"""

from zipline.pipeline.factors import CustomFactor
import numpy as np


class RandomFactor(CustomFactor):
    """Returns a random number, for demo purposes"""
    inputs = []
    window_length = 1 

    def compute(self, today, assets, out):

        out[:] = np.random.random(assets.shape) 

# TODO: move this class to its own file, perhaps in util
class SparseDataFactor(CustomFactor):
    """Abstract Base Class to be used for computing """

    def __init__(self):
        self.time_index = None
        self.curr_date = None # date for which time_index is accurate

    def bs_sparse_time(self, sid):
        # do a binary search of the dates array finding the index
        # where self.curr_date will lie.
        pass

    def cold_start(self, today, assets):
        # for each sid, do binary search of date array to find current index
        # the results can be shared across all factors that inherit from SparseDataFactor
        # this sets an array of ints: time_index
        self.time_index = np.full(assets.shape[0], -1)
        self.curr_date = today
        for asset in assets:
            self.time_index[asset.sid] = self.bs_sparse_time(asset.sid)

    def update_time_index(self, today):
        self.curr_date = today
        # for each asset check if today >= dates[self.time_index]
        # if so then increment self.time_index[asset.sid] += 1
        pass

    def compute(self, today, assets, out, *arrays):
        # for each asset in assets determine index from date (today)
        if self.time_index is None:
            self.cold_start(today, assets)

        if self.curr_date != today:
            self.update_time_index(today)

        out[:] = data[self.time_index]

# transfer data from memory
class CAPEX(SparseDataFactor):
    field = "CAPEX"

    def compute(self, today, assets, out):

        out[:] = 0.69
