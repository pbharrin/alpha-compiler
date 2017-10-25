
"""
Custom factor to get NASDAQ sector codes from a flat file
downloaded from NASDAQ.  No date information, so if sector
changes the most recent date's sector will be used.

Created by Peter Harrington (pbharrin) on 10/25/17.
"""
from zipline.pipeline.factors import CustomFactor
import pandas as pd
import numpy as np

# TODO: make this a paramater
BASE_PATH = "/Users/peterharrington/Documents/GitHub/alpha-compiler/alphacompiler/data/"
SID_FILE = "NASDAQ_sids.npy"

class NASDAQSectorCodes(CustomFactor):
    """Returns a value for an SID stored in memory."""
    inputs = []
    window_length = 1

    def __init__(self, *args, **kwargs):
        self.data = np.load(BASE_PATH + SID_FILE)

    def compute(self, today, assets, out):
        print "assets.shape: ",assets.shape
        out[:] = np.random.random(assets.shape)

if __name__ == '__main__':
    print "all done "