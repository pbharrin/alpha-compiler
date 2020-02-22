
"""

Created by Peter Harrington (pbharrin) on 8/5/19.
"""

from zipline.pipeline.factors import CustomFactor
import numpy as np


ZIPLINE_DATA_DIR = '/Users/peterharrington/.zipline/data/'  # TODO: get this from Zipline api
SID_FILE = "SHARDAR_sectors.npy"

STATIC_FILE = "SHARDAR_static.npy"


class SHARADARSectorCodes(CustomFactor):  # this will be depricated in favor of the multioutput below
    """Returns a value for an SID stored in memory."""
    inputs = []
    window_length = 1

    def __init__(self, *args, **kwargs):
        self.data = np.load(ZIPLINE_DATA_DIR + SID_FILE)

    def compute(self, today, assets, out):
        out[:] = self.data[assets]


class SHARADARStatic(CustomFactor):
    """Returns static values for an SID.
    This holds static data (does not change with time) like: exchange, sector, etc."""
    inputs = []
    window_length = 1
    outputs = ['sector', 'exchange', 'category']

    def __init__(self, *args, **kwargs):
        self.data = np.load(ZIPLINE_DATA_DIR + STATIC_FILE)

    def compute(self, today, assets, out):
        # out[:] = self.data[assets]
        out['sector'][:] = self.data[0, assets]
        out['exchange'][:] = self.data[1, assets]
        out['category'][:] = self.data[2, assets]