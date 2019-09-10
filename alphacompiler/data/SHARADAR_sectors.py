
"""

Created by Peter Harrington (pbharrin) on 8/5/19.
"""

from zipline.pipeline.factors import CustomFactor
import numpy as np


ZIPLINE_DATA_DIR = '/Users/peterharrington/.zipline/data/'  # TODO: get this from Zipline api
SID_FILE = "SHARDAR_sectors.npy"


class SHARADARSectorCodes(CustomFactor):
    """Returns a value for an SID stored in memory."""
    inputs = []
    window_length = 1

    def __init__(self, *args, **kwargs):
        self.data = np.load(ZIPLINE_DATA_DIR + SID_FILE)

    def compute(self, today, assets, out):
        out[:] = self.data[assets]