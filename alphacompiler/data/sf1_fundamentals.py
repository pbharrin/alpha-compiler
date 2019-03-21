
from alphacompiler.util.sparse_data import SparseDataFactor
from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
import os
# TODO: this should be deleted and only included as an example
# this code should go with your application code.
class Fundamentals(SparseDataFactor):
    # outputs = ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]
    outputs = ["marketcap", "pb"]

    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = len(get_ticker_sid_dict_from_bundle("quantopian-quandl")) + 1  # max(sid)+1 get this from the bundle
        self.data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "SF1.npy")