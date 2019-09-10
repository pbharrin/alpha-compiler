
from alphacompiler.util.sparse_data import SparseDataFactor
from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
import os
# TODO: this should be deleted and only included as an example
# this code should go with your application code.
class Fundamentals(SparseDataFactor):
    # outputs = ["marketcap", "pb"]  # basic risk sectors needed
    outputs = ['netinc', 'equity', 'bvps', 'sps', 'fcfps', 'price']

    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = len(get_ticker_sid_dict_from_bundle("sep")) + 1  # max(sid)+1 get this from the bundle

        self.data_path = '/Users/peterharrington/.zipline/data/' + 'SF1.npy'
