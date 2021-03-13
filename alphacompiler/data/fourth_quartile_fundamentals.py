from alphacompiler.util.sparse_data import SparseDataFactor
from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from zipline.utils.paths import zipline_root


# this code should go with your application code.
class Fundamentals(SparseDataFactor):
    outputs = ['Revenue', 'Net_Income', 'Total_Assets', 'Total_Current_Liabilities']

    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = len(get_ticker_sid_dict_from_bundle("iex")) + 1  # max(sid)+1 get this from the bundle

        self.data_path = zipline_root() + '/data/4th.npy'
