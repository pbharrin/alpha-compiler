
from alphacompiler.util.sparse_data import SparseDataFactor

class Fundamentals(SparseDataFactor):
    inputs = []
    window_length = 1
    outputs = ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]

    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = 3193  #(max sid +1) get this from the bundle
        self.data_path = "/Users/peterharrington/Documents/GitHub/alpha-compiler/alphacompiler/data/SF1.npy"
