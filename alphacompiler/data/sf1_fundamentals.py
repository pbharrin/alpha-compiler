
from alphacompiler.util.sparse_data import SparseDataFactor

# TODO: this should be deleted and only included as an example
# this code should go with your application code.  
class Fundamentals(SparseDataFactor):
    # outputs = ["ROE_ART", "BVPS_ARQ", "SPS_ART", "FCFPS_ARQ", "PRICE"]
    outputs = ["marketcap", "pb"]

    def __init__(self, *args, **kwargs):
        super(Fundamentals, self).__init__(*args, **kwargs)
        self.N = 956  # max(sid)+1 get this from the bundle
        self.data_path = "/Users/peterharrington/Documents/GitHub/alpha-compiler/alphacompiler/data/SF1.npy"
