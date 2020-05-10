import unittest
import os
from alphacompiler.data.load_quandl_sf1 import populate_raw_data_from_dump

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle


class Test_Populate_From_Dump(unittest.TestCase):
    def test_reads_file(self):
        bundle_name = 'sep'
        tickers = get_ticker_sid_dict_from_bundle(bundle_name)
        self.assertTrue(len(tickers) > 0)  # test the bundle is not empty

        BASE = os.path.dirname(os.path.realpath(__file__))
        RAW_FLDR = "raw"  # folder to store the raw text file

        fields = ['netinc', 'equity', 'bvps', 'sps', 'fcfps', 'price']  # basic QV
        dims = ['ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ', 'ARQ']

        populate_raw_data_from_dump(tickers, fields, dims, raw_path=os.path.join(BASE, RAW_FLDR))

        self.assertEquals(True, True)


if __name__ == '__main__':
    unittest.main()
