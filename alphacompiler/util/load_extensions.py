
"""
This is needed if you want to use a custom bundle, a little annoying.

Created by Peter Harrington (pbharrin) on 5/8/20.
"""


import os
from zipline.utils.run_algo import load_extensions


print("Loading Extensions\n")
load_extensions(
    default=True,
    extensions=[],
    strict=True,
    environ=os.environ,
)