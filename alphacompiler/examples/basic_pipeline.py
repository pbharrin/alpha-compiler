
"""
Basic usage of Pipeline.

Created by Peter Harrington (pbharrin) on 9/10/19.
"""
import pandas as pd

from zipline.data.bundles.core import register
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing

from alphacompiler.util.zipline_data_tools import make_pipeline_engine

def str2dt(datestr):
    return pd.to_datetime(datestr, utc=True)

# constants
BUNDLE = 'crsp'
data_dates = ('2015-01-06', '2015-01-30')
backtest_dates = ('2015-01-06', '2015-01-30')
pipeline_data_dates = (pd.to_datetime(data_dates[0], utc=True), pd.to_datetime(data_dates[1], utc=True))

# Step 1. Run Pipeline

# 1.0 dummy bundle register
register(BUNDLE, int)  # dummy register of a bundle

# 1.1 create the pipeline engine
spe = make_pipeline_engine(BUNDLE, pipeline_data_dates)

# 1.2 create your pipeline (this could be more elaborate)
pipe = Pipeline(columns={'Close': USEquityPricing.close.latest},)

# 1.3 run your pipeline with the pipeline engine
stocks = spe.run_pipeline(pipe, str2dt(backtest_dates[0]), str2dt(backtest_dates[1]))

print(stocks)
