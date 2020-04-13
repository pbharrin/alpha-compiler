
"""
This file plots the loading to different risk factors.

Created by Peter Harrington (pbharrin) on 11/11/18.
"""
import pandas as pd
import pyfolio as pf
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

# BACKTEST_FN = '../backtest/perf.h5'
BACKTEST_FN = '../backtest/perf_risk.h5'

FACTOR_LOADINGS_FILE = '../risk/factor_loadings.h5'


if __name__ == '__main__':
    results = pd.read_hdf(BACKTEST_FN, 'key')

    # load backtest of strategy
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(results)
    risk_factors_panel = pd.read_hdf(FACTOR_LOADINGS_FILE, 'key')

    # plot one risk exposure panel at a time

    vertical_sections = 1
    fig = plt.figure(0, figsize=(14, vertical_sections * 6))
    gs = gridspec.GridSpec(vertical_sections, 3, wspace=0.5, hspace=0.5)
    ax_sector_alloc = plt.subplot(gs[:1, :])
    ax = plt.subplot(gs[:1, :])

    sfe = pf.risk.compute_style_factor_exposures(positions, risk_factors_panel['HML'])
    print(sfe)
    pf.risk.plot_style_factor_exposures(sfe, 'HML', ax)

    # ---  plot all Style Factor Exposures in one plot  --- #

    # start of copied code (the following 14 lines were copied from:
    # https://github.com/quantopian/pyfolio/blob/master/pyfolio/tears.py#L1412 )
    # vertical_sections = len(risk_factors_panel.items)
    # fig = plt.figure(figsize=[14, vertical_sections * 6])
    # gs = gridspec.GridSpec(vertical_sections, 3, wspace=0.5, hspace=0.5)
    #
    # style_axes = []  # style axes
    # style_axes.append(plt.subplot(gs[0, :]))  # insert first axis
    # for i in range(1, len(risk_factors_panel.items)):
    #     print plt.subplot(gs[i, :])
    #     style_axes.append(plt.subplot(gs[i, :]))
    #
    # j = 0
    # for name, df in risk_factors_panel.iteritems():
    #     sfe = pf.risk.compute_style_factor_exposures(positions, df)
    #     pf.risk.plot_style_factor_exposures(sfe, name, style_axes[j])
    #     j += 1
     # end of copied code

    plt.show()
