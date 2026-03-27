# -*- coding:utf-8 -*-
"""
量化分析模块
Quant analysis: signal generators, backtesting engine, and stock screeners
"""
from tushare.quant.signals import (
    ma_crossover,
    macd_signal,
    rsi_signal,
    bollinger_signal,
    grid_trading,
    ma_macd_combo,
    valuation_dca,
    sector_rotation,
    dividend_dog,
)
from tushare.quant.backtest import SimpleBacktest, BacktestResult, plot_result
from tushare.quant.screener import (
    value_screen,
    growth_screen,
    quality_screen,
    momentum_screen,
    dividend_screen,
    combine_screens,
)
