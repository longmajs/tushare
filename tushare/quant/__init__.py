# -*- coding:utf-8 -*-
"""
量化分析模块
Quant analysis: signal generators and backtesting engine
"""
from tushare.quant.signals import (
    ma_crossover,
    macd_signal,
    rsi_signal,
    bollinger_signal,
)
from tushare.quant.backtest import SimpleBacktest, BacktestResult, plot_result
