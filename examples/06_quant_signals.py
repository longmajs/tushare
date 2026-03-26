# -*- coding:utf-8 -*-
"""
量化信号生成：MA 金叉、MACD、RSI、布林带
"""
import tushare as ts
from tushare.quant.signals import (
    ma_crossover, macd_signal, rsi_signal, bollinger_signal,
)

df = ts.get_k_data('600519', start='2023-01-01')

# ── MA 金叉 / 死叉 ────────────────────────────────────────
sig_ma = ma_crossover(df, fast=5, slow=20)
buys = sig_ma[sig_ma['signal'] == 'buy']
sells = sig_ma[sig_ma['signal'] == 'sell']
print(f"MA(5,20) 金叉次数: {len(buys)}，死叉次数: {len(sells)}")
print(buys[['date', 'signal', 'strength', 'ma_fast', 'ma_slow']].tail(3))

# ── MACD ─────────────────────────────────────────────────
sig_macd = macd_signal(df)  # 默认 fast=12, slow=26, signal=9
buys = sig_macd[sig_macd['signal'] == 'buy']
print(f"\nMACD 买入信号次数: {len(buys)}")
print(buys[['date', 'signal', 'macd', 'signal_line', 'hist']].tail(3))

# ── RSI 超买超卖 ──────────────────────────────────────────
sig_rsi = rsi_signal(df)  # 默认 period=14, overbought=70, oversold=30
oversold = sig_rsi[sig_rsi['signal'] == 'buy']   # RSI < 30 超卖买入
overbought = sig_rsi[sig_rsi['signal'] == 'sell'] # RSI > 70 超买卖出
print(f"\nRSI 超卖(买)次数: {len(oversold)}，超买(卖)次数: {len(overbought)}")
print(sig_rsi[sig_rsi['signal'] != 'hold'][['date', 'signal', 'rsi']].tail(5))

# ── 布林带突破 ────────────────────────────────────────────
sig_bb = bollinger_signal(df)  # 默认 period=20, std_dev=2
print(f"\n布林带信号：\n{sig_bb[sig_bb['signal'] != 'hold'][['date', 'signal', 'close', 'upper', 'lower']].tail(5)}")

# ── 组合：同时满足 MA + RSI 才操作 ───────────────────────
import pandas as pd
combined = sig_ma[['date', 'signal']].merge(
    sig_rsi[['date', 'rsi']], on='date'
)
# 金叉 + RSI 未超买（<70）才买入
strong_buy = combined[(combined['signal'] == 'buy') & (combined['rsi'] < 70)]
print(f"\n组合信号（MA金叉 + RSI<70）买入次数: {len(strong_buy)}")
print(strong_buy[['date', 'signal', 'rsi']].tail(5))
