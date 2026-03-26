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
print(buys[['date', 'signal', 'strength', 'ma_fast', 'ma_slow']].tail(3).to_string(index=False))

# ── MACD ─────────────────────────────────────────────────
# 列名: date, signal, strength, macd, macd_signal, macd_hist
sig_macd = macd_signal(df)
buys = sig_macd[sig_macd['signal'] == 'buy']
print(f"\nMACD 买入信号次数: {len(buys)}")
print(buys[['date', 'signal', 'macd', 'macd_signal', 'macd_hist']].tail(3).to_string(index=False))

# ── RSI 超买超卖 ──────────────────────────────────────────
# 列名: date, signal, strength, rsi
sig_rsi = rsi_signal(df)
oversold  = sig_rsi[sig_rsi['signal'] == 'buy']
overbought = sig_rsi[sig_rsi['signal'] == 'sell']
print(f"\nRSI 超卖(买)次数: {len(oversold)}，超买(卖)次数: {len(overbought)}")
print(sig_rsi[sig_rsi['signal'] != 'hold'][['date', 'signal', 'rsi']].tail(5).to_string(index=False))

# ── 布林带突破 ────────────────────────────────────────────
# 列名: date, signal, strength, bb_upper, bb_middle, bb_lower
sig_bb = bollinger_signal(df)
bb_signals = sig_bb[sig_bb['signal'] != 'hold']
print(f"\n布林带信号 {len(bb_signals)} 次：")
print(bb_signals[['date', 'signal', 'bb_upper', 'bb_lower']].tail(5).to_string(index=False))

# ── 组合：MA 金叉 + RSI 未超买（<70）才买入 ──────────────
import pandas as pd
combined = sig_ma[['date', 'signal']].merge(
    sig_rsi[['date', 'rsi']], on='date'
)
strong_buy = combined[(combined['signal'] == 'buy') & (combined['rsi'] < 70)]
print(f"\n组合信号（MA金叉 + RSI<70）买入次数: {len(strong_buy)}")
print(strong_buy[['date', 'signal', 'rsi']].tail(5).to_string(index=False))
