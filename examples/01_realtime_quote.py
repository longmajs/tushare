# -*- coding:utf-8 -*-
"""
实时报价查询
实际列名: name, open, pre_close, price, high, low, bid, ask,
          volume, amount, b1_v~b5_v, b1_p~b5_p, a1_v~a5_v, a1_p~a5_p,
          date, time, code
"""
import pandas as pd
import tushare as ts

# ── 单只股票 ──────────────────────────────────────────────
df = ts.get_realtime_quotes('600519')

# 计算涨跌幅
df['price'] = pd.to_numeric(df['price'])
df['pre_close'] = pd.to_numeric(df['pre_close'])
df['changepercent'] = (df['price'] - df['pre_close']) / df['pre_close'] * 100

print("=== 茅台实时报价 ===")
print(df[['name', 'price', 'open', 'pre_close', 'changepercent',
          'bid', 'ask', 'volume', 'amount', 'time']].T)

# ── 多只同时查 ────────────────────────────────────────────
codes = ['600519', '000858', '601318', '000001']
df = ts.get_realtime_quotes(codes)
df['price'] = pd.to_numeric(df['price'])
df['pre_close'] = pd.to_numeric(df['pre_close'])
df['changepercent'] = ((df['price'] - df['pre_close']) / df['pre_close'] * 100).round(2)
print("\n=== 多股报价 ===")
print(df[['code', 'name', 'price', 'changepercent', 'volume']].to_string(index=False))

# ── 五档买卖盘 ────────────────────────────────────────────
df = ts.get_realtime_quotes('000001')
print("\n=== 平安银行五档买卖盘 ===")
for i in range(1, 6):
    b_p = df[f'b{i}_p'].values[0]
    b_v = df[f'b{i}_v'].values[0]
    a_p = df[f'a{i}_p'].values[0]
    a_v = df[f'a{i}_v'].values[0]
    print(f"  卖{i}: {a_p:>8}  ({a_v:>5}手)    买{i}: {b_p:>8}  ({b_v:>5}手)")
