# -*- coding:utf-8 -*-
"""
实时报价查询
"""
import tushare as ts

# ── 单只股票 ──────────────────────────────────────────────
df = ts.get_realtime_quotes('600519')
print("=== 茅台实时报价 ===")
print(df[['name', 'price', 'open', 'pre_close', 'changepercent',
          'bid', 'ask', 'volume', 'amount', 'time']].T)

# ── 多只同时查 ────────────────────────────────────────────
codes = ['600519', '000858', '601318', '000001']
df = ts.get_realtime_quotes(codes)
print("\n=== 多股报价 ===")
print(df[['code', 'name', 'price', 'changepercent', 'volume']].to_string(index=False))

# ── 五档买卖盘 ────────────────────────────────────────────
df = ts.get_realtime_quotes('000001')
bid_cols = ['bid1', 'bid2', 'bid3', 'bid4', 'bid5']
ask_cols = ['ask1', 'ask2', 'ask3', 'ask4', 'ask5']
bvol_cols = ['bidvol1', 'bidvol2', 'bidvol3', 'bidvol4', 'bidvol5']
avol_cols = ['askvol1', 'askvol2', 'askvol3', 'askvol4', 'askvol5']

print("\n=== 平安银行五档买卖盘 ===")
for i in range(5):
    b = df[bid_cols[i]].values[0]
    bv = df[bvol_cols[i]].values[0]
    a = df[ask_cols[i]].values[0]
    av = df[avol_cols[i]].values[0]
    print(f"  卖{i+1}: {a:>8}  ({av:>5}手)    买{i+1}: {b:>8}  ({bv:>5}手)")
