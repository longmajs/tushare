# -*- coding:utf-8 -*-
"""
K线数据获取
"""
import tushare as ts

# ── 日K（默认前复权） ─────────────────────────────────────
df = ts.get_k_data('600519', start='2024-01-01', end='2024-12-31')
print("=== 茅台 2024 日K（最后5条）===")
print(df.tail())
print(f"共 {len(df)} 条")

# ── 不复权 ────────────────────────────────────────────────
df_nfq = ts.get_k_data('600519', start='2024-01-01', autype=None)
print("\n=== 不复权 ===")
print(df_nfq.tail(3))

# ── 后复权 ────────────────────────────────────────────────
df_hfq = ts.get_k_data('600519', start='2024-01-01', autype='hfq')
print("\n=== 后复权 ===")
print(df_hfq.tail(3))

# ── 分钟K ─────────────────────────────────────────────────
df_5m = ts.get_k_data('600519', ktype='5')    # 5分钟K
df_60m = ts.get_k_data('600519', ktype='60')  # 60分钟K
print(f"\n5分钟K: {len(df_5m)} 条，最新：\n{df_5m.tail(3)}")

# ── 周K / 月K ─────────────────────────────────────────────
df_w = ts.get_k_data('600519', start='2020-01-01', ktype='W')
df_m = ts.get_k_data('600519', start='2020-01-01', ktype='M')
print(f"\n周K: {len(df_w)} 条，月K: {len(df_m)} 条")

# ── 指数 ──────────────────────────────────────────────────
df_sh = ts.get_k_data('sh', index=True, start='2024-01-01')  # 上证指数
df_sz = ts.get_k_data('sz', index=True, start='2024-01-01')  # 深证成指
print(f"\n上证指数 {len(df_sh)} 条，深证成指 {len(df_sz)} 条")
print(df_sh.tail(3))
