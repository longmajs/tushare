# -*- coding:utf-8 -*-
"""
本地 Parquet 缓存与增量更新
依赖：pip install pyarrow
存储路径：~/.cache/tushare/parquet/
"""
import tushare as ts
from tushare.store import (
    ParquetStore, save_kline, load_kline,
    update_kline, data_qc,
)

# ── 1. 首次下载并保存 ─────────────────────────────────────
df = ts.get_k_data('600519', start='2020-01-01')
save_kline('600519', df)
print(f"已保存 {len(df)} 条")

# ── 2. 从本地读取 ─────────────────────────────────────────
df_local = load_kline('600519')
print(f"本地共 {len(df_local)} 条，最新日期：{df_local['date'].max()}")

# ── 3. 按日期过滤 ─────────────────────────────────────────
df_2024 = load_kline('600519', start='2024-01-01', end='2024-12-31')
print(f"2024年 {len(df_2024)} 条")

# ── 4. 增量更新（只拉最新缺失部分）──────────────────────
def fetch(code, start=None):
    return ts.get_k_data(code, start=start or '')

df_updated = update_kline('600519', fetch_func=fetch)
print(f"更新后共 {len(df_updated)} 条")

# ── 5. 数据质量检查 ───────────────────────────────────────
qc = data_qc(df_updated)
print("\n=== 数据质量报告 ===")
print(f"  NaN比例         : {qc['nan_ratio']:.2%}")
print(f"  重复日期        : {qc['duplicate_dates']}")
print(f"  零成交量天数    : {qc['zero_volume_trading_days']}")
print(f"  价格异常行数    : {len(qc['price_anomalies'])}")
print(f"  整体通过        : {qc['passed']}")

# ── 6. 面向对象用法 ───────────────────────────────────────
store = ParquetStore(store_dir='/tmp/my_tushare_cache')
store.save_kline('000858', ts.get_k_data('000858', start='2023-01-01'))
df = store.load_kline('000858', start='2024-01-01')
print(f"\n五粮液 2024 本地缓存: {len(df)} 条")
