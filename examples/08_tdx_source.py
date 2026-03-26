# -*- coding:utf-8 -*-
"""
通达信数据源（低延迟、无反爬限制）
依赖：pip install pytdx
"""
from tushare.stock.market_core import set_market_config, get_market_config
import tushare as ts

# ── 查看当前配置 ──────────────────────────────────────────
print("当前配置:", get_market_config())

# ── 启用 tdx，优先走通达信协议 ───────────────────────────
set_market_config(
    enable_tdx=True,
    prefer_sources=['tdx', 'tencent', 'sina'],  # 优先级顺序
)

# 启用后正常调用，自动走 tdx（失败会 fallback 到下一个源）
df = ts.get_k_data('600519', start='2024-01-01')
print(f"\n茅台日K（tdx优先）: {len(df)} 条")
print(df.tail(3))

df_rt = ts.get_realtime_quotes('600519')
print(f"\n实时报价: {df_rt[['name', 'price', 'time']].to_string(index=False)}")

# ── 直接使用 pytdx 底层 API ───────────────────────────────
from tushare.stock.market_core import _get_tdx_api, _probe_tdx_host

# 探测最快节点
best = _probe_tdx_host(timeout=3)
print(f"\n最优 TDX 节点: {best}")

api = _get_tdx_api()

# 茅台日K（上海=1，ktype=9）
bars = api.get_security_bars(9, 1, '600519', 0, 10)
print(f"\n茅台最近10根日K:")
for b in bars:
    print(f"  {b['datetime']}  开:{b['open']:.2f} 收:{b['close']:.2f} 量:{b['vol']}")

# 实时五档报价
quotes = api.get_security_quotes([(1, '600519'), (0, '000858')])
for q in quotes:
    print(f"\n代码:{q['code']} 现价:{q['price']} 买1:{q['bid1']} 卖1:{q['ask1']}")

# 分钟K（ktype: 0=5分, 1=15分, 2=30分, 3=60分）
bars_5m = api.get_security_bars(0, 1, '600519', 0, 20)
print(f"\n茅台5分钟K（最近20根）:")
for b in bars_5m[-5:]:
    print(f"  {b['datetime']}  收:{b['close']:.2f}")
