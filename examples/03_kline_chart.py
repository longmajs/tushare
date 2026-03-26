# -*- coding:utf-8 -*-
"""
K线图绘制
依赖：pip install mplfinance
"""
import tushare as ts
from tushare.visual import plot_kline

# ── 最简单用法：股票代码直接绘图 ──────────────────────────
plot_kline('600519', start='2024-06-01', end='2024-12-31')

# ── 保存到文件（不弹窗）──────────────────────────────────
plot_kline('600519',
           start='2024-01-01',
           end='2024-12-31',
           savefig='maotai_2024.png')

# ── 指定均线 ──────────────────────────────────────────────
plot_kline('000858',
           start='2024-01-01',
           indicators=['ma5', 'ma10', 'ma20', 'ma60'],
           title='五粮液 2024',
           savefig='wuliangye_2024.png')

# ── 分钟K ─────────────────────────────────────────────────
plot_kline('600519', ktype='30', title='茅台 30分钟K')

# ── 传入已有 DataFrame ────────────────────────────────────
df = ts.get_k_data('601318', start='2024-01-01')
plot_kline(df, title='中国平安 日K', savefig='pingan_2024.png')

# ── 不显示成交量 ──────────────────────────────────────────
plot_kline('600519', start='2024-01-01', volume=False)
