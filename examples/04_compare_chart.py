# -*- coding:utf-8 -*-
"""
多股对比走势图
依赖：pip install matplotlib
"""
from tushare.visual import plot_compare

# ── 白酒板块对比（归一化为涨跌幅%）────────────────────────
plot_compare(
    codes=['600519', '000858', '000596', '002304'],
    start='2024-01-01',
    end='2024-12-31',
    title='白酒板块 2024 对比',
    savefig='baijiu_compare.png',
)

# ── 银行板块对比 ──────────────────────────────────────────
plot_compare(
    codes=['601318', '601628', '601601'],
    start='2023-01-01',
    title='平安 / 国寿 / 太保 对比',
)

# ── 不归一化：直接比价格绝对值 ────────────────────────────
plot_compare(
    codes=['000001', '000002'],
    start='2024-01-01',
    normalize=False,
    title='平安银行 vs 万科 收盘价',
)
