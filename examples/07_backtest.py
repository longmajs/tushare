# -*- coding:utf-8 -*-
"""
简单回测：资金曲线、最大回撤、夏普比率
"""
import tushare as ts
from tushare.quant.signals import ma_crossover, macd_signal
from tushare.quant.backtest import SimpleBacktest

df = ts.get_k_data('600519', start='2020-01-01', end='2024-12-31')

# ── 基本回测（MA 金叉死叉策略）────────────────────────────
signals = ma_crossover(df, fast=5, slow=20)

bt = SimpleBacktest(
    initial_capital=100_000,  # 10万初始资金
    commission=0.001,          # 万一手续费
    slippage=0.001,            # 万一滑点
)
result = bt.run(df, signals)

print("=== MA(5,20) 策略回测结果 ===")
print(f"  初始资金    : ¥{result.metrics['initial_capital']:,.0f}")
print(f"  最终资金    : ¥{result.metrics['final_equity']:,.0f}")
print(f"  总收益率    : {result.metrics['total_return']:.2%}")
print(f"  年化收益率  : {result.metrics['annualized_return']:.2%}")
print(f"  最大回撤    : {result.metrics['max_drawdown']:.2%}")
print(f"  夏普比率    : {result.metrics['sharpe_ratio']:.2f}")
print(f"  胜率        : {result.metrics['win_rate']:.2%}")
print(f"  总交易次数  : {result.metrics['total_trades']}")
print(f"\n最近5笔交易:")
print(result.trades.tail(5)[['date', 'action', 'price', 'shares', 'pnl']].to_string(index=False))

# ── MACD 策略对比 ────────────────────────────────────────
signals_macd = macd_signal(df)
result_macd = bt.run(df, signals_macd)

print("\n=== MACD 策略回测结果 ===")
print(f"  总收益率    : {result_macd.metrics['total_return']:.2%}")
print(f"  年化收益率  : {result_macd.metrics['annualized_return']:.2%}")
print(f"  最大回撤    : {result_macd.metrics['max_drawdown']:.2%}")
print(f"  夏普比率    : {result_macd.metrics['sharpe_ratio']:.2f}")

# ── 资金曲线 DataFrame ───────────────────────────────────
print(f"\n资金曲线（前5行）:")
print(result.equity_curve.head())

# ── 可视化资金曲线（需要 matplotlib）────────────────────
try:
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(12, 5))
    result.equity_curve.plot(x='date', y='equity', ax=ax, label='MA(5,20)')
    result_macd.equity_curve.plot(x='date', y='equity', ax=ax, label='MACD')
    ax.set_title('策略资金曲线对比（茅台 2020-2024）')
    ax.set_ylabel('资金 (¥)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('backtest_equity.png', dpi=150)
    print("\n资金曲线已保存至 backtest_equity.png")
except ImportError:
    print("提示: pip install matplotlib 可绘制资金曲线")
