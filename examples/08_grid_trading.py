# -*- coding:utf-8 -*-
"""
网格交易策略回测 - Grid Trading Strategy Backtest

网格交易是一种机械式、低频的交易策略，适合震荡市。
策略原理：
- 在中心价下方设置多个网格买入线（越跌越买）
- 在中心价上方设置多个网格卖出线（越涨越卖）
- 完全机械化执行，无需预测市场

Grid trading is a mechanical, low-frequency strategy for range-bound markets.
- Place buy orders at grid lines below center (buy the dip)
- Place sell orders at grid lines above center (take profit)
- Fully mechanical execution, no market prediction needed
"""
import tushare as ts
from tushare.quant.signals import grid_trading, ma_crossover
from tushare.quant.backtest import SimpleBacktest, plot_result

# 获取数据 - 贵州茅台 2020-2024 年数据
df = ts.get_k_data('600519', start='2020-01-01', end='2024-12-31')

# ── 网格策略回测（百分比网格）────────────────────────────
# 网格参数：5% 间距，上下各 5 格，60 日均线作为中心
signals_grid = grid_trading(
    df,
    grid_spacing=0.05,      # 5% 网格间距
    grid_count=5,           # 上下各 5 格
    lookback=60,            # 60 日滚动中心
    mode='pct',             # 百分比模式
)

bt = SimpleBacktest(
    initial_capital=100_000,  # 10 万初始资金
    commission=0.001,          # 万一手续费
    slippage=0.001,            # 万一滑点
)
result_grid = bt.run(df, signals_grid)

print("=== 网格交易策略回测结果（百分比网格）===")
print(f"  总收益率     : {result_grid.metrics['total_return']:.2%}")
print(f"  年化收益率   : {result_grid.metrics['annualized_return']:.2%}")
print(f"  最大回撤     : {result_grid.metrics['max_drawdown']:.2%}")
print(f"  夏普比率     : {result_grid.metrics['sharpe_ratio']:.2f}")
print(f"  胜率         : {result_grid.metrics['win_rate']:.2%}")
print(f"  盈亏比       : {result_grid.metrics['profit_factor']:.2f}")
print(f"\n交易统计:")
print(f"  总交易次数   : {len(result_grid.trades)}")
print(f"  买入次数     : {(result_grid.trades['action'] == 'buy').sum()}")
print(f"  卖出次数     : {(result_grid.trades['action'] == 'sell').sum()}")
print(f"\n最近 5 笔交易:")
print(result_grid.trades.tail(5)[['date', 'action', 'price', 'shares']].to_string(index=False))

# ── 网格策略回测（绝对价格网格）────────────────────────────
# 适合价格相对稳定的标的，如 ETF、大盘股
signals_grid_abs = grid_trading(
    df,
    mode='abs',           # 绝对价格模式
    grid_spacing=50,      # 每格 50 元
    grid_count=5,         # 上下各 5 格
    center_price=1000,    # 手动设定中心价
)
result_grid_abs = bt.run(df, signals_grid_abs)

print("\n=== 网格交易策略回测结果（绝对价格网格）===")
print(f"  总收益率     : {result_grid_abs.metrics['total_return']:.2%}")
print(f"  年化收益率   : {result_grid_abs.metrics['annualized_return']:.2%}")
print(f"  最大回撤     : {result_grid_abs.metrics['max_drawdown']:.2%}")
print(f"  夏普比率     : {result_grid_abs.metrics['sharpe_ratio']:.2f}")

# ── MA 交叉策略对比 ────────────────────────────────────────
signals_ma = ma_crossover(df, fast=5, slow=20)
result_ma = bt.run(df, signals_ma)

print("\n=== MA(5,20) 交叉策略回测结果（对比）===")
print(f"  总收益率     : {result_ma.metrics['total_return']:.2%}")
print(f"  年化收益率   : {result_ma.metrics['annualized_return']:.2%}")
print(f"  最大回撤     : {result_ma.metrics['max_drawdown']:.2%}")
print(f"  夏普比率     : {result_ma.metrics['sharpe_ratio']:.2f}")

# ── 策略对比汇总 ─────────────────────────────────────────
print("\n=== 策略对比汇总 ===")
print(f"{'策略':<15} {'总收益':>10} {'年化':>10} {'回撤':>10} {'夏普':>8}")
print("-" * 55)
print(f"{'网格 (百分比)':<15} {result_grid.metrics['total_return']:>9.2%} "
      f"{result_grid.metrics['annualized_return']:>9.2%} "
      f"{result_grid.metrics['max_drawdown']:>9.2%} "
      f"{result_grid.metrics['sharpe_ratio']:>8.2f}")
print(f"{'网格 (绝对价)':<15} {result_grid_abs.metrics['total_return']:>9.2%} "
      f"{result_grid_abs.metrics['annualized_return']:>9.2%} "
      f"{result_grid_abs.metrics['max_drawdown']:>9.2%} "
      f"{result_grid_abs.metrics['sharpe_ratio']:>8.2f}")
print(f"{'MA 交叉':<15} {result_ma.metrics['total_return']:>9.2%} "
      f"{result_ma.metrics['annualized_return']:>9.2%} "
      f"{result_ma.metrics['max_drawdown']:>9.2%} "
      f"{result_ma.metrics['sharpe_ratio']:>8.2f}")

# ── 网格信号详情 ─────────────────────────────────────────
print(f"\n网格信号详情（最后 10 个交易日）:")
print(signals_grid.tail(10)[['date', 'signal', 'strength', 'grid_level']].to_string(index=False))

# ── 资金曲线 ─────────────────────────────────────────────
print(f"\n资金曲线（最后 5 行）:")
print(result_grid.equity_curve.tail())

# ── 可视化资金曲线 ────────────────────────────────────────
try:
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(14, 8))

    # 绘制三条资金曲线
    result_grid.equity_curve.plot(x='date', y='equity', ax=ax, label='Grid (pct 5%)', linewidth=2)
    result_grid_abs.equity_curve.plot(x='date', y='equity', ax=ax, label='Grid (abs 50)', linewidth=2)
    result_ma.equity_curve.plot(x='date', y='equity', ax=ax, label='MA(5,20)', linewidth=2, linestyle='--')

    ax.set_title('Grid Trading Strategy vs MA Crossover (Kweichow Moutai 2020-2024)', fontsize=14)
    ax.set_ylabel('Equity (¥)', fontsize=12)
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('grid_trading_backtest.png', dpi=150, bbox_inches='tight')
    print("\n资金曲线已保存至 grid_trading_backtest.png")

except ImportError:
    print("提示：pip install matplotlib 可绘制资金曲线")

# ── 使用建议 ─────────────────────────────────────────────
print("\n=== 网格交易使用建议 ===")
print("1. 适合震荡市，不适合强趋势行情")
print("2. 网格间距越大，交易频率越低，单笔利润越高")
print("3. 可结合趋势过滤器（如 MA200）使用，只在趋势向上时做多")
print("4. 建议先小额实盘测试，熟悉策略特性后再加大资金")
print("5. 震荡市：百分比网格；价格稳定标的：绝对价格网格")
