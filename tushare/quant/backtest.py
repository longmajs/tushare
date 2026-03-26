# -*- coding:utf-8 -*-
"""
简单回测引擎
Simple backtesting engine for evaluating signal-based strategies.
"""
import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass, field
from typing import List, Dict

LOG = logging.getLogger("tushare.quant.backtest")


@dataclass
class BacktestResult:
    """Container for backtest results."""
    equity_curve: pd.DataFrame  # date, equity, drawdown
    trades: pd.DataFrame  # date, action, price, shares, value, commission
    metrics: Dict[str, float] = field(default_factory=dict)


class SimpleBacktest:
    """
    简单回测引擎

    Parameters
    ----------
    initial_capital : float
        Starting capital (default 100000).
    commission : float
        Commission rate per trade (default 0.001 = 0.1%).
    slippage : float
        Slippage rate per trade (default 0.001 = 0.1%).
    """

    def __init__(self, initial_capital=100000, commission=0.001, slippage=0.001):
        self.initial_capital = initial_capital
        self.commission = commission
        self.slippage = slippage

    def run(self, df, signals_df):
        """
        Run backtest on price data with signals.

        Parameters
        ----------
        df : DataFrame
            Price data with 'date' and 'close' columns.
        signals_df : DataFrame
            Signal data with 'date' and 'signal' columns ('buy'/'sell'/'hold').

        Returns
        -------
        BacktestResult
        """
        if 'date' not in df.columns or 'close' not in df.columns:
            raise ValueError("df must contain 'date' and 'close' columns")
        if 'date' not in signals_df.columns or 'signal' not in signals_df.columns:
            raise ValueError("signals_df must contain 'date' and 'signal' columns")

        prices = df[['date', 'close']].copy().sort_values('date').reset_index(drop=True)
        signals = signals_df[['date', 'signal']].copy()
        merged = prices.merge(signals, on='date', how='left')
        merged['signal'] = merged['signal'].fillna('hold')

        cash = self.initial_capital
        shares = 0
        trades: List[Dict] = []
        equity_records: List[Dict] = []

        for _, row in merged.iterrows():
            price = row['close']
            signal = row['signal']
            date = row['date']

            if signal == 'buy' and shares == 0:
                # Apply slippage (buy at higher price)
                exec_price = price * (1 + self.slippage)
                affordable = int(cash / (exec_price * (1 + self.commission)))
                if affordable > 0:
                    cost = affordable * exec_price
                    comm = cost * self.commission
                    cash -= (cost + comm)
                    shares = affordable
                    trades.append({
                        'date': date, 'action': 'buy', 'price': exec_price,
                        'shares': affordable, 'value': cost, 'commission': comm,
                    })

            elif signal == 'sell' and shares > 0:
                # Apply slippage (sell at lower price)
                exec_price = price * (1 - self.slippage)
                revenue = shares * exec_price
                comm = revenue * self.commission
                cash += (revenue - comm)
                trades.append({
                    'date': date, 'action': 'sell', 'price': exec_price,
                    'shares': shares, 'value': revenue, 'commission': comm,
                })
                shares = 0

            equity = cash + shares * price
            equity_records.append({'date': date, 'equity': equity})

        equity_df = pd.DataFrame(equity_records)
        if len(equity_df) > 0:
            peak = equity_df['equity'].cummax()
            equity_df['drawdown'] = (equity_df['equity'] - peak) / peak
        else:
            equity_df['drawdown'] = []

        trades_df = pd.DataFrame(trades) if trades else pd.DataFrame(
            columns=['date', 'action', 'price', 'shares', 'value', 'commission']
        )

        metrics = self._compute_metrics(equity_df, trades_df)

        return BacktestResult(
            equity_curve=equity_df,
            trades=trades_df,
            metrics=metrics,
        )

    def _compute_metrics(self, equity_df, trades_df):
        """Compute performance metrics."""
        metrics = {}

        if len(equity_df) == 0:
            return metrics

        initial = self.initial_capital
        final = equity_df['equity'].iloc[-1]

        # Total return
        metrics['total_return'] = (final - initial) / initial

        # Annualized return
        n_days = len(equity_df)
        if n_days > 1:
            metrics['annualized_return'] = (final / initial) ** (252 / n_days) - 1
        else:
            metrics['annualized_return'] = 0.0

        # Max drawdown
        metrics['max_drawdown'] = equity_df['drawdown'].min() if len(equity_df) > 0 else 0.0

        # Sharpe ratio (daily returns, annualized)
        daily_returns = equity_df['equity'].pct_change().dropna()
        if len(daily_returns) > 1 and daily_returns.std() > 0:
            metrics['sharpe_ratio'] = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
        else:
            metrics['sharpe_ratio'] = 0.0

        # Win rate and profit factor from trades
        if len(trades_df) > 0:
            sells = trades_df[trades_df['action'] == 'sell']
            buys = trades_df[trades_df['action'] == 'buy']

            if len(sells) > 0 and len(buys) > 0:
                # Pair up buy/sell trades
                n_pairs = min(len(buys), len(sells))
                buy_values = buys['value'].values[:n_pairs] + buys['commission'].values[:n_pairs]
                sell_values = sells['value'].values[:n_pairs] - sells['commission'].values[:n_pairs]
                pnl = sell_values - buy_values

                wins = pnl[pnl > 0]
                losses = pnl[pnl <= 0]

                metrics['win_rate'] = len(wins) / len(pnl) if len(pnl) > 0 else 0.0

                gross_profit = wins.sum() if len(wins) > 0 else 0.0
                gross_loss = abs(losses.sum()) if len(losses) > 0 else 0.0
                metrics['profit_factor'] = (
                    gross_profit / gross_loss if gross_loss > 0 else float('inf')
                )
            else:
                metrics['win_rate'] = 0.0
                metrics['profit_factor'] = 0.0
        else:
            metrics['win_rate'] = 0.0
            metrics['profit_factor'] = 0.0

        return metrics


def plot_result(result):
    """
    绘制回测结果图表

    Plots equity curve, drawdown, and trade markers on price chart.

    Parameters
    ----------
    result : BacktestResult
        Output from SimpleBacktest.run()
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        LOG.error("matplotlib is required for plotting. Install with: pip install matplotlib")
        raise

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True,
                             gridspec_kw={'height_ratios': [3, 1, 1]})

    eq = result.equity_curve
    trades = result.trades

    # Equity curve
    ax1 = axes[0]
    ax1.plot(eq['date'], eq['equity'], label='Equity', color='steelblue', linewidth=1.5)
    ax1.set_ylabel('Equity')
    ax1.set_title('Backtest Result')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)

    # Trade markers on equity curve
    if len(trades) > 0:
        buys = trades[trades['action'] == 'buy']
        sells = trades[trades['action'] == 'sell']
        buy_eq = eq[eq['date'].isin(buys['date'])]
        sell_eq = eq[eq['date'].isin(sells['date'])]
        ax1.scatter(buy_eq['date'], buy_eq['equity'], marker='^', color='green',
                    s=80, zorder=5, label='Buy')
        ax1.scatter(sell_eq['date'], sell_eq['equity'], marker='v', color='red',
                    s=80, zorder=5, label='Sell')
        ax1.legend(loc='upper left')

    # Drawdown
    ax2 = axes[1]
    ax2.fill_between(eq['date'], eq['drawdown'], 0, color='coral', alpha=0.5)
    ax2.set_ylabel('Drawdown')
    ax2.grid(True, alpha=0.3)

    # Metrics annotation
    ax3 = axes[2]
    ax3.axis('off')
    m = result.metrics
    text_lines = [
        f"Total Return: {m.get('total_return', 0):.2%}",
        f"Annual Return: {m.get('annualized_return', 0):.2%}",
        f"Max Drawdown: {m.get('max_drawdown', 0):.2%}",
        f"Sharpe Ratio: {m.get('sharpe_ratio', 0):.2f}",
        f"Win Rate: {m.get('win_rate', 0):.2%}",
        f"Profit Factor: {m.get('profit_factor', 0):.2f}",
    ]
    ax3.text(0.5, 0.5, '  |  '.join(text_lines),
             transform=ax3.transAxes, ha='center', va='center',
             fontsize=10, family='monospace',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

    plt.tight_layout()
    plt.show()
