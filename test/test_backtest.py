# -*- coding:utf-8 -*-
"""
Tests for tushare.quant.backtest
Known scenario with expected metrics.
"""
import unittest
import pandas as pd
import numpy as np
from tushare.quant.backtest import SimpleBacktest, BacktestResult


class TestSimpleBacktest(unittest.TestCase):

    def _make_scenario(self):
        """Create a known buy-low sell-high scenario."""
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        prices = [100, 95, 90, 85, 80, 85, 90, 95, 100, 105]
        df = pd.DataFrame({'date': dates, 'close': prices})

        # Buy at day 5 (price=80), sell at day 10 (price=105)
        signals = pd.DataFrame({
            'date': dates,
            'signal': ['hold', 'hold', 'hold', 'hold', 'buy',
                        'hold', 'hold', 'hold', 'hold', 'sell'],
        })
        return df, signals

    def test_basic_run(self):
        df, signals = self._make_scenario()
        bt = SimpleBacktest(initial_capital=100000, commission=0.001, slippage=0.001)
        result = bt.run(df, signals)

        self.assertIsInstance(result, BacktestResult)
        self.assertEqual(len(result.equity_curve), 10)
        self.assertGreater(len(result.trades), 0)

    def test_profitable_trade(self):
        df, signals = self._make_scenario()
        bt = SimpleBacktest(initial_capital=100000, commission=0.0, slippage=0.0)
        result = bt.run(df, signals)

        # Buy at 80, sell at 105 = 31.25% gain on position
        self.assertGreater(result.metrics['total_return'], 0)
        self.assertGreater(result.metrics['win_rate'], 0)

    def test_commission_reduces_return(self):
        df, signals = self._make_scenario()
        bt_no_cost = SimpleBacktest(initial_capital=100000, commission=0.0, slippage=0.0)
        bt_with_cost = SimpleBacktest(initial_capital=100000, commission=0.01, slippage=0.01)

        r1 = bt_no_cost.run(df, signals)
        r2 = bt_with_cost.run(df, signals)

        self.assertGreater(r1.metrics['total_return'], r2.metrics['total_return'])

    def test_no_signals_no_trades(self):
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        df = pd.DataFrame({'date': dates, 'close': [100] * 10})
        signals = pd.DataFrame({'date': dates, 'signal': ['hold'] * 10})

        bt = SimpleBacktest()
        result = bt.run(df, signals)
        self.assertEqual(len(result.trades), 0)
        self.assertAlmostEqual(result.metrics['total_return'], 0.0)

    def test_metrics_keys(self):
        df, signals = self._make_scenario()
        bt = SimpleBacktest()
        result = bt.run(df, signals)

        expected_keys = {
            'total_return', 'annualized_return', 'max_drawdown',
            'sharpe_ratio', 'win_rate', 'profit_factor',
        }
        self.assertEqual(set(result.metrics.keys()), expected_keys)

    def test_drawdown_negative_or_zero(self):
        df, signals = self._make_scenario()
        bt = SimpleBacktest()
        result = bt.run(df, signals)
        self.assertTrue((result.equity_curve['drawdown'] <= 0).all())

    def test_invalid_input(self):
        bt = SimpleBacktest()
        with self.assertRaises(ValueError):
            bt.run(pd.DataFrame(), pd.DataFrame())

    def test_multiple_trades(self):
        dates = pd.date_range('2024-01-01', periods=20, freq='D')
        prices = [100, 90, 80, 90, 100, 110, 100, 90, 80, 90,
                  100, 110, 120, 110, 100, 90, 80, 90, 100, 110]
        df = pd.DataFrame({'date': dates, 'close': prices})
        signals = pd.DataFrame({
            'date': dates,
            'signal': ['hold', 'hold', 'buy', 'hold', 'hold', 'sell',
                        'hold', 'hold', 'buy', 'hold', 'hold', 'hold',
                        'sell', 'hold', 'hold', 'hold', 'buy', 'hold',
                        'hold', 'sell'],
        })

        bt = SimpleBacktest(commission=0.0, slippage=0.0)
        result = bt.run(df, signals)

        buy_trades = result.trades[result.trades['action'] == 'buy']
        sell_trades = result.trades[result.trades['action'] == 'sell']
        self.assertEqual(len(buy_trades), 3)
        self.assertEqual(len(sell_trades), 3)


if __name__ == '__main__':
    unittest.main()
