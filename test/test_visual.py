# -*- coding:utf-8 -*-
"""
Tests for tushare.quant.backtest.plot_result
Smoke tests that charts render without error.
"""
import unittest
import pandas as pd
from tushare.quant.backtest import SimpleBacktest, plot_result


class TestPlotResult(unittest.TestCase):

    def setUp(self):
        """Create a simple backtest result for plotting."""
        dates = pd.date_range('2024-01-01', periods=20, freq='D')
        prices = [100, 95, 90, 85, 80, 85, 90, 95, 100, 105,
                  110, 105, 100, 95, 90, 95, 100, 105, 110, 115]
        self.df = pd.DataFrame({'date': dates, 'close': prices})
        self.signals = pd.DataFrame({
            'date': dates,
            'signal': ['hold', 'hold', 'hold', 'hold', 'buy',
                        'hold', 'hold', 'hold', 'hold', 'sell',
                        'hold', 'hold', 'hold', 'hold', 'buy',
                        'hold', 'hold', 'hold', 'hold', 'sell'],
        })

    def test_plot_renders_without_error(self):
        """Smoke test: plot_result should not raise."""
        import matplotlib
        matplotlib.use('Agg')  # Non-interactive backend

        bt = SimpleBacktest(commission=0.001, slippage=0.001)
        result = bt.run(self.df, self.signals)
        # Should not raise
        plot_result(result)

        import matplotlib.pyplot as plt
        plt.close('all')

    def test_plot_with_no_trades(self):
        """Plot should work even with zero trades."""
        import matplotlib
        matplotlib.use('Agg')

        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        df = pd.DataFrame({'date': dates, 'close': [100] * 10})
        signals = pd.DataFrame({'date': dates, 'signal': ['hold'] * 10})

        bt = SimpleBacktest()
        result = bt.run(df, signals)
        plot_result(result)

        import matplotlib.pyplot as plt
        plt.close('all')


if __name__ == '__main__':
    unittest.main()
