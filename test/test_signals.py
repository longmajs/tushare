# -*- coding:utf-8 -*-
"""
Tests for tushare.quant.signals
Known input/output pairs for each signal generator.
"""
import unittest
import pandas as pd
import numpy as np
from tushare.quant.signals import (
    ma_crossover, macd_signal, rsi_signal, bollinger_signal,
)


def _make_price_df(prices):
    """Helper to create a price DataFrame from a list of close prices."""
    dates = pd.date_range('2024-01-01', periods=len(prices), freq='D')
    return pd.DataFrame({'date': dates, 'close': prices})


class TestMaCrossover(unittest.TestCase):

    def test_basic_crossover(self):
        # Create a price series that trends down then up, causing a golden cross
        prices = list(range(100, 80, -1)) + list(range(80, 120))
        df = _make_price_df(prices)
        result = ma_crossover(df, fast=3, slow=10)

        self.assertIn('signal', result.columns)
        self.assertIn('strength', result.columns)
        self.assertIn('ma_fast', result.columns)
        self.assertIn('ma_slow', result.columns)
        self.assertEqual(len(result), len(df))

        # Should have at least one buy signal after the reversal
        signals = result[result['signal'] == 'buy']
        self.assertGreater(len(signals), 0)

    def test_all_hold_for_flat(self):
        # Flat prices should produce no crossover signals
        prices = [100.0] * 50
        df = _make_price_df(prices)
        result = ma_crossover(df, fast=5, slow=20)
        non_hold = result[result['signal'] != 'hold']
        self.assertEqual(len(non_hold), 0)

    def test_strength_bounded(self):
        prices = list(range(50, 100)) + list(range(100, 50, -1))
        df = _make_price_df(prices)
        result = ma_crossover(df, fast=5, slow=20)
        self.assertTrue((result['strength'] >= 0).all())
        self.assertTrue((result['strength'] <= 1).all())

    def test_missing_columns(self):
        df = pd.DataFrame({'foo': [1, 2, 3]})
        with self.assertRaises(ValueError):
            ma_crossover(df)


class TestMacdSignal(unittest.TestCase):

    def test_basic_signals(self):
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(200) * 2)
        df = _make_price_df(prices.tolist())
        result = macd_signal(df)

        self.assertIn('macd', result.columns)
        self.assertIn('macd_signal', result.columns)
        self.assertIn('macd_hist', result.columns)
        self.assertEqual(len(result), 200)

        # Should generate some buy/sell signals in a random walk
        buy_count = (result['signal'] == 'buy').sum()
        sell_count = (result['signal'] == 'sell').sum()
        self.assertGreater(buy_count + sell_count, 0)

    def test_output_columns(self):
        prices = list(range(50, 100))
        df = _make_price_df(prices)
        result = macd_signal(df)
        expected_cols = {'date', 'signal', 'strength', 'macd', 'macd_signal', 'macd_hist'}
        self.assertEqual(set(result.columns), expected_cols)


class TestRsiSignal(unittest.TestCase):

    def test_overbought_oversold(self):
        # Sharp drop then sharp rise to trigger RSI signals
        prices = list(range(100, 60, -1)) + list(range(60, 120))
        df = _make_price_df(prices)
        result = rsi_signal(df, period=14)

        self.assertIn('rsi', result.columns)
        valid_rsi = result['rsi'].dropna()
        self.assertTrue((valid_rsi >= 0).all())
        self.assertTrue((valid_rsi <= 100).all())

    def test_output_columns(self):
        prices = list(range(50, 100))
        df = _make_price_df(prices)
        result = rsi_signal(df)
        expected_cols = {'date', 'signal', 'strength', 'rsi'}
        self.assertEqual(set(result.columns), expected_cols)

    def test_strength_bounded(self):
        np.random.seed(123)
        prices = 100 + np.cumsum(np.random.randn(200) * 3)
        df = _make_price_df(prices.tolist())
        result = rsi_signal(df)
        self.assertTrue((result['strength'] >= 0).all())
        self.assertTrue((result['strength'] <= 1).all())


class TestBollingerSignal(unittest.TestCase):

    def test_basic_signals(self):
        np.random.seed(99)
        prices = 100 + np.cumsum(np.random.randn(200) * 3)
        df = _make_price_df(prices.tolist())
        result = bollinger_signal(df)

        self.assertIn('bb_upper', result.columns)
        self.assertIn('bb_middle', result.columns)
        self.assertIn('bb_lower', result.columns)
        self.assertEqual(len(result), 200)

    def test_bands_relationship(self):
        prices = list(range(80, 130))
        df = _make_price_df(prices)
        result = bollinger_signal(df, period=10, std=2)
        valid = result.dropna(subset=['bb_upper', 'bb_lower', 'bb_middle'])
        self.assertTrue((valid['bb_upper'] >= valid['bb_middle']).all())
        self.assertTrue((valid['bb_lower'] <= valid['bb_middle']).all())

    def test_output_columns(self):
        prices = list(range(50, 100))
        df = _make_price_df(prices)
        result = bollinger_signal(df)
        expected_cols = {'date', 'signal', 'strength', 'bb_upper', 'bb_middle', 'bb_lower'}
        self.assertEqual(set(result.columns), expected_cols)


if __name__ == '__main__':
    unittest.main()
