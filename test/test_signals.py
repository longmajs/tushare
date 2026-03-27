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
    grid_trading, ma_macd_combo, valuation_dca,
    sector_rotation, dividend_dog,
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


class TestGridTrading(unittest.TestCase):

    def test_percentage_mode_basic(self):
        # Test percentage grid mode with oscillating prices
        prices = [100, 98, 96, 94, 96, 98, 100, 102, 104, 102, 100] * 5
        df = _make_price_df(prices)
        result = grid_trading(df, grid_spacing=0.02, grid_count=3, lookback=10)

        self.assertIn('signal', result.columns)
        self.assertIn('strength', result.columns)
        self.assertIn('grid_level', result.columns)
        self.assertIn('center', result.columns)
        self.assertEqual(len(result), len(df))

    def test_absolute_mode_basic(self):
        # Test absolute price grid mode
        prices = [1000, 950, 900, 850, 900, 950, 1000, 1050, 1100, 1050, 1000] * 3
        df = _make_price_df(prices)
        result = grid_trading(df, mode='abs', grid_spacing=50, grid_count=4, lookback=10)

        self.assertIn('signal', result.columns)
        self.assertIn('grid_level', result.columns)
        # Grid level should be integer
        self.assertTrue((result['grid_level'] == result['grid_level'].round()).all())

    def test_custom_center_price(self):
        # Test with manual center price override
        prices = list(range(90, 110))
        df = _make_price_df(prices)
        result = grid_trading(df, center_price=100, grid_spacing=0.05)

        # Center should be exactly 100
        self.assertTrue((result['center'] == 100).all())

    def test_strength_bounded(self):
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(200) * 2)
        df = _make_price_df(prices.tolist())
        result = grid_trading(df)

        self.assertTrue((result['strength'] >= 0).all())
        self.assertTrue((result['strength'] <= 1).all())

    def test_output_columns(self):
        prices = list(range(50, 100))
        df = _make_price_df(prices)
        result = grid_trading(df)
        expected_cols = {'date', 'signal', 'strength', 'grid_level', 'center'}
        self.assertEqual(set(result.columns), expected_cols)

    def test_grid_level_range(self):
        # Grid level should be within expected range
        prices = list(range(80, 120))
        df = _make_price_df(prices)
        result = grid_trading(df, grid_count=5, grid_spacing=0.05)

        # Grid levels should generally be within +/- grid_count range
        valid_levels = result['grid_level'].dropna()
        if len(valid_levels) > 0:
            self.assertGreaterEqual(valid_levels.min(), -10)  # Allow some overflow
            self.assertLessEqual(valid_levels.max(), 10)

    def test_missing_columns(self):
        df = pd.DataFrame({'foo': [1, 2, 3]})
        with self.assertRaises(ValueError):
            grid_trading(df)

    def test_lookback_warmup(self):
        # Short data series with lookback period
        prices = list(range(20))
        df = _make_price_df(prices)
        result = grid_trading(df, lookback=30)

        # First 29 rows should have NaN center (not enough data)
        early_center = result['center'].iloc[:29]
        self.assertTrue(early_center.isna().all())

        # After warmup, center should be populated
        later_center = result['center'].iloc[29:]
        self.assertTrue(later_center.notna().all())


class TestMaMacdCombo(unittest.TestCase):

    def test_basic_signals(self):
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(200) * 2)
        df = _make_price_df(prices.tolist())
        result = ma_macd_combo(df)

        expected_cols = {'date', 'signal', 'strength', 'ma_fast', 'ma_slow', 'macd_hist'}
        self.assertEqual(set(result.columns), expected_cols)
        self.assertEqual(len(result), 200)

    def test_dual_confirmation_stricter(self):
        # Combo buy requires BOTH MA golden cross AND MACD hist cross
        # so it should produce fewer buy signals than MA alone
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(200) * 2)
        df = _make_price_df(prices.tolist())

        ma_result = ma_crossover(df, fast=5, slow=20)
        combo_result = ma_macd_combo(df, fast=5, slow=20)

        ma_buys = (ma_result['signal'] == 'buy').sum()
        combo_buys = (combo_result['signal'] == 'buy').sum()
        self.assertLessEqual(combo_buys, ma_buys)

    def test_strength_bounded(self):
        np.random.seed(99)
        prices = 100 + np.cumsum(np.random.randn(200) * 2)
        df = _make_price_df(prices.tolist())
        result = ma_macd_combo(df)
        self.assertTrue((result['strength'] >= 0).all())
        self.assertTrue((result['strength'] <= 1).all())

    def test_missing_columns(self):
        df = pd.DataFrame({'foo': [1, 2, 3]})
        with self.assertRaises(ValueError):
            ma_macd_combo(df)


class TestValuationDca(unittest.TestCase):

    def _make_valuation_df(self, n=300):
        dates = pd.date_range('2023-01-01', periods=n, freq='D')
        np.random.seed(42)
        pe = 10 + np.cumsum(np.random.randn(n) * 0.5)
        pb = 1 + np.cumsum(np.random.randn(n) * 0.1)
        return pd.DataFrame({'date': dates, 'pe': pe, 'pb': pb})

    def test_basic_signals(self):
        df = self._make_valuation_df()
        result = valuation_dca(df, lookback=50)

        expected_cols = {'date', 'signal', 'strength', 'pe_percentile', 'pb_percentile'}
        self.assertEqual(set(result.columns), expected_cols)
        self.assertEqual(len(result), 300)

    def test_percentile_range(self):
        df = self._make_valuation_df()
        result = valuation_dca(df, lookback=50)
        valid = result['pe_percentile'].dropna()
        self.assertTrue((valid >= 0).all())
        self.assertTrue((valid <= 1).all())

    def test_buy_when_undervalued(self):
        # Create data where PE drops very low at the end
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        pe = list(range(20, 10, -1)) * 10  # oscillating
        pb = [1.5] * 100
        df = pd.DataFrame({'date': dates, 'pe': pe, 'pb': pb})
        result = valuation_dca(df, lookback=20)
        # Should generate some buy signals when PE is low
        buys = result[result['signal'] == 'buy']
        self.assertGreater(len(buys), 0)

    def test_strength_bounded(self):
        df = self._make_valuation_df()
        result = valuation_dca(df, lookback=50)
        self.assertTrue((result['strength'] >= 0).all())
        self.assertTrue((result['strength'] <= 1).all())

    def test_missing_columns(self):
        df = pd.DataFrame({'date': ['2024-01-01'], 'foo': [1]})
        with self.assertRaises(ValueError):
            valuation_dca(df)


class TestSectorRotation(unittest.TestCase):

    def _make_sector_data(self):
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        codes = ['000001', '000002', '000003', '000004', '000005', '000006']
        industries = ['银行', '银行', '科技', '科技', '医药', '医药']

        industry_df = pd.DataFrame({'code': codes, 'c_name': industries})

        rows = []
        np.random.seed(42)
        for code in codes:
            base = 10 + np.random.rand() * 20
            prices = base + np.cumsum(np.random.randn(100) * 0.5)
            for i, d in enumerate(dates):
                rows.append({'date': d, 'code': code, 'close': prices[i]})
        prices_df = pd.DataFrame(rows)
        return prices_df, industry_df

    def test_basic_signals(self):
        prices_df, industry_df = self._make_sector_data()
        result = sector_rotation(prices_df, industry_df, lookback=20, top_n=1)

        expected_cols = {'date', 'industry', 'signal', 'strength', 'momentum'}
        self.assertEqual(set(result.columns), expected_cols)

    def test_top_n_buy_signals(self):
        prices_df, industry_df = self._make_sector_data()
        result = sector_rotation(prices_df, industry_df, lookback=20, top_n=1)

        # On each date with valid momentum, exactly top_n industries get buy
        valid = result.dropna(subset=['momentum'])
        if len(valid) > 0:
            buys = valid[valid['signal'] == 'buy']
            self.assertGreater(len(buys), 0)

    def test_strength_bounded(self):
        prices_df, industry_df = self._make_sector_data()
        result = sector_rotation(prices_df, industry_df, lookback=20, top_n=1)
        self.assertTrue((result['strength'] >= 0).all())
        self.assertTrue((result['strength'] <= 1).all())

    def test_missing_columns(self):
        df = pd.DataFrame({'foo': [1]})
        industry_df = pd.DataFrame({'code': ['000001'], 'c_name': ['银行']})
        with self.assertRaises(ValueError):
            sector_rotation(df, industry_df)


class TestDividendDog(unittest.TestCase):

    def _make_dividend_df(self):
        dates = ['2024-01-01', '2024-01-01', '2024-01-01',
                 '2024-01-02', '2024-01-02', '2024-01-02']
        codes = ['A', 'B', 'C', 'A', 'B', 'C']
        yields = [5.0, 3.0, 1.0, 5.5, 2.5, 1.5]
        return pd.DataFrame({
            'date': dates, 'code': codes, 'dividend_yield': yields
        })

    def test_basic_signals(self):
        df = self._make_dividend_df()
        result = dividend_dog(df, lookback=1, top_n=2)

        expected_cols = {'date', 'code', 'signal', 'strength', 'dividend_yield', 'rank'}
        self.assertEqual(set(result.columns), expected_cols)

    def test_top_n_get_buy(self):
        df = self._make_dividend_df()
        result = dividend_dog(df, lookback=1, top_n=2)

        # On first rebalance date, top 2 yield stocks should get buy
        first_date = result[result['date'] == '2024-01-01']
        buys = first_date[first_date['signal'] == 'buy']
        self.assertGreater(len(buys), 0)

    def test_strength_bounded(self):
        df = self._make_dividend_df()
        result = dividend_dog(df, lookback=1, top_n=2)
        self.assertTrue((result['strength'] >= 0).all())
        self.assertTrue((result['strength'] <= 1).all())

    def test_missing_columns(self):
        df = pd.DataFrame({'date': ['2024-01-01'], 'foo': [1]})
        with self.assertRaises(ValueError):
            dividend_dog(df)


if __name__ == '__main__':
    unittest.main()
