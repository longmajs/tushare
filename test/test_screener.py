# -*- coding:utf-8 -*-
"""
Tests for tushare.quant.screener
"""
import unittest
import pandas as pd
import numpy as np
from tushare.quant.screener import (
    value_screen, growth_screen, quality_screen,
    momentum_screen, dividend_screen, combine_screens,
)


class TestValueScreen(unittest.TestCase):

    def _make_df(self):
        return pd.DataFrame({
            'code': ['A', 'B', 'C', 'D', 'E'],
            'pe': [5, 10, 15, 25, -3],
            'pb': [0.5, 1.0, 1.5, 3.0, 0.8],
        })

    def test_basic_filter(self):
        df = self._make_df()
        result = value_screen(df, pe_max=20, pb_max=2)
        # D (pe=25) and E (pe=-3) should be excluded
        self.assertEqual(len(result), 3)
        self.assertIn('score', result.columns)

    def test_score_range(self):
        df = self._make_df()
        result = value_screen(df, pe_max=20, pb_max=2)
        self.assertTrue((result['score'] >= 0).all())
        self.assertTrue((result['score'] <= 100).all())

    def test_sorted_descending(self):
        df = self._make_df()
        result = value_screen(df, pe_max=20, pb_max=2)
        scores = result['score'].tolist()
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_empty_result(self):
        df = self._make_df()
        result = value_screen(df, pe_max=1, pb_max=0.1)
        self.assertEqual(len(result), 0)

    def test_empty_input(self):
        with self.assertRaises(ValueError):
            value_screen(pd.DataFrame())

    def test_missing_columns(self):
        df = pd.DataFrame({'code': ['A'], 'foo': [1]})
        with self.assertRaises(ValueError):
            value_screen(df)


class TestGrowthScreen(unittest.TestCase):

    def _make_df(self):
        return pd.DataFrame({
            'code': ['A', 'B', 'C', 'D'],
            'mbrg': [30, 15, 5, 50],
            'nprg': [25, 12, 8, 40],
            'epsg': [20, 10, 3, 35],
        })

    def test_basic_filter(self):
        df = self._make_df()
        result = growth_screen(df, mbrg_min=10, nprg_min=10)
        # C (mbrg=5, nprg=8) should be excluded
        self.assertGreater(len(result), 0)
        self.assertNotIn('C', result['code'].values)

    def test_with_epsg_filter(self):
        df = self._make_df()
        result = growth_screen(df, mbrg_min=10, nprg_min=10, epsg_min=15)
        # B (epsg=10) should now also be excluded
        codes = result['code'].tolist()
        self.assertNotIn('B', codes)

    def test_score_range(self):
        df = self._make_df()
        result = growth_screen(df)
        if len(result) > 0:
            self.assertTrue((result['score'] >= 0).all())
            self.assertTrue((result['score'] <= 100).all())

    def test_missing_columns(self):
        df = pd.DataFrame({'code': ['A'], 'foo': [1]})
        with self.assertRaises(ValueError):
            growth_screen(df)


class TestQualityScreen(unittest.TestCase):

    def _make_df(self):
        return pd.DataFrame({
            'code': ['A', 'B', 'C'],
            'roe': [25, 18, 8],
            'net_profit_ratio': [15, 12, 5],
            'gross_profit_rate': [40, 30, 20],
        })

    def test_basic_filter(self):
        df = self._make_df()
        result = quality_screen(df, roe_min=15, net_profit_ratio_min=10)
        # C should be excluded
        self.assertGreater(len(result), 0)
        self.assertNotIn('C', result['code'].values)

    def test_with_gross_margin(self):
        df = self._make_df()
        result = quality_screen(df, roe_min=15, net_profit_ratio_min=10,
                                gross_profit_rate_min=35)
        codes = result['code'].tolist()
        self.assertIn('A', codes)
        self.assertNotIn('B', codes)

    def test_score_range(self):
        df = self._make_df()
        result = quality_screen(df, roe_min=5, net_profit_ratio_min=5)
        self.assertTrue((result['score'] >= 0).all())
        self.assertTrue((result['score'] <= 100).all())


class TestMomentumScreen(unittest.TestCase):

    def _make_df(self):
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        rows = []
        for code, start in [('A', 100), ('B', 50), ('C', 80)]:
            for i, d in enumerate(dates):
                # A goes up strongly, B flat, C down
                if code == 'A':
                    close = start + i * 2
                elif code == 'B':
                    close = start + i * 0.1
                else:
                    close = start - i * 0.5
                rows.append({'code': code, 'date': d, 'close': close})
        return pd.DataFrame(rows)

    def test_basic_screen(self):
        df = self._make_df()
        result = momentum_screen(df, period=20, min_return=5.0)
        self.assertIn('score', result.columns)
        self.assertIn('return_pct', result.columns)
        # A should pass (strong uptrend)
        self.assertIn('A', result['code'].values)

    def test_min_return_filter(self):
        df = self._make_df()
        result = momentum_screen(df, period=20, min_return=5.0)
        self.assertTrue((result['return_pct'] >= 5.0).all())

    def test_insufficient_data(self):
        df = pd.DataFrame({
            'code': ['A', 'A'],
            'date': ['2024-01-01', '2024-01-02'],
            'close': [100, 101],
        })
        result = momentum_screen(df, period=20)
        self.assertEqual(len(result), 0)


class TestDividendScreen(unittest.TestCase):

    def _make_df(self):
        return pd.DataFrame({
            'code': ['A', 'B', 'C', 'D'],
            'dividend_yield': [5.0, 3.5, 1.0, 4.0],
        })

    def test_basic_filter(self):
        df = self._make_df()
        result = dividend_screen(df, yield_min=3.0)
        self.assertEqual(len(result), 3)  # A, B, D
        self.assertNotIn('C', result['code'].values)

    def test_compute_from_divi(self):
        df = pd.DataFrame({
            'code': ['A', 'B'],
            'divi': [5.0, 1.0],
            'close': [100.0, 100.0],
        })
        result = dividend_screen(df, yield_min=3.0)
        self.assertEqual(len(result), 1)
        self.assertIn('A', result['code'].values)

    def test_score_range(self):
        df = self._make_df()
        result = dividend_screen(df, yield_min=1.0)
        self.assertTrue((result['score'] >= 0).all())
        self.assertTrue((result['score'] <= 100).all())

    def test_empty_input(self):
        with self.assertRaises(ValueError):
            dividend_screen(pd.DataFrame())


class TestCombineScreens(unittest.TestCase):

    def _make_results(self):
        r1 = pd.DataFrame({'code': ['A', 'B', 'C'], 'score': [80, 60, 40]})
        r2 = pd.DataFrame({'code': ['A', 'B', 'D'], 'score': [70, 90, 50]})
        return r1, r2

    def test_and_mode(self):
        r1, r2 = self._make_results()
        result = combine_screens([r1, r2], mode='and')
        # Only A and B are in both
        self.assertEqual(len(result), 2)
        self.assertIn('combined_score', result.columns)

    def test_or_mode(self):
        r1, r2 = self._make_results()
        result = combine_screens([r1, r2], mode='or')
        # A, B, C, D
        self.assertEqual(len(result), 4)

    def test_custom_weights(self):
        r1, r2 = self._make_results()
        result = combine_screens([r1, r2], mode='and', weights=[3, 1])
        # A: (80*0.75 + 70*0.25) = 77.5, B: (60*0.75 + 90*0.25) = 67.5
        a_score = result[result['code'] == 'A']['combined_score'].values[0]
        b_score = result[result['code'] == 'B']['combined_score'].values[0]
        self.assertGreater(a_score, b_score)

    def test_sorted_descending(self):
        r1, r2 = self._make_results()
        result = combine_screens([r1, r2], mode='and')
        scores = result['combined_score'].tolist()
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_invalid_weights_length(self):
        r1, r2 = self._make_results()
        with self.assertRaises(ValueError):
            combine_screens([r1, r2], weights=[1, 2, 3])

    def test_invalid_mode(self):
        r1, r2 = self._make_results()
        with self.assertRaises(ValueError):
            combine_screens([r1, r2], mode='xor')

    def test_empty_input(self):
        with self.assertRaises(ValueError):
            combine_screens([])


if __name__ == '__main__':
    unittest.main()
