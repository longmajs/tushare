# -*- coding:utf-8 -*-
"""
Tests for tushare.store.parquet_store
Uses a temporary directory to mock the filesystem.
"""
import unittest
import tempfile
import shutil
from pathlib import Path
from unittest import mock

import pandas as pd
import numpy as np

from tushare.store import parquet_store
from tushare.store.parquet_store import (
    save_kline, load_kline,
    save_daily_all, load_daily_all,
    _check_qc, DataQualityError,
)


class TestParquetStore(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.orig_store_root = parquet_store._STORE_ROOT
        self.orig_kline_dir = parquet_store._KLINE_DIR
        self.orig_daily_dir = parquet_store._DAILY_DIR

        parquet_store._STORE_ROOT = Path(self.tmpdir)
        parquet_store._KLINE_DIR = Path(self.tmpdir) / "kline"
        parquet_store._DAILY_DIR = Path(self.tmpdir) / "daily"

    def tearDown(self):
        parquet_store._STORE_ROOT = self.orig_store_root
        parquet_store._KLINE_DIR = self.orig_kline_dir
        parquet_store._DAILY_DIR = self.orig_daily_dir
        shutil.rmtree(self.tmpdir, ignore_errors=True)


class TestKlineStorage(TestParquetStore):

    def test_save_and_load(self):
        dates = pd.date_range('2024-01-01', periods=5, freq='D').strftime('%Y-%m-%d')
        df = pd.DataFrame({
            'date': dates.tolist(),
            'close': [100, 101, 102, 103, 104],
            'volume': [1000, 1100, 1200, 1300, 1400],
        })
        path = save_kline('000001', df)
        self.assertTrue(path.exists())

        loaded = load_kline('000001')
        self.assertEqual(len(loaded), 5)

    def test_incremental_merge(self):
        df1 = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'close': [100, 101],
            'volume': [1000, 1100],
        })
        df2 = pd.DataFrame({
            'date': ['2024-01-02', '2024-01-03'],
            'close': [102, 103],
            'volume': [1200, 1300],
        })

        save_kline('000001', df1)
        save_kline('000001', df2)

        loaded = load_kline('000001')
        self.assertEqual(len(loaded), 3)
        # Second write should override 2024-01-02
        row = loaded[loaded['date'] == '2024-01-02']
        self.assertEqual(row['close'].iloc[0], 102)

    def test_date_range_filter(self):
        dates = pd.date_range('2024-01-01', periods=10, freq='D').strftime('%Y-%m-%d')
        df = pd.DataFrame({
            'date': dates.tolist(),
            'close': list(range(100, 110)),
            'volume': [1000] * 10,
        })
        save_kline('000001', df)

        loaded = load_kline('000001', start='2024-01-03', end='2024-01-07')
        self.assertEqual(len(loaded), 5)

    def test_load_nonexistent(self):
        loaded = load_kline('999999')
        self.assertTrue(loaded.empty)

    def test_save_empty_df(self):
        path = save_kline('000001', pd.DataFrame())
        loaded = load_kline('000001')
        self.assertTrue(loaded.empty)


class TestDailyStorage(TestParquetStore):

    def test_save_and_load(self):
        df = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'close': [10.5, 20.3, 30.1],
            'volume': [5000, 6000, 7000],
        })
        path = save_daily_all('2024-01-15', df)
        self.assertTrue(path.exists())

        loaded = load_daily_all('2024-01-15')
        self.assertEqual(len(loaded), 3)

    def test_load_nonexistent_date(self):
        loaded = load_daily_all('1999-01-01')
        self.assertTrue(loaded.empty)

    def test_incremental_merge_by_code(self):
        df1 = pd.DataFrame({
            'code': ['000001', '000002'],
            'close': [10.0, 20.0],
        })
        df2 = pd.DataFrame({
            'code': ['000002', '000003'],
            'close': [21.0, 30.0],
        })

        save_daily_all('2024-01-15', df1)
        save_daily_all('2024-01-15', df2)

        loaded = load_daily_all('2024-01-15')
        self.assertEqual(len(loaded), 3)


class TestDataQC(unittest.TestCase):

    def test_clean_data_passes(self):
        df = pd.DataFrame({'date': ['2024-01-01'], 'close': [100], 'volume': [1000]})
        warnings = _check_qc(df, label='test')
        self.assertEqual(len(warnings), 0)

    def test_high_nan_ratio_raises(self):
        df = pd.DataFrame({'a': [np.nan] * 10, 'b': [np.nan] * 10})
        with self.assertRaises(DataQualityError):
            _check_qc(df, label='test')

    def test_duplicate_dates_warning(self):
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-01', '2024-01-02'],
            'close': [100, 101, 102],
        })
        warnings = _check_qc(df, label='test')
        self.assertTrue(any('duplicate' in w for w in warnings))

    def test_volume_zero_warning(self):
        df = pd.DataFrame({
            'date': ['2024-01-01'],
            'close': [100],
            'volume': [0],
        })
        warnings = _check_qc(df, label='test')
        self.assertTrue(any('volume=0' in w for w in warnings))

    def test_empty_df(self):
        warnings = _check_qc(pd.DataFrame(), label='test')
        self.assertEqual(len(warnings), 0)


if __name__ == '__main__':
    unittest.main()
