# -*- coding:utf-8 -*-
"""Local Parquet storage layer for tushare market data.

Provides persistent caching with incremental updates and data quality checks.
"""

from tushare.store.parquet_store import (
    ParquetStore,
    data_qc,
    load_daily_all,
    load_kline,
    save_daily_all,
    save_kline,
    update_kline,
)

__all__ = [
    "ParquetStore",
    "save_kline",
    "load_kline",
    "save_daily_all",
    "load_daily_all",
    "update_kline",
    "data_qc",
]
