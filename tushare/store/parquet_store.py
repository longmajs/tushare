# -*- coding:utf-8 -*-
"""Local Parquet storage layer with incremental update and data QC gates.

Storage layout under ``~/.cache/tushare/parquet/``::

    kline/<code>.parquet        — per-stock K-line history
    daily/<YYYY-MM-DD>.parquet  — full-market daily snapshot
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd

LOG = logging.getLogger("tushare.store")

_STORE_ROOT = Path(os.path.expanduser("~/.cache/tushare/parquet"))
_KLINE_DIR = _STORE_ROOT / "kline"
_DAILY_DIR = _STORE_ROOT / "daily"


# ---------------------------------------------------------------------------
# Data QC gates
# ---------------------------------------------------------------------------

class DataQualityError(Exception):
    """Raised when data fails quality checks."""


def _check_qc(df: pd.DataFrame, label: str = "") -> list:
    """Run quality checks and return list of warning strings.

    Raises DataQualityError only for hard failures (>50% NaN).
    """
    warnings = []
    if df.empty:
        return warnings

    # 1. NaN ratio check
    nan_ratio = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
    if nan_ratio > 0.5:
        raise DataQualityError(
            "%s NaN ratio %.1f%% exceeds 50%% threshold" % (label, nan_ratio * 100)
        )
    if nan_ratio > 0.05:
        warnings.append("%s NaN ratio %.1f%%" % (label, nan_ratio * 100))

    # 2. Duplicate dates
    if "date" in df.columns:
        n_dup = df["date"].duplicated().sum()
        if n_dup > 0:
            warnings.append("%s has %d duplicate dates" % (label, n_dup))

    # 3. Price anomalies (>20% daily change)
    if "close" in df.columns and len(df) > 1:
        close = pd.to_numeric(df["close"], errors="coerce")
        pct = close.pct_change().abs()
        n_anomaly = (pct > 0.20).sum()
        if n_anomaly > 0:
            warnings.append(
                "%s has %d rows with >20%% daily price change" % (label, n_anomaly)
            )

    # 4. Volume=0 on trading days
    if "volume" in df.columns:
        vol = pd.to_numeric(df["volume"], errors="coerce")
        n_zero = (vol == 0).sum()
        if n_zero > 0:
            warnings.append("%s has %d rows with volume=0" % (label, n_zero))

    for w in warnings:
        LOG.warning("QC: %s", w)
    return warnings


# ---------------------------------------------------------------------------
# K-line storage
# ---------------------------------------------------------------------------

def _kline_path(code: str) -> Path:
    return _KLINE_DIR / ("%s.parquet" % code)


def save_kline(code: str, df: pd.DataFrame) -> Path:
    """Save or incrementally merge K-line data for a stock code.

    Parameters
    ----------
    code : 6-digit stock code
    df : DataFrame with at least a ``date`` column

    Returns
    -------
    Path to the written parquet file.
    """
    if df is None or df.empty:
        LOG.info("save_kline(%s): empty dataframe, skip", code)
        return _kline_path(code)

    _check_qc(df, label="kline/%s" % code)

    _KLINE_DIR.mkdir(parents=True, exist_ok=True)
    path = _kline_path(code)

    if path.exists():
        existing = pd.read_parquet(path)
        merged = pd.concat([existing, df], ignore_index=True)
        if "date" in merged.columns:
            merged = merged.drop_duplicates(subset=["date"], keep="last")
            merged = merged.sort_values("date").reset_index(drop=True)
        df = merged

    df.to_parquet(path, index=False, engine="pyarrow")
    LOG.info("save_kline(%s): %d rows written", code, len(df))
    return path


def load_kline(
    code: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """Load K-line data for a stock code, optionally filtered by date range.

    Parameters
    ----------
    code : 6-digit stock code
    start : start date (inclusive), e.g. '2024-01-01'
    end : end date (inclusive), e.g. '2024-12-31'

    Returns
    -------
    DataFrame, or empty DataFrame if no data exists.
    """
    path = _kline_path(code)
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path)
    if "date" in df.columns:
        if start:
            df = df[df["date"] >= start]
        if end:
            df = df[df["date"] <= end]
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Daily all-market storage
# ---------------------------------------------------------------------------

def _daily_path(date: str) -> Path:
    return _DAILY_DIR / ("%s.parquet" % date)


def save_daily_all(date: str, df: pd.DataFrame) -> Path:
    """Save full-market daily snapshot for a given date.

    Parameters
    ----------
    date : date string, e.g. '2024-03-15'
    df : DataFrame with market data for that date

    Returns
    -------
    Path to the written parquet file.
    """
    if df is None or df.empty:
        LOG.info("save_daily_all(%s): empty dataframe, skip", date)
        return _daily_path(date)

    _check_qc(df, label="daily/%s" % date)

    _DAILY_DIR.mkdir(parents=True, exist_ok=True)
    path = _daily_path(date)

    if path.exists():
        existing = pd.read_parquet(path)
        merged = pd.concat([existing, df], ignore_index=True)
        if "code" in merged.columns:
            merged = merged.drop_duplicates(subset=["code"], keep="last")
            merged = merged.sort_values("code").reset_index(drop=True)
        df = merged

    df.to_parquet(path, index=False, engine="pyarrow")
    LOG.info("save_daily_all(%s): %d rows written", date, len(df))
    return path


def load_daily_all(date: str) -> pd.DataFrame:
    """Load full-market daily snapshot for a given date.

    Parameters
    ----------
    date : date string, e.g. '2024-03-15'

    Returns
    -------
    DataFrame, or empty DataFrame if no data exists for that date.
    """
    path = _daily_path(date)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)
