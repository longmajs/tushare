# -*- coding:utf-8 -*-
"""Local Parquet storage layer with incremental update and data QC gates.

Storage layout under ``~/.cache/tushare/parquet/``::

    kline/<code>.parquet        -- per-stock K-line history
    daily/<YYYY-MM-DD>.parquet  -- full-market daily snapshot

Requires ``pyarrow`` for Parquet I/O.  All public helpers accept an optional
*store_dir* so callers can redirect storage to a custom location.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional, Any

import pandas as pd

try:
    import pyarrow.parquet  # noqa: F401 — used by pandas engine="pyarrow"
except ImportError:
    raise ImportError(
        "pyarrow is required for tushare.store but is not installed.\n"
        "Install it with:  pip install pyarrow"
    )

LOG = logging.getLogger("tushare.store")

_DEFAULT_STORE_DIR = Path.home() / ".cache" / "tushare" / "parquet"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_store(store_dir: Optional[str] = None) -> Path:
    """Return a resolved Path for the storage root."""
    if store_dir is not None:
        return Path(store_dir)
    return _DEFAULT_STORE_DIR


def _kline_path(code: str, store_dir: Optional[str] = None) -> Path:
    return _resolve_store(store_dir) / "kline" / ("%s.parquet" % code)


def _daily_path(date: str, store_dir: Optional[str] = None) -> Path:
    return _resolve_store(store_dir) / "daily" / ("%s.parquet" % date)


# ---------------------------------------------------------------------------
# Data QC
# ---------------------------------------------------------------------------

def data_qc(df: pd.DataFrame) -> Dict[str, Any]:
    """Run quality checks on a DataFrame and return a results dict.

    Returns
    -------
    dict with keys:
        nan_ratio            – fraction of NaN values across the entire frame
        duplicate_dates      – list of date values that appear more than once
        price_anomalies      – DataFrame rows where daily close change > 20%
        zero_volume_trading_days – count of rows with volume == 0
        passed               – True if all checks are within acceptable thresholds
    """
    result: Dict[str, Any] = {
        "nan_ratio": 0.0,
        "duplicate_dates": [],
        "price_anomalies": pd.DataFrame(),
        "zero_volume_trading_days": 0,
        "passed": True,
    }

    if df is None or df.empty:
        return result

    # 1. NaN ratio
    total_cells = df.shape[0] * df.shape[1]
    nan_count = df.isnull().sum().sum()
    nan_ratio = nan_count / total_cells if total_cells > 0 else 0.0
    result["nan_ratio"] = nan_ratio
    if nan_ratio > 0.05:
        result["passed"] = False

    # 2. Duplicate dates
    if "date" in df.columns:
        dup_mask = df["date"].duplicated(keep=False)
        if dup_mask.any():
            result["duplicate_dates"] = df.loc[dup_mask, "date"].unique().tolist()
            result["passed"] = False

    # 3. Price anomalies (>20% daily change)
    if "close" in df.columns and len(df) > 1:
        close = pd.to_numeric(df["close"], errors="coerce")
        pct = close.pct_change().abs()
        anomaly_mask = pct > 0.20
        if anomaly_mask.any():
            result["price_anomalies"] = df.loc[anomaly_mask].copy()
            result["passed"] = False

    # 4. Zero-volume trading days
    if "volume" in df.columns:
        vol = pd.to_numeric(df["volume"], errors="coerce")
        n_zero = int((vol == 0).sum())
        result["zero_volume_trading_days"] = n_zero
        if n_zero > 0:
            result["passed"] = False

    return result


# ---------------------------------------------------------------------------
# K-line storage
# ---------------------------------------------------------------------------

def save_kline(
    code: str,
    df: pd.DataFrame,
    store_dir: Optional[str] = None,
) -> Path:
    """Save K-line data for *code* to ``{store_dir}/kline/{code}.parquet``.

    If the file already exists the new rows are merged (de-duplicated by date,
    keeping the latest version of each row).

    Parameters
    ----------
    code : stock code, e.g. ``"600000"``
    df : DataFrame with at least a ``date`` column
    store_dir : override storage root (default ``~/.cache/tushare/parquet/``)

    Returns
    -------
    Path to the written parquet file.
    """
    if df is None or df.empty:
        LOG.info("save_kline(%s): empty dataframe, skip", code)
        return _kline_path(code, store_dir)

    path = _kline_path(code, store_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        existing = pd.read_parquet(path, engine="pyarrow")
        merged = pd.concat([existing, df], ignore_index=True)
        if "date" in merged.columns:
            merged = merged.drop_duplicates(subset=["date"], keep="last")
            merged = merged.sort_values("date").reset_index(drop=True)
        df = merged

    df.to_parquet(path, index=False, engine="pyarrow")
    LOG.info("save_kline(%s): %d rows written to %s", code, len(df), path)
    return path


def load_kline(
    code: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    store_dir: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Load K-line data for *code*, optionally filtered by date range.

    Parameters
    ----------
    code : stock code
    start : start date (inclusive), e.g. ``'2024-01-01'``
    end : end date (inclusive), e.g. ``'2024-12-31'``
    store_dir : override storage root

    Returns
    -------
    DataFrame or ``None`` if no stored data exists.
    """
    path = _kline_path(code, store_dir)
    if not path.exists():
        return None

    df = pd.read_parquet(path, engine="pyarrow")
    if "date" in df.columns:
        if start:
            df = df[df["date"] >= start]
        if end:
            df = df[df["date"] <= end]
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Daily all-market storage
# ---------------------------------------------------------------------------

def save_daily_all(
    date: str,
    df: pd.DataFrame,
    store_dir: Optional[str] = None,
) -> Path:
    """Save full-market daily snapshot to ``{store_dir}/daily/{date}.parquet``.

    Parameters
    ----------
    date : date string, e.g. ``'2024-03-15'``
    df : DataFrame with market data for that date
    store_dir : override storage root

    Returns
    -------
    Path to the written parquet file.
    """
    if df is None or df.empty:
        LOG.info("save_daily_all(%s): empty dataframe, skip", date)
        return _daily_path(date, store_dir)

    path = _daily_path(date, store_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        existing = pd.read_parquet(path, engine="pyarrow")
        merged = pd.concat([existing, df], ignore_index=True)
        if "code" in merged.columns:
            merged = merged.drop_duplicates(subset=["code"], keep="last")
            merged = merged.sort_values("code").reset_index(drop=True)
        df = merged

    df.to_parquet(path, index=False, engine="pyarrow")
    LOG.info("save_daily_all(%s): %d rows written to %s", date, len(df), path)
    return path


def load_daily_all(
    date: str,
    store_dir: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Load full-market daily snapshot for *date*.

    Parameters
    ----------
    date : date string, e.g. ``'2024-03-15'``
    store_dir : override storage root

    Returns
    -------
    DataFrame or ``None`` if no data exists for that date.
    """
    path = _daily_path(date, store_dir)
    if not path.exists():
        return None
    return pd.read_parquet(path, engine="pyarrow")


# ---------------------------------------------------------------------------
# Incremental update
# ---------------------------------------------------------------------------

def update_kline(
    code: str,
    fetch_func: Callable[..., pd.DataFrame],
    store_dir: Optional[str] = None,
) -> pd.DataFrame:
    """Incrementally update stored K-line data for *code*.

    1. Load existing data (if any).
    2. Determine the last stored date.
    3. Call ``fetch_func(code, start=<last_date+1day>)`` to get new rows.
    4. Merge new rows with existing data and save.

    Parameters
    ----------
    code : stock code
    fetch_func : callable ``(code, start=...) -> DataFrame``
    store_dir : override storage root

    Returns
    -------
    The merged (full) DataFrame after update.
    """
    existing = load_kline(code, store_dir=store_dir)

    start = None
    if existing is not None and not existing.empty and "date" in existing.columns:
        last_date = existing["date"].max()
        # Advance one day so we don't re-fetch the last row
        try:
            next_day = (pd.Timestamp(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            start = next_day
        except Exception:
            start = None
        LOG.info("update_kline(%s): last stored date %s, fetching from %s", code, last_date, start)
    else:
        LOG.info("update_kline(%s): no existing data, full fetch", code)

    if start is not None:
        new_data = fetch_func(code, start=start)
    else:
        new_data = fetch_func(code)

    if new_data is None or new_data.empty:
        LOG.info("update_kline(%s): no new data returned", code)
        return existing if existing is not None else pd.DataFrame()

    save_kline(code, new_data, store_dir=store_dir)

    # Return the full merged result
    result = load_kline(code, store_dir=store_dir)
    return result if result is not None else pd.DataFrame()


# ---------------------------------------------------------------------------
# Convenience class
# ---------------------------------------------------------------------------

class ParquetStore:
    """Object-oriented wrapper around the module-level storage functions.

    Parameters
    ----------
    store_dir : root directory for parquet files
                (default ``~/.cache/tushare/parquet/``)
    """

    def __init__(self, store_dir: Optional[str] = None):
        self.store_dir = store_dir

    def save_kline(self, code: str, df: pd.DataFrame) -> Path:
        return save_kline(code, df, store_dir=self.store_dir)

    def load_kline(
        self, code: str, start: Optional[str] = None, end: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        return load_kline(code, start=start, end=end, store_dir=self.store_dir)

    def save_daily_all(self, date: str, df: pd.DataFrame) -> Path:
        return save_daily_all(date, df, store_dir=self.store_dir)

    def load_daily_all(self, date: str) -> Optional[pd.DataFrame]:
        return load_daily_all(date, store_dir=self.store_dir)

    def update_kline(
        self, code: str, fetch_func: Callable[..., pd.DataFrame]
    ) -> pd.DataFrame:
        return update_kline(code, fetch_func, store_dir=self.store_dir)

    @staticmethod
    def data_qc(df: pd.DataFrame) -> Dict[str, Any]:
        return data_qc(df)
