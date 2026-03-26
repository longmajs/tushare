# -*- coding:utf-8 -*-
"""Core market data engine for non-Pro public sources.

This module centralizes HTTP behaviors, retries, caching and parser logic for
行情/K线相关接口 so that legacy wrappers in ``trading.py`` stay stable.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
import warnings
from io import StringIO
from typing import Callable, Dict, List, Optional

import pandas as pd

from tushare.stock import cons as ct
from tushare.util import dateu as du
from tushare.util.http import (
    DataSourceUnavailable,
    HttpClient,
    MarketConfig,
    MarketError,
    ParseError,
    RateLimited,
    UnsupportedFeature,
    _safe_json_loads,
    get_client,
    get_config,
    reset_client,
)

LOG = logging.getLogger("tushare.market")

# ---------------------------------------------------------------------------
# Optional pytdx support — lazy singleton connection
# ---------------------------------------------------------------------------
try:
    from pytdx.hq import TdxHq_API
    _HAS_PYTDX = True
except ImportError:
    TdxHq_API = None
    _HAS_PYTDX = False

_TDX_API = None
_TDX_BEST_HOST = None  # cached after first successful probe

# Known-good TDX hosts (mix of IPs and domain names from broker CDNs)
_TDX_HOSTS = [
    'sztdx.gtjas.com', 'shtdx.gtjas.com', 'jstdx.gtjas.com',
    '115.238.56.198', '115.238.90.165', '180.153.18.170',
    '60.191.117.167', '218.75.126.9', '60.12.136.250',
]


def _probe_tdx_host(timeout=3):
    """Try each host in _TDX_HOSTS and return the first one that connects."""
    import socket
    for host in _TDX_HOSTS:
        try:
            s = socket.create_connection((host, ct.T_PORT), timeout=timeout)
            s.close()
            LOG.info("pytdx: selected host %s", host)
            return host
        except (socket.timeout, OSError):
            continue
    return None


def _get_tdx_api() -> "TdxHq_API":
    """Return a lazy pytdx connection singleton, auto-selecting the best host."""
    global _TDX_API, _TDX_BEST_HOST
    if not _HAS_PYTDX:
        raise DataSourceUnavailable("pytdx is not installed")
    if _TDX_API is not None:
        try:
            # Quick heartbeat check — if connection is dead, reconnect.
            _TDX_API.get_security_count(0)
            return _TDX_API
        except Exception:
            _TDX_API = None
    if _TDX_BEST_HOST is None:
        _TDX_BEST_HOST = _probe_tdx_host()
    if _TDX_BEST_HOST is None:
        raise DataSourceUnavailable("pytdx: no reachable TDX host found")
    api = TdxHq_API(heartbeat=True)
    try:
        api.connect(_TDX_BEST_HOST, ct.T_PORT)
    except Exception as exc:
        _TDX_BEST_HOST = None  # force re-probe next time
        raise DataSourceUnavailable("pytdx connect failed: %s" % exc)
    _TDX_API = api
    return _TDX_API


def _tdx_market_code(code: str) -> int:
    """Map 6-digit stock code to pytdx market code (0=SZ, 1=SH)."""
    return ct._market_code(code)


_TDX_KTYPE_MAP = {
    "D": 9, "W": 5, "M": 6,
    "5": 0, "15": 1, "30": 2, "60": 3,
}

_CONFIG = get_config()
_CLIENT = None



def set_market_config(
    timeout=None,
    retries=None,
    cache_ttl=None,
    prefer_sources=None,
    enable_tdx=None,
):
    """Update runtime market settings for public-source engine."""
    global _CLIENT
    if timeout is not None:
        _CONFIG.timeout = float(timeout)
    if retries is not None:
        _CONFIG.retries = int(retries)
    if cache_ttl is not None:
        if isinstance(cache_ttl, dict):
            _CONFIG.cache_ttl.update(cache_ttl)
        else:
            raise TypeError("cache_ttl should be a dict of ttl seconds")
    if prefer_sources is not None:
        _CONFIG.prefer_sources = list(prefer_sources)
    if enable_tdx is not None:
        _CONFIG.enable_tdx = bool(enable_tdx)

    _CONFIG.legacy_mode = os.environ.get("TS_MARKET_LEGACY", "0") == "1"
    reset_client()
    return {
        "timeout": _CONFIG.timeout,
        "retries": _CONFIG.retries,
        "cache_ttl": dict(_CONFIG.cache_ttl),
        "prefer_sources": list(_CONFIG.prefer_sources),
        "enable_tdx": _CONFIG.enable_tdx,
        "legacy_mode": _CONFIG.legacy_mode,
        "cache_dir": _CONFIG.cache_dir,
    }



def get_market_config():
    return {
        "timeout": _CONFIG.timeout,
        "retries": _CONFIG.retries,
        "cache_ttl": dict(_CONFIG.cache_ttl),
        "prefer_sources": list(_CONFIG.prefer_sources),
        "enable_tdx": _CONFIG.enable_tdx,
        "legacy_mode": _CONFIG.legacy_mode,
        "cache_dir": _CONFIG.cache_dir,
    }



def is_legacy_mode() -> bool:
    _CONFIG.legacy_mode = os.environ.get("TS_MARKET_LEGACY", "0") == "1"
    return _CONFIG.legacy_mode



def _client() -> HttpClient:
    return get_client()


# ---------------------------------------------------------------------------
# pytdx data fetchers
# ---------------------------------------------------------------------------

def fetch_tdx_k_data(code=None, start="", end="", ktype="D", autype="qfq", index=False):
    """Fetch K-line data via pytdx local protocol.

    Returns a DataFrame compatible with fetch_tencent_k_data output, or raises
    DataSourceUnavailable if pytdx is not installed or connection fails.
    """
    if code is None:
        raise ValueError("code is required")
    if not _HAS_PYTDX or not _CONFIG.enable_tdx:
        raise DataSourceUnavailable("pytdx source is disabled or not installed")

    ktype = str(ktype).strip().upper()
    if ktype not in _TDX_KTYPE_MAP:
        raise TypeError("ktype %s not supported by pytdx source" % ktype)

    api = _get_tdx_api()
    mkt = 1 if index else _tdx_market_code(code)
    tdx_ktype = _TDX_KTYPE_MAP[ktype]

    all_rows = []
    for i in range(100):
        try:
            ds = api.get_security_bars(tdx_ktype, mkt, code, i * 800, 800)
        except Exception as exc:
            raise DataSourceUnavailable("pytdx get_security_bars failed: %s" % exc)
        if not ds:
            break
        all_rows.extend(ds)
        if len(ds) < 800:
            break

    if not all_rows:
        return pd.DataFrame(columns=ct.KLINE_TT_COLS_MINS + ["code"])

    df = pd.DataFrame(all_rows)
    df["date"] = df["datetime"].apply(lambda x: str(x)[:10] if ktype in ct.K_LABELS else str(x)[:16])
    df = df.rename(columns={"vol": "volume"})
    for col in ["open", "close", "high", "low", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["code"] = code

    if start:
        df = df[df["date"] >= start]
    if end:
        df = df[df["date"] <= end]

    cols = ct.KLINE_TT_COLS_MINS + ["code"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols].reset_index(drop=True)


def fetch_tdx_realtime_quotes(symbols=None):
    """Fetch realtime quotes via pytdx local protocol.

    Returns a DataFrame compatible with fetch_sina_realtime_quotes output, or
    raises DataSourceUnavailable if pytdx is not available.
    """
    if symbols is None:
        return None
    if not _HAS_PYTDX or not _CONFIG.enable_tdx:
        raise DataSourceUnavailable("pytdx source is disabled or not installed")

    api = _get_tdx_api()

    if isinstance(symbols, (list, tuple, set, pd.Series)):
        code_list = [str(c) for c in symbols]
    else:
        code_list = [str(symbols)]

    params = [(ct._market_code(c), c) for c in code_list]
    try:
        ds = api.get_security_quotes(params)
    except Exception as exc:
        raise DataSourceUnavailable("pytdx get_security_quotes failed: %s" % exc)

    if not ds:
        return None

    df = pd.DataFrame(ds)
    result = pd.DataFrame({
        "code": df["code"],
        "name": "",
        "open": pd.to_numeric(df.get("open", 0), errors="coerce"),
        "pre_close": pd.to_numeric(df.get("last_close", 0), errors="coerce"),
        "price": pd.to_numeric(df.get("price", 0), errors="coerce"),
        "high": pd.to_numeric(df.get("high", 0), errors="coerce"),
        "low": pd.to_numeric(df.get("low", 0), errors="coerce"),
        "volume": pd.to_numeric(df.get("vol", 0), errors="coerce"),
        "amount": pd.to_numeric(df.get("amount", 0), errors="coerce"),
    })
    return result


# ---------------------------------------------------------------------------
# Source cascade fallback
# ---------------------------------------------------------------------------

def _fetch_with_fallback(sources: List[str], fetchers: Dict[str, Callable], **kwargs):
    """Try fetchers in source order; return first success or raise last error.

    Parameters
    ----------
    sources : list of source names, e.g. ["tencent", "sina", "tdx"]
    fetchers : dict mapping source name to a callable
    **kwargs : passed to each fetcher
    """
    last_exc = None
    for src in sources:
        fn = fetchers.get(src)
        if fn is None:
            continue
        try:
            result = fn(**kwargs)
            if result is not None:
                LOG.info("cascade hit source=%s", src)
                return result
        except (DataSourceUnavailable, RateLimited, Exception) as exc:
            LOG.warning("cascade source=%s failed: %s", src, exc)
            last_exc = exc
            continue
    if last_exc is not None:
        raise DataSourceUnavailable("all sources exhausted, last error: %s" % last_exc)
    return None



def _parse_json_assignment(raw_text: str) -> dict:
    text = raw_text.strip()
    if "=" in text and text[0] not in "[{":
        text = text.split("=", 1)[1]
    text = text.rstrip(";")
    try:
        return json.loads(text)
    except Exception as exc:
        raise ParseError("json assignment parse failed: %s" % exc)



def _pick_symbol(code: str, index: bool) -> str:
    if index:
        return ct.INDEX_SYMBOL.get(code, ct._code_to_symbol(code))
    return ct._code_to_symbol(code)



def _format_minute_date(v):
    s = str(v)
    if len(s) >= 12 and s.isdigit():
        return "%s-%s-%s %s:%s" % (s[0:4], s[4:6], s[6:8], s[8:10], s[10:12])
    return s



def _kline_cache_key(symbol: str, ktype: str, autype: Optional[str], start: str, end: str) -> str:
    return "kline:%s:%s:%s:%s:%s" % (symbol, ktype, autype if autype is not None else "none", start or "", end or "")



def fetch_tencent_k_data(code=None, start="", end="", ktype="D", autype="qfq", index=False):
    """Fetch K data from Tencent public endpoints with resilient key detection."""
    if code is None:
        raise ValueError("code is required")

    symbol = _pick_symbol(code, index)
    ktype = str(ktype).strip().upper()
    if ktype not in ct.K_LABELS and ktype not in ct.K_MIN_LABELS:
        raise TypeError("ktype input error.")

    if start:
        end = du.today() if not end else end

    # Stock index codes and no-autype should use non-adjusted payload.
    adj = None if autype is None else str(autype).strip().lower()
    if adj not in (None, "", "qfq", "hfq"):
        warnings.warn("unsupported autype=%s, fallback to qfq" % autype)
        adj = "qfq"

    # cache key for final DataFrame
    cache_key = _kline_cache_key(symbol, ktype, adj, start or "", end or "")
    cache_ttl = _CONFIG.cache_ttl["kline_min" if ktype in ct.K_MIN_LABELS else "kline_daily"]
    cached = _client().cache.get(cache_key)
    if cached is not None:
        return cached

    if ktype in ct.K_MIN_LABELS:
        period = "m%s" % ktype
        url = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
        params = {
            "param": "%s,%s,,640" % (symbol, period),
            "_var": "%s_today" % period,
            "r": "0.%s" % random.randint(10**15, 10**16 - 1),
        }
        text = _client().get_text(
            url,
            params=params,
            ttl=_CONFIG.cache_ttl["kline_min"],
            cache_key="raw:%s" % cache_key,
            source="tencent",
            endpoint="mkline",
        )
        payload = _parse_json_assignment(text)
        data_obj = payload.get("data", {}).get(symbol, {})
        rows = data_obj.get(period) or data_obj.get("m%s" % ktype) or []
        if not rows:
            return pd.DataFrame(columns=ct.KLINE_TT_COLS_MINS + ["code"])
        df = pd.DataFrame(rows, columns=ct.KLINE_TT_COLS_MINS)
        df["date"] = df["date"].map(_format_minute_date)
        df["code"] = symbol if index else code
        for col in ["open", "close", "high", "low", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        _client().cache.set(cache_key, df, cache_ttl)
        return df

    period = ct.TT_K_TYPE[ktype]
    adj_param = "" if adj in (None, "") else adj

    # Build request list — chunk by 2-year windows to avoid the ~640 bar cap.
    if start and end:
        year_starts = du.tt_dates(start, end)
    else:
        year_starts = [None]

    all_frames = []
    for yr in year_starts:
        if yr is not None:
            chunk_start = "%s-01-01" % yr
            chunk_end = "%s-12-31" % (yr + 1)
        else:
            chunk_start = start or ""
            chunk_end = end or ""

        var_suffix = adj_param if adj_param else "raw"
        chunk_cache_key = _kline_cache_key(symbol, ktype, adj, chunk_start, chunk_end)
        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {
            "_var": "kline_%s_%s" % (period, var_suffix),
            "param": "%s,%s,%s,%s,640,%s" % (symbol, period, chunk_start, chunk_end, adj_param),
            "r": "0.%s" % random.randint(10**15, 10**16 - 1),
        }
        text = _client().get_text(
            url,
            params=params,
            ttl=_CONFIG.cache_ttl["kline_daily"],
            cache_key="raw:%s" % chunk_cache_key,
            source="tencent",
            endpoint="fqkline",
        )
        payload = _parse_json_assignment(text)
        data_obj = payload.get("data", {}).get(symbol)
        if not data_obj:
            continue

        candidate_keys = []
        if adj_param:
            candidate_keys.append("%s%s" % (adj_param, period))
        candidate_keys.extend([period, "qfq%s" % period, "hfq%s" % period])

        rows = None
        for key in candidate_keys:
            if key in data_obj and isinstance(data_obj.get(key), list):
                rows = data_obj.get(key)
                break
        if rows is None:
            for _, value in data_obj.items():
                if isinstance(value, list):
                    rows = value
                    break
        if rows:
            cols = ct.KLINE_TT_COLS_MINS if len(rows[0]) == 6 else ct.KLINE_TT_COLS
            chunk_df = pd.DataFrame(rows, columns=cols)
            chunk_df["code"] = symbol if index else code
            for col in ["open", "close", "high", "low", "volume"]:
                chunk_df[col] = pd.to_numeric(chunk_df[col], errors="coerce")
            if "amount" in chunk_df.columns:
                chunk_df["amount"] = pd.to_numeric(chunk_df["amount"], errors="coerce")
            if "turnoverratio" in chunk_df.columns:
                chunk_df["turnoverratio"] = pd.to_numeric(chunk_df["turnoverratio"], errors="coerce")
            all_frames.append(chunk_df)

    if not all_frames:
        return pd.DataFrame(columns=ct.KLINE_TT_COLS + ["code"])
    df = pd.concat(all_frames, ignore_index=True)
    df = df.drop_duplicates(subset=["date"], keep="first")
    if start and end:
        df = df[(df["date"] >= start) & (df["date"] <= end)]
    _client().cache.set(cache_key, df, cache_ttl)
    return df



def _normalize_symbols(symbols) -> list:
    if isinstance(symbols, (list, tuple, set, pd.Series)):
        return [ct._code_to_symbol(str(code)) for code in symbols]
    return [ct._code_to_symbol(str(symbols))]



def fetch_sina_realtime_quotes(symbols=None):
    if symbols is None:
        return None
    symbol_list = _normalize_symbols(symbols)
    symbols_joined = ",".join(symbol_list)
    url = "http://hq.sinajs.cn"
    params = {"rn": str(random.randint(1000000000000, 9999999999999)), "list": symbols_joined}
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    cache_key = "rt:%s" % symbols_joined
    text = _client().get_text(
        url,
        params=params,
        headers=headers,
        encoding="gbk",
        ttl=_CONFIG.cache_ttl["realtime"],
        cache_key=cache_key,
        source="sina",
        endpoint="hq",
    )

    reg = re.compile(r'var hq_str_(.+?)="(.*?)";')
    rows = reg.findall(text)
    if not rows:
        return None
    data_list = []
    codes = []
    for sym, row in rows:
        if not row:
            continue
        parts = row.split(",")
        if len(parts) <= 1:
            continue
        data_list.append(parts)
        if sym.startswith(("sh", "sz")):
            codes.append(sym[2:])
        elif sym.startswith("gb_"):
            codes.append(sym[3:])
        else:
            codes.append(sym)

    if not data_list:
        return None
    if len(data_list[0]) == 28:
        df = pd.DataFrame(data_list, columns=ct.US_LIVE_DATA_COLS)
    else:
        normalized = []
        width = len(ct.LIVE_DATA_COLS)
        for row in data_list:
            if len(row) < width:
                row = row + [''] * (width - len(row))
            elif len(row) > width:
                row = row[:width]
            normalized.append(row)
        df = pd.DataFrame(normalized, columns=ct.LIVE_DATA_COLS)
        if "s" in df.columns:
            df = df.drop("s", axis=1)
    df["code"] = codes
    for col in [c for c in df.columns if c.endswith("_v")]:
        df[col] = df[col].map(lambda x: x[:-2] if isinstance(x, str) and len(x) > 2 else x)
    return df


def _fetch_today_all_node(node: str, max_pages: int = 200) -> pd.DataFrame:
    frames = []
    for page in range(1, max_pages + 1):
        url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
        params = {
            "num": 80,
            "sort": "code",
            "asc": 0,
            "node": node,
            "symbol": "",
            "_s_r_a": "page",
            "page": page,
        }
        cache_key = "today_all:%s:%s" % (node, page)
        try:
            text = _client().get_text(
                url,
                params=params,
                ttl=_CONFIG.cache_ttl["today_all"],
                cache_key=cache_key,
                source="sina",
                endpoint="Market_Center.getHQNodeData",
            ).strip()
        except (DataSourceUnavailable, RateLimited):
            # Degrade gracefully when upstream blocks scraper traffic.
            break
        if not text or text == "null":
            break
        try:
            payload = _safe_json_loads(text)
        except Exception as exc:
            raise ParseError("today_all page=%s parse failed: %s" % (page, exc))
        if not payload:
            break
        df = pd.DataFrame(payload)
        if df.empty:
            break
        for col in ct.DAY_TRADING_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df = df[ct.DAY_TRADING_COLUMNS]
        if "symbol" in df.columns:
            df = df.drop("symbol", axis=1)
        frames.append(df)
        if len(df) < 80:
            break
    if not frames:
        return pd.DataFrame(columns=[c for c in ct.DAY_TRADING_COLUMNS if c != "symbol"])
    return pd.concat(frames, ignore_index=True)



def fetch_today_all() -> pd.DataFrame:
    hs_a = _fetch_today_all_node("hs_a")
    shfx = _fetch_today_all_node("shfxjs", max_pages=1)
    frames = [df for df in [hs_a, shfx] if not df.empty]
    if not frames:
        return pd.DataFrame(columns=[c for c in ct.DAY_TRADING_COLUMNS if c != "symbol"])
    data = pd.concat(frames, ignore_index=True)
    data = data.drop_duplicates(subset=["code"], keep="first")
    return data



def _parse_tick_table(html_text: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html_text), parse_dates=False)
    if not tables:
        return pd.DataFrame(columns=ct.TODAY_TICK_COLUMNS)
    df = tables[0]
    if len(df.columns) >= len(ct.TODAY_TICK_COLUMNS):
        df = df.iloc[:, : len(ct.TODAY_TICK_COLUMNS)]
    df.columns = ct.TODAY_TICK_COLUMNS
    df["pchange"] = df["pchange"].map(lambda x: str(x).replace("%", "") if pd.notna(x) else x)
    return df



def fetch_today_ticks(code=None, date=None, retry_count=3, pause=0.001):
    if code is None or len(str(code)) != 6:
        return None
    symbol = ct._code_to_symbol(str(code))
    if date is None:
        date = du.today()
    page_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Transactions.getAllPageTime"
    params = {"date": date, "symbol": symbol}
    cache_key = "ticks:pages:%s:%s" % (symbol, date)
    text = _client().get_text(
        page_url,
        params=params,
        ttl=_CONFIG.cache_ttl["today_ticks"],
        cache_key=cache_key,
        source="sina",
        endpoint="CN_Transactions.getAllPageTime",
    )
    try:
        payload = _safe_json_loads(text)
    except Exception as exc:
        raise ParseError("today ticks page parser failed: %s" % exc)
    if isinstance(payload, dict):
        pages = payload.get("detailPages", [])
    elif isinstance(payload, list):
        if not payload:
            pages = []
        elif all(isinstance(item, dict) and "page" in item for item in payload):
            pages = payload
        elif isinstance(payload[0], dict) and "detailPages" in payload[0]:
            pages = payload[0].get("detailPages", [])
        else:
            pages = []
    else:
        pages = []
    if not pages:
        return pd.DataFrame(columns=ct.TODAY_TICK_COLUMNS)

    frames = []
    for item in pages:
        page_no = item.get("page")
        if page_no is None:
            continue
        detail_url = "http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php"
        detail_params = {"symbol": symbol, "date": date, "page": int(page_no)}
        detail_key = "ticks:detail:%s:%s:%s" % (symbol, date, page_no)
        html_text = _client().get_text(
            detail_url,
            params=detail_params,
            ttl=_CONFIG.cache_ttl["today_ticks"],
            cache_key=detail_key,
            source="sina",
            endpoint="vMS_tradedetail",
        )
        if 'id="datatbl"' not in html_text:
            continue
        frames.append(_parse_tick_table(html_text))
        if pause > 0:
            time.sleep(pause)
    if not frames:
        return pd.DataFrame(columns=ct.TODAY_TICK_COLUMNS)
    return pd.concat(frames, ignore_index=True)



def fetch_tick_data(code=None, date=None, src="sn", retry_count=3, pause=0.001):
    if src not in ct.TICK_SRCS:
        raise UnsupportedFeature(ct.TICK_SRC_ERROR)
    if date is None:
        date = du.today()
    # Historical downxls endpoints are no longer reliable; reuse the stable page-detail path.
    df = fetch_today_ticks(code=code, date=date, retry_count=retry_count, pause=pause)
    if df is None or df.empty:
        return pd.DataFrame(columns=ct.TICK_COLUMNS)
    mapped = pd.DataFrame(
        {
            "time": df["time"],
            "price": df["price"],
            "change": df["change"],
            "volume": df["volume"],
            "amount": df["amount"],
            "type": df["type"],
        }
    )
    return mapped
