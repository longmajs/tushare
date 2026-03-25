# -*- coding:utf-8 -*-
"""Shared HTTP client infrastructure with retries, caching and rate-limit handling.

Extracted from market_core.py so all tushare modules can benefit from
resilient HTTP requests without reimplementing retry/cache logic.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pickle
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOG = logging.getLogger("tushare.http")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class MarketError(Exception):
    """Base exception for market engine."""


class DataSourceUnavailable(MarketError):
    """Raised when upstream source is unavailable or blocked."""


class ParseError(MarketError):
    """Raised when upstream payload format changed and parser failed."""


class UnsupportedFeature(MarketError):
    """Raised when a legacy interface can not be supported reliably."""


class RateLimited(MarketError):
    """Raised when source throttles requests."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class MarketConfig:
    timeout: float = 10.0
    retries: int = 3
    backoff_factor: float = 0.25
    prefer_sources: List[str] = field(default_factory=lambda: ["tencent", "sina", "tdx"])
    enable_tdx: bool = True
    cache_dir: str = field(default_factory=lambda: os.path.expanduser("~/.cache/tushare"))
    cache_ttl: Dict[str, int] = field(
        default_factory=lambda: {
            "kline_daily": 86400,
            "kline_min": 60,
            "realtime": 8,
            "today_all": 8,
            "today_ticks": 8,
            "tick": 8,
        }
    )
    legacy_mode: bool = field(default_factory=lambda: os.environ.get("TS_MARKET_LEGACY", "0") == "1")


# ---------------------------------------------------------------------------
# File cache
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()


class _FileCache:
    def __init__(self, cache_dir: str):
        self._root = Path(cache_dir)
        self._root.mkdir(parents=True, exist_ok=True)

    def _path_for(self, cache_key: str) -> Path:
        digest = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()
        return self._root / (digest + ".pkl")

    def get(self, cache_key: str):
        path = self._path_for(cache_key)
        if not path.exists():
            return None
        try:
            with path.open("rb") as fh:
                payload = pickle.load(fh)
            if payload.get("expires_at", 0) < _now():
                try:
                    path.unlink()
                except OSError:
                    pass
                return None
            return payload.get("value")
        except Exception:
            return None

    def set(self, cache_key: str, value, ttl: int):
        path = self._path_for(cache_key)
        payload = {"expires_at": _now() + max(1, int(ttl)), "value": value}
        try:
            with path.open("wb") as fh:
                pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            return


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

class HttpClient:
    def __init__(self, cfg: MarketConfig):
        self.cfg = cfg
        self.cache = _FileCache(cfg.cache_dir)
        self.session = requests.Session()
        retry = Retry(
            total=max(0, int(cfg.retries)),
            read=max(0, int(cfg.retries)),
            connect=max(0, int(cfg.retries)),
            backoff_factor=max(0.0, float(cfg.backoff_factor)),
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.default_headers = {
            "User-Agent": "Mozilla/5.0 (compatible; tushare-market/2.0)",
            "Accept": "*/*",
        }

    def get_text(
        self,
        url: str,
        *,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        encoding: Optional[str] = None,
        ttl: Optional[int] = None,
        cache_key: Optional[str] = None,
        source: str = "",
        endpoint: str = "",
    ) -> str:
        merged_headers = dict(self.default_headers)
        if headers:
            merged_headers.update(headers)
        resolved_ttl = ttl if ttl is not None else 0
        if cache_key and resolved_ttl > 0:
            cached = self.cache.get(cache_key)
            if cached is not None:
                LOG.info("cache hit source=%s endpoint=%s key=%s", source, endpoint, cache_key)
                return cached

        started = _now()
        resp = None
        try:
            resp = self.session.get(url, params=params, headers=merged_headers, timeout=self.cfg.timeout)
            if resp.status_code == 429:
                raise RateLimited("source=%s endpoint=%s returned 429" % (source, endpoint))
            if resp.status_code >= 400:
                raise DataSourceUnavailable(
                    "source=%s endpoint=%s status=%s" % (source, endpoint, resp.status_code)
                )
            text = resp.content.decode(encoding, errors="ignore") if encoding else resp.text
        except RateLimited:
            raise
        except requests.RequestException as exc:
            raise DataSourceUnavailable("source=%s endpoint=%s err=%s" % (source, endpoint, exc))
        elapsed = _now() - started
        LOG.info(
            "request ok source=%s endpoint=%s status=%s elapsed=%.3fs",
            source,
            endpoint,
            resp.status_code if resp is not None else "n/a",
            elapsed,
        )

        if cache_key and resolved_ttl > 0:
            self.cache.set(cache_key, text, resolved_ttl)
        return text

    def get_bytes(
        self,
        url: str,
        *,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        ttl: Optional[int] = None,
        cache_key: Optional[str] = None,
        source: str = "",
        endpoint: str = "",
    ) -> bytes:
        """Fetch URL and return raw response bytes (for binary payloads like XLS)."""
        merged_headers = dict(self.default_headers)
        if headers:
            merged_headers.update(headers)
        resolved_ttl = ttl if ttl is not None else 0
        if cache_key and resolved_ttl > 0:
            cached = self.cache.get(cache_key)
            if cached is not None:
                LOG.info("cache hit source=%s endpoint=%s key=%s", source, endpoint, cache_key)
                return cached

        started = _now()
        resp = None
        try:
            resp = self.session.get(url, params=params, headers=merged_headers, timeout=self.cfg.timeout)
            if resp.status_code == 429:
                raise RateLimited("source=%s endpoint=%s returned 429" % (source, endpoint))
            if resp.status_code >= 400:
                raise DataSourceUnavailable(
                    "source=%s endpoint=%s status=%s" % (source, endpoint, resp.status_code)
                )
            data = resp.content
        except RateLimited:
            raise
        except requests.RequestException as exc:
            raise DataSourceUnavailable("source=%s endpoint=%s err=%s" % (source, endpoint, exc))
        elapsed = _now() - started
        LOG.info(
            "request ok source=%s endpoint=%s status=%s elapsed=%.3fs",
            source, endpoint,
            resp.status_code if resp is not None else "n/a",
            elapsed,
        )

        if cache_key and resolved_ttl > 0:
            self.cache.set(cache_key, data, resolved_ttl)
        return data


# ---------------------------------------------------------------------------
# Singleton client
# ---------------------------------------------------------------------------

_CONFIG = MarketConfig()
_CLIENT = None


def get_client() -> HttpClient:
    """Return the shared HttpClient singleton."""
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = HttpClient(_CONFIG)
    return _CLIENT


def get_config() -> MarketConfig:
    """Return the shared MarketConfig singleton."""
    return _CONFIG


def reset_client():
    """Force re-creation of the HttpClient on next access."""
    global _CLIENT
    _CLIENT = None


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def _safe_json_loads(text):
    """Parse JSON, falling back to JS-style object handling."""
    text = text.strip()
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        pass
    if not text:
        raise json.JSONDecodeError("empty payload", text, 0)
    # Strip wrapping chars (e.g. parentheses from JSONP-like responses).
    if text[0] in ("(", ")") or text[-1] in ("(", ")"):
        text = text.strip("()")
    # Strip var assignment prefix.
    if "=" in text and text[0] not in "[{":
        text = text.split("=", 1)[1].strip().rstrip(";")
    # Quote unquoted JS-style keys: {key: value} -> {"key": value}
    text = re.sub(r'(?<=[{,])\s*(\w+)\s*:', r' "\1":', text)
    return json.loads(text)
