# -*- coding:utf-8 -*-

import datetime
import logging
import os
import time

import pandas as pd

from tushare.stock import cons as ct

LOG = logging.getLogger("tushare.dateu")

# ---------------------------------------------------------------------------
# Local calendar cache (avoids re-downloading on every call)
# ---------------------------------------------------------------------------
_CAL_CACHE_DIR = os.path.expanduser("~/.cache/tushare")
_CAL_CACHE_FILE = os.path.join(_CAL_CACHE_DIR, "calAll.csv")
_trade_cal_df = None


def year_qua(date):
    mon = date[5:7]
    mon = int(mon)
    return[date[0:4], _quar(mon)]


def _quar(mon):
    if mon in [1, 2, 3]:
        return '1'
    elif mon in [4, 5, 6]:
        return '2'
    elif mon in [7, 8, 9]:
        return '3'
    elif mon in [10, 11, 12]:
        return '4'
    else:
        return None


def today():
    day = datetime.datetime.today().date()
    return str(day)


def get_year():
    year = datetime.datetime.today().year
    return year


def get_month():
    month = datetime.datetime.today().month
    return month

def get_hour():
    return datetime.datetime.today().hour


def today_last_year():
    lasty = datetime.datetime.today().date() + datetime.timedelta(-365)
    return str(lasty)


def day_last_week(days=-7):
    lasty = datetime.datetime.today().date() + datetime.timedelta(days)
    return str(lasty)


def get_now():
    return time.strftime('%Y-%m-%d %H:%M:%S')


def int2time(timestamp):
    datearr = datetime.datetime.utcfromtimestamp(timestamp)
    timestr = datearr.strftime("%Y-%m-%d %H:%M:%S")
    return timestr


def diff_day(start=None, end=None):
    d1 = datetime.datetime.strptime(end, '%Y-%m-%d')
    d2 = datetime.datetime.strptime(start, '%Y-%m-%d')
    delta = d1 - d2
    return delta.days


def get_quarts(start, end):
    idx = pd.period_range('Q'.join(year_qua(start)), 'Q'.join(year_qua(end)),
                          freq='Q-JAN')
    return [str(d).split('Q') for d in idx][::-1]


def trade_cal():
    """交易日历 — with local cache fallback.

    isOpen=1是交易日，isOpen=0为休市.
    First tries remote CSV; on failure falls back to local cached copy.
    Successful fetches are persisted locally for offline use.
    """
    global _trade_cal_df
    if _trade_cal_df is not None:
        return _trade_cal_df

    # Try remote source first
    try:
        df = pd.read_csv(ct.ALL_CAL_FILE)
        # Cache locally for offline fallback
        try:
            os.makedirs(_CAL_CACHE_DIR, exist_ok=True)
            df.to_csv(_CAL_CACHE_FILE, index=False)
        except OSError:
            pass
        _trade_cal_df = df
        return df
    except Exception as exc:
        LOG.warning("trade_cal remote fetch failed: %s, trying local cache", exc)

    # Fallback to local cache
    if os.path.exists(_CAL_CACHE_FILE):
        try:
            df = pd.read_csv(_CAL_CACHE_FILE)
            _trade_cal_df = df
            return df
        except Exception as exc:
            LOG.warning("trade_cal local cache read failed: %s", exc)

    return pd.DataFrame(columns=["calendarDate", "isOpen"])


def _parse_date(date):
    """Normalize date input to datetime.date object."""
    if isinstance(date, datetime.date) and not isinstance(date, datetime.datetime):
        return date
    if isinstance(date, datetime.datetime):
        return date.date()
    if isinstance(date, str):
        return datetime.datetime.strptime(date, '%Y-%m-%d').date()
    raise TypeError("date must be str (YYYY-MM-DD), datetime.date, or datetime.datetime")


def _trading_days_set():
    """Return a set of trading day strings from the calendar."""
    df = trade_cal()
    if df.empty or 'isOpen' not in df.columns:
        return set()
    return set(df[df.isOpen == 1]['calendarDate'].astype(str).values)


def _all_cal_dates():
    """Return sorted list of all calendar date strings."""
    df = trade_cal()
    if df.empty or 'calendarDate' not in df.columns:
        return []
    return sorted(df['calendarDate'].astype(str).values)


def is_holiday(date):
    """判断是否为休市日，返回True or False"""
    dt = _parse_date(date)
    date_str = str(dt)

    # Weekends are always holidays
    if dt.isoweekday() in [6, 7]:
        return True

    trading_days = _trading_days_set()
    if not trading_days:
        # Calendar unavailable — fallback to weekend-only check
        return False

    return date_str not in trading_days


def is_trading_day(date):
    """判断是否为交易日。

    Parameters
    ----------
    date : str or datetime.date
        Date in 'YYYY-MM-DD' format or a date object.

    Returns
    -------
    bool
    """
    return not is_holiday(date)


def next_trading_day(date):
    """返回给定日期之后的下一个交易日。

    Parameters
    ----------
    date : str or datetime.date

    Returns
    -------
    str in 'YYYY-MM-DD' format
    """
    dt = _parse_date(date)
    trading_days = _trading_days_set()

    for i in range(1, 30):
        candidate = dt + datetime.timedelta(days=i)
        candidate_str = str(candidate)
        if trading_days:
            if candidate_str in trading_days:
                return candidate_str
        else:
            # Calendar unavailable — skip weekends only
            if candidate.isoweekday() not in [6, 7]:
                return candidate_str
    # Should not reach here, but return next weekday as fallback
    return str(dt + datetime.timedelta(days=1))


def prev_trading_day(date):
    """返回给定日期之前的上一个交易日。

    Parameters
    ----------
    date : str or datetime.date

    Returns
    -------
    str in 'YYYY-MM-DD' format
    """
    dt = _parse_date(date)
    trading_days = _trading_days_set()

    for i in range(1, 30):
        candidate = dt - datetime.timedelta(days=i)
        candidate_str = str(candidate)
        if trading_days:
            if candidate_str in trading_days:
                return candidate_str
        else:
            if candidate.isoweekday() not in [6, 7]:
                return candidate_str
    return str(dt - datetime.timedelta(days=1))


def filter_suspended(codes, date=None):
    """Filter out suspended stocks from a list of codes.

    Parameters
    ----------
    codes : list of str
        Stock codes to filter.
    date : str or None
        Date to check (default: today).

    Returns
    -------
    list of str — codes that are NOT suspended.
    """
    try:
        from tushare.stock.classifying import get_suspended
        sus_df = get_suspended()
        if sus_df is None or sus_df.empty:
            return list(codes)
        suspended_codes = set(sus_df['code'].astype(str).values)
        return [c for c in codes if str(c) not in suspended_codes]
    except Exception as exc:
        LOG.warning("filter_suspended failed: %s, returning all codes", exc)
        return list(codes)


def filter_delisted(codes):
    """Filter out delisted (terminated) stocks from a list of codes.

    Parameters
    ----------
    codes : list of str
        Stock codes to filter.

    Returns
    -------
    list of str — codes that are NOT delisted.
    """
    try:
        from tushare.stock.classifying import get_terminated
        term_df = get_terminated()
        if term_df is None or term_df.empty:
            return list(codes)
        terminated_codes = set(term_df['code'].astype(str).values)
        return [c for c in codes if str(c) not in terminated_codes]
    except Exception as exc:
        LOG.warning("filter_delisted failed: %s, returning all codes", exc)
        return list(codes)


def last_tddate():
    today_d = datetime.datetime.today().date()
    weekday = today_d.isoweekday()
    if weekday == 7:  # Sunday
        return str(today_d - datetime.timedelta(days=2))
    elif weekday == 6:  # Saturday
        return str(today_d - datetime.timedelta(days=1))
    else:
        return prev_trading_day(str(today_d))


def tt_dates(start='', end=''):
    startyear = int(start[0:4])
    endyear = int(end[0:4])
    dates = [d for d in range(startyear, endyear+1, 2)]
    return dates


def _random(n=13):
    from random import randint
    start = 10**(n-1)
    end = (10**n)-1
    return str(randint(start, end))

def get_q_date(year=None, quarter=None):
    dt = {'1': '-03-31', '2': '-06-30', '3': '-09-30', '4': '-12-31'}
    return '%s%s'%(str(year), dt[str(quarter)])


