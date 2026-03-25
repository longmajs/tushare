# -*- coding:utf-8 -*-
"""深交所融资融券数据接口"""
import pandas as pd
import time
import logging
from tushare.stock import cons as ct
from tushare.stock import ref_vars as rv
from tushare.util import dateu as du
from tushare.util.http import get_client

LOG = logging.getLogger("tushare.reference.margins_sz")


def sz_margins(start=None, end=None, retry_count=3, pause=0.001):
    """获取深市融资融券数据列表"""
    data = pd.DataFrame()
    if start is None and end is None:
        end = du.today()
        start = du.day_last_week()
    if start is None or end is None:
        ct._write_msg(rv.MAR_SZ_HZ_MSG2)
        return None
    try:
        date_range = pd.date_range(start=start, end=end, freq='B')
        if len(date_range)>261:
            ct._write_msg(rv.MAR_SZ_HZ_MSG)
        else:
            ct._write_head()
            for date in date_range:
                data = pd.concat([data, _sz_hz(str(date.date()), retry_count, pause)])
    except Exception:
        ct._write_msg(ct.DATA_INPUT_ERROR_MSG)
    else:
        return data


def _sz_hz(date='', retry_count=3, pause=0.001):
    for _ in range(retry_count):
        time.sleep(pause)
        ct._write_console()
        try:
            content = get_client().get_bytes(
                rv.MAR_SZ_HZ_URL%(ct.P_TYPE['http'], ct.DOMAINS['szse'],
                                ct.PAGES['szsefc'], date),
                source='szse', endpoint='margins_hz')
            if len(content) <= 200:
                return pd.DataFrame()
            df = pd.read_html(content, skiprows=[0])[0]
            df.columns = rv.MAR_SZ_HZ_COLS
            df['opDate'] = date
        except Exception as e:
            LOG.warning("%s", e)
        else:
            return df
    raise IOError(ct.NETWORK_URL_ERROR_MSG)


def sz_margin_details(date='', retry_count=3, pause=0.001):
    """获取深市融资融券明细列表"""
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            content = get_client().get_bytes(
                rv.MAR_SZ_MX_URL%(ct.P_TYPE['http'], ct.DOMAINS['szse'],
                                ct.PAGES['szsefc'], date),
                source='szse', endpoint='margins_mx')
            if len(content) <= 200:
                return pd.DataFrame()
            df = pd.read_html(content, skiprows=[0])[0]
            df.columns = rv.MAR_SZ_MX_COLS
            df['stockCode'] = df['stockCode'].map(lambda x: str(x).zfill(6))
            df['opDate'] = date
        except Exception as e:
            LOG.warning("%s", e)
        else:
            return df
    raise IOError(ct.NETWORK_URL_ERROR_MSG)
