# -*- coding:utf-8 -*-
"""
上海银行间同业拆放利率（Shibor）数据接口
Created on 2014/07/31
@author: Jimmy Liu
@group : waditu
@contact: jimmysoa@sina.cn
"""
import pandas as pd
from tushare.stock import cons as ct
from tushare.util import dateu as du
from tushare.util.http import get_client
from io import BytesIO
import logging
LOG = logging.getLogger("tushare.shibor")


def _fetch_shibor_excel(data_type, label_key, year, skiprows=None):
    """Shared helper for all shibor/LPR Excel downloads."""
    lab = ct.SHIBOR_TYPE[label_key]
    lab = lab.encode('utf-8')
    url = ct.SHIBOR_DATA_URL % (ct.P_TYPE['http'], ct.DOMAINS['shibor'],
                                 ct.PAGES['dw'], data_type,
                                 year, lab, year)
    content = get_client().get_bytes(url, source="shibor", endpoint=data_type)
    kwargs = {}
    if skiprows is not None:
        kwargs['skiprows'] = skiprows
    df = pd.read_excel(BytesIO(content), **kwargs)
    return df


def shibor_data(year=None):
    """
    获取上海银行间同业拆放利率（Shibor）
    Parameters
    ------
      year:年份(int)

    Return
    ------
    date:日期
    ON:隔夜拆放利率
    1W:1周拆放利率
    2W:2周拆放利率
    1M:1个月拆放利率
    3M:3个月拆放利率
    6M:6个月拆放利率
    9M:9个月拆放利率
    1Y:1年拆放利率
    """
    year = du.get_year() if year is None else year
    try:
        df = _fetch_shibor_excel('Shibor', 'Shibor', year)
        df.columns = ct.SHIBOR_COLS
        df['date'] = df['date'].map(lambda x: x.date())
        df['date'] = df['date'].astype('datetime64[D]')
        return df
    except Exception:
        return None

def shibor_quote_data(year=None):
    """
    获取Shibor银行报价数据
    Parameters
    ------
      year:年份(int)

    Return
    ------
    date:日期
    bank:报价银行名称
    ON:隔夜拆放利率
    ON_B:隔夜拆放买入价
    ON_A:隔夜拆放卖出价
    1W_B:1周买入
    1W_A:1周卖出
    2W_B:买入
    2W_A:卖出
    1M_B:买入
    1M_A:卖出
    3M_B:买入
    3M_A:卖出
    6M_B:买入
    6M_A:卖出
    9M_B:买入
    9M_A:卖出
    1Y_B:买入
    1Y_A:卖出
    """
    year = du.get_year() if year is None else year
    try:
        df = _fetch_shibor_excel('Quote', 'Quote', year, skiprows=[0])
        df.columns = ct.SHIBOR_Q_COLS
        df['date'] = df['date'].map(lambda x: x.date())
        df['date'] = df['date'].astype('datetime64[D]')
        return df
    except Exception:
        return None

def shibor_ma_data(year=None):
    """
    获取Shibor均值数据
    Parameters
    ------
      year:年份(int)

    Return
    ------
    date:日期
       其它分别为各周期5、10、20均价
    """
    year = du.get_year() if year is None else year
    try:
        df = _fetch_shibor_excel('Shibor_Tendency', 'Tendency', year, skiprows=[0])
        df.columns = ct.SHIBOR_MA_COLS
        df['date'] = df['date'].map(lambda x: x.date())
        df['date'] = df['date'].astype('datetime64[D]')
        return df
    except Exception:
        return None


def lpr_data(year=None):
    """
    获取贷款基础利率（LPR）
    Parameters
    ------
      year:年份(int)

    Return
    ------
    date:日期
    1Y:1年贷款基础利率
    """
    year = du.get_year() if year is None else year
    try:
        df = _fetch_shibor_excel('LPR', 'LPR', year, skiprows=[0])
        df.columns = ct.LPR_COLS
        df['date'] = df['date'].map(lambda x: x.date())
        df['date'] = df['date'].astype('datetime64[D]')
        return df
    except Exception:
        return None


def lpr_ma_data(year=None):
    """
    获取贷款基础利率均值数据
    Parameters
    ------
      year:年份(int)

    Return
    ------
    date:日期
    1Y_5:5日均值
    1Y_10:10日均值
    1Y_20:20日均值
    """
    year = du.get_year() if year is None else year
    try:
        df = _fetch_shibor_excel('LPR_Tendency', 'LPR_Tendency', year, skiprows=[0])
        df.columns = ct.LPR_MA_COLS
        df['date'] = df['date'].map(lambda x: x.date())
        df['date'] = df['date'].astype('datetime64[D]')
        return df
    except Exception:
        return None
