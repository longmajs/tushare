# -*- coding:utf-8 -*-
"""OSS 融资融券数据和股票增发接口（静态 CSV 文件）"""
import pandas as pd
import logging
from tushare.stock import cons as ct

LOG = logging.getLogger("tushare.reference.margins_oss")


def margin_detail(date=''):
    """沪深融券融券明细"""
    date = str(date).replace('-', '')
    df = pd.read_csv(ct.MG_URL%(ct.P_TYPE['http'],
                                ct.DOMAINS['oss'], date[0:6], 'mx', date),
                     dtype={'code': object})
    return df


def margin_target(date=''):
    """沪深融券融券标的"""
    date = str(date).replace('-', '')
    df = pd.read_csv(ct.MG_URL%(ct.P_TYPE['http'],
                                ct.DOMAINS['oss'], date[0:6], 'bd', date),
                     dtype={'code': object})
    return df


def margin_offset(date):
    """融资融券可充抵保证金证券"""
    date = str(date).replace('-', '')
    df = pd.read_csv(ct.MG_URL%(ct.P_TYPE['http'],
                                ct.DOMAINS['oss'], date[0:6], 'cd', date),
                     dtype={'code': object})
    return df


def margin_zsl(date='', broker=''):
    """融资融券充抵保证金折算率"""
    date = str(date).replace('-', '')
    df = pd.read_csv(ct.MG_ZSL_URL%(ct.P_TYPE['http'],
                                    ct.DOMAINS['oss'], date[0:6], broker, date),
                     dtype={'code': object})
    return df


def stock_issuance(start_date='', end_date=''):
    """股票增发"""
    df = pd.read_csv(ct.ZF%(ct.P_TYPE['http'],
                            ct.DOMAINS['oss'], 'zf'),
                     dtype={'code': object})
    if start_date != '' and start_date is not None:
        df = df[df.issue_date >= start_date]
    if end_date != '' and end_date is not None:
        df = df[df.issue_date <= end_date]
    df['prem'] = (df['close'] - df['price']) / df['price'] * 100
    df['prem'] = df['prem'].map(ct.FORMAT)
    df['prem'] = df['prem'].astype(float)
    return df
