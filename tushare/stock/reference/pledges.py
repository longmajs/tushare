# -*- coding:utf-8 -*-
"""股权质押数据接口"""
import pandas as pd
import numpy as np
import logging
from tushare.stock import cons as ct

LOG = logging.getLogger("tushare.reference.pledges")


def stock_pledged():
    """股票质押数据"""
    df = pd.read_csv(ct.GPZY_URL%(ct.P_TYPE['http'],
                                  ct.DOMAINS['oss'], 'gpzy'),
                     dtype={'code': object})
    return df


def pledged_detail():
    """股票质押明细数据"""
    df = pd.read_csv(ct.GPZY_D_URL%(ct.P_TYPE['http'],
                                    ct.DOMAINS['oss'], 'gpzy_detail'),
                     dtype={'code': object, 'ann_date': object, 'end_date': object})
    df['code'] = df['code'].map(lambda x: str(x).zfill(6))
    df['end_date'] = np.where(df['end_date'] == '--', np.NaN, df['end_date'])
    return df
