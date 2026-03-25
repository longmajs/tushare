# -*- coding:utf-8 -*-
"""沪深港通资金流向数据接口"""
import pandas as pd
import numpy as np
import json
import logging
from tushare.stock import cons as ct
from tushare.stock import ref_vars as rv
from tushare.util.netbase import Client

LOG = logging.getLogger("tushare.reference.moneyflow")


def moneyflow_hsgt():
    """获取沪深港通资金流向"""
    clt = Client(rv.HSGT_DATA%(ct.P_TYPE['http'], ct.DOMAINS['em']),
                 ref=rv.HSGT_REF%(ct.P_TYPE['http'], ct.DOMAINS['em'], ct.PAGES['index']))
    content = clt.gvalue()
    content = content.decode('utf-8')
    js = json.loads(content)
    df = pd.DataFrame(js)
    df['DateTime'] = df['DateTime'].map(lambda x: x[0:10])
    df = df.replace('-', np.NaN)
    df = df[rv.HSGT_TEMP]
    df.columns = rv.HSGT_COLS
    df = df.sort_values('date', ascending=False)
    return df
