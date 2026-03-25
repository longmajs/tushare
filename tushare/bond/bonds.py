# -*- coding:utf-8 -*-
"""
债券数据接口
Created on 2017/10/01
@author: Jimmy Liu
@group : waditu
@contact: jimmysoa@sina.cn
"""
import pandas as pd
import logging
from tushare.util.http import get_client, _safe_json_loads

LOG = logging.getLogger("tushare.bond")

SINA_CBOND_URL = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple"

CBOND_COLS = ['symbol', 'code', 'name', 'trade', 'pricechange', 'changepercent',
              'buy', 'sell', 'settlement', 'open', 'high', 'low',
              'volume', 'amount', 'ticktime', 'per', 'pb',
              'mktcap', 'nmc', 'turnoverratio']


def get_cbonds(node='hskzz_z', page=1, num=200):
    """
    获取可转债实时行情列表

    Parameters
    ----------
    node : str
        板块节点，默认 hskzz_z（沪深可转债）
    page : int
        页码
    num : int
        每页数量

    Returns
    -------
    DataFrame
        可转债行情数据
    """
    try:
        params = {
            'page': page,
            'num': num,
            'sort': 'symbol',
            'asc': 0,
            'node': node,
            '_s_r_a': 'page',
        }
        text = get_client().get_text(
            SINA_CBOND_URL,
            params=params,
            encoding='gbk',
            source="sina",
            endpoint="cbond_list",
        )
        data = _safe_json_loads(text)
        if not data:
            return pd.DataFrame(columns=CBOND_COLS)
        df = pd.DataFrame(data)
        for col in ['trade', 'pricechange', 'changepercent', 'buy', 'sell',
                     'settlement', 'open', 'high', 'low', 'volume', 'amount']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except Exception as exc:
        LOG.warning("get_cbonds failed: %s", exc)
        return pd.DataFrame(columns=CBOND_COLS)


def get_bond_info(code):
    """获取债券详情（暂未实现）"""
    pass
