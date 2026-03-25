# -*- coding:utf-8 -*-
"""股东和基金持仓数据接口"""
import pandas as pd
import numpy as np
import time
import json
import re
import logging
from tushare.stock import cons as ct
from tushare.stock import ref_vars as rv
from tushare.util import dateu as du
from tushare.util.http import get_client
from tushare.stock.reference._utils import _random

LOG = logging.getLogger("tushare.reference.shareholders")


def xsg_data(year=None, month=None, retry_count=3, pause=0.001):
    """获取限售股解禁数据"""
    year = du.get_year() if year is None else year
    month = du.get_month() if month is None else month
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            text = get_client().get_text(
                rv.XSG_URL%(ct.P_TYPE['http'], ct.DOMAINS['em'],
                            ct.PAGES['emxsg'], year, month),
                source='em', endpoint='xsg')
        except Exception as e:
            LOG.warning("%s", e)
        else:
            da = text[3:len(text)-3]
            lst = []
            for row in da.split('","'):
                lst.append([data for data in row.split(',')])
            df = pd.DataFrame(lst)
            df = df[[1, 3, 4, 5, 6]]
            for col in [5, 6]:
                df[col] = df[col].astype(float)
            df[5] = df[5]/10000
            df[6] = df[6]*100
            df[5] = df[5].map(ct.FORMAT)
            df[6] = df[6].map(ct.FORMAT)
            df.columns = rv.XSG_COLS
            return df
    raise IOError(ct.NETWORK_URL_ERROR_MSG)


def fund_holdings(year, quarter, retry_count=3, pause=0.001):
    """获取基金持股数据"""
    start, end = rv.QUARTS_DIC[str(quarter)]
    if quarter == 1:
        start = start % str(year-1)
        end = end%year
    else:
        start, end = start%year, end%year
    ct._write_head()
    df, pages = _holding_cotent(start, end, 0, retry_count, pause)
    for idx in range(1, pages):
        df = pd.concat([df, _holding_cotent(start, end, idx, retry_count, pause)],
                  ignore_index=True)
    return df


def _holding_cotent(start, end, pageNo, retry_count, pause):
    for _ in range(retry_count):
        time.sleep(pause)
        if pageNo > 0:
            ct._write_console()
        try:
            text = get_client().get_text(
                rv.FUND_HOLDS_URL%(ct.P_TYPE['http'], ct.DOMAINS['163'],
                     ct.PAGES['163fh'], ct.PAGES['163fh'],
                     pageNo, start, end, _random(5)),
                source='163', endpoint='fund_holdings')
            text = text.replace('--', '0')
            lines = json.loads(text)
            data = lines['list']
            df = pd.DataFrame(data)
            df = df.drop(['CODE', 'ESYMBOL', 'EXCHANGE', 'NAME', 'RN', 'SHANGQIGUSHU',
                              'SHANGQISHIZHI', 'SHANGQISHULIANG'], axis=1)
            for col in ['GUSHU', 'GUSHUBIJIAO', 'SHIZHI', 'SCSTC27']:
                df[col] = df[col].astype(float)
            df['SCSTC27'] = df['SCSTC27']*100
            df['GUSHU'] = df['GUSHU']/10000
            df['GUSHUBIJIAO'] = df['GUSHUBIJIAO']/10000
            df['SHIZHI'] = df['SHIZHI']/10000
            df['GUSHU'] = df['GUSHU'].map(ct.FORMAT)
            df['GUSHUBIJIAO'] = df['GUSHUBIJIAO'].map(ct.FORMAT)
            df['SHIZHI'] = df['SHIZHI'].map(ct.FORMAT)
            df['SCSTC27'] = df['SCSTC27'].map(ct.FORMAT)
            df.columns = rv.FUND_HOLDS_COLS
            df = df[['code', 'name', 'date', 'nums', 'nlast', 'count',
                         'clast', 'amount', 'ratio']]
        except Exception as e:
            LOG.warning("%s", e)
        else:
            if pageNo == 0:
                return df, int(lines['pagecount'])
            else:
                return df
    raise IOError(ct.NETWORK_URL_ERROR_MSG)


def top10_holders(code=None, year=None, quarter=None, gdtype='0',
                  retry_count=3, pause=0.001):
    """获取前十大股东数据"""
    if code is None:
        return None
    else:
        code = ct._code_to_symbol(code)
    gdtype = 'LT' if gdtype == '1' else ''
    qdate = ''
    if (year is not None) & (quarter is not None):
        qdate = du.get_q_date(year, quarter)
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            text = get_client().get_text(
                rv.TOP10_HOLDERS_URL%(ct.P_TYPE['http'], ct.DOMAINS['gw'],
                                    gdtype, code.upper()),
                source='gw', endpoint='top10_holders')
            reg = re.compile(r'= \'\[(.*?)\]\';')
            lines = reg.findall(text)[0]
            jss = json.loads('[%s]' %lines)
            summ = []
            data = pd.DataFrame()
            for row in jss:
                qt = row.get('jzrq')
                hold = row.get('ljcy')
                change = row.get('ljbh')
                props = row.get('ljzb')
                arow = [qt, hold, change, props]
                summ.append(arow)
                ls = row.get('sdgdList', [])
                dlist = []
                for inrow in ls:
                    dlist.append([qt, inrow['gdmc'], inrow['cgs'],
                                  inrow['zzgs'], inrow['gbxz'], inrow['zjqk']])
                ddata = pd.DataFrame(dlist, columns=rv.TOP10_PER_COLS)
                data = pd.concat([data, ddata], ignore_index=True)
            df = pd.DataFrame(summ, columns=rv.TOP10_SUMM_COLS)
            if qdate != '':
                df = df[df.quarter == qdate]
                data = data[data.quarter == qdate]
        except Exception as e:
            LOG.warning("%s", e)
        else:
            return df, data
    raise IOError(ct.NETWORK_URL_ERROR_MSG)
