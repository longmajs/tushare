# -*- coding:utf-8 -*-
"""新股和可转债申购数据接口"""
import pandas as pd
import time
import lxml.html
from lxml import etree
from io import StringIO
import logging
from tushare.stock import cons as ct
from tushare.stock import ref_vars as rv

LOG = logging.getLogger("tushare.reference.ipo")


def new_stocks(retry_count=3, pause=0.001):
    """获取新股上市数据"""
    data = pd.DataFrame()
    ct._write_head()
    df = _newstocks(data, 1, retry_count, pause)
    return df


def _newstocks(data, pageNo, retry_count, pause):
    for _ in range(retry_count):
        time.sleep(pause)
        ct._write_console()
        try:
            html = lxml.html.parse(rv.NEW_STOCKS_URL%(ct.P_TYPE['http'], ct.DOMAINS['vsf'],
                         ct.PAGES['newstock'], pageNo))
            res = html.xpath('//table[@id="NewStockTable"]/tr')
            if len(res) == 0:
                return data
            sarr = [etree.tostring(node).decode('utf-8') for node in res]
            sarr = ''.join(sarr)
            sarr = sarr.replace('<font color="red">*</font>', '')
            sarr = '<table>%s</table>'%sarr
            df = pd.read_html(StringIO(sarr), skiprows=[0, 1])[0]
            df = df.drop([df.columns[idx] for idx in [12, 13, 14]], axis=1)
            df.columns = rv.NEW_STOCKS_COLS
            df['code'] = df['code'].map(lambda x: str(x).zfill(6))
            df['xcode'] = df['xcode'].map(lambda x: str(x).zfill(6))
            res = html.xpath('//table[@class="table2"]/tr[1]/td[1]/a/text()')
            tag = '下一页'
            hasNext = True if tag in res else False
            data = pd.concat([data, df], ignore_index=True)
            pageNo += 1
            if hasNext:
                data = _newstocks(data, pageNo, retry_count, pause)
        except Exception as ex:
            LOG.warning("%s", ex)
        else:
            return data


def new_cbonds(default=1, retry_count=3, pause=0.001):
    """获取可转债申购列表"""
    data = pd.DataFrame()
    if default == 1:
        data = _newcbonds(1, retry_count, pause)
    else:
        for page in range(1, 50):
            df = _newcbonds(page, retry_count, pause)
            if df is not None:
                data = pd.concat([data, df], ignore_index=True)
            else:
                break
    return data


def _newcbonds(pageNo, retry_count, pause):
    for _ in range(retry_count):
        time.sleep(pause)
        if pageNo != 1:
            ct._write_console()
        try:
            html = lxml.html.parse(rv.NEW_CBONDS_URL%(ct.P_TYPE['http'], ct.DOMAINS['sstar'],
                         pageNo))
            res = html.xpath('//table/tr')
            if len(res) == 0:
                return None
            sarr = [etree.tostring(node).decode('utf-8') for node in res]
            sarr = ''.join(sarr)
            sarr = '<table>%s</table>'%sarr
            df = pd.read_html(StringIO(sarr), skiprows=[0])
            if len(df) < 1:
                return None
            df = df[0]
            df = df.drop([df.columns[14], df.columns[15]], axis=1)
            df.columns = rv.NEW_CBONDS_COLS
            df['scode'] = df['scode'].map(lambda x: str(x).zfill(6))
            df['xcode'] = df['xcode'].map(lambda x: str(x).zfill(6))
        except Exception as ex:
            LOG.warning("%s", ex)
        else:
            return df
