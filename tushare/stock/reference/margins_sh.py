# -*- coding:utf-8 -*-
"""上交所融资融券数据接口"""
import pandas as pd
import time
import json
import logging
from tushare.stock import cons as ct
from tushare.stock import ref_vars as rv
from tushare.util import dateu as du
from tushare.util.netbase import Client
from tushare.stock.reference._utils import _random

LOG = logging.getLogger("tushare.reference.margins_sh")


def sh_margins(start=None, end=None, retry_count=3, pause=0.001):
    """获取沪市融资融券数据列表"""
    start = du.today_last_year() if start is None else start
    end = du.today() if end is None else end
    if du.diff_day(start, end) < 0:
        return None
    start, end = start.replace('-', ''), end.replace('-', '')
    data = pd.DataFrame()
    ct._write_head()
    df = _sh_hz(data, start=start, end=end,
                retry_count=retry_count, pause=pause)
    return df


def _sh_hz(data, start=None, end=None,
           pageNo='', beginPage='', endPage='',
           retry_count=3, pause=0.001):
    for _ in range(retry_count):
        time.sleep(pause)
        ct._write_console()
        try:
            tail = rv.MAR_SH_HZ_TAIL_URL%(pageNo, beginPage, endPage)
            if pageNo == '':
                pageNo = 6
                tail = ''
            else:
                pageNo += 5
            beginPage = pageNo
            endPage = pageNo + 4
            url = rv.MAR_SH_HZ_URL%(ct.P_TYPE['http'], ct.DOMAINS['sseq'],
                                    ct.PAGES['qmd'], _random(5),
                                    start, end, tail, _random())
            ref = rv.MAR_SH_HZ_REF_URL%(ct.P_TYPE['http'], ct.DOMAINS['sse'])
            clt = Client(url, ref=ref, cookie=rv.MAR_SH_COOKIESTR)
            lines = clt.gvalue()
            lines = lines.decode('utf-8')
            lines = lines[19:-1]
            lines = json.loads(lines)
            pagecount = int(lines['pageHelp'].get('pageCount'))
            datapage = int(pagecount/5+1 if pagecount%5>0 else pagecount/5)
            df = pd.DataFrame(lines['result'], columns=rv.MAR_SH_HZ_COLS)
            df['opDate'] = df['opDate'].map(lambda x: '%s-%s-%s'%(x[0:4], x[4:6], x[6:8]))
            data = pd.concat([data, df], ignore_index=True)
            if beginPage < datapage*5:
                data = _sh_hz(data, start=start, end=end, pageNo=pageNo,
                       beginPage=beginPage, endPage=endPage,
                       retry_count=retry_count, pause=pause)
        except Exception as e:
            LOG.warning("%s", e)
        else:
            return data
    raise IOError(ct.NETWORK_URL_ERROR_MSG)


def sh_margin_details(date='', symbol='', start='', end='',
                      retry_count=3, pause=0.001):
    """获取沪市融资融券明细列表"""
    date = date if date == '' else date.replace('-', '')
    start = start if start == '' else start.replace('-', '')
    end = end if end == '' else end.replace('-', '')
    if (start != '') & (end != ''):
        date = ''
    data = pd.DataFrame()
    ct._write_head()
    df = _sh_mx(data, date=date, start=start, end=end, symbol=symbol,
                retry_count=retry_count, pause=pause)
    return df


def _sh_mx(data, date='', start='', end='', symbol='',
           pageNo='', beginPage='', endPage='',
           retry_count=3, pause=0.001):
    for _ in range(retry_count):
        time.sleep(pause)
        ct._write_console()
        try:
            tail = '&pageHelp.pageNo=%s&pageHelp.beginPage=%s&pageHelp.endPage=%s'%(pageNo,
                                    beginPage, endPage)
            if pageNo == '':
                pageNo = 6
                tail = ''
            else:
                pageNo += 5
            beginPage = pageNo
            endPage = pageNo + 4
            ref = rv.MAR_SH_HZ_REF_URL%(ct.P_TYPE['http'], ct.DOMAINS['sse'])
            clt = Client(rv.MAR_SH_MX_URL%(ct.P_TYPE['http'], ct.DOMAINS['sseq'],
                                    ct.PAGES['qmd'], _random(5), date,
                                    symbol, start, end, tail,
                                    _random()), ref=ref, cookie=rv.MAR_SH_COOKIESTR)
            lines = clt.gvalue()
            lines = lines.decode('utf-8')
            lines = lines[19:-1]
            lines = json.loads(lines)
            pagecount = int(lines['pageHelp'].get('pageCount'))
            datapage = int(pagecount/5+1 if pagecount%5>0 else pagecount/5)
            if pagecount == 0:
                return data
            if pageNo == 6:
                ct._write_tips(lines['pageHelp'].get('total'))
            df = pd.DataFrame(lines['result'], columns=rv.MAR_SH_MX_COLS)
            df['opDate'] = df['opDate'].map(lambda x: '%s-%s-%s'%(x[0:4], x[4:6], x[6:8]))
            data = pd.concat([data, df], ignore_index=True)
            if beginPage < datapage*5:
                data = _sh_mx(data, start=start, end=end, pageNo=pageNo,
                       beginPage=beginPage, endPage=endPage,
                       retry_count=retry_count, pause=pause)
        except Exception as e:
            LOG.warning("%s", e)
        else:
            return data
    raise IOError(ct.NETWORK_URL_ERROR_MSG)
