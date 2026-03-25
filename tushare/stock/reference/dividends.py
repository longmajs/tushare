# -*- coding:utf-8 -*-
"""分红送股和业绩预告数据接口"""
import pandas as pd
import time
import lxml.html
from lxml import etree
import re
from io import StringIO
import logging
from tushare.stock import cons as ct
from tushare.stock import ref_vars as rv

LOG = logging.getLogger("tushare.reference.dividends")


def profit_data(year=2017, top=25, retry_count=3, pause=0.001):
    """获取分配预案数据"""
    if top == 'all':
        ct._write_head()
        df, pages = _dist_cotent(year, 0, retry_count, pause)
        for idx in range(1, int(pages)):
            df = pd.concat([df, _dist_cotent(year, idx, retry_count, pause)], ignore_index=True)
        return df
    elif top <= 25:
        df, pages = _dist_cotent(year, 0, retry_count, pause)
        return df.head(top)
    else:
        if isinstance(top, int):
            ct._write_head()
            allPages = top/25+1 if top%25>0 else top/25
            df, pages = _dist_cotent(year, 0, retry_count, pause)
            if int(allPages) < int(pages):
                pages = allPages
            for idx in range(1, int(pages)):
                df = pd.concat([df, _dist_cotent(year, idx, retry_count, pause)], ignore_index=True)
            return df.head(top)
        else:
            print(ct.TOP_PARAS_MSG)


def _fun_divi(x):
    reg = re.compile(r'分红(.*?)元', re.UNICODE)
    res = reg.findall(x)
    return 0 if len(res)<1 else float(res[0])


def _fun_into(x):
    reg1 = re.compile(r'转增(.*?)股', re.UNICODE)
    reg2 = re.compile(r'送股(.*?)股', re.UNICODE)
    res1 = reg1.findall(x)
    res2 = reg2.findall(x)
    res1 = 0 if len(res1)<1 else float(res1[0])
    res2 = 0 if len(res2)<1 else float(res2[0])
    return res1 + res2


def _dist_cotent(year, pageNo, retry_count, pause):
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            if pageNo > 0:
                ct._write_console()
            html = lxml.html.parse(rv.DP_163_URL%(ct.P_TYPE['http'], ct.DOMAINS['163'],
                     ct.PAGES['163dp'], year, pageNo))
            res = html.xpath('//div[@class="fn_rp_list"]/table')
            sarr = [etree.tostring(node).decode('utf-8') for node in res]
            sarr = ''.join(sarr)
            df = pd.read_html(sarr, skiprows=[0])[0]
            df = df.drop(df.columns[0], axis=1)
            df.columns = rv.DP_163_COLS
            df['divi'] = df['plan'].map(_fun_divi)
            df['shares'] = df['plan'].map(_fun_into)
            df = df.drop('plan', axis=1)
            df['code'] = df['code'].astype(object)
            df['code'] = df['code'].map(lambda x: str(x).zfill(6))
            pages = []
            if pageNo == 0:
                page = html.xpath('//div[@class="mod_pages"]/a')
                if len(page)>1:
                    asr = page[len(page)-2]
                    pages = asr.xpath('text()')
        except Exception as e:
            LOG.warning("%s", e)
        else:
            if pageNo == 0:
                return df, pages[0] if len(pages)>0 else 0
            else:
                return df
    raise IOError(ct.NETWORK_URL_ERROR_MSG)


def profit_divis():
    """获取分送送股数据"""
    ct._write_head()
    p = 'cfidata.aspx?sortfd=&sortway=&curpage=1&fr=content&ndk=A0A1934A1939A1957A1966A1983&xztj=&mystock='
    df = _profit_divis(1, pd.DataFrame(), p)
    df = df.drop([3], axis=1)
    df.columns = ct.PROFIT_DIVIS
    df['code'] = df['code'].map(lambda x: str(x).zfill(6))
    return df


def _profit_divis(pageNo, dataArr, nextPage):
    ct._write_console()
    html = lxml.html.parse('%sdata.cfi.cn/%s'%(ct.P_TYPE['http'], nextPage))
    res = html.xpath("//table[@class=\"table_data\"]/tr")
    sarr = [etree.tostring(node).decode('utf-8') for node in res]
    sarr = ''.join(sarr)
    sarr = sarr.replace('--', '0')
    sarr = '<table>%s</table>'%sarr
    df = pd.read_html(sarr, skiprows=[0])[0]
    dataArr = pd.concat([dataArr, df], ignore_index=True)
    nextPage = html.xpath('//div[@id="content"]/div[2]/a[last()]/@href')[0]
    np_val = nextPage.split('&')[2].split('=')[1]
    if pageNo < int(np_val):
        return _profit_divis(int(np_val), dataArr, nextPage)
    else:
        return dataArr


def forecast_data(year, quarter):
    """获取业绩预告数据"""
    if ct._check_input(year, quarter) is True:
        ct._write_head()
        data = _get_forecast_data(year, quarter, 1, pd.DataFrame())
        df = pd.DataFrame(data, columns=ct.FORECAST_COLS)
        df['code'] = df['code'].map(lambda x: str(x).zfill(6))
        return df


def _get_forecast_data(year, quarter, pageNo, dataArr):
    ct._write_console()
    try:
        gparser = etree.HTMLParser(encoding='GBK')
        html = lxml.html.parse(ct.FORECAST_URL%(ct.P_TYPE['http'], ct.DOMAINS['vsf'],
                                                ct.PAGES['fd'], year, quarter, pageNo,
                                                ct.PAGE_NUM[1]),
                               parser=gparser)
        res = html.xpath("//table[@class=\"list_table\"]/tr")
        sarr = [etree.tostring(node).decode('utf-8') for node in res]
        sarr = ''.join(sarr)
        sarr = sarr.replace('--', '0')
        sarr = '<table>%s</table>'%sarr
        df = pd.read_html(sarr)[0]
        df = df.drop([4, 5, 8], axis=1)
        df.columns = ct.FORECAST_COLS
        dataArr = pd.concat([dataArr, df], ignore_index=True)
        nextPage = html.xpath('//div[@class="pages"]/a[last()]/@onclick')
        if len(nextPage)>0:
            pageNo = re.findall(r'\d+', nextPage[0])[0]
            return _get_forecast_data(year, quarter, pageNo, dataArr)
        else:
            return dataArr
    except Exception as e:
        LOG.warning("%s", e)
