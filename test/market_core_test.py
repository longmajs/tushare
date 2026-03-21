# -*- coding:utf-8 -*-

import unittest
from unittest import mock

import pandas as pd

from tushare.stock import market_core as mc
from tushare.stock import trading


class FakeClient(object):
    def __init__(self, payloads):
        self.payloads = payloads
        self.cache = mock.Mock()
        self.cache.get.return_value = None
        self.cache.set.return_value = None

    def get_text(self, url, **kwargs):
        for key, value in self.payloads.items():
            if key in url or kwargs.get('endpoint') == key:
                if callable(value):
                    return value(url, **kwargs)
                return value
        raise AssertionError('Unexpected URL: %s endpoint=%s' % (url, kwargs.get('endpoint')))


class MarketCoreTest(unittest.TestCase):

    def test_fetch_tencent_k_data_prefers_qfq_key(self):
        payload = (
            'kline_day_qfq={"code":0,"data":{"sh600000":'
            '{"qfqday":[["2026-03-20","10","11","12","9","1000"]],"qt":{}}}}'
        )
        fake = FakeClient({'fqkline': payload})
        with mock.patch('tushare.stock.market_core._client', return_value=fake):
            df = mc.fetch_tencent_k_data(code='600000', ktype='D', autype='qfq')
        self.assertEqual(len(df), 1)
        self.assertIn('open', df.columns)
        self.assertEqual(df.iloc[0]['code'], '600000')

    def test_fetch_realtime_quotes_handles_variable_column_size(self):
        fields = ['name'] + ['1'] * 33
        line = 'var hq_str_sh600000="%s";' % ','.join(fields)
        fake = FakeClient({'hq': line})
        with mock.patch('tushare.stock.market_core._client', return_value=fake):
            df = mc.fetch_sina_realtime_quotes('600000')
        self.assertEqual(len(df), 1)
        self.assertIn('code', df.columns)
        self.assertEqual(df.iloc[0]['code'], '600000')

    def test_fetch_today_ticks_accepts_list_payload(self):
        pages_payload = '[{"page":1,"begin_ts":"09:30:00","end_ts":"09:30:03"}]'
        html = (
            '<table id="datatbl"><tbody><tr><th>time</th><th>price</th><th>pchange</th>'
            '<th>change</th><th>volume</th><th>amount</th><th>type</th></tr>'
            '<tr><td>09:30:00</td><td>10.00</td><td>0.00%</td><td>0.00</td><td>100</td>'
            '<td>1000</td><td>买盘</td></tr></tbody></table>'
        )
        fake = FakeClient({
            'CN_Transactions.getAllPageTime': pages_payload,
            'vMS_tradedetail': html,
        })
        with mock.patch('tushare.stock.market_core._client', return_value=fake):
            df = mc.fetch_today_ticks('600000', date='2026-03-20')
        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), ['time', 'price', 'pchange', 'change', 'volume', 'amount', 'type'])


class TradingCompatTest(unittest.TestCase):

    def test_get_hist_data_redirects_to_k(self):
        sample = pd.DataFrame(
            [
                {
                    'date': '2026-03-20',
                    'open': 10.0,
                    'close': 11.0,
                    'high': 12.0,
                    'low': 9.0,
                    'volume': 1000.0,
                    'turnoverratio': 1.0,
                    'code': '600000',
                }
            ]
        )
        with mock.patch('tushare.stock.trading.is_legacy_mode', return_value=False), \
             mock.patch('tushare.stock.trading.fetch_tencent_k_data', return_value=sample):
            df = trading.get_hist_data('600000', start='2026-03-20', end='2026-03-20')
        self.assertFalse(df.empty)
        self.assertEqual(df.index.name, 'date')
        self.assertIn('ma5', df.columns)

    def test_get_today_ticks_degrade_returns_empty_df(self):
        with mock.patch('tushare.stock.trading.is_legacy_mode', return_value=False), \
             mock.patch('tushare.stock.trading.fetch_today_ticks', side_effect=mc.DataSourceUnavailable('blocked')):
            df = trading.get_today_ticks('600000')
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ['time', 'price', 'pchange', 'change', 'volume', 'amount', 'type'])


if __name__ == '__main__':
    unittest.main()
