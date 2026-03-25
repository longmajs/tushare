# -*- coding:utf-8 -*-
"""投资参考数据接口 — 模块化重构，保持向后兼容"""
from tushare.stock.reference.dividends import profit_data, profit_divis, forecast_data
from tushare.stock.reference.ipo import new_stocks, new_cbonds
from tushare.stock.reference.shareholders import top10_holders, fund_holdings, xsg_data
from tushare.stock.reference.margins_sh import sh_margins, sh_margin_details
from tushare.stock.reference.margins_sz import sz_margins, sz_margin_details
from tushare.stock.reference.margins_oss import (margin_detail, margin_target,
    margin_offset, margin_zsl, stock_issuance)
from tushare.stock.reference.pledges import stock_pledged, pledged_detail
from tushare.stock.reference.moneyflow import moneyflow_hsgt

__all__ = [
    'profit_data', 'profit_divis', 'forecast_data',
    'new_stocks', 'new_cbonds',
    'top10_holders', 'fund_holdings', 'xsg_data',
    'sh_margins', 'sh_margin_details',
    'sz_margins', 'sz_margin_details',
    'margin_detail', 'margin_target', 'margin_offset', 'margin_zsl', 'stock_issuance',
    'stock_pledged', 'pledged_detail',
    'moneyflow_hsgt',
]
