# -*- coding:utf-8 -*-
"""
K线图可视化
Created on 2026/03/26
"""
import logging

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

try:
    import mplfinance as mpf
    _HAS_MPF = True
except ImportError:
    _HAS_MPF = False

from tushare.stock.trading import get_k_data

LOG = logging.getLogger("tushare.visual")


def plot_kline(code_or_df, start=None, end=None, ktype='D',
               indicators=None, volume=True, savefig=None,
               title=None, style='charles', figsize=(14, 8),
               mav_colors=None):
    """
    绘制K线图（蜡烛图），支持均线叠加和成交量。

    Parameters
    ----------
    code_or_df : str or DataFrame
        股票代码（如 '600848'）或包含 date/open/close/high/low/volume 列的 DataFrame
    start : str
        开始日期，格式 YYYY-MM-DD
    end : str
        结束日期，格式 YYYY-MM-DD
    ktype : str
        K线类型，D=日K W=周K M=月K 5/15/30/60=分钟K，默认 D
    indicators : list
        均线指标列表，如 ['ma5', 'ma10', 'ma20']，默认 ['ma5', 'ma10', 'ma20']
    volume : bool
        是否显示成交量，默认 True
    savefig : str
        保存路径，为 None 时调用 plt.show()
    title : str
        图表标题，默认自动生成
    style : str
        mplfinance 样式，默认 'charles'
    figsize : tuple
        图表尺寸，默认 (14, 8)
    mav_colors : list
        均线颜色列表

    Returns
    -------
    None
    """
    if not _HAS_MPF:
        LOG.warning('mplfinance is not installed. '
                    'Install it with: pip install mplfinance')
        return

    if indicators is None:
        indicators = ['ma5', 'ma10', 'ma20']

    if isinstance(code_or_df, str):
        code = code_or_df
        data = get_k_data(code, start=start or '', end=end or '', ktype=ktype)
        if data is None or data.empty:
            LOG.warning('No data available for %s', code)
            return
        chart_title = title or '%s K-Line (%s)' % (code, ktype.upper())
    elif isinstance(code_or_df, pd.DataFrame):
        data = code_or_df.copy()
        chart_title = title or 'K-Line (%s)' % ktype.upper()
    else:
        LOG.warning('code_or_df must be a stock code string or a DataFrame')
        return

    ohlcv = _prepare_ohlcv(data)
    if ohlcv.empty:
        LOG.warning('No valid OHLCV data')
        return

    mav_periods = _parse_mav(indicators)
    kwargs = dict(
        type='candle',
        style=style,
        volume=volume,
        figsize=figsize,
        title=chart_title,
        warn_too_much_data=1000,
    )
    if mav_periods:
        kwargs['mav'] = tuple(mav_periods)
    if mav_colors and mav_periods:
        kwargs['mavcolors'] = mav_colors[:len(mav_periods)]

    if savefig:
        kwargs['savefig'] = savefig
        LOG.info('Saved K-line chart to %s', savefig)
    mpf.plot(ohlcv, **kwargs)

    if not savefig:
        plt.show()


def plot_compare(codes, start=None, end=None, field='close',
                 normalize=True, title=None, savefig=None,
                 figsize=(14, 6)):
    """
    多股对比走势图。将各股按起始日归一化为百分比变化后绘制在同一坐标轴上。

    Parameters
    ----------
    codes : list
        股票代码列表，如 ['600519', '000858']
    start : str
        开始日期
    end : str
        结束日期
    field : str
        比较字段，默认 'close'
    normalize : bool
        是否归一化（百分比变化），默认 True
    title : str
        图表标题
    savefig : str
        保存路径
    figsize : tuple
        图表尺寸

    Returns
    -------
    None
    """
    fig, ax = plt.subplots(figsize=figsize)
    has_data = False

    for code in codes:
        data = get_k_data(code, start=start or '', end=end or '', ktype='D')
        if data is None or data.empty:
            LOG.warning('No data for %s, skipping', code)
            continue
        df = data[['date', field]].copy()
        df[field] = pd.to_numeric(df[field], errors='coerce')
        df = df.dropna(subset=[field])
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        if normalize and len(df) > 0:
            base = df[field].iloc[0]
            if base != 0:
                df[field] = (df[field] / base - 1) * 100
        ax.plot(df['date'], df[field], label=code)
        has_data = True

    if not has_data:
        LOG.warning('No data available for any of the provided codes')
        plt.close(fig)
        return

    ax.set_title(title or 'Stock Comparison (%s)' % field)
    ax.set_xlabel('Date')
    ylabel = '%s change (%%)' % field if normalize else field
    ax.set_ylabel(ylabel)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    plt.tight_layout()

    if savefig:
        fig.savefig(savefig, dpi=150, bbox_inches='tight')
        LOG.info('Saved comparison chart to %s', savefig)
    else:
        plt.show()


def _prepare_ohlcv(data):
    """Convert tushare DataFrame to mplfinance-compatible format."""
    df = data.copy()
    for col in ['open', 'close', 'high', 'low', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
    df = df.sort_index()
    required = ['open', 'high', 'low', 'close', 'volume']
    missing = [c for c in required if c not in df.columns]
    if missing:
        LOG.warning('Missing columns for OHLCV: %s', missing)
        return pd.DataFrame()
    df.index.name = 'Date'
    df = df.rename(columns={
        'open': 'Open', 'high': 'High', 'low': 'Low',
        'close': 'Close', 'volume': 'Volume',
    })
    return df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()


def _parse_mav(indicators):
    """Extract moving average periods from indicator names like 'ma5', 'ma20'."""
    periods = []
    for ind in indicators:
        s = ind.lower().replace('ma', '')
        try:
            periods.append(int(s))
        except ValueError:
            continue
    return sorted(periods)
