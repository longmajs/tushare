# -*- coding:utf-8 -*-
"""
K线图可视化
Created on 2026/03/26
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

try:
    import mplfinance as mpf
except ImportError:
    raise ImportError(
        'mplfinance is required for K-line charts. '
        'Install it with: pip install mplfinance'
    )

from tushare.stock.trading import get_k_data


def plot_kline(code, start=None, end=None, ktype='D',
               indicators=None, volume=True,
               title=None, savefig=None, style='charles',
               figsize=(14, 8), mav_colors=None):
    """
    绘制K线图（蜡烛图），支持均线叠加和成交量。

    Parameters
    ----------
    code : str
        股票代码，如 '600848'
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
    title : str
        图表标题，默认自动生成
    savefig : str
        保存路径，为 None 时交互显示
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
    if indicators is None:
        indicators = ['ma5', 'ma10', 'ma20']

    data = get_k_data(code, start=start or '', end=end or '', ktype=ktype)
    if data is None or data.empty:
        print('No data available for %s' % code)
        return

    ohlcv = _prepare_ohlcv(data)
    if ohlcv.empty:
        print('No valid OHLCV data for %s' % code)
        return

    mav_periods = _parse_mav(indicators)
    kwargs = dict(
        type='candle',
        style=style,
        volume=volume,
        figsize=figsize,
        title=title or '%s K-Line (%s)' % (code, ktype.upper()),
        warn_too_much_data=1000,
    )
    if mav_periods:
        kwargs['mav'] = tuple(mav_periods)
    if mav_colors and mav_periods:
        kwargs['mavcolors'] = mav_colors[:len(mav_periods)]

    if savefig:
        kwargs['savefig'] = savefig
    mpf.plot(ohlcv, **kwargs)


def plot_compare(codes, start=None, end=None, field='close',
                 normalize=True, title=None, savefig=None,
                 figsize=(14, 6)):
    """
    多股对比走势图。

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
    for code in codes:
        data = get_k_data(code, start=start or '', end=end or '', ktype='D')
        if data is None or data.empty:
            print('No data for %s, skipping' % code)
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
        print('Saved to %s' % savefig)
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
