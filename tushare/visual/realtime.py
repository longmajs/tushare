# -*- coding:utf-8 -*-
"""
分时图与Tick回放可视化
Created on 2026/03/26
"""
import logging

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator
import pandas as pd
import numpy as np

from tushare.stock.trading import get_realtime_quotes, get_tick_data

LOG = logging.getLogger("tushare.visual")


def plot_timesharing(code_or_df, savefig=None, title=None, figsize=(14, 7)):
    """
    分时图：价格线 vs 均价线，成交量按买卖着色。

    Parameters
    ----------
    code_or_df : str or DataFrame
        股票代码（如 '600848'），自动获取当日 tick 数据；
        或已有的 tick DataFrame（须含 time/price/volume 列）
    savefig : str
        保存路径，为 None 时调用 plt.show()
    title : str
        图表标题
    figsize : tuple
        图表尺寸

    Returns
    -------
    None
    """
    if isinstance(code_or_df, str):
        code = code_or_df
        quotes = get_realtime_quotes(code)
        if quotes is None or quotes.empty:
            LOG.warning('No realtime data for %s', code)
            return

        row = quotes.iloc[0]
        name = row.get('name', code)
        pre_close = float(row.get('pre_close', 0))

        ticks = get_tick_data(code)
        if ticks is None or ticks.empty:
            LOG.warning('No tick data for %s today', code)
            return

        chart_title = title or '%s (%s) Time-sharing' % (name, code)
    elif isinstance(code_or_df, pd.DataFrame):
        ticks = code_or_df.copy()
        name = 'Stock'
        pre_close = 0
        chart_title = title or 'Time-sharing'
    else:
        LOG.warning('code_or_df must be a stock code string or a DataFrame')
        return

    df = _prepare_tick_df(ticks)
    if df.empty:
        LOG.warning('No valid tick data')
        return

    df['avg_price'] = (df['amount'].cumsum()) / (df['volume'].cumsum()) \
        if df['volume'].cumsum().iloc[-1] > 0 else df['price']

    fig, (ax_price, ax_vol) = plt.subplots(
        2, 1, figsize=figsize, height_ratios=[3, 1],
        sharex=True, gridspec_kw={'hspace': 0.05}
    )

    # Price line
    ax_price.plot(df['time_dt'], df['price'], color='#1f77b4',
                  linewidth=1.2, label='Price')
    ax_price.plot(df['time_dt'], df['avg_price'], color='#ff7f0e',
                  linewidth=1.0, linestyle='--', label='Avg Price')
    if pre_close > 0:
        ax_price.axhline(y=pre_close, color='gray', linewidth=0.8,
                         linestyle=':', alpha=0.7, label='Pre Close')

    ax_price.set_ylabel('Price')
    ax_price.legend(loc='upper left', fontsize=8)
    ax_price.set_title(chart_title)
    ax_price.grid(True, alpha=0.3)

    # Volume bars colored by buy/sell
    colors = df['type'].map(
        {'买盘': '#e74c3c', '卖盘': '#2ecc71', '中性盘': '#95a5a6',
         'buy': '#e74c3c', 'sell': '#2ecc71', 'neutral': '#95a5a6'}
    ).fillna('#95a5a6')

    ax_vol.bar(df['time_dt'], df['volume'], color=colors, width=0.0005, alpha=0.7)
    ax_vol.set_ylabel('Volume')
    ax_vol.grid(True, alpha=0.3)
    ax_vol.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax_vol.xaxis.set_major_locator(MaxNLocator(nbins=10))

    fig.autofmt_xdate()
    plt.tight_layout()

    if savefig:
        fig.savefig(savefig, dpi=150, bbox_inches='tight')
        LOG.info('Saved time-sharing chart to %s', savefig)
    else:
        plt.show()


def plot_tick(code_or_df, date=None, savefig=None, title=None, figsize=(14, 7)):
    """
    历史Tick回放：价格走势 + 成交量着色。

    Parameters
    ----------
    code_or_df : str or DataFrame
        股票代码（如 '600848'），配合 date 获取历史 tick 数据；
        或已有的 tick DataFrame（须含 time/price/volume 列）
    date : str
        日期，格式 YYYY-MM-DD（仅当 code_or_df 为字符串时生效）
    savefig : str
        保存路径，为 None 时调用 plt.show()
    title : str
        图表标题
    figsize : tuple
        图表尺寸

    Returns
    -------
    None
    """
    if isinstance(code_or_df, str):
        code = code_or_df
        ticks = get_tick_data(code, date=date)
        if ticks is None or ticks.empty:
            LOG.warning('No tick data for %s on %s', code, date)
            return
        chart_title = title or '%s Tick Replay (%s)' % (code, date)
        tick_date = date
    elif isinstance(code_or_df, pd.DataFrame):
        ticks = code_or_df.copy()
        chart_title = title or 'Tick Replay'
        tick_date = date
    else:
        LOG.warning('code_or_df must be a stock code string or a DataFrame')
        return

    df = _prepare_tick_df(ticks, date=tick_date)
    if df.empty:
        LOG.warning('No valid tick data')
        return

    df['avg_price'] = (df['amount'].cumsum()) / (df['volume'].cumsum()) \
        if df['volume'].cumsum().iloc[-1] > 0 else df['price']

    fig, (ax_price, ax_vol) = plt.subplots(
        2, 1, figsize=figsize, height_ratios=[3, 1],
        sharex=True, gridspec_kw={'hspace': 0.05}
    )

    # Price line
    ax_price.plot(df['time_dt'], df['price'], color='#1f77b4',
                  linewidth=1.0, label='Price')
    ax_price.plot(df['time_dt'], df['avg_price'], color='#ff7f0e',
                  linewidth=0.8, linestyle='--', label='Avg Price')
    ax_price.set_ylabel('Price')
    ax_price.legend(loc='upper left', fontsize=8)
    ax_price.set_title(chart_title)
    ax_price.grid(True, alpha=0.3)

    # Volume bars
    colors = df['type'].map(
        {'买盘': '#e74c3c', '卖盘': '#2ecc71', '中性盘': '#95a5a6',
         'buy': '#e74c3c', 'sell': '#2ecc71', 'neutral': '#95a5a6'}
    ).fillna('#95a5a6')

    ax_vol.bar(df['time_dt'], df['volume'], color=colors, width=0.0003, alpha=0.7)
    ax_vol.set_ylabel('Volume')
    ax_vol.grid(True, alpha=0.3)
    ax_vol.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax_vol.xaxis.set_major_locator(MaxNLocator(nbins=10))

    fig.autofmt_xdate()
    plt.tight_layout()

    if savefig:
        fig.savefig(savefig, dpi=150, bbox_inches='tight')
        LOG.info('Saved tick chart to %s', savefig)
    else:
        plt.show()


def _prepare_tick_df(ticks, date=None):
    """Normalize tick DataFrame for plotting."""
    df = ticks.copy()
    for col in ['price', 'volume', 'amount']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['price', 'volume'])

    if 'time' in df.columns:
        if date:
            df['time_dt'] = pd.to_datetime(date + ' ' + df['time'].astype(str),
                                           errors='coerce')
        else:
            today = pd.Timestamp.now().strftime('%Y-%m-%d')
            df['time_dt'] = pd.to_datetime(today + ' ' + df['time'].astype(str),
                                           errors='coerce')
        df = df.dropna(subset=['time_dt'])
        df = df.sort_values('time_dt')

    if 'amount' not in df.columns or df['amount'].isna().all():
        df['amount'] = df['price'] * df['volume']

    if 'type' not in df.columns:
        df['type'] = '中性盘'

    return df
