# -*- coding:utf-8 -*-
"""
分时图与Tick回放可视化
Created on 2026/03/26
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import matplotlib.dates as mdates

from tushare.stock.trading import get_realtime_quotes, get_tick_data


def plot_timesharing(code, title=None, savefig=None, figsize=(14, 7)):
    """
    分时图：价格线 vs 均价线，成交量按买卖着色。

    Parameters
    ----------
    code : str
        股票代码，如 '600848'
    title : str
        图表标题
    savefig : str
        保存路径，为 None 时交互显示
    figsize : tuple
        图表尺寸

    Returns
    -------
    None
    """
    quotes = get_realtime_quotes(code)
    if quotes is None or quotes.empty:
        print('No realtime data for %s' % code)
        return

    row = quotes.iloc[0]
    name = row.get('name', code)
    pre_close = float(row.get('pre_close', 0))

    ticks = get_tick_data(code)
    if ticks is None or ticks.empty:
        print('No tick data for %s today' % code)
        return

    df = _prepare_tick_df(ticks)
    if df.empty:
        print('No valid tick data for %s' % code)
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
    ax_price.set_title(title or '%s (%s) Time-sharing' % (name, code))
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
        print('Saved to %s' % savefig)
    else:
        plt.show()


def plot_tick(code, date, title=None, savefig=None, figsize=(14, 7)):
    """
    历史Tick回放：价格走势 + 成交量着色。

    Parameters
    ----------
    code : str
        股票代码
    date : str
        日期，格式 YYYY-MM-DD
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
    ticks = get_tick_data(code, date=date)
    if ticks is None or ticks.empty:
        print('No tick data for %s on %s' % (code, date))
        return

    df = _prepare_tick_df(ticks, date=date)
    if df.empty:
        print('No valid tick data for %s on %s' % (code, date))
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
    ax_price.set_title(title or '%s Tick Replay (%s)' % (code, date))
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
        print('Saved to %s' % savefig)
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
