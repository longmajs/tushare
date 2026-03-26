# -*- coding:utf-8 -*-
"""
量化信号生成器
Signal generators for quantitative trading strategies.

Each function accepts a DataFrame with at minimum 'date' and 'close' columns,
and returns a DataFrame with: date, signal ('buy'/'sell'/'hold'),
strength (0-1), and relevant indicator values.
"""
import pandas as pd
import numpy as np
import logging

LOG = logging.getLogger("tushare.quant.signals")


def _validate_df(df, required_cols=('date', 'close')):
    """Validate input DataFrame has required columns."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    return df.copy()


def ma_crossover(df, fast=5, slow=20):
    """
    移动均线交叉信号

    Parameters
    ----------
    df : DataFrame
        Must contain 'date' and 'close' columns.
    fast : int
        Fast moving average period (default 5).
    slow : int
        Slow moving average period (default 20).

    Returns
    -------
    DataFrame
        Columns: date, signal, strength, ma_fast, ma_slow
    """
    data = _validate_df(df)
    data = data.sort_values('date').reset_index(drop=True)

    data['ma_fast'] = data['close'].rolling(window=fast, min_periods=fast).mean()
    data['ma_slow'] = data['close'].rolling(window=slow, min_periods=slow).mean()

    data['signal'] = 'hold'
    data['strength'] = 0.0

    prev_fast = data['ma_fast'].shift(1)
    prev_slow = data['ma_slow'].shift(1)

    # Golden cross: fast crosses above slow
    golden = (prev_fast <= prev_slow) & (data['ma_fast'] > data['ma_slow'])
    # Death cross: fast crosses below slow
    death = (prev_fast >= prev_slow) & (data['ma_fast'] < data['ma_slow'])

    data.loc[golden, 'signal'] = 'buy'
    data.loc[death, 'signal'] = 'sell'

    # Strength based on divergence magnitude relative to price
    divergence = (data['ma_fast'] - data['ma_slow']).abs() / data['close']
    max_div = divergence.max()
    if max_div > 0:
        data['strength'] = (divergence / max_div).clip(0, 1)
    data.loc[data['signal'] == 'hold', 'strength'] = 0.0

    return data[['date', 'signal', 'strength', 'ma_fast', 'ma_slow']].copy()


def macd_signal(df, fast_period=12, slow_period=26, signal_period=9):
    """
    MACD 柱状图交叉信号

    Parameters
    ----------
    df : DataFrame
        Must contain 'date' and 'close' columns.
    fast_period : int
        Fast EMA period (default 12).
    slow_period : int
        Slow EMA period (default 26).
    signal_period : int
        Signal line EMA period (default 9).

    Returns
    -------
    DataFrame
        Columns: date, signal, strength, macd, macd_signal, macd_hist
    """
    data = _validate_df(df)
    data = data.sort_values('date').reset_index(drop=True)

    ema_fast = data['close'].ewm(span=fast_period, adjust=False).mean()
    ema_slow = data['close'].ewm(span=slow_period, adjust=False).mean()

    data['macd'] = ema_fast - ema_slow
    data['macd_signal'] = data['macd'].ewm(span=signal_period, adjust=False).mean()
    data['macd_hist'] = data['macd'] - data['macd_signal']

    prev_hist = data['macd_hist'].shift(1)

    data['signal'] = 'hold'
    data['strength'] = 0.0

    # Histogram crosses above zero
    buy_cross = (prev_hist <= 0) & (data['macd_hist'] > 0)
    # Histogram crosses below zero
    sell_cross = (prev_hist >= 0) & (data['macd_hist'] < 0)

    data.loc[buy_cross, 'signal'] = 'buy'
    data.loc[sell_cross, 'signal'] = 'sell'

    # Strength from histogram magnitude
    max_hist = data['macd_hist'].abs().max()
    if max_hist > 0:
        strength = (data['macd_hist'].abs() / max_hist).clip(0, 1)
        data.loc[data['signal'] != 'hold', 'strength'] = strength

    return data[['date', 'signal', 'strength', 'macd', 'macd_signal', 'macd_hist']].copy()


def rsi_signal(df, period=14, overbought=70, oversold=30):
    """
    RSI 超买超卖信号

    Parameters
    ----------
    df : DataFrame
        Must contain 'date' and 'close' columns.
    period : int
        RSI period (default 14).
    overbought : float
        Overbought threshold (default 70).
    oversold : float
        Oversold threshold (default 30).

    Returns
    -------
    DataFrame
        Columns: date, signal, strength, rsi
    """
    data = _validate_df(df)
    data = data.sort_values('date').reset_index(drop=True)

    delta = data['close'].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    # Use Wilder's smoothing for subsequent values
    for i in range(period, len(data)):
        avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (period - 1) + loss.iloc[i]) / period

    rs = avg_gain / avg_loss.replace(0, np.nan)
    data['rsi'] = 100 - (100 / (1 + rs))

    prev_rsi = data['rsi'].shift(1)

    data['signal'] = 'hold'
    data['strength'] = 0.0

    # Buy when RSI crosses above oversold from below
    buy_cond = (prev_rsi <= oversold) & (data['rsi'] > oversold)
    # Sell when RSI crosses below overbought from above
    sell_cond = (prev_rsi >= overbought) & (data['rsi'] < overbought)

    data.loc[buy_cond, 'signal'] = 'buy'
    data.loc[sell_cond, 'signal'] = 'sell'

    # Strength: how far into overbought/oversold territory
    buy_strength = ((oversold - data['rsi']) / oversold).clip(0, 1)
    sell_strength = ((data['rsi'] - overbought) / (100 - overbought)).clip(0, 1)
    data.loc[buy_cond, 'strength'] = buy_strength
    data.loc[sell_cond, 'strength'] = sell_strength

    return data[['date', 'signal', 'strength', 'rsi']].copy()


def bollinger_signal(df, period=20, std=2):
    """
    布林带突破信号

    Parameters
    ----------
    df : DataFrame
        Must contain 'date' and 'close' columns.
    period : int
        Bollinger Bands period (default 20).
    std : float
        Number of standard deviations (default 2).

    Returns
    -------
    DataFrame
        Columns: date, signal, strength, bb_upper, bb_middle, bb_lower
    """
    data = _validate_df(df)
    data = data.sort_values('date').reset_index(drop=True)

    data['bb_middle'] = data['close'].rolling(window=period, min_periods=period).mean()
    rolling_std = data['close'].rolling(window=period, min_periods=period).std()
    data['bb_upper'] = data['bb_middle'] + std * rolling_std
    data['bb_lower'] = data['bb_middle'] - std * rolling_std

    prev_close = data['close'].shift(1)

    data['signal'] = 'hold'
    data['strength'] = 0.0

    # Buy: price crosses above lower band (bounce from oversold)
    buy_cond = (prev_close <= data['bb_lower'].shift(1)) & (data['close'] > data['bb_lower'])
    # Sell: price crosses below upper band (reversal from overbought)
    sell_cond = (prev_close >= data['bb_upper'].shift(1)) & (data['close'] < data['bb_upper'])

    data.loc[buy_cond, 'signal'] = 'buy'
    data.loc[sell_cond, 'signal'] = 'sell'

    # Strength: distance from band relative to bandwidth
    bandwidth = data['bb_upper'] - data['bb_lower']
    bandwidth = bandwidth.replace(0, np.nan)

    buy_dist = (data['bb_lower'] - data['close']).abs() / bandwidth
    sell_dist = (data['close'] - data['bb_upper']).abs() / bandwidth
    data.loc[buy_cond, 'strength'] = buy_dist.clip(0, 1)
    data.loc[sell_cond, 'strength'] = sell_dist.clip(0, 1)

    return data[['date', 'signal', 'strength', 'bb_upper', 'bb_middle', 'bb_lower']].copy()
