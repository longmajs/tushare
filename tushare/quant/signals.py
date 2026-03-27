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


def grid_trading(df, grid_spacing=0.05, grid_count=5, lookback=60,
                 center_price=None, mode='pct'):
    """
    网格交易信号 - Grid Trading Signal Generator

    Grid trading is a mechanical, low-frequency strategy that profits from
    range-bound markets. It places buy orders at grid lines below center
    (buy the dip) and sell orders at grid lines above center (take profit).

    Parameters
    ----------
    df : DataFrame
        Must contain 'date' and 'close' columns.
    grid_spacing : float
        Spacing between grid lines.
        If mode='pct': percentage (default 0.05 = 5%).
        If mode='abs': absolute price (default 5.0).
    grid_count : int
        Number of grid lines on each side of center (default 5).
        Total grid spans 2*grid_count+1 levels (including center).
    lookback : int
        Rolling window days for auto center detection (default 60).
    center_price : float, optional
        Manual center price. If None, uses rolling median of lookback period.
    mode : str
        'pct' for percentage grids (default), 'abs' for absolute price grids.

    Returns
    -------
    DataFrame
        Columns: date, signal, strength, grid_level, center

    Notes
    -----
    Grid trading performs best in range-bound/sideways markets.
    In strong trending markets, expect whipsaw losses or breakout risk.
    Consider combining with a trend filter for improved results.

    Examples
    --------
    >>> df = ts.get_k_data('600519', start='2023-01-01')
    >>> signals = grid_trading(df, grid_spacing=0.05, grid_count=5)
    >>> signals = grid_trading(df, mode='abs', grid_spacing=50, center_price=1000)
    """
    data = _validate_df(df)
    data = data.sort_values('date').reset_index(drop=True)

    # Calculate center price
    if center_price is not None:
        data['center'] = float(center_price)
    else:
        # Auto-detect: rolling median of lookback period
        data['center'] = data['close'].rolling(window=lookback, min_periods=lookback).median()

    # Calculate grid levels based on mode
    if mode == 'pct':
        # Percentage-based grids
        spacing_pct = float(grid_spacing)
        # Grid levels: -grid_count, ..., -1, 0, 1, ..., +grid_count
        # Price at level N = center * (1 + N * spacing_pct)
        data['grid_upper'] = data['center'] * (1 + grid_count * spacing_pct)
        data['grid_lower'] = data['center'] * (1 - grid_count * spacing_pct)
        data['grid_step'] = data['center'] * spacing_pct
    else:
        # Absolute price grids
        spacing_abs = float(grid_spacing)
        data['grid_upper'] = data['center'] + grid_count * spacing_abs
        data['grid_lower'] = data['center'] - grid_count * spacing_abs
        data['grid_step'] = spacing_abs

    # Calculate which grid level the price is at
    # grid_level = round((close - center) / grid_step)
    # Avoid division by zero
    data['grid_level'] = ((data['close'] - data['center']) /
                          data['grid_step'].replace(0, np.nan)).round(0).fillna(0).astype(int)

    # Calculate distance from center (normalized by total grid range)
    total_range = grid_count * data['grid_step']
    total_range = total_range.replace(0, np.nan)
    data['distance_from_center'] = ((data['close'] - data['center']).abs() / total_range).clip(0, 1)

    # Initialize signals
    data['signal'] = 'hold'
    data['strength'] = 0.0

    prev_close = data['close'].shift(1)
    prev_grid = data['grid_level'].shift(1)

    # Buy signal: price crosses BELOW a grid line (moving to a lower grid level)
    # This means we're buying the dip at a grid support level
    buy_cond = (prev_grid.notna()) & (data['grid_level'] < prev_grid) & (data['grid_level'] <= 0)

    # Sell signal: price crosses ABOVE a grid line (moving to a higher grid level)
    # This means we're taking profit at a grid resistance level
    sell_cond = (prev_grid.notna()) & (data['grid_level'] > prev_grid) & (data['grid_level'] > 0)

    data.loc[buy_cond, 'signal'] = 'buy'
    data.loc[sell_cond, 'signal'] = 'sell'

    # Strength based on distance from center
    # Higher strength for grid levels farther from center
    data.loc[data['signal'] != 'hold', 'strength'] = data.loc[data['signal'] != 'hold', 'distance_from_center']

    return data[['date', 'signal', 'strength', 'grid_level', 'center']].copy()


def ma_macd_combo(df, fast=5, slow=20, macd_fast=12, macd_slow=26, macd_signal=9):
    """
    均线 +MACD 双重确认信号 - MA + MACD Combo Signal

    Generates buy signals only when BOTH MA golden cross AND MACD histogram
    crosses above zero (dual confirmation). Sell signals trigger on EITHER
    MA death cross OR MACD histogram crossing below zero.

    Parameters
    ----------
    df : DataFrame
        Must contain 'date' and 'close' columns.
    fast : int
        Fast MA period (default 5).
    slow : int
        Slow MA period (default 20).
    macd_fast : int
        MACD fast EMA period (default 12).
    macd_slow : int
        MACD slow EMA period (default 26).
    macd_signal : int
        MACD signal line period (default 9).

    Returns
    -------
    DataFrame
        Columns: date, signal, strength, ma_fast, ma_slow, macd_hist

    Notes
    -----
    This strategy is conservative on entries (dual confirmation reduces
    false positives) but aggressive on exits (either indicator triggers exit).
    """
    data = _validate_df(df)
    data = data.sort_values('date').reset_index(drop=True)

    # Compute MA indicators
    data['ma_fast'] = data['close'].rolling(window=fast, min_periods=fast).mean()
    data['ma_slow'] = data['close'].rolling(window=slow, min_periods=slow).mean()

    # Compute MACD histogram
    ema_fast = data['close'].ewm(span=macd_fast, adjust=False).mean()
    ema_slow = data['close'].ewm(span=macd_slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal_line = macd_line.ewm(span=macd_signal, adjust=False).mean()
    data['macd_hist'] = macd_line - macd_signal_line

    # Initialize signals
    data['signal'] = 'hold'
    data['strength'] = 0.0

    # Detect crossover events
    prev_ma_fast = data['ma_fast'].shift(1)
    prev_ma_slow = data['ma_slow'].shift(1)
    prev_hist = data['macd_hist'].shift(1)

    # MA golden/death cross
    ma_golden = (prev_ma_fast <= prev_ma_slow) & (data['ma_fast'] > data['ma_slow'])
    ma_death = (prev_ma_fast >= prev_ma_slow) & (data['ma_fast'] < data['ma_slow'])

    # MACD histogram crosses
    hist_up = (prev_hist <= 0) & (data['macd_hist'] > 0)
    hist_down = (prev_hist >= 0) & (data['macd_hist'] < 0)

    # Buy requires BOTH confirmations; sell requires EITHER
    buy_cond = ma_golden & hist_up
    sell_cond = ma_death | hist_down

    data.loc[buy_cond, 'signal'] = 'buy'
    data.loc[sell_cond, 'signal'] = 'sell'

    # Compute strength (average of MA divergence and MACD histogram magnitude)
    ma_div = (data['ma_fast'] - data['ma_slow']).abs() / data['close']
    max_ma_div = ma_div.max()
    ma_strength = (ma_div / max_ma_div).clip(0, 1) if max_ma_div > 0 else 0.0

    hist_abs = data['macd_hist'].abs()
    max_hist = hist_abs.max()
    hist_strength = (hist_abs / max_hist).clip(0, 1) if max_hist > 0 else 0.0

    combined_strength = ((ma_strength + hist_strength) / 2).clip(0, 1).fillna(0)
    data.loc[data['signal'] != 'hold', 'strength'] = combined_strength
    data.loc[data['signal'] == 'hold', 'strength'] = 0.0

    return data[['date', 'signal', 'strength', 'ma_fast', 'ma_slow', 'macd_hist']].copy()


def valuation_dca(df, pe_column='pe', pb_column='pb', lookback=250,
                  buy_threshold=0.3, sell_threshold=0.7):
    """
    估值定投策略 - Valuation-based DCA (Dollar-Cost Averaging) Strategy

    Uses PE percentile over lookback period to determine valuation level.
    Buy when PE percentile < buy_threshold (undervalued),
    Sell when PE percentile > sell_threshold (overvalued).

    Parameters
    ----------
    df : DataFrame
        Must contain 'date' and columns specified by pe_column and pb_column.
    pe_column : str
        Name of PE ratio column (default 'pe').
    pb_column : str
        Name of PB ratio column (default 'pb').
    lookback : int
        Rolling window for percentile calculation (default 250, ~1 year).
    buy_threshold : float
        PE percentile threshold for buy signals (default 0.3).
    sell_threshold : float
        PE percentile threshold for sell signals (default 0.7).

    Returns
    -------
    DataFrame
        Columns: date, signal, strength, pe_percentile, pb_percentile

    Notes
    -----
    This strategy is suitable for long-term DCA investing. It buys more
    when valuations are low and reduces exposure when valuations are high.
    """
    data = _validate_df(df, required_cols=('date', pe_column, pb_column))
    data = data.sort_values('date').reset_index(drop=True)

    # Compute rolling percentiles
    def rolling_percentile(series, window):
        def pct_rank(x):
            if len(x) < 2:
                return np.nan
            return (x.values < x.values[-1]).sum() / (len(x) - 1)
        return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=False)

    data['pe_percentile'] = rolling_percentile(data[pe_column], lookback)
    data['pb_percentile'] = rolling_percentile(data[pb_column], lookback)

    # Initialize signals
    data['signal'] = 'hold'
    data['strength'] = 0.0

    # Signal logic based on PE percentile
    buy_cond = data['pe_percentile'] < buy_threshold
    sell_cond = data['pe_percentile'] > sell_threshold

    data.loc[buy_cond, 'signal'] = 'buy'
    data.loc[sell_cond, 'signal'] = 'sell'

    # Compute strength
    buy_strength = ((buy_threshold - data['pe_percentile']) / buy_threshold).clip(0, 1)
    sell_strength = ((data['pe_percentile'] - sell_threshold) / (1 - sell_threshold)).clip(0, 1)

    data.loc[buy_cond, 'strength'] = buy_strength
    data.loc[sell_cond, 'strength'] = sell_strength

    return data[['date', 'signal', 'strength', 'pe_percentile', 'pb_percentile']].copy()


def sector_rotation(prices_df, industry_df, lookback=60, top_n=3):
    """
    行业轮动策略 - Sector Rotation Momentum Strategy

    Calculates momentum for each industry sector over lookback period,
    ranks sectors, and generates buy signals for top_n sectors and sell
    signals for bottom_n sectors.

    Parameters
    ----------
    prices_df : DataFrame
        Must contain 'date', 'code', and 'close' columns.
        Long-format price data for multiple stocks.
    industry_df : DataFrame
        Must contain 'code' and 'c_name' columns.
        Maps stock codes to industry sectors (e.g. from get_industry_classified()).
    lookback : int
        Momentum lookback period in trading days (default 60).
    top_n : int
        Number of top/bottom sectors to signal (default 3).

    Returns
    -------
    DataFrame
        Columns: date, industry, signal, strength, momentum

    Notes
    -----
    Sector momentum is computed from the equal-weight average close price
    of all constituent stocks in each industry. This strategy performs best
    when distinct sectors exhibit persistent trends (momentum effect).

    Examples
    --------
    >>> industry = ts.get_industry_classified()
    >>> prices = ...  # concat get_k_data for each code
    >>> signals = sector_rotation(prices, industry, lookback=60, top_n=3)
    """
    # Validate inputs
    _validate_df(prices_df, required_cols=('date', 'code', 'close'))
    missing = [c for c in ('code', 'c_name') if c not in industry_df.columns]
    if missing:
        raise ValueError(f"industry_df missing required columns: {missing}")

    data = prices_df[['date', 'code', 'close']].copy()
    data = data.merge(industry_df[['code', 'c_name']], on='code', how='inner')

    # Build equal-weight sector index
    sector_avg = (data.groupby(['date', 'c_name'])['close']
                  .mean()
                  .reset_index()
                  .rename(columns={'c_name': 'industry', 'close': 'avg_close'}))
    sector_avg = sector_avg.sort_values(['industry', 'date']).reset_index(drop=True)

    # Compute momentum
    sector_avg['momentum'] = (
        sector_avg.groupby('industry')['avg_close']
        .transform(lambda s: s.pct_change(periods=lookback))
    )

    # Rank sectors on each date
    sector_avg['rank'] = (
        sector_avg.groupby('date')['momentum']
        .rank(method='first', ascending=True, na_option='bottom')
    )
    n_industries = sector_avg.groupby('date')['industry'].transform('count')

    # Assign signals
    sector_avg['signal'] = 'hold'
    sector_avg['strength'] = 0.0

    buy_mask = sector_avg['rank'] > (n_industries - top_n)
    sell_mask = sector_avg['rank'] <= top_n

    sector_avg.loc[buy_mask, 'signal'] = 'buy'
    sector_avg.loc[sell_mask, 'signal'] = 'sell'

    # Strength calculations
    buy_strength = (sector_avg['rank'] - (n_industries - top_n)) / top_n
    sell_strength = (top_n - sector_avg['rank'] + 1) / top_n

    sector_avg.loc[buy_mask, 'strength'] = buy_strength.loc[buy_mask].clip(0, 1)
    sector_avg.loc[sell_mask, 'strength'] = sell_strength.loc[sell_mask].clip(0, 1)

    return sector_avg[['date', 'industry', 'signal', 'strength', 'momentum']].copy()


def dividend_dog(df, dividend_column='dividend_yield', lookback=250, top_n=10):
    """
    股息犬策略信号 - Dividend Yield Dog Strategy

    Ranks stocks by dividend yield and generates buy signals for top_n
    highest-yield stocks with annual rebalancing. Based on the classic
    "Dogs of the Dow" strategy adapted for any stock universe.

    Parameters
    ----------
    df : DataFrame
        Must contain 'date', 'code', and a dividend yield column.
        The dividend yield column name is specified by dividend_column.
        Tip: compute dividend_yield as cash / (10 * close_price) using
        data from profit_data() or profit_divis() merged with price data.
    dividend_column : str
        Name of the dividend yield column (default 'dividend_yield').
    lookback : int
        Rebalancing interval in trading days (default 250, ~1 year).
    top_n : int
        Number of top-yield stocks to select (default 10).

    Returns
    -------
    DataFrame
        Columns: date, code, signal, strength, dividend_yield, rank

    Notes
    -----
    The strategy rebalances every ``lookback`` trading days. On each
    rebalance date, stocks are ranked by dividend yield. The top_n
    highest-yield stocks receive buy signals; stocks that drop out of
    the top_n receive sell signals.

    Examples
    --------
    >>> basics = ts.get_stock_basics()
    >>> # merge with price data to compute yield
    >>> signals = dividend_dog(df, dividend_column='dividend_yield', top_n=10)
    """
    _validate_df(df, required_cols=('date', 'code', dividend_column))
    data = df[['date', 'code', dividend_column]].copy()
    data = data.sort_values(['date', 'code']).reset_index(drop=True)

    # Identify unique dates and rebalance points
    unique_dates = data['date'].drop_duplicates().sort_values().reset_index(drop=True)
    rebal_indices = list(range(0, len(unique_dates), lookback))
    rebalance_dates = set(unique_dates.iloc[rebal_indices].values)

    data['signal'] = 'hold'
    data['strength'] = 0.0
    data['rank'] = np.nan

    prev_top_codes = set()

    for rdate in sorted(rebalance_dates):
        mask = data['date'] == rdate
        snapshot = data.loc[mask].copy()

        if snapshot.empty:
            continue

        # Rank by dividend yield descending (rank 1 = highest yield)
        snapshot['_rank'] = snapshot[dividend_column].rank(
            method='first', ascending=False, na_option='bottom'
        ).astype(int)

        top_mask = snapshot['_rank'] <= top_n
        current_top_codes = set(snapshot.loc[top_mask, 'code'].values)

        # Buy signal for new top_n stocks
        snapshot['signal'] = 'hold'
        snapshot.loc[top_mask, 'signal'] = 'buy'

        # Sell signal for stocks that dropped out of top_n
        dropped = prev_top_codes - current_top_codes
        if dropped:
            drop_mask = snapshot['code'].isin(dropped)
            snapshot.loc[drop_mask, 'signal'] = 'sell'
            snapshot.loc[drop_mask, 'strength'] = 1.0

        # Buy strength: rank-1 gets 1.0, rank-top_n gets 1/top_n
        buy_idx = snapshot.index[top_mask]
        snapshot.loc[buy_idx, 'strength'] = (
            (top_n - snapshot.loc[buy_idx, '_rank'] + 1) / top_n
        ).clip(0, 1)

        data.loc[mask, 'signal'] = snapshot['signal'].values
        data.loc[mask, 'strength'] = snapshot['strength'].values
        data.loc[mask, 'rank'] = snapshot['_rank'].values

        prev_top_codes = current_top_codes

    data = data.rename(columns={dividend_column: 'dividend_yield'})
    return data[['date', 'code', 'signal', 'strength', 'dividend_yield', 'rank']].copy()
