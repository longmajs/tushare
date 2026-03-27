# -*- coding:utf-8 -*-
"""
股票筛选模块
Stock screening module: filter and rank stocks by fundamental and technical criteria.

Each screener function accepts a DataFrame with stock data and returns a filtered
DataFrame with an added 'score' column (0-100) for ranking.
"""
import pandas as pd
import numpy as np
import logging

LOG = logging.getLogger("tushare.quant.screener")


def _validate_screen_df(df, required_cols):
    """Validate that the input DataFrame contains the required columns."""
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    if len(df) == 0:
        raise ValueError("Input DataFrame is empty")
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    return df.copy()


def _compute_percentile_score(series, ascending=True):
    """
    Compute a 0-100 percentile score for a numeric series.

    Parameters
    ----------
    series : pd.Series
        Numeric values to rank.
    ascending : bool
        If True, lower values get higher scores (good for PE, PB).
        If False, higher values get higher scores (good for ROE, growth).

    Returns
    -------
    pd.Series
        Scores from 0 to 100.
    """
    if len(series) == 0:
        return series
    ranked = series.rank(ascending=ascending, method='min', na_option='bottom')
    score = ((ranked - 1) / max(len(series) - 1, 1)) * 100
    return score.round(2)


def value_screen(df, pe_max=20, pb_max=2, pe_min=0, pb_min=0):
    """
    价值筛选 - 基于市盈率/市净率筛选低估值股票

    Screen for undervalued stocks based on PE and PB ratios.

    Parameters
    ----------
    df : DataFrame
        Stock data with at minimum 'pe' and 'pb' columns.
        Typically from get_stock_basics().
    pe_max : float
        Maximum PE ratio (default 20). Stocks above this are excluded.
    pb_max : float
        Maximum PB ratio (default 2). Stocks above this are excluded.
    pe_min : float
        Minimum PE ratio (default 0). Filters out negative PE (losses).
    pb_min : float
        Minimum PB ratio (default 0). Filters out negative PB.

    Returns
    -------
    DataFrame
        Filtered stocks with added 'score' column (0-100, higher = more undervalued).
        Sorted by score descending.
    """
    required = ['pe', 'pb']
    data = _validate_screen_df(df, required)

    if 'code' not in data.columns and data.index.name == 'code':
        data = data.reset_index()

    data['pe'] = pd.to_numeric(data['pe'], errors='coerce')
    data['pb'] = pd.to_numeric(data['pb'], errors='coerce')

    mask = (
        (data['pe'] >= pe_min) &
        (data['pe'] <= pe_max) &
        (data['pb'] >= pb_min) &
        (data['pb'] <= pb_max)
    )
    result = data[mask].copy()

    if len(result) == 0:
        result['score'] = pd.Series(dtype=float)
        return result

    pe_score = _compute_percentile_score(result['pe'], ascending=True)
    pb_score = _compute_percentile_score(result['pb'], ascending=True)
    result['score'] = ((pe_score + pb_score) / 2).round(2)

    return result.sort_values('score', ascending=False).reset_index(drop=True)


def growth_screen(df, mbrg_min=10, nprg_min=10, epsg_min=None):
    """
    成长筛选 - 基于收入/利润增长率筛选高成长股票

    Screen for high-growth stocks based on revenue and earnings growth rates.

    Parameters
    ----------
    df : DataFrame
        Stock data with growth columns. Typically from get_growth_data(year, quarter).
        Required columns: 'mbrg' (revenue growth %), 'nprg' (net profit growth %).
        Optional: 'epsg' (EPS growth %).
    mbrg_min : float
        Minimum revenue growth rate % (default 10).
    nprg_min : float
        Minimum net profit growth rate % (default 10).
    epsg_min : float or None
        Minimum EPS growth rate %. If None, not used as filter.

    Returns
    -------
    DataFrame
        Filtered stocks with added 'score' column (0-100, higher = faster growth).
        Sorted by score descending.
    """
    required = ['mbrg', 'nprg']
    data = _validate_screen_df(df, required)

    if 'code' not in data.columns and data.index.name == 'code':
        data = data.reset_index()

    data['mbrg'] = pd.to_numeric(data['mbrg'], errors='coerce')
    data['nprg'] = pd.to_numeric(data['nprg'], errors='coerce')

    mask = (data['mbrg'] >= mbrg_min) & (data['nprg'] >= nprg_min)

    if 'epsg' in data.columns and epsg_min is not None:
        data['epsg'] = pd.to_numeric(data['epsg'], errors='coerce')
        mask = mask & (data['epsg'] >= epsg_min)

    result = data[mask].copy()

    if len(result) == 0:
        result['score'] = pd.Series(dtype=float)
        return result

    mbrg_score = _compute_percentile_score(result['mbrg'], ascending=False)
    nprg_score = _compute_percentile_score(result['nprg'], ascending=False)

    if 'epsg' in result.columns:
        epsg_score = _compute_percentile_score(result['epsg'], ascending=False)
        result['score'] = ((mbrg_score + nprg_score + epsg_score) / 3).round(2)
    else:
        result['score'] = ((mbrg_score + nprg_score) / 2).round(2)

    return result.sort_values('score', ascending=False).reset_index(drop=True)


def quality_screen(df, roe_min=15, net_profit_ratio_min=10,
                   gross_profit_rate_min=None):
    """
    质量筛选 - 基于 ROE/利润率筛选高质量股票

    Screen for high-quality stocks based on profitability metrics.

    Parameters
    ----------
    df : DataFrame
        Stock data with profitability columns. Typically from get_profit_data(year, quarter).
        Required columns: 'roe' (return on equity %), 'net_profit_ratio' (net margin %).
        Optional: 'gross_profit_rate' (gross margin %).
    roe_min : float
        Minimum ROE % (default 15).
    net_profit_ratio_min : float
        Minimum net profit margin % (default 10).
    gross_profit_rate_min : float or None
        Minimum gross profit margin %. If None, not used as filter.

    Returns
    -------
    DataFrame
        Filtered stocks with added 'score' column (0-100, higher = better quality).
        Sorted by score descending.
    """
    required = ['roe', 'net_profit_ratio']
    data = _validate_screen_df(df, required)

    if 'code' not in data.columns and data.index.name == 'code':
        data = data.reset_index()

    data['roe'] = pd.to_numeric(data['roe'], errors='coerce')
    data['net_profit_ratio'] = pd.to_numeric(data['net_profit_ratio'], errors='coerce')

    mask = (data['roe'] >= roe_min) & (data['net_profit_ratio'] >= net_profit_ratio_min)

    if 'gross_profit_rate' in data.columns and gross_profit_rate_min is not None:
        data['gross_profit_rate'] = pd.to_numeric(data['gross_profit_rate'], errors='coerce')
        mask = mask & (data['gross_profit_rate'] >= gross_profit_rate_min)

    result = data[mask].copy()

    if len(result) == 0:
        result['score'] = pd.Series(dtype=float)
        return result

    roe_score = _compute_percentile_score(result['roe'], ascending=False)
    margin_score = _compute_percentile_score(result['net_profit_ratio'], ascending=False)

    if 'gross_profit_rate' in result.columns:
        gross_score = _compute_percentile_score(result['gross_profit_rate'], ascending=False)
        result['score'] = ((roe_score + margin_score + gross_score) / 3).round(2)
    else:
        result['score'] = ((roe_score + margin_score) / 2).round(2)

    return result.sort_values('score', ascending=False).reset_index(drop=True)


def momentum_screen(df, period=20, min_return=5.0):
    """
    动量筛选 - 基于价格动量筛选强势股票

    Screen for stocks with strong price momentum over a lookback period.

    Parameters
    ----------
    df : DataFrame
        Price data with 'code', 'date', and 'close' columns.
        Should contain daily price data for multiple stocks.
    period : int
        Lookback period in trading days (default 20).
    min_return : float
        Minimum return % over the period to pass the screen (default 5.0).

    Returns
    -------
    DataFrame
        One row per stock with columns: code, return_pct, score.
        Sorted by score descending.
    """
    required = ['code', 'date', 'close']
    data = _validate_screen_df(df, required)

    data['date'] = pd.to_datetime(data['date'])
    data['close'] = pd.to_numeric(data['close'], errors='coerce')
    data = data.sort_values(['code', 'date'])

    records = []
    for code, group in data.groupby('code'):
        group = group.sort_values('date').reset_index(drop=True)
        if len(group) < period:
            continue
        recent_close = group['close'].iloc[-1]
        past_close = group['close'].iloc[-period]
        if past_close > 0:
            ret = ((recent_close - past_close) / past_close) * 100
            records.append({'code': code, 'return_pct': round(ret, 2)})

    if len(records) == 0:
        return pd.DataFrame(columns=['code', 'return_pct', 'score'])

    result = pd.DataFrame(records)
    result = result[result['return_pct'] >= min_return].copy()

    if len(result) == 0:
        result['score'] = pd.Series(dtype=float)
        return result

    result['score'] = _compute_percentile_score(result['return_pct'], ascending=False)

    return result.sort_values('score', ascending=False).reset_index(drop=True)


def dividend_screen(df, yield_min=3.0):
    """
    股息筛选 - 基于股息率筛选高股息股票

    Screen for stocks with high dividend yields.

    Parameters
    ----------
    df : DataFrame
        Stock data with dividend information. Must contain 'code' and
        'dividend_yield' column, or 'divi'/'cash' and a price column.
    yield_min : float
        Minimum dividend yield % (default 3.0).

    Returns
    -------
    DataFrame
        Filtered stocks with 'dividend_yield' and 'score' columns.
        Sorted by score descending.
    """
    if df is None or not isinstance(df, pd.DataFrame) or len(df) == 0:
        raise ValueError("Input must be a non-empty pandas DataFrame")

    data = df.copy()

    if 'code' not in data.columns and data.index.name == 'code':
        data = data.reset_index()

    # Try to find or compute dividend_yield
    if 'dividend_yield' not in data.columns:
        price_col = None
        if 'close' in data.columns:
            price_col = 'close'
        elif 'price' in data.columns:
            price_col = 'price'
        elif 'trade' in data.columns:
            price_col = 'trade'

        if 'divi' in data.columns and price_col is not None:
            data['divi'] = pd.to_numeric(data['divi'], errors='coerce')
            data[price_col] = pd.to_numeric(data[price_col], errors='coerce')
            data['dividend_yield'] = (data['divi'] / data[price_col] * 100).round(2)
        elif 'cash' in data.columns and price_col is not None:
            data['cash'] = pd.to_numeric(data['cash'], errors='coerce')
            data[price_col] = pd.to_numeric(data[price_col], errors='coerce')
            data['dividend_yield'] = (data['cash'] / 10 / data[price_col] * 100).round(2)
        else:
            raise ValueError(
                "DataFrame must contain 'dividend_yield' column, or "
                "'divi'/'cash' and a price column ('close'/'price'/'trade')"
            )

    data['dividend_yield'] = pd.to_numeric(data['dividend_yield'], errors='coerce')
    result = data[data['dividend_yield'] >= yield_min].copy()

    if len(result) == 0:
        result['score'] = pd.Series(dtype=float)
        return result

    result['score'] = _compute_percentile_score(result['dividend_yield'], ascending=False)

    return result.sort_values('score', ascending=False).reset_index(drop=True)


def combine_screens(results, mode='and', weights=None):
    """
    组合筛选 - 将多个筛选结果合并

    Combine multiple screener results using AND (intersection) or OR (union) logic.

    Parameters
    ----------
    results : list of DataFrame
        List of DataFrames returned by screener functions. Each must have a
        'code' column (or 'code' as index) and a 'score' column.
    mode : str
        'and' for intersection (stock must pass ALL screens, default).
        'or' for union (stock must pass ANY screen).
    weights : list of float or None
        Weights for each screener's score when computing combined score.
        Must have same length as results. If None, equal weights are used.

    Returns
    -------
    DataFrame
        Combined results with columns: code, combined_score, and individual
        screener scores (score_0, score_1, ...).
        Sorted by combined_score descending.
    """
    if not results or not isinstance(results, (list, tuple)):
        raise ValueError("results must be a non-empty list of DataFrames")

    n = len(results)

    if weights is None:
        weights = [1.0 / n] * n
    else:
        if len(weights) != n:
            raise ValueError(f"weights length ({len(weights)}) must match results length ({n})")
        total = sum(weights)
        if total <= 0:
            raise ValueError("Sum of weights must be positive")
        weights = [w / total for w in weights]

    dfs = []
    for i, r in enumerate(results):
        if r is None or not isinstance(r, pd.DataFrame):
            raise ValueError(f"results[{i}] is not a DataFrame")
        d = r.copy()
        if 'code' not in d.columns and d.index.name == 'code':
            d = d.reset_index()
        if 'code' not in d.columns:
            raise ValueError(f"results[{i}] must have a 'code' column")
        if 'score' not in d.columns:
            d['score'] = 50.0
        d = d[['code', 'score']].rename(columns={'score': f'score_{i}'})
        dfs.append(d)

    if mode == 'and':
        combined = dfs[0]
        for d in dfs[1:]:
            combined = combined.merge(d, on='code', how='inner')
    elif mode == 'or':
        combined = dfs[0]
        for d in dfs[1:]:
            combined = combined.merge(d, on='code', how='outer')
    else:
        raise ValueError(f"mode must be 'and' or 'or', got '{mode}'")

    if len(combined) == 0:
        combined['combined_score'] = pd.Series(dtype=float)
        return combined

    score_cols = [f'score_{i}' for i in range(n)]
    for col in score_cols:
        combined[col] = combined[col].fillna(0)

    combined['combined_score'] = sum(
        combined[f'score_{i}'] * weights[i] for i in range(n)
    ).round(2)

    return combined.sort_values('combined_score', ascending=False).reset_index(drop=True)
