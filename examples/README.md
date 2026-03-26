# tushare 使用示例

## 目录

| 示例文件 | 说明 |
|----------|------|
| [01_realtime_quote.py](01_realtime_quote.py) | 实时报价查询 |
| [02_kline_data.py](02_kline_data.py) | K线数据获取（日K/分钟K/多数据源） |
| [03_kline_chart.py](03_kline_chart.py) | K线图绘制（蜡烛图 + 均线 + 成交量） |
| [04_compare_chart.py](04_compare_chart.py) | 多股对比走势图 |
| [05_local_cache.py](05_local_cache.py) | 本地 Parquet 缓存与增量更新 |
| [06_quant_signals.py](06_quant_signals.py) | 量化信号：MA 金叉、MACD、RSI、布林带 |
| [07_backtest.py](07_backtest.py) | 简单回测：资金曲线、最大回撤、夏普比率 |
| [08_tdx_source.py](08_tdx_source.py) | 通达信数据源（pytdx 低延迟） |

## 快速开始

```bash
pip install tushare mplfinance pyarrow pytdx
```

```python
import tushare as ts

# 实时报价
df = ts.get_realtime_quotes('600519')
print(df[['name', 'price', 'changepercent']].T)

# 日K数据
df = ts.get_k_data('600519', start='2024-01-01')
print(df.tail())
```
