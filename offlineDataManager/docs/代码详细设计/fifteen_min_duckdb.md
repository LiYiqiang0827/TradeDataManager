# TDX全市场15分钟OHLC DuckDB

## 数据链路

```text
TDX get_security_bars(KLINE_TYPE_15MIN)
    -> coreClient.tdx_client.TdxClient.download_fifteen_minute()
    -> service/service_fifteenMinute.py
    -> core/offline_db_client.py
    -> core/tdx_fifteen_min_store.py
    -> data/db_fifteenMinute.db (DuckDB)
    -> coreClient.data_provider.get_fifteenMin()
```

旧的移动硬盘淘宝CSV转换库保留在
`data/fifteenMinute/fifteen_min.duckdb`，但统一查询接口不再读取它。

## 时间与价格口径

- TDX原生不复权OHLC、成交量（股）、成交额（元）。
- 每个完整交易日16根：09:45–11:30、13:15–15:00。
- 第一根09:45采用TDX原生口径，包含开盘集合竞价。
- `adj_factor`从本地日复权因子表写入；调用方可按需计算前复权价格。
- 每次正常增量结束都核验请求区间的同日因子，即使所有股票已跳过下载也执行。
  只对库中实际存在的日期，按最多60个交易日分批读取本地因子并批量修复NULL或
  旧值；不读取全库历史因子，不逐日扫描分钟大表。修复复用当前写连接、与行情
  写入严格顺序，原始OHLC、成交量和成交额保持原样。
  仅接受同股票同日期的正有限因子；源因子缺失时保留未知值，不用1或跨日因子
  代替。日志的`factor_repair`列出更新行数与剩余NULL行数。
- 默认从2024-09-01请求，实际最早日期受TDX服务器保留深度限制。

## 表

- `fifteen_min_bars`：15分钟事实表，主键`(ts_code, datetime)`。
- `fifteen_min_days`：逐股票逐日行数与完整性状态。
- `fifteen_min_sync`：逐股票已请求覆盖区间，用于断点续传和每日增量。
- `fifteen_min_meta`：schema版本。

## 使用

```bash
# 单股验证
PYTHONPATH=.:offlineDataManager/scripts .venv/bin/python \
  offlineDataManager/scripts/service/service_fifteenMinute.py \
  --ts-codes 000001.SZ --start-date 20240901 --end-date 20260924

# 全市场首次回补；默认起点20240901，终点为最近收盘交易日
PYTHONPATH=.:offlineDataManager/scripts .venv/bin/python \
  offlineDataManager/scripts/service/service_fifteenMinute.py

# 查看库状态
PYTHONPATH=.:offlineDataManager/scripts .venv/bin/python \
  offlineDataManager/scripts/service/service_fifteenMinute.py --stats-only
```

无参数重复运行会跳过已覆盖区间，只下载新增日期。`--force`用于强制重拉；
`--include-bj`用于额外包含北交所。

仅修复因子的统一写接口（不联网、不重下行情）：

```python
from core.offline_db_client import get_adj_factor, repair_fifteen_min_adj_factors

factors = get_adj_factor(trade_date="20261008", columns=["ts_code", "trade_date", "adj_factor"])
result = repair_fifteen_min_adj_factors(factors)
```

批量维护可通过`FifteenMinuteWriter.repair_adj_factors(factors)`复用同一写连接；
传入的DataFrame可包含多日。完全相同的重复因子会去重，同一键出现冲突值则
拒绝该批次，重复执行在源因子不变时更新0行。
