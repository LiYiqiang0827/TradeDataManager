# 全市场1分钟OHLC Parquet数据集

## 边界

`coreClient.get_oneMin()`只读取全市场完整OHLC分钟数据，不访问旧的
`policy_minute.db`，也不联网补数。旧接口`get_minute()`保持原有分时价格语义。

## 目录

Parquet和DuckDB目录表默认都放在本机：

```text
offlineDataManager/data/oneMinute/
  catalog.duckdb
  parquet/schema_v2/
    year=2026/month=09/trade_date=20260924/part-000.parquet
    _manifest/trade_date=20260924.json
```

移动硬盘保留历史源CSV，默认递归扫描`/Volumes/My Passport/分钟数据/1min`下的年度目录。
数据目录可以用
`TRADING_AGENT_ONE_MIN_DATA_DIR`覆盖，Parquet目录可以用
`TRADING_AGENT_ONE_MIN_PARQUET_ROOT`覆盖。原始CSV目录可用
`TRADING_AGENT_ONE_MIN_CSV_ROOT`覆盖。

`catalog.duckdb`中的`one_min_ingest_catalog`表记录每个日期的数据源、源路径、大小、
修改时间、是否仍存在、Parquet路径、转换状态、行数、股票数、每股分钟数范围、
导入时间和错误。新增CSV、发生变化的CSV、目标Parquet丢失或上次失败的日期会进入
`pending`队列；源文件和目标均未变化的日期直接跳过。
`one_min_source_files`表同时记录每一个实际CSV文件。同一交易日出现完全相同的副本时，
只选择无`(1)`后缀的原文件转换；内容不同的同日CSV会标为`source_conflict`，不会自动覆盖。

## 数据源切换与更新

- 2025-01-02至2026-05-06：保留原CSV OHLC，删除09:30并重排为240根。
- 2026-05-07起：以TDX `get_security_bars(KLINE_TYPE_1MIN)`完整OHLCVA为主。
- 后续每日增量：`service_oneMinute.py`从TDX下载，不再依赖移动硬盘是否挂载。

TDX下载先按交易日写入`oneMinute/tdx_stage`。全市场全部股票成功后，才按交易日
原子替换正式Parquet；失败时保留staging和`one_min_tdx_sync`断点，下次自动续传。
每日调度把oneMin放在daily和adj_factor之后，确保新分区带有当日复权因子。

TDX历史服务器对不同股票的最早可用日期并不一致。历史替换时，以当日日线表的
实际交易股票集合作为完整性基准：TDX已返回的股票使用TDX，仍缺少的股票从同日
旧Parquet分区保留，并统一删除09:30、重算`time_idx=0..239`。这种分区在清单中沿用
`tdx_with_csv_fallback` 标记；旧分区可能含CSV、TDX或混合数据，这个名字不能证明
每只保留股票实际来自CSV。全部股票由本次TDX数据覆盖时标记为
`tdx`。未来每日增量若TDX缺少任一实际交易股票且没有既有分区可补齐，质量门会
拒绝替换正式数据。TDX区间内由旧CSV产生、但不在上交所交易日历中的分区会被删除。

TDX可能为当日未交易的股票返回全日平价占位，导致分钟股票集合比日线实际交易
集合多。合并时仅允许排除满足全部证据的日线外股票：同一交易日交易时段内
239或240根分钟线，所有OHLC均为同一个正价格、所有成交量严格为0，且成交额
只能为0或TDX零额解码的精确浮点哨兵值`5.877471754111438e-39`。
逐股代码、行数、价格、金额与判定依据记录在manifest的
`excluded_non_trading_placeholders`中，不计入正式股票数和TDX血缘。
任何存在成交、价格变动、空值或证据不完整的日线外股票仍会阻断合并，须先核查
日线是否漏数；该排除规则不等同于已确认停牌公告。

2026-09-29 起的新合并结果在逐日 manifest 中增加 `source_lineage`：

- `tdx_downloaded_codes`：本次真正写入TDX数据的股票列表。
- `retained_previous_partition_codes`：从同日旧分区保留的股票列表。
- `stock_codes_by_source`：逐股归入 `tdx`、`csv` 或 `unknown`。
- `previous_manifest_status`、`previous_manifest_sha256`：继承依据的状态与哈希。

只继承同一交易日明确的逐股来源记录；旧清单仅有数量、记录缺失、日期错位或来源
冲突时写 `unknown`，不根据 `tdx://` 路径或下载覆盖起止日期猜测。旧
`csv_fallback_stock_count/rows` 字段保留兼容，其实际语义是本次保留的旧分区数量。
新代码不追改已有分区，也不声称恢复了旧数据血缘；需要重新下载并核验后才能消除
旧股票日的未知来源。分钟聚合与15分钟价格一致，只是内部一致性校验，不是来源证明。

```bash
# 日常增量到最近已收盘交易日
PYTHONPATH=.:offlineDataManager/scripts .venv/bin/python \
  offlineDataManager/scripts/service/service_oneMinute.py

# 指定单日
PYTHONPATH=.:offlineDataManager/scripts .venv/bin/python \
  offlineDataManager/scripts/service/service_oneMinute.py --trade-date 20260924

# 修复指定股票在已有完整正式分区中的缺失分钟，并原子合并回Parquet
PYTHONPATH=.:offlineDataManager/scripts .venv/bin/python \
  offlineDataManager/scripts/service/service_oneMinute.py \
  --trade-date 20260924 --ts-codes 000001.SZ,600000.SH --force --merge-existing
```

`--merge-existing` 仅接受单日和明确的股票清单，并要求 `--force`。服务会把下载结果先写入staging，再与该日已有正式分区合并、校验日线实际交易股票集合，最后原子替换正式分区；若现有分区不完整或校验失败，正式数据不会被替换。未传 `--merge-existing` 的 `--ts-codes` 行为保持不变，只写staging。

## 历史CSV导入

```bash
.venv/bin/python offlineDataManager/scripts/core/import_one_min.py \
  --start-date 20260901 --end-date 20260924
```

导入幂等。目标分区和清单都存在时默认跳过；需要重建时传`--overwrite`。
每个交易日的清单记录行数、股票数、每股分钟数分布、重复键、排序检查和文件大小。
CSV扫描和转换统一由`coreClient/csv_client.py`的`CSVClient`负责，只作为历史数据
重建和恢复入口。CSV转换同样强制采用240根统一口径。

## 查询

```python
from coreClient.data_provider import get_oneMin

# 全市场单日
day = get_oneMin(trade_date="20260924")

# 单股日期范围
stock = get_oneMin("000001.SZ", start_date="20260901", end_date="20260924")

# 多股单日
stocks = get_oneMin(ts_codes=["000001.SZ", "600000.SH"], trade_date="20260924")
```

全市场读取只允许单个`trade_date`，防止误把多日全市场约数亿行一次性载入内存；
日期范围读取必须指定一只或多只股票。查询由DuckDB执行Parquet过滤和列裁剪，
PyArrow用于CSV流式转换和Parquet写入。

默认字段为`ts_code, name, trade_date, datetime, time_idx, open, high, low, close,
vol, amount, adj_factor`。所有市场统一只保留09:31至11:30、13:01至15:00，
完整交易日为240根，`time_idx`为0至239。09:30记录以及源CSV中的15:01至15:30
盘后固定价格交易在转换时直接丢弃，
不会写入Parquet，也不会被查询接口返回。
