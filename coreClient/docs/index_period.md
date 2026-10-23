# 指数周线、月线预聚合

## 存储与每日更新

数据库为 `TradingDatabase/offline/db_cn_index.db`：

- `tbl_cn_index_daily`：原始指数日线。
- `tbl_cn_index_weekly`：自然周（周一至周日）。
- `tbl_cn_index_monthly`：自然月。

`service_index_daily` 每次日线更新完成后调用 `IndexPeriodStore.refresh()`。
`scheduler_updateData` 和 `scheduler_once` 已经使用这个服务，因此无需新增独立定时进程。
每轮读取一致的日线快照，反映历史修订和补录；只写入内容变化的周期，周/月线在同一个事务中提交。
服务即使没有新增日线，也会执行派生数据刷新。失败会使服务报错，不能把派生更新失败当作成功。

历史回填与维护（在 TradingDataManager 根目录）：

```sh
.venv/bin/python -m coreClient.index_period
.venv/bin/python -m coreClient.index_period --codes 000001.SH 000688.SH
```

原始日线不会被修改。只有收盘基点、没有开高低价的成立基准记录不作为K线聚合。
表初始化由 `SCHEMA_SQL_INDEX` 引用模块的同一份 schema，避免出现两份表结构。

## 字段与接口

周期主键为 `(ts_code, period_key)`，周键为周一日期 `YYYYMMDD`，月键为 `YYYYMM`。
`trade_date` 是周期内最后一个实际交易日，不是未来的周五或自然月末。
`open/high/low/close` 为期内首开、最高、最低、末收；`vol/amount` 为加总。
`pre_close/change/pct_chg` 以之前一个周期的收盘价计算。
成交量、成交额单位沿用指数日线：**手、千元**。

```python
from coreClient.data_provider import get_index_weekly, get_index_monthly
weekly = get_index_weekly(ts_code="000001.SH", start_date="20260101", source="database_only")
monthly = get_index_monthly(ts_code="000688.SH", end_date="20261023", source="database_only")
```

支持单个或一组 `ts_code`、`trade_date`、`start_date`、`end_date`。
这些是纯读取接口，不建空库、不下载、不在查询时重建全量周期。
`trade_date` 参数匹配存储的实际末交易日；历史日期所在的未完结周期应使用 `as_of_rows`。
RemoteProvider 同步加入两个只读白名单接口，远端仍强制 `database_only`。

## 实时叠加

实时页面每轮只读预聚合周期，调用 `overlay_index_period`，不会重聚合完整日线。
当日 tick 日K输入使用同样的手/千元单位；对同周期旧数据保留开盘，最高取较大值、最低取较小值，收盘使用最新价。
成交量、成交额使用“此前交易日累计 + 当日最新累计”。

表内 `before_last_*` 是排除最后一个源日之后的前缀数据。
即使离线库已经写入今天的日线，也先使用这个前缀再叠加实时快照，避免重复加量，
也避免把旧的当日极值留在被最新快照替换的K线中。反复刷新不累计叠加，也不写回正式周期表。
周/月切换到新周期时直接新建一根内存K线，使用当日开盘价。

历史复盘 `IndexPeriodStore.as_of_rows` 复用已结束周期；如果选定日期早于数据库中当前周期的末交易日，
只读取这一周/这一月截至选定日的日线拟合一根，完整历史不重聚合。
