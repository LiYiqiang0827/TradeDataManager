# 指数原生一分钟与十五分钟数据

2026-10-22 修复：指数 K 线必须使用 `pytdx.api.get_index_bars`。指数协议比个股
多出涨跌家数，使用 `get_security_bars` 解析会使后续记录错位，出现错误日期与价格。

## 代码与存储

- 下载、完整性校验和持久化：`coreClient/index_minute.py`，`IndexMinuteDownloader`。
- 更新入口：`offlineDataManager/scripts/service/service_intraday_index.py`。
- 规范数据库：`$TRADING_DATABASE_ROOT/offline/db_cn_index_minute.db`，默认为
  `~/TradingDatabase/offline/db_cn_index_minute.db`。
- 表：`tbl_cn_index_minute`（主键 ts_code、period、datetime）与
  `tbl_cn_index_minute_ctrl`（仅记录通过校验的完整交易日）。
- 下载审计：数据库同目录下 `index_minute_downloads/YYYYMMDD_HHMMSS.json`。
- 旧 `policy_minute.db/tbl_minute_index` 保持只读，避免混入不同的量纲。

支持网页的八个指数：000001.SH、399001.SZ、399006.SZ、000688.SH、000300.SH、
000016.SH、000905.SH、000852.SH。其他指数需要先验证源端支持后扩展常量。

## 运行

在 TradingDataManager 根目录、配置好项目环境变量后：

```sh
.venv/bin/python -m offlineDataManager.scripts.service.service_intraday_index --all-history
.venv/bin/python -m offlineDataManager.scripts.service.service_intraday_index
.venv/bin/python -m offlineDataManager.scripts.service.service_intraday_index --trade-date 20261022
```

首次下载全部源端可用历史；增量更新从各指数、各周期的最后成功日期向前重叠七个自然日。
默认截至最近已完成的交易日（16:30 后允许当天）。完整历史请求分页直到源端结束；
源端没有固定保留深度保证，审计报告记录实际起止日期和终止原因。当前实测一分钟
大多始于 2026-06-08，十五分钟大多始于 2024-10-15，个别指数略早。

每日离线 scheduler 在 16:35 运行，18:00/20:00/22:00 完整更新开始时再补增量。
不依赖个股复权因子，也不因个股下载失败而跳过指数更新。

## 数据契约与验收

- 两种周期均为原生 OHLC，十五分钟不会从每分钟收盘价拼接。
- 一分钟每天 240 根（09:31—11:30、13:01—15:00）；十五分钟每天 16 根，
  第一根 09:45、最后一根 15:00。过滤午休、15:00 以后及请求结束日期之后的数据。
- 按完整交易日写入。少根、日期错误、非有限/非正价格、负成交额或严重 OHLC
  矛盾会拒绝该批次，控制表不会宣称下载成功。
- 收盘记录偶见 0.01 点以内的 high/low 与最终收盘值精度差异：保留原值，
  `quality_flags` 标记 `closing_ohlc_boundary_rounding`；其他异常仍拒绝。
- 逐日对照本地交易日历与指数日线。收盘差异超过 0.1% 时拒绝写入。
- `raw_volume` 是指数源端原始字段，不能作为个股成交股数。`amount` 是源端发布
  的成交额（元）。部分指数源端成交额是交易所全市场口径，例如深成指与其成分股
  日线成交额不完全相同；保留口径，不强行对齐。
- 网页分钟副图使用成交额，明确显示为“成交额”；不计算不适用于该口径的个股 VWAP。

## 读取接口

```python
from coreClient.data_provider import get_index_oneMin, get_index_fifteenMin
one = get_index_oneMin("000001.SH", trade_date="20261022", source="database_only")
fifteen = get_index_fifteenMin("000001.SH", start_date="20260722", end_date="20261022", source="database_only")
```

默认纯数据库读取，返回 OHLC、amount、raw_volume、quality_flags 和来源审计字段。
`source="online"` 使用同一指数专用下载器，仅返回数据、不写数据库。

旧 `get_minute_index` 兼容接口优先规范库、较早历史回退旧库，返回 `price=close`，
兼容 `vol` 明确为“万元成交额”（`volume_unit=turnover_10000_yuan`），不是股数。
其 online 路由也使用指数专用解码器。
