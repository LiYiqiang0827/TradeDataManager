# 代码详细设计/check_database.md

`scripts/core/check_database.py` 用于检查并自动修复三张核心日频数据表：

- `daily` → `tbl_cn_day`
- `daily_basic` → `tbl_cn_daily_basic`
- `adj_factor` → `tbl_cn_adj_factor`

## 判断规则

检查范围只包含 `tbl_cn_tradecal` 中 `exchange='SSE' AND is_open=1` 的日期。

某个交易日满足以下任一条件就进入重拉列表：

1. 当日数据量为0；
2. 当日数据量低于前后相邻交易日较大值的90%；
3. 指定了股票代码，并且当日缺少任一指定代码。

区间首尾也会读取区间外最近一个交易日作为相邻基准。比例阈值可通过
`neighbor_ratio_threshold` 或 `--neighbor-ratio-threshold` 修改。

## 自动修复

统一接口默认启用自动修复。发现异常后分别调用：

- `CNDataDown.update_daily(start_date=date, end_date=date)`
- `CNDataDown.update_daily_basic(start_date=date, end_date=date)`
- `CNDataDown.update_adj_factor(start_date=date, end_date=date)`

下载器按交易日重拉全市场数据。重拉完成后再次检查，并在返回值中同时保留：

- `initial_redownload_dates`：首次发现的异常日期；
- `repair_attempts`：每次下载的方法、插入行数及错误；
- `redownload_dates`：复查后仍然异常的日期；
- `initial_reports` / `reports`：修复前和修复后的逐日明细。

如果本次实际修复了 `daily` 或 `adj_factor`，且复查通过，统一接口还会检查
`cn_daily` 与 `cn_adj_factor` 断点是否一致并已到最近收盘日，然后依次调用：

- `CNDataDown.update_week()`，同时原子覆盖 `tbl_cn_week` 和 `tbl_cn_week_origin`；
- `CNDataDown.update_month()`，同时原子覆盖 `tbl_cn_month` 和 `tbl_cn_month_origin`。

`daily_basic` 修复不会触发周月线重算，因为它不是周月线的输入。派生表结果写入
`derived_regeneration`，包括触发表、状态、每个方法的返回行数和异常。可通过
`regenerate_derived=False` 或 `--no-derived-regeneration` 关闭。

如果只需要检查，传 `auto_repair=False` 或使用 `--no-repair`。

## Python API

```python
from core.check_database import check_database

# 多表、日期范围、全市场；发现异常后自动重拉并复查
result = check_database(
    ["daily", "daily_basic", "adj_factor"],
    start_date="20260901",
    end_date="20260915",
)

# 修复基础数据，但明确不重算周月线
result = check_database(
    "daily",
    trade_date="20260910",
    regenerate_derived=False,
)

# 单表、单日、单只股票
result = check_database(
    "daily",
    trade_date="20260910",
    tscode="000001.SZ",
)

# 多只股票，只读检查
result = check_database(
    ["daily", "adj_factor"],
    trade_date="20260910",
    ts_code=["000001.SZ", "600000.SH"],
    auto_repair=False,
)
```

`start_date`/`end_date`/`trade_date` 分别支持 `startdate`/`enddate`/`tradedate`
别名；`ts_code` 和 `tscode` 也是等价参数。同一参数的两种写法不能同时传入。

也可以分别调用纯检查函数：

```python
from core.check_database import (
    check_daily_data,
    check_daily_basic_data,
    check_adj_factor_data,
)
```

三个独立函数只返回诊断结果；自动下载和复查由统一的 `check_database` 接口负责。

## 命令行

```bash
# 默认自动修复
.venv/bin/python offlineDataManager/scripts/core/check_database.py \
  --tables daily,daily_basic,adj_factor \
  --start-date 20260901 \
  --end-date 20260915

# 单日、多只股票
.venv/bin/python offlineDataManager/scripts/core/check_database.py \
  --tables daily,adj_factor \
  --trade-date 20260910 \
  --ts-codes 000001.SZ,600000.SH

# 只检查；若复查结果仍有异常则返回退出码2
.venv/bin/python offlineDataManager/scripts/core/check_database.py \
  --tables daily \
  --trade-date 20260910 \
  --no-repair \
  --fail-on-issue

# 自动修复日线，但不重新生成周/月线派生表
.venv/bin/python offlineDataManager/scripts/core/check_database.py \
  --tables daily \
  --trade-date 20260910 \
  --no-derived-regeneration
```

脚本会从项目 `.env` 安全读取简单的 `export KEY=value` 配置，但不会执行其中的
shell 命令。指定自定义数据库并启用自动修复时，必须向 Python API 传入对应的
`downloader`，防止误写默认数据库。
