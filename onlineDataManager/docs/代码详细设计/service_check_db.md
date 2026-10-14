# service/service_check_db.py 详细设计

> **SQLite 落盘检查 CLI 工具**
> **版本**:v6.7 新增(2026-09-15)
> **职责**:统一入口 — 给日常 debug / Cron / AI Agent 提供"快速检查 SQLite 落盘数据状态"

---

## §1 职责

- 给所有 `core.check_db` 中的 check 函数提供 **CLI 入口**
- 检查指定 `--trade-date` 的所有 kind 落盘情况(行数 / ts_code 数 / 时间范围)
- 检查游标(cursors)+ 单游标 + DB 文件列表 + 表列表 + 表信息
- 支持 pretty(人读)+ json(脚本/AI)双格式
- 单 kind 异常不影响其他 kind

## §2 设计要点

### 2.1 service 层薄

**不**重写业务逻辑,全部委托给 `core.check_db` 中已有函数:

| kind | 委托给 core.check_db 的函数 |
|---|---|
| snapshot | `check_snapshot(trade_date, year_month)` |
| orderbook | `check_orderbook(trade_date, year_month)` |
| minute | `check_minute(trade_date, year_month)` |
| auction | `check_auction(trade_date, year_month)` |
| zt | `check_zt(trade_date, year_month)` |
| limitperf | `check_limitperformance(trade_date, year_month)`(别名 limitperformance)|
| break | `check_break(trade_date, year_month)` |
| anomaly | `check_anomaly(trade_date, year_month)` |
| hot | `check_hot(trade_date, year_month)` |
| watchlist | `check_watchlist(trade_date, year_month)`(单独走 query_watchlist,**不**走 _check_kind)|
| cursor | `check_cursor(stream_key, year_month)` |
| cursors | `check_cursors(year_month)`(全部游标)|
| db_list | `check_db_list()` |
| table_list | `check_table_list(year_month)` |
| table_info | `check_table_info(year_month, table_name)` |
| overview | `check_overview(trade_date, year_month)` |
| **all** | 等价 `--kind snapshot,orderbook,...,hot,watchlist,cursors` |

### 2.2 trade_date 默认今天

- `--trade-date` 不传默认今天(YYYYMMDD)
- `--year-month` 不传从 trade_date 推导(YYYYMMDD → YYYYMM)
- 例外:`db_list` / `table_list` / `table_info` 不需要 trade_date(只要 year_month)

### 2.3 输出维度

每个 kind 返回的字段:

```json
{
  "exists": true,
  "row_count": 2948,
  "ts_code_count": 82,
  "earliest_data_ts": "...",   // ISO 格式
  "latest_data_ts": "...",     // ISO 格式
  "earliest_save_ts": "...",   // 落盘时间
  "latest_save_ts": "...",
  "columns": [...]              // 仅 table_info 特有
}
```

watchlist 特殊字段:
```json
{
  "exists": true,
  "row_count": 620,            // 一天允许多份,行数累加
  "ts_code_count": 124,
  "sources_count": {"latest_limit_lb": 30, "prev_lianban": 420, ...}
}
```

## §3 用法

```bash
# 默认(需 --trade-date,不传则今天)
python3 -m service.service_check_db --trade-date 20260915

# 单 kind
python3 -m service.service_check_db --kind zt --trade-date 20260915

# 多 kind
python3 -m service.service_check_db --kind snapshot,orderbook,minute --trade-date 20260915

# watchlist
python3 -m service.service_check_db --kind watchlist --trade-date 20260915 --format json

# 单游标
python3 -m service.service_check_db --kind cursor --stream-key online:auction:stream --trade-date 20260915

# 全部游标
python3 -m service.service_check_db --kind cursors --trade-date 20260915

# 表信息
python3 -m service.service_check_db --kind table_info --year-month 202609 --table-name watchlist_20260915

# DB 文件列表
python3 -m service.service_check_db --kind db_list

# 一键总览
python3 -m service.service_check_db --kind all --trade-date 20260915
```

## §4 参数详解

| 参数 | 默认 | 说明 |
|---|---|---|
| `--kind` | `all` | kind 列表(逗号分隔)/ `all` |
| `--trade-date` | 今天(YYYYMMDD) | 必填(除 db_list 外)— 查哪天 |
| `--year-month` | 从 trade-date 推(YYYYMM)| db_list/table_info 必填 |
| `--stream-key` | `""` | cursor 专用:`online:auction:stream` 等 |
| `--table-name` | `""` | table_info 专用:`watchlist_20260915` |
| `--format` | `pretty` | `pretty` / `json` |

## §5 输出示例(pretty)

```
=== SQLite 检查结果(2026-09-15 10:46:31, trade_date=20260915, ym=202609)===

--- ZT ---
    exists: True
    row_count: 2948
    ts_code_count: 82

--- WATCHLIST ---
    exists: True
    row_count: 620
    ts_code_count: 124
    sources_count: {'latest_limit_lb': 30, 'prev_lianban': 420, ...}

--- CURSORS ---
    count: 8
    cursors: (8 项)
      - {'stream_key': 'online:snapshot:stream', 'last_id': '...', 'exists': True}
      ...
```

## §6 设计原则

1. **service 层薄** — argparse + 调用 core.check_db,无业务逻辑
2. **错误隔离** — 单 kind 异常不影响其他 kind
3. **pretty 输出人读**;**json 输出供脚本/AI**
4. **exit code** — 脚本本身崩溃才非 0;业务问题靠 `--format json` 让 caller 自己解析
5. **保持命名一致** — `--kind` 跟 service_check_redis 风格统一
6. **别名规范化** — `--kind limitperformance` 自动规范化为 `limitperf`

## §7 不做的事

- **不**重写 core.check_db 任何函数
- **不**做 Cron 自动跑(用户没要,只做 CLI 工具)
- **不**做 web/dashboard(用户没要)
- **不**加 `--exit-non-zero`(业务问题用 wrapper 脚本做告警)

## §8 与 service_check_redis 的区别

| 维度 | service_check_redis | service_check_db |
|---|---|---|
| 数据源 | Redis online:* | SQLite online_data_YYYYMM.db |
| 必填参数 | 无(--kind 决定) | `--trade-date`(除 db_list)|
| check 函数数 | 13 | 14 |
| 检查维度 | key 存在 / TTL / size | 表存在 / row_count / 时间范围 |
| 与 watchlist 关系 | 三件套细分 + codes/sources | 1 天多份累加 + sources 分布 |

## §9 历史变更

- **2026-09-15 v6.7**:新建
