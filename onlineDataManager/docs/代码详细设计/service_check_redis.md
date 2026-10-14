# service/service_check_redis.py 详细设计

> **Redis 状态检查 CLI 工具**
> **版本**:v6.7 新增(2026-09-15)
> **职责**:统一入口 — 给日常 debug / Cron / AI Agent 提供"快速检查 Redis online: 数据状态"

---

## §1 职责

- 给所有 `core.check_redis` 中的 check 函数提供 **CLI 入口**(无需写 `python3 -c "..."`)
- 支持单 kind / 多 kind / 全 kind / overview / all_keys SCAN
- 支持 snapshot / orderbook / minute / auction 的单股细化
- 支持 pretty(人读)+ json(脚本/AI)双格式
- 单 kind 异常不影响其他 kind

## §2 设计要点

### 2.1 service 层薄

**不**重写业务逻辑,全部委托给 `core.check_redis` 中已有函数:

| kind | 委托给 core.check_redis 的函数 |
|---|---|
| watchlist | `check_watchlist()` + `check_watchlist_three()`(v6.7 新增)|
| snapshot | `check_snapshot()` 或 `check_snapshot_for_stock(ts_code)` |
| orderbook | `check_orderbook()` 或 `check_orderbook_for_stock(ts_code)` |
| minute | `check_minute()` 或 `check_minute_for_stock(ts_code)` |
| auction | `check_auction()` 或 `check_auction_for_stock(ts_code)` |
| zt | `check_zt()` |
| limitperf | `check_limitperformance()`(别名 limitperformance)|
| break | `check_break()` |
| anomaly | `check_anomaly()` |
| hot | `check_hot()` |
| meta | `check_meta()` |
| cursor | `check_cursor(stream_key)` |
| all_keys | `check_all_keys(pattern)` |
| overview | `check_overview()` |
| **all** | 等价 `--kind watchlist,snapshot,orderbook,minute,auction,zt,break,anomaly,hot,limitperf,meta` |

### 2.2 watchlist 三件套细分

`watchlist` kind 同时返回基础 + 三件套(stream/timeline/archive):
```json
{
  "watchlist": {
    "**base**": { "count": 124, "sources_count": 124, ... },
    "**three**": {
      "stream_length": 620, "stream_ttl": 42268,
      "timeline_size": 5, "timeline_ttl": 42268,
      "archive_count": 5,
      "latest_archive_key": "online:watchlist:archive:1789439363.000",
      "latest_archive_size": 124,
      "codes_count": 124, "source_dist": {...}
    }
  }
}
```

## §3 用法

```bash
# 一键总览(11 个 kind + overview 等价)
python3 -m service.service_check_redis

# 单 kind
python3 -m service.service_check_redis --kind watchlist

# 多 kind(逗号分隔)
python3 -m service.service_check_redis --kind watchlist,zt,break,anomaly,hot

# 单股细化(仅 snapshot/orderbook/minute/auction)
python3 -m service.service_check_redis --kind snapshot --ts-code 600519.SH

# cursor
python3 -m service.service_check_redis --kind cursor --stream-key online:auction:stream

# SCAN 所有 key(支持 pattern)
python3 -m service.service_check_redis --kind all_keys --pattern 'online:auction:*'

# JSON 输出(供脚本/AI)
python3 -m service.service_check_redis --kind all --format json

# 单 kind 三件套细分
python3 -m service.service_check_redis --kind watchlist_three
```

## §4 参数详解

| 参数 | 默认 | 说明 |
|---|---|---|
| `--kind` | `all` | kind 列表(逗号分隔)/ `all` / `overview` / `all_keys` |
| `--ts-code` | `""` | 单股细化(只 snapshot/orderbook/minute/auction 支持)|
| `--stream-key` | `""` | cursor 专用:`online:auction:stream` 等 |
| `--pattern` | `online:*` | all_keys 专用:SCAN pattern |
| `--format` | `pretty` | `pretty` / `json` |

## §5 输出示例(pretty)

```
=== Redis 检查结果(2026-09-15 10:44:55)===

--- WATCHLIST ---
  [基础]
    count: 124
    sources_count: 124
    ...
  [三件套(stream/timeline/archive)]
    stream_length: 620
    stream_ttl: 42268
    ...

--- ZT ---
    timeline_size: 20
    latest_archive_key: online:zt:archive:1789440275.990465
    latest_archive_size: 29
    stream_length: 3496
```

## §6 设计原则

1. **service 层薄** — argparse + 调用 core.check_redis,无业务逻辑
2. **错误隔离** — 单 kind 异常不影响其他 kind
3. **pretty 输出人读**;**json 输出供脚本/AI**
4. **exit code** — 脚本本身崩溃才非 0;业务问题靠 `--format json` 让 caller 自己解析
5. **保持命名一致** — `--kind` 跟 service_check_db 风格统一
6. **别名规范化** — `--kind limitperformance` 自动规范化为 `limitperf`

## §7 不做的事

- **不**重写 core.check_redis 任何函数
- **不**做 Cron 自动跑(用户没要,只做 CLI 工具)
- **不**做 web/dashboard(用户没要)
- **不**加 `--exit-non-zero`(业务问题用 wrapper 脚本做告警)

## §8 历史变更

- **2026-09-15 v6.7**:新建
- **2026-09-15 v6.7 修真值**:`core/check_redis.py` 的 `check_cursor` 多传了 1 个参数 `"$"` 给 `get_meta`(只接 1 个参数),顺手修
- **2026-09-15 v6.7 修真值**:`core/check_redis.py` 加 `check_watchlist_three()` 配合 watchlist 三件套
