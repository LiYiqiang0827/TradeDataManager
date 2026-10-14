# core/watchlist_fetch.py 详细设计

> **watchlist 组装策略**:从 offlineDataManager db 拉昨日涨停 + 连板 → 拼成监控列表
> **行数**:689 行(`wc -l`,2026-09-15 校正)
> **数据源**:`~/TradingAgent/offlineDataManager/data/db_cn_kpl.db`(**双表** cn_kpl_list + cn_kpl_limit_performance)
> **版本**:v4 双数据源(2026-09-13)
| **v6.14 更新(2026-09-16)**:`WatchlistEntry.watchlist_timestamp` 类型注解 `str = ""` → `int = 0`(对齐其它 kind 时间戳统一为 int 毫秒);L587-603 `ts_iso = datetime.fromtimestamp(time.time()).isoformat(milliseconds)` → `ts_unix_ms = int(time.time() * 1000)`。`write_watchlist_payload` 写入 Redis STREAM 时字段值也是 int 毫秒字符串。

---

## §1 职责

`service_writeredis_watchlist` 调 `generate_watchlist(prev_window_days=10, min_level=2, use_fallback=True)`:

1. **决策数据源**(`fetch_kpl_ctrl_max_dates()`):
   - 从 `tbl_kpl_ctrl` 读 `cn_kpl_list` / `cn_kpl_limit_performance` 两表 `max_date`
   - 谁新用谁(`limit_performance` 优先,因为字段精确)
2. **拉最新日涨停股**:从最新表取最新一天的 `(ts_code, board_count)`
   - `>=2 板` → `priority=3`(最新连板股)
   - `1 板` → `priority=1`(最新首板)
3. **拉历史窗口连板**:从 `cn_kpl_list` 拉 `prev_window_days` 天的 `>=2 板`
   - 已存在于最新日:不重复入,但 `source` 标 `'latest_lb+prev'`
   - 仅历史出现:`priority=2`
4. 去重 → 按 `priority DESC → lu_time ASC` 排序 → 输出 `Watchlist`

## §2 调用链

```python
# service_writeredis_watchlist 内部:
from core.watchlist_fetch import generate_watchlist

wl = generate_watchlist(
    prev_window_days=10,
    min_level=2,
    use_fallback=True,
)  # -> Watchlist
```

**内部流程**:
```
generate_watchlist
  ├─ fetch_kpl_ctrl_max_dates()      # 读 tbl_kpl_ctrl 决策数据源
  ├─ (二选一或 fallback)
  │   ├─ _fetch_from_limit_performance(latest_date)
  │   │   └─ _connect_kpl() -> SELECT ts_code, board_count, lu_time FROM cn_kpl_limit_performance WHERE trade_date=?
  │   └─ _fetch_from_kpl_list(latest_date, prev_window_days)
  │       └─ _connect_kpl() -> SELECT ts_code, board_count, lu_time FROM cn_kpl_list WHERE trade_date IN (?, ?, ...)
  ├─ 拼 WatchlistEntry 列表
  └─ 返回 Watchlist(dataclass)
```

> ⚠️ doc 早期写"读 zt_pool 库 / 调用 read_db(...)"全错,真值是读 kpl.db 的 cn_kpl_list 或 cn_kpl_limit_performance 两表,**不再用单独的 zt_pool 表**。

## §3 输出格式

```python
@dataclass(frozen=True)
class WatchlistEntry:
    ts_code: str
    source: str                       # 'latest_limit_lb' / 'latest_limit' / 'prev_lianban' / 'latest_lb+prev' 等
    watchlist_timestamp: int = 0      # v6.14: int 毫秒时间戳(生成瞬间 wall clock);历史老数据可能是 str(ISO),需 `persist_watchlist` 兼容
    priority: int = 0                 # 0-3,v4 含义见下
    reason: str = "盘前最近涨停股"

@dataclass
class Watchlist:
    entries: list[WatchlistEntry]     # 已按 priority DESC → lu_time ASC 排序
    latest_date: str                  # 最新有数据的 trade_date
    prev_window: tuple[str, str]      # (start, end)
    latest_source: str                # 'limit_performance' / 'list'
    latest_lb_count: int              # 最新日连板股数
    latest_count: int                 # 最新日全部涨停数(含 1 板)
    prev_lb_count: int                # 历史窗口连板股数

    @property
    def ts_codes(self) -> list[str]:
        return [e.ts_code for e in self.entries]

    @property
    def count(self) -> int:
        return len(self.entries)

    def sources_map(self) -> dict[str, str]:
        return {e.ts_code: e.source for e in self.entries}
```

**priority 含义(v4 拍板)**:
| 值 | 含义 |
|---:|---|
| 3 | 最新日 + `>=2 板`(最新连板股) |
| 2 | 历史窗口(10 天)+ `>=2 板`(已连板股) |
| 1 | 最新日 + `1 板`(最新首板) |
| 0 | 占位(未用)|

## §4 与其他模块的关系

| 谁会用 | 用哪些 |
|---|---|
| **service_writeredis_watchlist** | 主调用方,用 `wl.ts_codes` 写 Redis SET,用 `wl.sources_map()` 写 sources HASH |
| **service_savedata_watchlist** | 用 `wl.entries` 落盘 |

## §5 历史变更

- **2026-09-11 v3 重构**:从同花顺 + KPL 双接口改单一 offlineDataManager db(T+0,比 MyATM T+1 及时)
- **2026-09-12 v3**:精简字段(删 `data_timestamp` / `save_timestamp`,加 `watchlist_timestamp`)
- **2026-09-13 v4**:**双数据源** — `tbl_kpl_ctrl` 指向 `cn_kpl_list` + `cn_kpl_limit_performance`,谁新用谁;`generate_watchlist` 签名变更(去掉 `trade_date`,加 `prev_window_days/min_level/use_fallback` 三参);返回类型从 `dict` 改为 `Watchlist` dataclass
- **2026-09-16 v6.14**:`watchlist_timestamp` 类型从 `str`(ISO)→ `int`(毫秒);L587-603 `ts_iso` → `ts_unix_ms`;落盘兼容 3 种格式(int 毫秒 / float 秒 / str ISO)在 `core/persist_client.persist_watchlist` 统一处理
