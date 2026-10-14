# service/savedata_loop.py 详细设计

> **所有 savedata_* 的基类**:`SavedataDaemon` + `main()` 通用入口
> **行数**:116 行(`wc -l`,2026-09-15 校正)
> **受众**:AI 加新 kind 的 savedata / 改落盘节奏时

---

## §1 设计要点

- **10 个 savedata daemon**(auction / snapshot / orderbook / minute / zt / break / anomaly / hot / limitperformance / **watchlist**)都继承 `SavedataDaemon`
- 9 个走基类默认 `_do_persist` → `persist_kind` 路由;1 个 watchlist override `_do_persist` 直连 SQLite(`replace_watchlist` 整体替换)
- scheduler 在 morning_savedata / afternoon_savedata 阶段统一 spawn,15:35 **closed 阶段** SIGTERM(`SAVEDATA_PHASES` 末段是 `closed`,无 `post` 阶段)
- 每个子类只定义 `KIND` + `DEFAULT_INTERVAL`,基类搞定一切
- **15:35 切到 closed 阶段由 scheduler kill,service 不判断时间**(单职责)

## §2 子类示例

```python
from service.savedata_loop import SavedataDaemon
from service.savedata_loop import main as base_main

class SavedataSnapshot(SavedataDaemon):
    KIND = "snapshot"
    DEFAULT_INTERVAL = 900.0   # 15 分钟

if __name__ == "__main__":
    import sys
    sys.exit(base_main(SavedataSnapshot))
```

实际 10 个子类(每个 savedata_*.py 都这么写):
- `SavedataAuction` / `SavedataSnapshot` / `SavedataOrderbook`
- `SavedataMinute`(简单继承,见下注)
- `SavedataZt` / `SavedataBreak` / `SavedataAnomaly` / `SavedataHot` / `SavedataLimitperformance`
- `SavedataWatchlist`(特殊,override `_do_persist` 直接调 `replace_watchlist`,**不**走 `persist_kind`)

## §3 SavedataDaemon 类

### 3.1 类属性

| 属性 | 类型 | 说明 |
|---|---|---|
| `KIND` | str(子类必须)| kind 名("snapshot" / "zt" 等)|
| `DEFAULT_INTERVAL` | float(子类必须)| 落盘间隔秒(默认 900s = 15min)|
| `TRADE_DATE` | str | None = 今天;固定值 = 强制某日 |

### 3.2 方法

| 方法 | 真实签名(`savedata_loop.py`)| 用途 |
|---|---|---|
| `__init__` | `(self, log: logging.Logger | None = None)` | 初始化 logger + `self.r = OnlineRedis()`(line 55-59) |
| `_get_trade_date` | `(self) -> str` | 内部:取 trade_date(优先 `TRADE_DATE` 属性,否则今天 YYYYMMDD)|
| `_do_persist` | `(self) -> int` | 内部:`td = self._get_trade_date(); return persist_kind(self.r, kind=self.KIND, trade_date=td)`(line 67-70)|
| `run` | `(self) -> int` | **daemon 模式**:**第一轮立即跑**(不等 interval)→ 进 `while True: time.sleep(DEFAULT_INTERVAL) → _do_persist()`(line 72-89)|
| `run_once` | `(self) -> int` | **--once 模式**:只跑一次,返回 0/1(line 90-99)|

> ⚠️ **2026-09-15 校正**:原 doc 写 "run line 75-86 / run_once 89-98",实际 `run` 在 **L72-89**、`run_once` 在 **L90-99**(代码多了 3 行的导入)。

**`run()` 实际流程(line 72-89)**:
```python
def run(self) -> int:
    self.log.info(f"savedata[{self.KIND}] daemon 启动")
    # 第一轮立即跑(避免空转 N 秒)
    try:
        n = self._do_persist()
    except Exception as e:
        self.log.error(f"初始落盘失败: {e}", exc_info=True)
    while True:
        time.sleep(self.DEFAULT_INTERVAL)
        try:
            n = self._do_persist()
        except Exception as e:
            self.log.error(f"落盘失败: {e}", exc_info=True)
```

## §4 main(cls, extra_args=None) 通用入口

```python
def main(cls, extra_args=None) -> int:
    """解析 --once/--interval 后实例化子类运行"""
```

### 4.1 argparse 参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--interval N` | `cls.DEFAULT_INTERVAL` | 落盘间隔秒 |
| `--once` | False | 只跑一次,跑完退出 |

### 4.2 行为

- `args.once == True` → `daemon.run_once()` → 立即返回 rc
- 否则 → `daemon.run()` → daemon 模式(第一轮立即跑 → while True: sleep interval → _do_persist)

## §5 特殊子类

### 5.1 `SavedataMinute`(看似简单,实则路由到独立 helper)

**`SavedataMinute` 看似简单** — 它就是简单继承 `SavedataDaemon`(只定义 `KIND = "minute"` + `DEFAULT_INTERVAL = 900.0`),基类自动调 `persist_kind(r, *, kind="minute", trade_date=...)`。

但 **`persist_kind` 内部路由表(line 632-633)把 `kind="minute"` 指向独立 helper**:
```python
def persist_kind(redis_client, *, kind, trade_date):
    if kind in ("auction", "snapshot", "orderbook"):
        return persist_stream_kind(redis_client, kind=kind, trade_date=trade_date)
    elif kind == "minute":
        return persist_minute(redis_client, trade_date=trade_date)  # ← 独立函数!
    elif kind == "zt":
        return persist_zt(redis_client, trade_date=trade_date)
    ...
```

**`persist_minute()` 函数真值**:`core/persist_client.py:209-275`,不走 STREAM,走 ZSET `online:minute:{ts_code}`(`get_minute_bars` → ZRANGE 0-239)。Round 15 体检曾把"有独立 persist_minute"误改成"没有",**实际是有的**。

### 5.2 `SavedataWatchlist`(真的特殊)

watchlist 是"整体替换"语义(`replace_watchlist` 清空后 INSERT 整批),不是"逐条 INSERT",所以 **override `_do_persist`**,不走基类 `persist_kind` 路由。

**真实代码**(`service_savedata_watchlist.py:54-95`):
```python
from core.sqlite_client import connect, replace_watchlist   # ← 在 core.sqlite_client,不是 persist_client
from core.watchlist_fetch import generate_watchlist

class SavedataWatchlist(SavedataDaemon):
    KIND = "watchlist"
    DEFAULT_INTERVAL = 1800.0   # 30 分钟

    def __init__(self, log=None):
        super().__init__(log=log)
        self.log = log or setup_logger("service_savedata_watchlist")  # 独立 logger,避免和 writeredis 冲突
        self.round_idx = 0

    def _do_persist(self) -> int:
        td = self._get_trade_date()                                                # 1. 取 trade_date
        wl = generate_watchlist(                                                    # 2. 调生成器(v4 双数据源真值)
            prev_window_days=10, min_level=2, use_fallback=True,
        )
        ym = td[:6]
        conn = connect(ym)                                                          # 3. 主动 connect(子类层唯一)
        try:
            rows = []
            for entry in wl.entries:                                                # 4. 转行(WatchlistEntry → dict)
                rows.append({
                    "ts_code": entry.ts_code,
                    "source": entry.source,
                    "priority": entry.priority,
                    "reason": entry.reason,
                    "watchlist_timestamp": entry.watchlist_timestamp,
                })
            n = replace_watchlist(conn, trade_date=td, rows=rows)                   # 5. 整体替换(真签名)
            self.round_idx += 1
            self.r.set_meta("watchlist_last_persist_ts", datetime.now().isoformat()) # 6. 元信息
            self.r.set_meta("watchlist_persist_round", self.round_idx)
            return n
        except Exception as e:
            self.r.set_meta("watchlist_savedata_last_error", str(e))
            return 0
        finally:
            conn.close()
```

**注意点**:
- `replace_watchlist` 在 `core/sqlite_client`(**不是** `core/persist_client`)
- 真实签名 `replace_watchlist(conn, *, trade_date, rows)`,**没有** `r / codes` 参数
- `self._get_codes()` 方法**不存在**(doc 旧版误引用,基类只有 `_get_trade_date`)
- watchlist savedata **不**走 `persist_kind`,因为 `persist_kind` 路由表里**没有** `kind == "watchlist"` 分支(line 644-646 raise ValueError)

## §6 子类 DEFAULT_INTERVAL 速查

| savedata | DEFAULT_INTERVAL | 说明 |
|---|---:|---|
| watchlist | 1800s | 自选股 30min,整体替换 |
| auction | 99999s | 仅 --once 触发(9:29/15:10 兜底)|
| snapshot | 900s | 15 分钟 |
| orderbook | 900s | 15 分钟 |
| minute | 900s | 15 分钟 |
| zt / break / anomaly / hot | 1800s | MyATM 低频,30min 兜底 |
| limitperformance | 900s | 15 分钟 |

## §7 与其他模块的关系

| 谁会用 | 用哪些方法 |
|---|---|
| **10 个 savedata_*.py** | 继承 `SavedataDaemon` + 调 `main(SavedataXxx)` |
| **scheduler_onlineData.py:289** | `spawn_service(name, args)` → 内部 `subprocess.Popen(["python3", "-m", "service.service_savedata_xxx"])` |
| **scheduler_once.py** | 同上,加 `--once` 参数 |

## §8 历史变更

- **2026-09-11**:基类重构,从每个 savedata 单独写循环改为继承 `SavedataDaemon`
- **2026-09-12 v3**:加 `TRADE_DATE` 支持(回溯某天测试用)
- **2026-09-15 v1.x**:第 16 轮体检修 5 处(`run` 流程补全"第一轮立即跑"+ §5 SavedataMinute 改正"有独立 persist_minute"+ §5 SavedataWatchlist 示例 5 处改正 + §7 subprocess 描述 + 措辞调整)
