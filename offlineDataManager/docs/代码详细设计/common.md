# 代码详细设计/common.md

`scripts/service/common.py` — service 通用工具。**共 7 个函数**(2026-09-17 大改):

## 职责

### 1. 现有 date_range 工具(2026-09-15 起)
- **`resolve_date_range(conn, ctrl_key, args)`** — 根据 CLI args + ctrl 断点决定实际拉取日期范围

### 2. watchlist-driven 工具(2026-09-17 新增)
3 个 service 共用,处理 watchlist / ts-codes / 指数 3 种参数模式:
- **`load_watchlist_pairs(watchlist_csv)`** — 从 csv 读 (ts_code, trade_date) 对
- **`build_ts_codes_pairs(args)`** — 从 --ts-codes + 日期参数笛卡尔积算对
- **`build_index_pairs(args)`** — 指数 service 专用,默认 5 指数 × 日期范围
- **`resolve_market_pairs(args)`** — 3 种模式的统一入口
- **`run_market_loop(...)`** — 循环调 down.update_*(...) 共用循环
- **`add_market_args(parser)`** — 给 service argparse 加共用参数

## 入口

被 9 个 service import:
- `service_tradecal` / `service_daily` / `service_adj_factor` / `service_kpl_list` / `service_kpl_concept_cons` / `service_kpl_limit_performance` — 用 `resolve_date_range`
- **`service_intraday`**(2026-09-17 新) — 用 `resolve_market_pairs` + `run_market_loop` + `add_market_args`
- **`service_ticks`**(2026-09-17 新) — 同上
- **`service_intraday_index`**(2026-09-17 新) — 用 `build_index_pairs` + `run_market_loop` + `add_market_args`

---

## 关键函数

### 1. `resolve_date_range(conn, ctrl_key, args, default_start="20150101") -> tuple`

```python
def resolve_date_range(conn, ctrl_key: str, args, default_start: str = "20150101") -> tuple:
    """根据 args + ctrl 决定实际拉取日期范围
    
    优先级:
    1. --trade-date(单日,start_date = end_date = trade_date)
    2. --start-date + --end-date(日期段)
    3. 增量(默认 start_date=default_start, end_date=今天)
    
    Args:
        conn: SQLite 连接(用来读 tbl_ctrl)
        ctrl_key: tbl_ctrl 里对应的 key
        args: argparse 参数,需要含 trade_date / start_date / end_date
        default_start: 默认 start_date(YYYYMMDD)
    
    Returns:
        (start_date, end_date, desc) 三元组
    """
```

**决策树**:
```
1. args.trade_date?
   → (td, td, "单日 {td}")
   
2. args.start_date / args.start / args.end_date / args.end
   → sd, ed
   如果 sd < ctrl.max_date,自动调整为 ctrl.max_date
   → (sd, ed, "日期段 {sd} ~ {ed}")
   
3. 默认增量
   → sd = default_start, ed = today
```

**关键行为**:
- **从 `args` 取参用 `getattr`**,支持 `start` / `start_date` 两种命名(week / month 用 `start`)
- **`get_ctrl(conn, key)` 读断点**,在 `tbl_basic_ctrl` 或 `tbl_news_ctrl`(取决于传什么 conn)
- **sd < last_ctrl 时自动调整**(log warning),不报错

---

### 2. `load_watchlist_pairs(watchlist_csv: str) -> list`

```python
def load_watchlist_pairs(watchlist_csv: str) -> list:
    """从 watchlist csv 读 (ts_code, trade_date) 对列表
    
    csv 必须有 ts_code 和 trade_date 两列(YYYYMMDD 或 YYYY-MM-DD 都接受,
    内部统一转 YYYY-MM-DD)
    
    Args:
        watchlist_csv: csv 文件绝对路径或相对路径
                       (找不到时兜底找 ~/TradingAgent/policyStudy/.../watchlist/)
    
    Returns:
        [(ts_code, trade_date), ...] 列表,dash 格式日期
    
    Raises:
        FileNotFoundError, ValueError(缺列)
    """
```

**关键行为**:
- 路径解析:绝对路径 → 用;相对路径 → 找 `<PROJECT_ROOT>/scripts/`;找不到再找 policyStudy watchlist
- 重复 `(ts_code, trade_date)` 对自动 drop_duplicates

---

### 3. `build_ts_codes_pairs(args) -> list`

```python
def build_ts_codes_pairs(args) -> list:
    """从 --ts-codes + 日期参数 笛卡尔积算 (ts_code, trade_date) 对
    
    日期三种模式(按优先级):
      1. --trade-date 单日
      2. --start-date / --end-date 日期段(过交易日历过滤周末/节假日)
      3. 无日期参数(空,需业务层校验)
    
    Returns:
        [(ts_code, trade_date), ...] 列表,dash 格式
    """
```

---

### 4. `build_index_pairs(args) -> list`

```python
def build_index_pairs(args) -> list:
    """指数 service 专用:默认 INDEX_CODES_HERE × 日期范围
    
    日期三种模式(跟 ts-codes 同):
      1. --trade-date 单日 × INDEX_CODES_HERE
      2. --start-date / --end-date 区间 × INDEX_CODES_HERE
      3. 空(返回空,业务层校验)
    """
```

5 指数固定来自 `offline_downloader.INDEX_CODES_HERE`。

---

### 5. `resolve_market_pairs(args) -> tuple`

```python
def resolve_market_pairs(args) -> tuple:
    """watchlist-driven service 共用入口:解析 args → 待下载 (ts_code, trade_date) 对
    
    优先级:
      1. --watchlist csv:读 (ts_code, trade_date) 对
      2. --ts-codes + 日期参数:笛卡尔积
      3. 都没有:返回 ([], 'no_pairs')
    
    Returns:
        (pairs: list, source: str)
        source = 'watchlist' / 'ts_codes' / 'no_pairs'
    """
```

---

### 6. `run_market_loop(down, pairs, *, update_method, client, rate, force, batch_size, logger) -> dict`

```python
def run_market_loop(
    down,
    pairs: list,
    *,
    update_method: str,  # 'update_minute' / 'update_ticks' / 'update_minute_index'
    client,
    rate: float = 0.15,
    force: bool = False,
    batch_size: int = 100,
    logger=None,
) -> dict:
    """循环调 down.update_*(...) 拉取每对 (ts_code, trade_date)
    
    流程:
      - 每 batch_size 对打印一次进度
      - 异常隔离:某对失败不中断,继续下一对
      - 已下载(inserted == 0)归为 skipped,新写入归为 inserted
    
    Returns:
        {
            "total": 总对数,
            "inserted": 新插入对数,
            "skipped": 跳过对数(已下载或拉取空),
            "failed": 失败对数(异常),
            "elapsed": 总耗时秒,
            "inserted_rows": 总新插入行数,
        }
    """
```

---

### 7. `add_market_args(parser, default_rate=0.15)`

```python
def add_market_args(parser: argparse.ArgumentParser, default_rate: float = 0.15) -> None:
    """给 service argparse 加上 watchlist-driven 共用参数
    
    加的 7 个参数:
      --watchlist   csv 路径(可选)
      --ts-codes    逗号分隔的 ts_code 列表(可选)
      --trade-date  单日 YYYYMMDD(可选)
      --start-date  日期段起始 YYYY-MM-DD(可选)
      --end-date    日期段结束 YYYY-MM-DD(可选)
      --rate        限速秒数(默认 0.15)
      --force       强制重拉
    """
```

---

## 数据流(watchlist-driven 路径)

```
service_intraday.main()  # 或 service_ticks / service_intraday_index
  │
  ├─ add_market_args(parser)          # 注册 7 个参数
  ├─ resolve_market_pairs(args)        # 算待下载对
  ├─ ensure_schema(...)               # 幂等建表
  └─ run_market_loop(
       down, pairs,
       update_method="update_minute",  # 或 "update_ticks" / "update_minute_index"
       client=TdxClient(),
       rate=args.rate,
       force=args.force,
     )
     │
     └─ for (ts, td) in pairs:
          method = getattr(down, update_method)
          inserted = method(ts, td, client=client, rate=rate, force=force)
          # 内部:has_data → 拉数据 → upsert_rows → mark → 限速
```

---

## 修改指南

### 加新的 `--xxx` 参数
- 在 `add_market_args` 里加一行 `parser.add_argument(...)`
- 在 `resolve_market_pairs` / `build_*_pairs` 里 `getattr(args, "xxx", None)` 取参
- 想让其他 service 复用就保持纯函数风格

### 加新 `run_market_loop` 的统计字段
- 在 stats dict 里加 key
- 在 return 之前给 stats[key] 赋值

### 加新 update_method
- 在 `run_market_loop` 已经支持任意 `update_*` 方法(用 `getattr(down, update_method)` 拿)
- service 里只改 `update_method="..."` 参数即可

## 注意事项

- **不要在 `resolve_date_range` / `resolve_market_pairs` 里直接调 `update_*`**,只返回结果
- **`get_ctrl` 是 `offline_db_client` 的内部 helper**,在文件顶部 lazy import 避免循环依赖
- **`build_index_pairs` 必须从 `offline_downloader` import `INDEX_CODES_HERE`**,所以这个函数依赖 `coreClient/tdx_config` 同级的常量
- **`run_market_loop` 的 batch_size 默认 100**:每 100 对打印一次进度,可调
- **`add_market_args` 必须放在 `parser.parse_args()` 之前**

## 数据流(date_range 路径)

```
service_daily.main()
  ↓
sd, ed, desc = resolve_date_range(down.conn_basic, "cn_daily", args)
  ↓
内部:
  - today = datetime.now().strftime("%Y%m%d")
  - if args.trade_date: return (td, td, desc)
  - if args.start_date/end_date:
    sd = args.start_date or args.start or default_start
    ed = args.end_date or args.end or today
    last = get_ctrl(conn, "cn_daily")
    if last and sd < last: sd = last (log warning)
  - else: sd = default_start, ed = today
  ↓
return (sd, ed, desc)
  ↓
n = down.update_daily(start_date=sd, end_date=ed)
```

## 修改记录

- 2026-09-15:初版,只 1 个 `resolve_date_range`
- **2026-09-17:大改**,加 6 个 watchlist-driven 工具函数,为 3 个新 service(service_intraday / service_ticks / service_intraday_index)复用;`resolve_date_range` 接口不变
