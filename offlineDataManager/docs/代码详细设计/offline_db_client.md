# 代码详细设计/offline_db_client.md

`scripts/core/offline_db_client.py` — 核心 I/O 层。

**3 个 DB 连接管理 + 通用 upsert/replace + 28 个 `get_*` 只读接口**(2026-09-15 加 `get_suspend` / `get_top_list` / `get_top_inst` / `get_block_trade` / `get_ggt_daily` / `get_hsgt_top10` / `get_limit_list` / `get_moneyflow` / `get_margin` / `get_margin_detail` / `get_cyq_perf` / `get_daily_basic` / `get_index_basic` / `get_index_daily`)。

## 职责

1. **多 DB 管理**:`init_db` / `get_conn`(`DB_PATHS` 字典 + `SCHEMA_SQLS` 字典映射)
2. **通用 upsert**:`upsert_df(conn, df, table, key_cols)`
3. **覆盖更新(事务化)**:`replace_table` 单表覆盖；`replace_tables` 多表原子覆盖
4. **基础工具**:`snap_ts()` / `_norm_date_yyyymmdd` / `_norm_datetime_string` / `_norm_to_list`
5. **ctrl 断点 helper**:`get_ctrl` / `update_ctrl` / `_get_news_ctrl_value` / `_update_news_ctrl_value`
6. **状态查询**:`show_status` / `_show_status_one`(遍历 3 DB 各表)
7. **28 个 `get_*` 只读接口**

## 入口

- **被 service / scheduler / test / onlineDataManager import** 作为 28 个 `get_*` 接口的来源
- **`CNDataDown` 也 import 内部 helper**(`init_db` / `get_conn` / `snap_ts` / `upsert_df` / `replace_table` / `get_ctrl` / `update_ctrl` / `get_table_min_max`)

## 关键模块结构

### 1. 多 DB 管理(行 31-71)

```python
DB_PATHS = {"basic": ..., "kpl": ..., "news": ...}
SCHEMA_SQLS = {"basic": ..., "kpl": ..., "news": ...}

def snap_ts() -> str: ...              # ISO 时间戳(写入字段用)
def init_db(db_name, verbose=True) -> Connection: ...
def get_conn(db_name) -> Connection: ...  # 不建表
```

`init_db` 行为:
- 取 `DB_PATHS[db_name]` 和 `SCHEMA_SQLS[db_name]`
- 调 `conn.executescript(schema_sql)`(幂等建表)
- 启用 WAL 模式(`PRAGMA journal_mode=WAL`)
- verbose=True 时 `logger.info` 记录

### 2. 通用 upsert(行 77-111)

```python
def upsert_df(conn, df, table, key_cols) -> int:
    """INSERT OR IGNORE,主键冲突跳过"""
```

行为:
- 从 `PRAGMA table_info` 取表实际列
- 过滤 `df` 中不在表里的列(避免列名漂移报错)
- `INSERT OR IGNORE INTO table (col1, col2, ...) VALUES (?, ?, ...)`
- 对每行做类型清洗:None → "";有 `isoformat()` → ISO 字符串;其他原样
- 调 `executemany` + `conn.commit()`
- 返回 rowcount(实际新插入的行数)

### 3. 覆盖更新(行 117-159,2026-09-15 修复:加事务)

```python
def replace_table(conn, df, table) -> int:
    """DELETE 全表 → 重新 INSERT(整体包在 BEGIN/COMMIT 事务里)"""
```

行为:
- 取表列 + 过滤 df
- `BEGIN` → `DELETE FROM table` → `executemany INSERT` → `COMMIT`
- 异常时 `ROLLBACK` + `raise`(保留原表)
- 用于周 K / 月 K 这种"重新计算整个数据集"的场景

**重要**:`replace_table` 2026-09-15 加了事务化。中途崩了表不会被清空。

### 4. ctrl 断点(行 160-206)

```python
def get_ctrl(conn, key) -> Optional[str]:
    """读 db_cn_basic.db:tbl_basic_ctrl 的 max_date"""

def update_ctrl(conn, key, max_date):
    """INSERT OR REPLACE 推进断点，只前进不后退"""

def _get_news_ctrl_value(conn, src) -> Optional[str]:
    """读 db_cn_news.db:tbl_news_ctrl 的 max_date"""

def _update_news_ctrl_value(conn, src, max_date):
    """推进 news 源断点"""
```

**两个 ctrl 表对应两个 helper 对**:
- `get_ctrl` / `update_ctrl` 配 `tbl_basic_ctrl`(在 basic DB)
- `_get_news_ctrl_value` / `_update_news_ctrl_value` 配 `tbl_news_ctrl`(在 news DB)

**kpl 表的断点(2026-09-15 修复版)**:`tbl_kpl_ctrl` 已废弃,kpl 表的断点也走 `get_ctrl` / `update_ctrl` 写到 `tbl_basic_ctrl`(key 前缀 `cn_kpl_*`)。

### 5. 状态查询(行 212-251)

```python
def show_status(db_path=None): ...  # 默认查 3 DB,db_path 不为空时只查 1 个

def _show_status_one(db_path, db_name=None): ...  # 单 DB
```

`show_status` 打印每个表的行数 + 最小日期(DATE_COLS 顺序:trade_date / cal_date / datetime)。

### 6. 34 个 get_* 接口(行 325-2037)

| 函数 | 表 | 默认 | 主要参数 |
|---|---|---|---|
| `get_day` | `tbl_cn_day` | `qfq=True` | ts_code/ts_codes, start_date, end_date, trade_date, qfq, columns, conn |
| `get_week` | `tbl_cn_week` / `tbl_cn_week_origin` | `qfq=True` | ts_code/ts_codes,日期,qfq,columns,conn |
| `get_month` | `tbl_cn_month` / `tbl_cn_month_origin` | `qfq=True` | 同上 |
| `get_tradecal` | `tbl_cn_tradecal` | `market='SSE'`, `is_open=True` | start_date, end_date, trade_date, day_num, market, is_open, columns, conn |
| `get_basic` | `tbl_cn_basic` | 全 None | exchange, market, list_status(白名单校验), columns, conn |
| `get_adj_factor` | `tbl_cn_adj_factor` | — | ts_code/ts_codes, start_date, end_date, trade_date, columns, conn |
| `get_ctrl_basic` | `tbl_basic_ctrl` | key=None=全部 | key(单/列表), conn |
| `get_kpl_list` | `tbl_cn_kpl_list` | — | start_date, end_date, trade_date, ts_codes, themes(LIKE), lu_descs(LIKE), **tags(精确 IN,'涨停'/'炸板'/两者都有,2026-09-15 加)**, status(关键字展开), columns, conn(**数据来源 tushare**) |
| `get_kpl_concept_cons` | `tbl_cn_kpl_concept_cons` | — | 同上 + themes(name 精确 IN), descs(LIKE), min_hot_num |
| `get_kpl_limit_performance` | `tbl_cn_kpl_limit_performance` | — | 同上 + board_counts(IN), min_amplitude, only_broken(**数据来源同花顺 KPL**) |
| `get_news` | `tbl_news` | `limit=5000`, `offset=0` | start_date/end_date, trade_date, start_datetime/end_datetime, title, content, src, limit, offset, columns, conn |
| `get_major_news` | `tbl_major_news` | 同 news | 同 news |
| `get_cctv_news` | `tbl_cctv_news` | — | start_date, end_date, trade_date, content, columns, conn(datetime 是 YYYYMMDD 无时间) |
| `get_stk_limit` | `tbl_cn_stk_limit` | — | ts_code/ts_codes, start_date, end_date, trade_date, columns, conn(涨跌停价 2026-09-15 新增) |
| `get_suspend` | `tbl_cn_suspend` | — | ts_code/ts_codes, start_date, end_date, trade_date, suspend_type, columns, conn(停复牌 2026-09-15 新增) |
| `get_top_list` | `tbl_cn_top_list` | — | ts_code/ts_codes, start_date, end_date, trade_date, columns, conn(龙虎榜每日 2026-09-15 新增,kpl DB) |
| `get_top_inst` | `tbl_cn_top_inst` | — | 同上 + exalter/side(龙虎榜机构 2026-09-15 新增,kpl DB) |
| `get_block_trade` | `tbl_cn_block_trade` | — | ts_code, start_date, end_date, trade_date, columns, conn(大宗 2026-09-15 新增,kpl DB) |
| `get_ggt_daily` | `tbl_cn_ggt_daily` | — | start_date, end_date, trade_date, columns, conn(港股通日 2026-09-15 新增,kpl DB) |
| `get_hsgt_top10` | `tbl_cn_hsgt_top10` | — | ts_code, start_date, end_date, trade_date, market_type(1=沪/3=深), columns, conn(沪深股通 2026-09-15 新增,kpl DB) |
| `get_limit_list` | `tbl_cn_limit_list` | — | ts_code, start_date, end_date, trade_date, limit(U/D/Z), columns, conn(涨跌停列表 2026-09-15 新增,kpl DB) |
| `get_moneyflow` | `tbl_cn_moneyflow` | — | ts_code/ts_codes, start_date, end_date, trade_date, columns, conn(资金流向 2026-09-15 新增,basic DB) |
| `get_margin` | `tbl_cn_margin` | — | exchange_id, start_date, end_date, trade_date, columns, conn(融资融券汇总 2026-09-15 新增,basic DB) |
| `get_margin_detail` | `tbl_cn_margin_detail` | — | ts_code/ts_codes, start_date, end_date, trade_date, columns, conn(融资融券明细 2026-09-15 新增,basic DB) |
| `get_cyq_perf` | `tbl_cn_cyq_perf` | — | ts_code/ts_codes, start_date, end_date, trade_date, min_winner_rate, max_winner_rate, columns, conn(筹码及胜率 2026-09-15 新增,basic DB) |
| `get_daily_basic` | `tbl_cn_daily_basic` | — | ts_code/ts_codes, start_date, end_date, trade_date, columns, conn(每日指标 2026-09-15 新增,basic DB) |
| `get_index_basic` | `tbl_cn_index_basic` | — | ts_code/ts_codes, market, publisher, columns, conn(指数基本信息 2026-09-15 新增,index DB) |
| `get_index_daily` | `tbl_cn_index_daily` | — | ts_code/ts_codes, start_date, end_date, trade_date, columns, conn(12 只指数日线行情 2026-09-15 新增,index DB) |
| `get_news_ctrl` | `tbl_news_ctrl` | src=None=全部 | src(单/列表), conn |
| ~~`get_kpl_ctrl`~~ | ~~`tbl_kpl_ctrl`~~ | — | **2026-09-15 已删除,改用 get_ctrl_basic(key='cn_kpl_*')** |

详细参数说明见每个函数的 docstring(都超过 30 行)。

### 7. 辅助函数(行 263-322)

- `_norm_date_yyyymmdd(d)`:把 `str` / `datetime` / `date` 归一到 `YYYYMMDD`
- `_norm_datetime_string(d)`:归一到 `YYYY-MM-DD HH:MM:SS`
- `_norm_to_list(val, valid_set, param_name)`:把 `None` / `str` / `Iterable` 归一到 `list`;None → []
- `_iso_year_week(yyyymmdd)`:YYYYMMDD → (iso_year, iso_week)
- `_ym(yyyymmdd)`:YYYYMMDD → YYYYMM
- `_get_freq(...)`:week / month 共用的读取函数

## 关键设计

### qfq 计算(get_day 内)
```python
df_adj = pd.read_sql_query(
    "SELECT ts_code, trade_date, adj_factor FROM tbl_cn_adj_factor "
    "WHERE ts_code IN (...)", conn, params=ts_codes_in_df
)
latest_adj = df_adj.groupby("ts_code")["adj_factor"].last().reset_index()
df = df.merge(df_adj, on=["ts_code", "trade_date"], how="left")
df = df.merge(latest_adj, on="ts_code", how="left")
df["adj_factor"] = df["adj_factor"].fillna(1.0)
df["adj_factor_latest"] = df["adj_factor_latest"].fillna(1.0)
qfq_factor = df["adj_factor"] / df["adj_factor_latest"]
# 价 × qfq_factor, vol / qfq_factor, amount 不变
```

### status 关键字展开(get_kpl_list 内)
读 `SELECT DISTINCT status FROM tbl_cn_kpl_list`,展开成精确 IN 列表。
支持的语法:
- `'非首板'` → 排除 '首板'
- `'N板以上'`(N=1-9)→ 解析每条 status 的连板数,过滤 ≥ N
- `'N天'` → 模糊前缀 `'N天%'`
- `'天M板'` → 模糊后缀 `'%天M板'`

### get_news 日期模式优先级
1. `trade_date`(单日,自动 [00:00:00, 23:59:59])
2. `start_datetime` + `end_datetime`(精确)
3. `start_date` + `end_date`(自动补 00:00:00 / 23:59:59)

`trade_date` 跟 `*_datetime` 互斥。

### None vs [] 语义
- `None` = 不过滤(返回全部)
- `[]` = 显式空(返回 0 行)

对 `get_ctrl_basic` / `get_news_ctrl` / `get_kpl_list.ts_codes` 等,**先判 None,再判 list 长度**。

## 数据流

```
调用方
  ↓ import get_day, get_kpl_list, ...
core.offline_db_client.get_xxx(...)
  ↓
[可选用传入 conn,否则 get_conn("basic"/"kpl"/"news")]
  ↓
拼 SQL:SELECT cols FROM table WHERE ... ORDER BY ...
  ↓
pd.read_sql_query(sql, conn, params=params)
  ↓
[get_day 额外 join tbl_cn_adj_factor 算 qfq]
[get_kpl_list 额外读 DISTINCT status 展开关键字]
  ↓
返回 pd.DataFrame
```

## 修改指南

### 加新 get_* 接口
1. **表 key 形状分类**:
   - `(ts_code, date_col)` → 复制 `_get_freq` 模板(week / month)
   - 单 key(no date) → 独立函数(像 `get_basic` / `get_ctrl_basic` / `get_adj_factor`)
   - `(cal_date, exchange)` → 独立函数(像 `get_tradecal`)
2. 复用 `_norm_date_yyyymmdd` 和 `columns` 校验模式
3. **`_norm_to_list` 必须传 whitelist tuple** 给 `exchange` / `market` / `list_status` 这种业务字段
4. **None/[] 区分**:None 走全表,[] 显式返 0 行
5. 在 `if __name__ == "__main__"` 加 3-5 个 smoke test
6. **同步更新 5 个文档位置的接口数字**(N + 1):
   - `README.md` L51 "提供 N 个只读 `get_*` 接口"
   - `00_架构文档.md` L31 "(N 个 get_* 函数)" + L305 + L311
   - `QuickStart.md` L145 "N 个 `get_*` 函数"
   - 本文件 L5 + L15 + L19 + L103 "N 个 `get_*` 接口" 4 处

### 日期格式约定
- `get_day` / `get_week` / `get_month` / `get_basic` / `get_ctrl_basic` / `get_adj_factor` / `get_kpl_list` / `get_kpl_concept_cons` / `get_kpl_limit_performance` / `get_stk_limit` / `get_suspend` / `get_top_list` / `get_top_inst` / `get_block_trade` / `get_ggt_daily` / `get_hsgt_top10` / `get_limit_list` / `get_moneyflow` / `get_margin` / `get_margin_detail` / `get_cyq_perf` / `get_daily_basic` / `get_index_basic` / `get_index_daily` 的日期参数(`start_date` / `end_date` / `trade_date`)接受**任一**字符串格式,内部 `_norm_date_yyyymmdd()` 归一到 YYYYMMDD:
  - `YYYYMMDD`(8 位,推荐,QuickStart 用法)
  - `YYYY-MM-DD`(10 位,带横线)
  - `YYYY/MM/DD`(10 位,带斜杠)
  - `datetime` / `date` 对象
- `get_news` / `get_major_news` 额外支持 `start_datetime` / `end_datetime` 接受 `YYYY-MM-DD HH:MM:SS`(精确时分秒);也支持 `YYYYMMDD HH:MM:SS`(无横线,函数自动加横线归一)
  - `_norm_date_yyyymmdd` 接受 `date` 对象(代码 type hint 写 `Union[str, datetime]`,但 `str(date(2024,9,1))` = `'2024-09-01'` 走 fallback 路径能正确归一,实测可用)
- `get_cctv_news` 的 `start_date` / `end_date` 走同 `_norm_date_yyyymmdd`,但 **DB 里 datetime 是 YYYYMMDD 不带时间**,不接受 `*_datetime` 参数

### 改 upsert 行为
`upsert_df` 是 INSERT OR IGNORE,**不更新已有行**。如果需要 UPSERT(更新已有),改 SQL 为 `INSERT OR REPLACE` 或 `ON CONFLICT DO UPDATE`(SQLite 3.24+)。

### 改 replace_table 行为
已经是事务化,**不要再加 BEGIN**(会嵌套报错)。如果要换 SQL 引擎,需要重写。

## 注意事项

- **`get_day` 的 qfq 路径对 NaN 安全**(`fillna(1.0)`)
- **get_news 默认 limit=5000**(全表 945 万行,不分页会慢)
- **get_cctv_news 的 datetime 是 YYYYMMDD**(无时间),不接受 `*_datetime` 参数
- **get_kpl_concept_cons 的 ts_codes 实际查 con_code**(API 故意保留)
- **`_norm_to_list` 把 None 和 [] 都归一成 []**,但调用方可以**先 `if x is None` 区分**
