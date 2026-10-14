# 代码详细设计/offline_downloader.md

`scripts/core/offline_downloader.py` — 数据下载与写入层。

**类 `CNDataDown` 持有 4 个 DB 连接,提供 31 个 `update_*` 方法把外部数据写到本地 SQLite**:
- 2026-09-15 起 27 个 tushare/kpl 方法(见下方表格)
- **2026-09-17 加 3 个 tdx 方法**:`update_minute` / `update_ticks` / `update_minute_index`
- 2026-09-17 加 `INDEX_CODES_HERE` 常量(5 大盘指数)

## 职责

1. **管理 4 个 DB 连接**(`conn_basic` / `conn_kpl` / `conn_news` + **policy db 连接由 `core.offline_db_client` 间接管理**)
2. **调 Tushare / kpl / tdx 客户端拉数据**
3. **DataFrame 清洗 + 写入**(`upsert_df` / `replace_table` / `upsert_rows`)
4. **断点推进**(`update_ctrl` / `_update_news_ctrl_value` / `mark_downloaded`)
5. **派生表重算**(week / month)
6. **状态输出**(`show_status`)

## 入口

- **被 15 个 service import**(`from core.offline_downloader import CNDataDown`)
  - 12 个 tushare service + 3 个新增的 tdx service(`service_intraday` / `service_ticks` / `service_intraday_index`)
- 每个 service 的 `main()` 里 `down = CNDataDown()` 初始化后调对应 `update_*` 方法

## 类签名

```python
class CNDataDown:
    def __init__(self):
        self.client = TushareClient()  # 单例
        self.conn_basic = init_db("basic", verbose=False)
        self.conn_kpl = init_db("kpl", verbose=False)
        self.conn_news = init_db("news", verbose=False)
        self.conn = self.conn_basic  # 兼容旧代码
        # 懒加载:
        # self._kpl_client = KPLClient()  # 首次 update_kpl_limit_performance 时才 import
```

## 模块常量(2026-09-17 加)

```python
INDEX_CODES_HERE: List[str] = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创50
    "000016.SH",  # 上证50
]
```

5 大盘指数列表,跟 `tdx_config.INDEX_CODES`(4 个,不含 000016.SH)不同;`service_intraday_index` 默认按这个列表拉指数。**policy 研究特定全集**,online/offline 业务仍按 tdx_config.INDEX_CODES 走 4 个。

## 31 个 update_* 方法(2026-09-17 加 3 个 tdx 方法)

| # | 方法 | 表 | 数据源 | 循环粒度 | 写入方式 | 断点 key | 断点位置 |
|---|---|---|---|---|---|---|---|
| 1 | `update_basic(bFull=False)` | tbl_cn_basic | tushare stock_basic | 无(全量一次性) | upsert(全量 DELETE 后) | `cn_basic`(snap_ts) | tbl_basic_ctrl |
| 2 | `update_tradecal(start_date, end_date)` | tbl_cn_tradecal | tushare trade_cal | 3 交易所分别按日期循环 | upsert | `cn_tradecal_<SSE/SZSE>` | tbl_basic_ctrl |
| 3 | `update_kpl_list(start_date, end_date, tag)` | tbl_cn_kpl_list | tushare pro.kpl_list | 按日期循环 + 多 tag(默认 ['涨停','炸板']) | upsert(主键 ts_code+trade_date+tag) | `cn_kpl_list` | tbl_basic_ctrl |
| 4 | `update_kpl_limit_performance(start_date, end_date, trade_date)` | tbl_cn_kpl_limit_performance | kpl API(今天→实时接口 / 历史→历史接口)| 按日期循环 | upsert | `cn_kpl_limit_performance` | tbl_basic_ctrl |
| 5 | `update_kpl_concept_cons(start_date, end_date)` | tbl_cn_kpl_concept_cons | tushare pro.kpl_concept_cons | 按日期循环,每天 OFFSET 分页 | upsert | `cn_kpl_concept_cons` | tbl_basic_ctrl |
| 6 | `update_news(src_list, start_date, end_date)` | tbl_news | tushare pro.news | **7 源(默认 active),**每源按日期循环 | upsert + md5(content) | `tbl_news_ctrl[src]` | tbl_news_ctrl |
| 7 | `update_major_news(start_date)` | tbl_major_news | tushare pro.major_news | 整体一次拉(不分日) | upsert + md5(title) | `tbl_news_ctrl[major_news]` | tbl_news_ctrl |
| 8 | `update_cctv_news(start_date)` | tbl_cctv_news | tushare pro.cctv_news | 按日期循环,每天一次 | upsert(无 md5 字段) | `tbl_news_ctrl[cctv_news]` | tbl_news_ctrl |
| 9 | `update_daily(start_date, end_date)` | tbl_cn_day | tushare pro.daily | 按日期循环,每天 OFFSET 分页 | upsert | `cn_daily` | tbl_basic_ctrl |
| 10 | `update_adj_factor(start_date, end_date)` | tbl_cn_adj_factor | tushare pro.adj_factor | 按日期循环,每天 OFFSET 分页 | upsert | `cn_adj_factor` | tbl_basic_ctrl |
| 11 | `update_week(start_date, end_date)` | tbl_cn_week + tbl_cn_week_origin | **本地派生**(day及day × adj_factor) | 一次性全量 | **replace_tables**(成对原子覆盖) | 无(全量覆盖) | — |
| 12 | `update_month(start_date, end_date)` | tbl_cn_month + tbl_cn_month_origin | **本地派生**(day及day × adj_factor) | 一次性全量 | **replace_tables**(成对原子覆盖) | 无(全量覆盖) | — |
| 13 | `update_stk_limit(start_date, end_date)` | tbl_cn_stk_limit | tushare pro.stk_limit | 按日期循环,每天 1 次 | upsert(主键 trade_date+ts_code) | `cn_stk_limit` | tbl_basic_ctrl |
| 14 | `update_suspend(start_date, end_date)` | tbl_cn_suspend | tushare pro.suspend_d | 按月循环(实测范围参数有效) | upsert(主键 trade_date+ts_code) | `cn_suspend` | tbl_basic_ctrl |
| 16 | `update_top_list(start_date, end_date)` | tbl_cn_top_list | tushare pro.top_list | 按日循环(必填 trade_date) | upsert(主键 trade_date+ts_code) | `cn_top_list` | tbl_basic_ctrl |
| 17 | `update_top_inst(start_date, end_date)` | tbl_cn_top_inst | tushare pro.top_inst | 按日循环(必填 trade_date) | upsert(主键 trade_date+ts_code+exalter+side) | `cn_top_inst` | tbl_basic_ctrl |
| 18 | `update_block_trade(start_date, end_date)` | tbl_cn_block_trade | tushare pro.block_trade | 按月循环 + OFFSET 分页 | upsert(主键 trade_date+ts_code+buyer+seller) | `cn_block_trade` | tbl_basic_ctrl |
| 19 | `update_ggt_daily(start_date, end_date)` | tbl_cn_ggt_daily | tushare pro.ggt_daily | 按月循环 | upsert(主键 trade_date) | `cn_ggt_daily` | tbl_basic_ctrl |
| 19 | `update_hsgt_top10(start_date, end_date)` | tbl_cn_hsgt_top10 | tushare pro.hsgt_top10 | 按月循环 | upsert(主键 trade_date+ts_code+market_type) | `cn_hsgt_top10` | tbl_basic_ctrl |
| 20 | `update_limit_list(start_date, end_date)` | tbl_cn_limit_list | tushare pro.limit_list_d | 按月循环 + 3 个 limit_type | upsert(主键 trade_date+ts_code+"limit") | `cn_limit_list` | tbl_basic_ctrl |
| 21 | `update_moneyflow(start_date, end_date)` | tbl_cn_moneyflow | tushare pro.moneyflow | 按月循环 + OFFSET 分页 | upsert(主键 trade_date+ts_code) | `cn_moneyflow` | tbl_basic_ctrl |
| 22 | `update_margin(start_date, end_date)` | tbl_cn_margin | tushare pro.margin | 按日循环(无 range) | upsert(主键 trade_date+exchange_id) | `cn_margin` | tbl_basic_ctrl |
| 23 | `update_margin_detail(start_date, end_date)` | tbl_cn_margin_detail | tushare pro.margin_detail | 按月循环 + OFFSET 分页 | upsert(主键 trade_date+ts_code) | `cn_margin_detail` | tbl_basic_ctrl |
| 24 | `update_cyq_perf(start_date, end_date)` | tbl_cn_cyq_perf | tushare pro.cyq_perf | 按日循环(逐日 trade_date) | upsert(主键 trade_date+ts_code) | `cn_cyq_perf` | tbl_basic_ctrl |
| 25 | `update_daily_basic(start_date, end_date)` | tbl_cn_daily_basic | tushare pro.daily_basic | 按日循环(limit=6000 + status='L') | upsert(主键 trade_date+ts_code) | `cn_daily_basic` | tbl_basic_ctrl |
| 26 | `update_index_basic()` | tbl_cn_index_basic | tushare pro.index_basic | 全量覆盖(~8000 行,~0.4 秒) | **replace_table**(覆盖更新) | `cn_index_basic` | tbl_basic_ctrl |
| 27 | `update_index_daily(start_date, end_date)` | tbl_cn_index_daily | tushare pro.index_daily | 按 ts_code 循环 + limit/offset 分页 | upsert(主键 trade_date+ts_code) | `cn_index_daily` | tbl_basic_ctrl |
| 28 | `update_all_daily()` | 多个 | — | 调度 1-5 步骤 | — | — | — |

### 29-31. 3 个 tdx 方法(2026-09-17 新增)

跟 1-27 tushare 系列完全不同的范式:
- **数据源**:tdx(通达信),不走 tushare
- **入参模式**:单只单日 `(ts_code, trade_date, *, client, rate, force)`,caller 必须传 `TdxClient` 实例
- **落库目标**:`policy_minute.db` / `policy_ticks.db`(用 `core.offline_db_client.upsert_rows` / `mark_downloaded` / `has_data`)
- **断点机制**:`has_data("minute"/"ticks"/"minute_index", ts, td)` 查 ctrl 表
- **失败重试**:不内置,单对失败抛异常由 `service.common.run_market_loop` 捕获

| # | 方法 | 表 | 数据源 | 入参 | 写入方式 | 断点 key | 用途 |
|---|---|---|---|---|---|---|---|
| 29 | `update_minute(ts_code, trade_date, *, client, rate, force)` | tbl_minute | tdx `get_history_minute` | 单只单日 | upsert_rows("minute", df) + mark_downloaded("minute", ...) | `has_data("minute", ts, td)` | 个股分钟 K |
| 30 | `update_ticks(ts_code, trade_date, *, client, rate, force)` | tbl_tick | tdx `get_history_ticks` | 单只单日 | upsert_rows("ticks", df) + mark_downloaded("ticks", ...) | `has_data("ticks", ts, td)` | 个股分笔成交 |
| 31 | `update_minute_index(ts_code, trade_date, *, client, rate, force)` | tbl_minute_index | tdx `get_history_minute`(指数代码) | 单只单日 | upsert_rows("minute_index", df) + mark_downloaded("minute_index", ...) | `has_data("minute_index", ts, td)` | 大盘指数分钟 K |

**调用模式**(service 一侧):

```python
from tdx_client import TdxClient
client = TdxClient()  # 调用方创建并复用
down = CNDataDown()

# 1 对拉取
inserted = down.update_minute("000006.SZ", "2026-08-28", client=client, rate=0.15)
# 返回 240(新插)或 0(已下载/拉取空)

# 循环(由 service_intraday / service_ticks / service_intraday_index 内部
# 通过 service.common.run_market_loop 统一管理)
```

**关键设计**:
- **TdxClient 由 caller 传**(不内置在 `CNDataDown.__init__`):service 复用 1 个 client 实例,避免重复连接
- **rate=0 表示不限速**,默认 0.15s/对(pytdx ~70 req/s 安全值)
- **force=True 忽略 ctrl**:重拉场景,业务层负责风险评估
- **`has_data` 查的是 `tbl_*_ctrl`**:已经 mark 的对自动 skip,实现幂等
- **policy db 物理位置**:2026-09-17 从 `policyStudy/data/` 搬到 `offlineDataManager/data/`,路径走 `settings.DB_PATH_POLICY_MINUTE` / `DB_PATH_POLICY_TICKS`

### 13. `update_all_daily()` — 整体流程
按顺序调 1-5(基础数据 + kpl + news),返回 dict 各步结果。**不调** week / month(由 service_week / service_month 独立跑)。

## 内部辅助函数

### `_aggregate_daily_to_freq(df_day, freq)` — week/month 聚合核心
```python
def _aggregate_daily_to_freq(self, df_day, freq="weekly"):
    """按 weekly (ISO 周) / monthly (自然月) 聚合"""
    # 1. 加 _group 列
    #    - monthly: _group = trade_date[:6]  (YYYYMM)
    #    - weekly:  _group = ISO year-week
    # 2. groupby(ts_code, _group).agg({
    #       open: 'first', high: 'max', low: 'min', close: 'last',
    #       vol: 'sum', amount: 'sum', trade_date: 'last'
    #    })
    # 3. sort by (ts_code, trade_date)
    # 4. pre_close = groupby(ts_code)['close'].shift(1)
    # 5. change = close - pre_close
    # 6. pct_chg = (change / pre_close) * 100
```

**前置过滤**:聚合前过滤 `open > 0 & close > 0`(避免新股预占位行污染)。

**调频点**:`update_week` 调 `freq='weekly'`,`update_month` 调 `freq='monthly'`。

## 关键设计

### 1. 不复权写入(写入侧),前复权计算(读取侧)
- `update_daily` 写入的是 tushare pro.daily 返回的原始 OHLCV
- tushare pro.daily 接口**不支持复权参数**(官方明确"未复权行情")
- `update_week` / `update_month` 同时生成前复权表和原始不复权 origin 表
- 好处:原始数据可追溯,任意时刻根据 adj_factor 重算

### 2. 派生表覆盖更新
`update_week` / `update_month` 走 `replace_tables`,同一频率的前复权表和 origin 表在一个事务中成对覆盖。

### 3. 断点推进规则
- 必须在 `if inserted > 0:` 之后才 `update_ctrl` 或 `last_success_date = td`
- 空日(`df is None or len(df) == 0`)直接 `continue`,**不推进断点**
- 防止"空日被记为已拉,后续数据丢失"

### 4. OFFSET 分页(防单日 > limit)
所有按日期循环的方法:
- `update_daily`: `limit=6000`, 单日最多 50 页
- `update_adj_factor`: `limit=6000`, 单日最多 50 页
- `update_kpl_list`: `limit=8000`, 单日最多 50 页,**2026-09-15 改造支持 tag='涨停'/'炸板'**
- `update_kpl_concept_cons`: `limit=3000`, 单日最多 50 页
- `update_news`: `limit=1500`, 单日最多 100 页

### 5. md5 计算字段差异
- `update_news`:基于 `content` 算 md5
- `update_major_news`:基于 `title` 算 md5
- `update_cctv_news`:**不算 md5**,直接 upsert。`tbl_cctv_news` 的 schema 主键是 `(datetime, md5)` 但写入时 df 没有 md5 字段,SQLite 主键检查只看 datetime,实际一天只一条(由 `service_cctv_news` 按日期循环保证)

`tbl_news` 跟 `tbl_major_news` 的 md5 字段语义**跨表不一致**;`tbl_cctv_news` 不算 md5。都不影响功能(主键保证唯一)。

### 6. 9 源新闻聚合 + 2 源 disabled
- `NEWS_SRC_LIST = ['sina', 'wallstreetcn', '10jqka', 'eastmoney', 'yuncaijing', 'fenghuang', 'jinrongjie', 'cls', 'yicai']`
- `NEWS_SRC_DISABLED = {'yuncaijing', 'fenghuang'}`(2026-09-11 实测 7 天连续 0 行)
- `get_active_news_srcs()` 返回 NEWS_SRC_LIST - DISABLED
- scheduler 默认跳过 disabled 源以省 API 配额;手动调 `update_news(src_list=...)` 可强制启用

## 数据流(以 update_daily 为例)

```
CNDataDown().update_daily(start_date, end_date)
  ↓
end_date = _fmt_yyyymmdd(end_date) or today
sd = start_date or get_ctrl(conn_basic, "cn_daily") or "20150101"
  ↓
cur = sd..end_date
  for each td:
    offset = 0
    while True:
      df = self.client.daily(trade_date=td, limit=6000, offset=offset)
      if df is empty: break
      df["snap_ts"] = snap_ts()
      inserted = upsert_df(conn_basic, df, "tbl_cn_day", key_cols=["ts_code", "trade_date"])
      if inserted > 0:
        day_inserted += inserted
        last_success_date = td
      if len(df) < 6000: break  # 当天拉完
      offset += 6000
      if page_count >= 50: break  # 安全上限
    cur += 1 day
  ↓
if last_success_date: update_ctrl(conn_basic, "cn_daily", last_success_date)
return total_inserted
```

## 修改指南

### 加新 update_* 方法
模板:
```python
def update_xxx(self, start_date=None, end_date=None) -> int:
    table = "tbl_xxx"
    ctrl_key = "cn_xxx"
    t0 = time.time()
    total_inserted = 0
    end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
    if start_date:
        sd = _fmt_yyyymmdd(start_date)
    else:
        last = get_ctrl(self.conn_basic, ctrl_key)
        sd = last if last else "DEFAULT_START"
    cur = datetime.strptime(sd, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    last_success_date = None
    while cur <= end:
        td = cur.strftime("%Y%m%d")
        # ... 拉数据 + 写入 ...
        cur += timedelta(days=1)
    if last_success_date:
        update_ctrl(self.conn_basic, ctrl_key, last_success_date)
    return total_inserted
```

### 改 update_basic 拉更多字段
- `client.stock_basic(list_status, fields=...)` 的 `fields` 改更全
- 同时改 `SCHEMA_SQL_BASIC` 的 `tbl_cn_basic` schema
- 改 `get_basic` 的 `BASIC_COLS` 列表

### 加新表
1. 在对应 `SCHEMA_SQL_*` 字符串里加 `CREATE TABLE IF NOT EXISTS`
2. 在 `init_db` 自动生效(`CREATE TABLE IF NOT EXISTS` 幂等)
3. **如果加 ctrl 表**:在 `tbl_basic_ctrl` 加 key,不新建独立 ctrl 表
4. **同步更新 4 处顶层文档的"表数"数字**:
   - `README.md` 数据规模表 + 一句话摘要
   - `00_架构文档.md` 数据库分库表 + 关键事实
   - `01_数据详细设计文档.md` L1 + L16 表数 + 一句话总结
   - `QuickStart.md` 期望输出示例

## 注意事项

- **`self.client.daily` / `adj_factor` / `kpl_list` 等是 `TushareClient` 的方法**,不是直接 tushare pro 接口。`TushareClient` 在 `~/TradingAgent/coreClient/tushare_client.py`
- **`TushareClient` 完整方法清单**(在 `coreClient/tushare_client.py`):
  - `stock_basic(**params)` — 股票基本信息,被 `update_basic` 调
  - `trade_cal(**params)` — 交易日历,被 `update_tradecal` 调
  - `daily(**params)` — 日 K,**注意:只能拿不复权原始数据**(无复权参数)
  - `adj_factor(**params)` — 复权因子,被 `update_adj_factor` 调
  - `kpl_list(**params)` — 开盘啦涨停榜,被 `update_kpl_list` 调
  - `kpl_concept_cons(**params)` — 开盘啦题材成分,被 `update_kpl_concept_cons` 调
  - `news(**params)` — 新闻快讯,被 `update_news` 调
  - 其他几个**走 `self.client.pro.*`** 入口(裸 tushare pro):`update_major_news` 用 `self.client.pro.major_news`,`update_cctv_news` 用 `self.client.pro.cctv_news`,`update_adj_factor` 也走 `self.client.pro.adj_factor`
- **`self.client.pro.daily` 跟 `self.client.daily` 是不同的入口**(`pro` 是裸 tushare pro,`daily` 是包装)
- **`_kpl_client` 懒加载**避免启动时强依赖 kpl_client
- **`replace_table` 已经是事务化的**,不要在外面再 BEGIN
- **不要改 `update_*` 的断点位置**(全部在 `tbl_basic_ctrl` / `tbl_news_ctrl`),跟 schema 对应
