# data_provider.md — 统一数据访问层

> **版本**:v1.0(2026-09-18 新建)
> **文件路径**:`~/TradingAgent/coreClient/data_provider.py`(单文件 1194 行)
> **依赖**:`offlineDataManager/scripts/core/offline_db_client.py`(db 端)+ `coreClient/{tushare_client,kpl_client,tdx_client}.py`(online 端)
> **目的**:**policyStudy 等上层研究 Agent 的唯一数据入口**。提供统一的 20 个 `get_*` 函数,每个函数自动在 `database` 和 `online` 两种模式间路由 — 上层 Agent **不用关心**数据从本地 SQLite 来还是从 Tushare / 通达信 / 开盘啦 HTTP 来。

---

## 1. 为什么需要 data_provider?

**动机**(2026-09-17 用户原话):

> 下一步这样 coreClient 里面专门建一个 data_provider 里面做这样的事情,提供目前 offline 数据库有的全部数据接口,每个数据接口是这样的,有个数据选项是 source 默认是 database 如果 source 是 database 那么调用 offline_db_client 来获取数据,如果是 online 那么就从对应的数据源的 client 提供的接口来拿数据,比如 tdx_client/tushare_client 或者 offline_downloader.py ths_client 等等。
>
> 你理解一下我的意思,做这个的目的是未来 policyStudy 在做研究的时候 AI 只要读懂 data_provider 提供的数据接口即可离线或在线拿到数据,有些数据库没有记录的数据,也可以通过在线的方式拿。

**核心价值**:
1. **接口 1:1 镜像 offline_db_client 签名** + `source` 参数(上层代码不改)
2. **`source="database"`** → 走本地 SQLite(`offline_db_client`)
3. **`source="online"`** → 走对应 client(Tushare / 通达信 / 开盘啦)
4. **`source="auto"`**(预留) → db 优先,缺失 fallback online
5. **自动对齐 schema** —— db 和 online 列名差异由 `_align_cols` 工具填 None 占位

---

## 2. 公共 API 风格

### 2.1 调用方式

```python
from coreClient.data_provider import get_day, get_basic

# 默认 db 模式
df = get_day(ts_code="000006.SZ", start_date="2026-09-01", end_date="2026-09-18")

# 显式 db 模式
df = get_day(ts_code="000006.SZ", start_date="2026-09-01", end_date="2026-09-18", source="database")

# online 模式
df = get_day(ts_code="000006.SZ", start_date="2026-09-01", end_date="2026-09-18", source="online")

# auto 模式(预留,目前等价于 database)
df = get_day(ts_code="000006.SZ", start_date="2026-09-01", end_date="2026-09-18", source="auto")
```

### 2.2 source 参数语义

**2026-09-20 实现核验补充**：历史回放请使用 `source="database_only"`，只读本地、缺数返回空表，不联网。现有 `database` 实际会在空表时 fallback online；`auto` 当前实现会报错。以下旧表中的 database/auto 说明不能作为严格离线保证。所有模式均不保证字段在历史时点已发布，研究侧还需约束可见时间。

新闻本地读取新增 keyword-only 参数 `start_datetime`、`end_datetime`、`limit`（默认5000，None表示不限）、`offset`，透传底层已有能力。精确时间/分页参数用于 `database_only`；如进入 online 路径会明确拒绝，防止时间窗口被忽略。普通 `start_date/end_date` 仍表示整日，不能把含时分秒的值传入它们来期待截断。查询结果刚好5000条时不可假定完整；应分页或在有界日期窗口用 `limit=None`。

| 取值 | 含义 | 适用场景 |
|---|---|---|
| `"database"` (默认) | 走本地 SQLite(`offline_db_client`) | 历史研究 / 离线分析 |
| `"online"` | 走对应 client(Tushare / 通达信 / 开盘啦) | db 没数据时 / 实时数据 |
| `"auto"` | db 优先,缺失自动 fallback online | **预留,目前等价 database** |

### 2.3 返回值

**所有接口**返回 `pd.DataFrame`(无数据时返回空 DataFrame,不会返回 None)。

**统一列对齐**:db 和 online 列名差异由 `_align_cols(df, target_cols)` 自动处理:
- online 多的列保留(但可能没数据)
- online 少的列填 `None` 占位(保持 schema 一致)

---

## 3. 完整接口清单(30 个)

> **本节最后更新**:2026-09-18(新增 12 个扩展接口:suspend/top_list/block_trade 等)

| # | 接口 | 类别 | 用途 | db 模式 | online 模式 |
|---|---|---|---|---|---|
| 1 | `get_minute` | 高频 K | 个股分钟 K(240 根/天) | ✅ | ✅ |
| 2 | `get_minute_index` | 高频 K | 指数分钟 K(5 指数) | ✅ | ✅ |
| 3 | `get_ticks` | 高频成交 | 个股分笔成交 | ✅ | ✅ |
| 4 | `get_day` | 日 K | 日 K(支持前复权) | ✅ | ✅ |
| 5 | `get_week` | 周 K | 周 K | ✅ | ⚠️ 不支持 |
| 6 | `get_month` | 月 K | 月 K | ✅ | ⚠️ 不支持 |
| 7 | `get_basic` | 股票档案 | 股票基本信息 | ✅ | ✅ |
| 8 | `get_adj_factor` | 复权因子 | 复权因子 | ✅ | ✅ |
| 9 | `get_stk_limit` | 涨跌停 | 涨跌停价格(每日) | ✅ | ✅ |
| 10 | `get_daily_basic` | 每日指标 | 换手率/量比/PE/PB | ✅ | ✅ |
| 11 | `get_moneyflow` | 资金流向 | 主力/中单/小单流向 | ✅ | ✅ |
| 12 | `get_index_basic` | 指数档案 | 指数基本信息 | ✅ | ✅ |
| 13 | `get_index_daily` | 指数日 K | 指数日 K | ✅ | ✅ |
| 14 | `get_tradecal` | 交易日历 | 交易日历 | ✅ | ✅ |
| 15 | `get_kpl_list` | KPL 榜单 | 涨停/炸板/跌停榜 | ✅ | ✅ |
| 16 | `get_kpl_concept_cons` | KPL 题材 | 题材成分股 | ✅ | ✅ |
| 17 | `get_kpl_limit_performance` | KPL 涨停详情 | 连板/封单/封板时间 | ✅ | ✅ |
| 18 | `get_news` | 新闻 | 新闻快讯(多源) | ✅ | ✅ |
| 19 | `get_suspend` | 停复牌 | 停复牌信息 | ✅ | ✅ |
| 20 | `get_top_list` | 龙虎榜 | 龙虎榜每日榜单 | ✅ | ✅ |
| 21 | `get_top_inst` | 龙虎榜 | 龙虎榜机构/营业部明细 | ✅ | ✅ |
| 22 | `get_block_trade` | 大宗交易 | 大宗交易 | ✅ | ✅ |
| 23 | `get_ggt_daily` | 港股通 | 港股通每日成交(南向) | ✅ | ✅ |
| 24 | `get_hsgt_top10` | 沪深港通 | 沪深港通 top10(北向) | ✅ | ✅ |
| 25 | `get_limit_list` | 涨跌停清单 | 涨停/跌停每日清单 | ✅ | ✅ |
| 26 | `get_margin` | 融资融券 | 融资融券汇总(交易所维度) | ✅ | ✅ |
| 27 | `get_margin_detail` | 融资融券 | 融资融券明细(个股维度) | ✅ | ✅ |
| 28 | `get_cyq_perf` | 筹码 | 筹码胜率(每日) | ✅ | ✅ |
| 29 | `get_major_news` | 头条新闻 | 头条/重要新闻 | ✅ | ✅ |
| 30 | `get_cctv_news` | CCTV 新闻 | CCTV 新闻(联播/朝闻天下) | ✅ | ⚠️ 无源 |

### 3.1 按 online 模式能力分类

| online 支持能力 | 接口 |
|---|---|
| ✅ 完全支持(全功能) | get_minute / get_minute_index / get_ticks / get_day / get_basic / get_adj_factor / get_stk_limit / get_daily_basic / get_moneyflow / get_index_basic / get_index_daily / get_tradecal / get_kpl_list / get_kpl_concept_cons / get_kpl_limit_performance / get_news / get_suspend / get_top_list / get_top_inst / get_block_trade / get_ggt_daily / get_hsgt_top10 / get_limit_list / get_margin / get_margin_detail / get_cyq_perf / get_major_news(共 27 个) |
| ⚠️ online 不支持(返回空) | get_week / get_month / get_cctv_news(共 3 个) |

### 3.2 按 online 必传参数分类

| 类型 | 接口 |
|---|---|
| **必传 ts_code**(tushare 不支持全市场) | get_block_trade / get_hsgt_top10 / get_margin_detail / get_cyq_perf |
| **ts_code 可选**(trade_date 单日即可) | get_suspend / get_top_list / get_top_inst / get_limit_list / get_margin / get_ggt_daily / get_major_news |
| **必传日期**(单日 / 多日) | get_minute / get_minute_index / get_ticks / get_kpl_list / get_kpl_concept_cons / get_kpl_limit_performance / get_news / get_index_daily / get_tradecal(必传 start+end)/ get_adj_factor(必传 start+end)/ get_moneyflow / get_daily_basic |
| **必传日期 + ≤ 30 交易日**(用户原话) | get_minute / get_minute_index / get_ticks / get_kpl_limit_performance |

---

## 4. 接口详细说明

### 4.1 `get_minute` — 个股分钟 K(240 根/天)

**用途**:单只股票某天或某段时间的 1 分钟 K 线,240 根/天(9:30 ~ 15:00,含集合竞价)。

**签名**:
```python
def get_minute(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 类型 | 必填 | 含义 |
|---|---|---|---|
| `ts_code` | str | 否 | 单只股票,e.g. `"000006.SZ"`(与 `ts_codes` 互斥) |
| `ts_codes` | list | 否 | 多只股票列表(与 `ts_code` 互斥) |
| `trade_date` | str | 否 | 单日 `'YYYY-MM-DD'` 或 `'YYYYMMDD'`(与 `start_date+end_date` 互斥) |
| `start_date` | str | 否 | 起始日(与 `trade_date` 互斥) |
| `end_date` | str | 否 | 结束日(与 `trade_date` 互斥) |
| `source` | str | 否 | `"database"`(默认)/ `"online"` / `"auto"` |

**模式对比**:

| 模式 | 数据源 | 限制 |
|---|---|---|
| `database` | `tbl_minute`(本地 SQLite) | 无限制,直接读 |
| `online` | 通达信 `pytdx`(`tdx_client`) | **必须传日期**:无日期报错;`ts_code/ts_codes` 不能全空;**单只单日 / 单只多日 / 多只单日** 二选一;**总调用次数 ≤ 30** |

**online 模式约束**(用户原话):

| 约束 | 行为 |
|---|---|
| 无日期 | ValueError `必须传 trade_date 或 (start_date + end_date)` |
| `trade_date` + `start_date/end_date` 同时 | ValueError `互斥` |
| `ts_codes` + `start_date/end_date` 同时(笛卡尔积爆炸) | ValueError `多 ts_code 跟多 trade_date 不能同时` |
| 调用次数 > 30(单 ts 多日 或 多 ts 单日) | ValueError `超过限制` |
| 多日请求 | 自动过交易日历过滤(去除周末/节假日) |
| `ts_codes` 不传 | 默认 5 指数(`INDEX_CODES_HERE`)|

**db / online 返回列**:

| 列名 | 类型 | 含义 |
|---|---|---|
| `ts_code` | str | 股票代码(含后缀) |
| `trade_date` | str | 交易日 `'YYYY-MM-DD'` |
| `time` | str | 时间 `HH:MM`(e.g. `"09:30"`) |
| `open` | float | 开盘价(元) |
| `high` | float | 最高价 |
| `low` | float | 最低价 |
| `close` | float | 收盘价 |
| `vol` | float | 成交量(手) |
| `amount` | float | 成交额(元) |
| `created_at` | str | db 写入时间戳,online 为 None |

**示例**:

```python
# db 模式:拉 2026-09-10 单日 000006.SZ 分钟 K
df = get_minute(ts_code="000006.SZ", trade_date="2026-09-10")
# → 240 行

# online 模式:拉 2026-09-10 单日
df = get_minute(ts_code="000006.SZ", trade_date="2026-09-10", source="online")

# online 模式:多日(自动拆日循环 + 拼接)
df = get_minute(ts_code="000006.SZ", start_date="2026-09-08", end_date="2026-09-10", source="online")
# → 3 × 240 = 720 行

# online 模式:多只单日
df = get_minute(ts_codes=["000006.SZ", "000001.SZ"], trade_date="2026-09-10", source="online")
# → 480 行
```

---

### 4.2 `get_minute_index` — 指数分钟 K(5 指数)

**用途**:大盘指数分钟 K,**默认拉 5 指数**(上证指数 / 深证成指 / 创业板指 / 科创 50 / 上证 50)。

**签名**:
```python
def get_minute_index(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:同 `get_minute`,**`ts_code/ts_codes` 可选(默认 5 指数)**。

**默认 5 指数**(`offline_downloader.INDEX_CODES_HERE`):

| ts_code | 名称 |
|---|---|
| `000001.SH` | 上证指数 |
| `399001.SZ` | 深证成指 |
| `399006.SZ` | 创业板指 |
| `000688.SH` | 科创 50 |
| `000016.SH` | 上证 50 |

**返回列**:同 `get_minute`。

**示例**:

```python
# db 模式:5 指数全表
df = get_minute_index()  # 全表 5 指数 × 全交易日

# db 模式:单日 5 指数
df = get_minute_index(trade_date="2026-09-10")
# → 5 × 240 = 1200 行

# online 模式:默认 5 指数 × 单日
df = get_minute_index(trade_date="2026-09-10", source="online")
# → 1200 行

# online 模式:多日 5 指数(5 × N ≤ 30 即 N ≤ 6)
df = get_minute_index(start_date="2026-09-08", end_date="2026-09-10", source="online")
# → 5 × 3 × 240 = 3600 行
```

---

### 4.3 `get_ticks` — 个股分笔成交

**用途**:单只股票某天的**分笔成交明细**(几千 ~ 几万行/天)。用于精确追踪成交方向、主力买卖、大单小单。

**签名**:
```python
def get_ticks(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:同 `get_minute`。

**返回列**:

| 列名 | 类型 | 含义 |
|---|---|---|
| `ts_code` | str | 股票代码 |
| `trade_date` | str | 交易日 |
| `time` | str | 时间 `HH:MM:SS` |
| `price` | float | 成交价 |
| `vol` | int | 成交量(手) |
| `amount` | float | 成交额 |
| `bs_flag` | str | 买卖方向(盘后逐笔:tushare 增量;盘中:由通达信 5 档推) |
| `created_at` | str | db 写入时间戳,online 为 None |

**示例**:

```python
# db 模式:单只单日
df = get_ticks(ts_code="000006.SZ", trade_date="2026-09-10")

# online 模式:多日(自动拆日循环)
df = get_ticks(ts_code="000006.SZ", start_date="2026-09-08", end_date="2026-09-10", source="online")
```

---

### 4.4 `get_day` — 日 K(支持前复权)

**用途**:单只或多只股票的日 K 线。**qfq=True 时前复权**(默认)。

**签名**:
```python
def get_day(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    trade_date: Optional[str] = None,
    qfq: bool = True,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 类型 | 必填 | 含义 |
|---|---|---|---|
| `ts_code` | str | 否 | 单只股票 |
| `ts_codes` | list | 否 | 多只股票(online 自动 join 成逗号串 `xxx,yyy`) |
| `start_date` | str | 否 | 起始日(与 `trade_date` 互斥) |
| `end_date` | str | 否 | 结束日(与 `start_date` 必须同时传) |
| `trade_date` | str | 否 | 单日(与 `start_date/end_date` 互斥) |
| `qfq` | bool | 否 | 是否前复权(默认 True) |
| `source` | str | 否 | `"database"`(默认)/ `"online"` |

**模式对比**:

| 模式 | qfq=True | qfq=False |
|---|---|---|
| `database` | 读 `tbl_cn_day` + `tbl_cn_adj_factor` 自己 join 算 | 读 `tbl_cn_day` 不复权 |
| `online` | `daily_qfq_range`(全市场拉 + 客户端过滤 ts_code)| `tushare.daily`(支持 ts_code 逗号分隔) |

**online 模式约束**:

| 约束 | 行为 |
|---|---|
| 必须传 `trade_date` 或 `start_date+end_date` | 否则 ValueError |
| 无 `ts_code/ts_codes` | ValueError(tushare.daily **不支持全市场**) |
| 有 `ts_codes` | tushare.daily 一次调,服务端过滤(`ts_code='xxx,yyy,zzz'`) |

**返回列**(前复权 qfq=True):

| 列名 | 类型 | 含义 |
|---|---|---|
| `ts_code` | str | 股票代码 |
| `trade_date` | str | 交易日 `'YYYY-MM-DD'` |
| `open` | float | 开盘价(**前复权**) |
| `high` | float | 最高价 |
| `low` | float | 最低价 |
| `close` | float | 收盘价 |
| `pre_close` | float | 昨收 |
| `change` | float | 涨跌额 |
| `pct_chg` | float | 涨跌幅(%) |
| `vol` | float | 成交量(手) |
| `amount` | float | 成交额(千元) |
| `snap_ts` | str | db 写入时间戳,online 为 None |

**示例**:

```python
# db 模式:单只多日前复权
df = get_day(ts_code="000006.SZ", start_date="2026-09-01", end_date="2026-09-18")

# db 模式:多只某一天
df = get_day(ts_codes=["000006.SZ", "000001.SZ"], trade_date="2026-09-10")

# online 模式:多只多日(1 次 daily 调用,ts_code 服务端过滤)
df = get_day(ts_codes=["000006.SZ", "000001.SZ"], start_date="2026-09-08", end_date="2026-09-10", source="online")
```

---

### 4.5 `get_week` / `get_month` — 周 K / 月 K

**签名**:
```python
def get_week(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```
(同 `get_month`)

**⚠️ online 模式不支持**:

> tushare.weekly / tushare.monthly 需高积分,本项目未启用。
> - `source="online"` → 返回**空 DataFrame** + 警告日志
> - `source="database"` → 正常返回

**db 模式支持**:周 K / 月 K 全套参数。

**返回列**:同 `get_day`(无 `qfq` 概念)。

**示例**:

```python
# db 模式
df = get_week(ts_code="000006.SZ", start_date="2026-01-01", end_date="2026-09-18")

# online 模式 → 空
df = get_week(ts_code="000006.SZ", source="online")
# → 0 行 + logger.warning("[data_provider] get_week online 模式不支持")
```

---

### 4.6 `get_basic` — 股票基本信息

**用途**:股票列表 + 行业/地域/上市日期/交易所等。

**签名**:
```python
def get_basic(
    exchange: Optional[str] = None,
    market: Optional[str] = None,
    list_status: str = "L",
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 类型 | 必填 | 含义 |
|---|---|---|---|
| `exchange` | str | 否 | 交易所:`"SSE"`(上交所)/ `"SZSE"`(深交所)/ `"BSE"`(北交所)/ `"HKEX"`(港交所) |
| `market` | str | 否 | 市场类别:`"主板"` / `"创业板"` / `"科创板"` / `"CDR"`(中概股回归)/ `"北交所"` |
| `list_status` | str | 否 | 上市状态:`"L"`(上市,默认)/ `"D"`(退市)/ `"P"`(暂停)/ `"G"`(过会未交易) |
| `source` | str | 否 | `"database"` / `"online"` |

**返回列**(18 列):

| 列名 | 类型 | 含义 |
|---|---|---|
| `ts_code` | str | 股票代码 |
| `symbol` | str | 股票代码(无后缀) |
| `name` | str | 股票名称 |
| `industry` | str | 所属行业(中信一级)|
| `fullname` | str | 公司全称 |
| `enname` | str | 英文名 |
| `cnspell` | str | 拼音缩写 |
| `market` | str | 市场类别 |
| `exchange` | str | 交易所 |
| `curr_type` | str | 货币类型(`CNY` / `HKD` / `USD`) |
| `list_status` | str | 上市状态 |
| `list_date` | str | 上市日期 |
| `delist_date` | str | 退市日期(未退市为 None) |
| `is_hs` | str | 是否沪深港通(`N`/`S`/`H`) |
| `act_ent_type` | str | 实控人企业性质 |
| `act_name` | str | 实控人名称 |
| `area` | str | 地域 |
| `snap_ts` | str | db 写入时间戳,online 为 None |

**示例**:

```python
# db 模式:默认上市状态(5565 行)
df = get_basic()

# 退市股票
df = get_basic(list_status="D")

# online 模式:上交所主板
df = get_basic(exchange="SSE", market="主板", source="online")
```

---

### 4.7 `get_adj_factor` — 复权因子

**用途**:复权因子,做前复权计算必需。

**签名**:
```python
def get_adj_factor(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:**必须传 `ts_code` 或 `ts_codes`**(tushare.adj_factor 不支持全市场)。无 ts_code 报错。

**返回列**:

| 列名 | 类型 | 含义 |
|---|---|---|
| `ts_code` | str | 股票代码 |
| `trade_date` | str | 交易日 |
| `adj_factor` | float | 复权因子 |
| `snap_ts` | str | db 写入时间戳,online 为 None |

---

### 4.8 `get_stk_limit` — 涨跌停价格

**用途**:每日每只股票的涨停价 / 跌停价。

**签名**:
```python
def get_stk_limit(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:**必须传 `ts_code` 或 `ts_codes`**(tushare.stk_limit 不支持全市场)。

**返回列**:

| 列名 | 类型 | 含义 |
|---|---|---|
| `trade_date` | str | 交易日 |
| `ts_code` | str | 股票代码 |
| `up_limit` | float | 涨停价 |
| `down_limit` | float | 跌停价 |
| `snap_ts` | str | db 写入时间戳 |

---

### 4.9 `get_daily_basic` — 每日指标(PE/PB/换手率等)

**用途**:每日每只股票的换手率 / 量比 / PE / PB / 总市值 / 流通市值 等。

**签名**:
```python
def get_daily_basic(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

> **注**:outer 没暴露 `ts_codes` 参数(db 端也不支持多 ts_code),如需多 ts_code 循环自行处理。

**online 模式约束**:无 `ts_code` 时 tushare.daily_basic 会返回全市场数据(单次 ~ 6000 行)。

**返回列**(17 列):

| 列名 | 类型 | 含义 |
|---|---|---|
| `ts_code` | str | 股票代码 |
| `trade_date` | str | 交易日 |
| `close` | float | 收盘价 |
| `turnover_rate` | float | 换手率(%) |
| `turnover_rate_f` | float | 换手率(自由流通股,%) |
| `volume_ratio` | float | 量比 |
| `pe` | float | 市盈率(总市值/净利润,TTM) |
| `pe_ttm` | float | 市盈率 TTM |
| `pb` | float | 市净率(总市值/净资产) |
| `ps` | float | 市销率 |
| `ps_ttm` | float | 市销率 TTM |
| `total_share` | float | 总股本(万股) |
| `float_share` | float | 流通股本(万股) |
| `free_share` | float | 自由流通股本(万股) |
| `total_mv` | float | 总市值(万元) |
| `circ_mv` | float | 流通市值(万元) |
| `snap_ts` | str | db 写入时间戳 |

---

### 4.10 `get_moneyflow` — 资金流向(主力/中单/小单)

**用途**:每日每只股票的主力/中单/小单买卖净额。

**签名**:
```python
def get_moneyflow(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:**必须传 `ts_code` 或 `ts_codes`**(tushare.moneyflow 不支持全市场)。

**返回列**(19 列):

| 列名 | 类型 | 含义 |
|---|---|---|
| `trade_date` | str | 交易日 |
| `ts_code` | str | 股票代码 |
| `buy_sm_vol` | float | 小单买入量(手) |
| `buy_sm_amount` | float | 小单买入金额(万元) |
| `sell_sm_vol` | float | 小单卖出量 |
| `sell_sm_amount` | float | 小单卖出金额 |
| `buy_md_vol` | float | 中单买入量 |
| `buy_md_amount` | float | 中单买入金额 |
| `sell_md_vol` | float | 中单卖出量 |
| `sell_md_amount` | float | 中单卖出金额 |
| `buy_lg_vol` | float | 大单买入量 |
| `buy_lg_amount` | float | 大单买入金额 |
| `sell_lg_vol` | float | 大单卖出量 |
| `sell_lg_amount` | float | 大单卖出金额 |
| `buy_elg_vol` | float | 特大单买入量 |
| `buy_elg_amount` | float | 特大单买入金额 |
| `sell_elg_vol` | float | 特大单卖出量 |
| `sell_elg_amount` | float | 特大单卖出金额 |
| `net_mf_vol` | float | 资金净流入量(手) |
| `net_mf_amount` | float | 资金净流入额(万元) |
| `snap_ts` | str | db 写入时间戳 |

---

### 4.11 `get_index_basic` — 指数基本信息

**签名**:
```python
def get_index_basic(
    ts_code: Optional[str] = None,
    market: Optional[str] = None,
    publisher: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 含义 |
|---|---|
| `ts_code` | 指数代码(`"000300.SH"` 沪深 300)|
| `market` | 交易所/服务商:`"SSE"`(上交所)/ `"SZSE"`(深交所)/ `"MSCI"` / `"CSI"`(中证)/ `"SW"`(申万)/ `"CICC"`(中金)/ `"OTH"` |
| `publisher` | 发布商 |
| `source` | `"database"` / `"online"` |

**返回列**:

| 列名 | 含义 |
|---|---|
| `ts_code` | 指数代码 |
| `name` | 指数简称 |
| `market` | 交易所/服务商 |
| `publisher` | 发布方 |
| `category` | 指数类别(主题/规模/行业/...) |
| `base_date` | 基期 |
| `base_point` | 基点 |
| `list_date` | 发布日期 |
| `snap_ts` | db 写入时间戳 |

**示例**:

```python
# db 模式:全市场指数
df = get_index_basic()

# online 模式:中证指数
df = get_index_basic(market="CSI", source="online")
```

---

### 4.12 `get_index_daily` — 指数日 K

**签名**:
```python
def get_index_daily(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:必须传 `ts_code`(tushare.index_daily 不支持全市场)。

**返回列**:同 `get_day`(无 `qfq` 概念)。

---

### 4.13 `get_tradecal` — 交易日历

**用途**:判定某天是否交易日、获取某区间交易日列表。

**签名**:
```python
def get_tradecal(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    market: str = "SSE",
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 含义 |
|---|---|
| `start_date` | 起始日 |
| `end_date` | 结束日 |
| `market` | 交易所:`"SSE"`(默认)/ `"SZSE"` / `"BSE"` |
| `source` | `"database"` / `"online"` |

**注意**:online 模式 tushare 字段名是 `exchange`,本接口**统一用 `market`**(db 和 online 端通过 `_to_yyyymmdd` 转换)。

**返回列**:

| 列名 | 含义 |
|---|---|
| `cal_date` | 日历日期 |
| `is_open` | 是否交易日:`1` = 开 / `0` = 不开 |
| `market` | 交易所 |

**示例**:

```python
# 某天是否交易日
df = get_tradecal(start_date="2026-09-18", end_date="2026-09-18", market="SSE")
# → 1 行,is_open=0 或 1

# 某月所有交易日
df = get_tradecal(start_date="2026-09-01", end_date="2026-09-30")
# → 30 行,is_open=1 的是交易日
```

---

### 4.14 `get_kpl_list` — KPL 涨停/炸板/跌停榜单

**用途**:开盘啦提供的每日涨停股 / 炸板股 / 跌停股榜单。

**签名**:
```python
def get_kpl_list(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tags: Optional[Union[str, list]] = "涨停",
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 含义 |
|---|---|
| `trade_date` | 单日(与 `start_date/end_date` 互斥) |
| `start_date` | 起始日 |
| `end_date` | 结束日 |
| `tags` | str 或 list:`"涨停"` / `"炸板"` / `"跌停"` / `"自然涨停"` 等(可组合) |
| `source` | `"database"` / `"online"` |

**online 模式约束**:
- 必须传日期(单日 / 多日二选一)
- `tags=list`:自动**循环每个 tag**,合并结果并去重

**返回列**(25 列,db 和 online 一致):

| 列名 | 含义 |
|---|---|
| `ts_code` | 股票代码 |
| `name` | 股票名称 |
| `trade_date` | 交易日 |
| `lu_time` | 涨停时间(unix int,需转 datetime) |
| `ld_time` | 跌停时间 |
| `open_time` | 开盘时间 |
| `last_time` | 最近时间 |
| `lu_desc` | 涨停描述 |
| `tag` | 标签(`涨停`/`炸板`/`跌停`) |
| `theme` | 题材 |
| `net_change` | 净变化 |
| `bid_amount` | 封单金额(万) |
| `status` | 状态 |
| `bid_change` | 封单变化 |
| `bid_turnover` | 封单换手 |
| `lu_bid_vol` | 涨停封单量 |
| `pct_chg` | 涨跌幅 |
| `bid_pct_chg` | 封单涨跌幅 |
| `rt_pct_chg` | 实时涨跌幅 |
| `limit_order` | 涨停委托 |
| `amount` | 成交额 |
| `turnover_rate` | 换手率 |
| `free_float` | 自由流通市值 |
| `lu_limit_order` | 涨停价封单 |
| `snap_ts` | db 写入时间戳 |

**示例**:

```python
# db 模式:单日涨停榜
df = get_kpl_list(trade_date="2026-09-17", tags="涨停")

# db 模式:多日涨停榜
df = get_kpl_list(start_date="2026-09-10", end_date="2026-09-17", tags="涨停")

# online 模式:单 tag 单日
df = get_kpl_list(trade_date="2026-09-17", tags="涨停", source="online")

# online 模式:多 tag 单日(循环合并)
df = get_kpl_list(trade_date="2026-09-17", tags=["涨停", "炸板"], source="online")
```

---

### 4.15 `get_kpl_concept_cons` — KPL 题材成分股

**用途**:某题材在某段时间内的成分股列表。

**签名**:
```python
def get_kpl_concept_cons(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 含义 |
|---|---|
| `trade_date` | 单日 |
| `start_date` | 起始日 |
| `end_date` | 结束日 |
| `ts_code` | 单只股票(过滤) |
| `ts_codes` | 多只股票(过滤) |
| `source` | `"database"` / `"online"` |

**online 模式约束**:
- tushare.kpl_concept_cons **不支持 ts_code 参数**
- 有 ts_code/ts_codes 过滤时 → 拉全部再客户端过滤(数据量小,trade_date 通常 1-2 天)
- 必须传日期(单日 / 多日二选一)

**返回列**:

| 列名 | 含义 |
|---|---|
| `trade_date` | 交易日 |
| `ts_code` | 股票代码 |
| `name` | 股票名称 |
| `theme` | 题材 |
| `theme_id` | 题材 ID |
| `hot_num` | 热度值 |
| `cons_num` | 题材成分股数 |
| `desc` | 题材描述 |
| `snap_ts` | db 写入时间戳 |

---

### 4.16 `get_kpl_limit_performance` — KPL 涨停表现

**用途**:涨停股详细表现(连板数 / 封单金额 / 封板时间 / 炸板次数 / 涨幅 等)。这是**policyStudy 涨停研究**的核心数据源。

**签名**:
```python
def get_kpl_limit_performance(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    sort_by: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 含义 |
|---|---|
| `trade_date` | 单日 `YYYY-MM-DD`(与 `start_date/end_date` 互斥) |
| `start_date` | 起始日 |
| `end_date` | 结束日 |
| `sort_by` | 排序:`'board_then_time'`(连板高→低,时间早→晚)/ `'time'` / `'board'` / `None` |
| `source` | `"database"` / `"online"` |

**online 模式约束**(2026-09-18 用户原话):

> 除了我们的 get minute/ticks/minute_index 之外,kpl limit performance 也是一样的它不支持 ts_code 这个参数,它只能根据日期来拉数据,一样的设置限制一次不超过 30 个交易日。

| 约束 | 行为 |
|---|---|
| 必须传日期(单日 / 多日) | 无日期报错 |
| `trade_date` + `start_date/end_date` 同时 | 互斥报错 |
| **总调用次数 ≤ 30**(用户明确要求) | 超过报错 |
| 多日请求 | 自动过交易日历过滤 |
| **trade_date = today(今日)** | 走 KPL `fetch_realtime_limit_performance()`(盘中可用,2026-09-14 用户自加) |
| **trade_date = 历史日** | 走 KPL `get_daily_limit_performance(date)`(apphis 历史接口) |

**关键设计:今日 vs 历史日的接口路由**

| trade_date | 走的 KPL 接口 | host | 用途 |
|---|---|---|---|
| `today`(`datetime.now()`) | `fetch_realtime_limit_performance()` | `apphwhq`(实时盯盘) | 盘中可用,无日期参数,内部自动取 today |
| 历史日 | `get_daily_limit_performance(YYYYMMDD)` | `apphis`(历史) | 必须传 `Day=YYYY-MM-DD` |

> **这是用户 2026-09-18 特别强调的**:**盘中拉 today 用 apphis 历史接口会返 0 行**,必须用 `apphwhq` 实时接口(2026-09-14 用户自加)。

**返回列**(24 列,db 和 online 一致):

| 列名 | 含义 |
|---|---|
| `trade_date` | 交易日 |
| `ts_code` | 股票代码 |
| `name` | 股票名称 |
| `board_count` | 连板数 |
| `lu_time` | 封板时间(unix int) |
| `theme` | 题材 |
| `limit_reason` | 涨停原因 |
| `is_break` | 是否炸板 |
| `amplitude` | 振幅 |
| `turnover_rate` | 换手率 |
| `limit_order` | 封单金额(万) |
| `close_price` | 收盘价 |
| `pct_chg` | 涨跌幅 |
| `board_period` | 连板期间 |
| `free_float` | 自由流通市值 |
| `main_in` | 主力流入 |
| `main_out` | 主力流出 |
| `theme_id` | 题材 ID |
| `sector_id` | 板块 ID |
| `board_type` | 板型(1-5) |
| `tag` | 标签 |
| `remark` | 备注 |
| `amount` | 成交额 |
| `snap_ts` | db 写入时间戳 |

**示例**:

```python
# db 模式:单日涨停详情
df = get_kpl_limit_performance(trade_date="2026-09-17", sort_by="board_then_time")

# db 模式:多日
df = get_kpl_limit_performance(start_date="2026-09-10", end_date="2026-09-17")

# online 模式:历史日
df = get_kpl_limit_performance(trade_date="2026-09-17", source="online")

# online 模式:今日(盘中可用)
df = get_kpl_limit_performance(source="online")
# → 等价于 trade_date=today → 走 fetch_realtime_limit_performance()
```

---

### 4.17 `get_news` — 新闻快讯(多源聚合)

**用途**:多源新闻聚合,默认全部 9 个活跃源(新浪 / 第一财经 / 财联社 / 东方财富 / 同花顺 / 凤凰财经 / 网易财经 / 21 经济 / 证券时报)。

**签名**:
```python
def get_news(
    src: Optional[Union[str, list]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:

| 参数 | 含义 |
|---|---|
| `src` | str 或 list:`"sina"` / `"yicai"` / `"cls"` / `"eastmoney"` / `"10jqka"` 等(None = 全部) |
| `start_date` | 起始日(YYYY-MM-DD)|
| `end_date` | 结束日 |
| `source` | `"database"` / `"online"` |

**online 模式约束**:
- 必须传日期(单日 / 多日二选一)
- `src=list`:自动**循环每个 src**,合并结果并去重(按 `datetime+src+title` 去重)

**返回列**(8 列):

| 列名 | 含义 |
|---|---|
| `datetime` | 新闻时间(含时间戳) |
| `src` | 来源 |
| `title` | 标题 |
| `content` | 正文(online 可能为空,tushare.news 不返回) |
| `channels` | 渠道(多源整合时) |
| `score` | 重要度评分 |
| `md5` | 内容 MD5(去重用) |
| `snap_ts` | db 写入时间戳 |

**示例**:

```python
# db 模式:全部源
df = get_news(start_date="2026-09-10", end_date="2026-09-17")

# db 模式:单源
df = get_news(src="sina", start_date="2026-09-10", end_date="2026-09-17")

# online 模式:多源(循环合并)
df = get_news(src=["sina", "yicai"], start_date="2026-09-10", end_date="2026-09-17", source="online")
```

### 4.18 `get_suspend` — 停复牌信息

**用途**:每日停牌 / 复牌记录。

**签名**:
```python
def get_suspend(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    suspend_type: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:`ts_code` 可选(`tushare.suspend_d` 支持 trade_date 单日查全市场)。

**返回列**:`ts_code / trade_date / suspend_timing / suspend_type(S 停牌 R 复牌) / snap_ts`

---

### 4.19 `get_top_list` — 龙虎榜每日榜单

**用途**:每日龙虎榜上榜股票(涨停/跌停/换手率异常 等)。

**签名**:
```python
def get_top_list(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:`ts_code` 可选(`tushare.top_list` 支持 trade_date 单日查全市场)。

**返回列(15 列)**:`trade_date / ts_code / name / close / pct_change / turnover_rate / amount / l_sell / l_buy / l_amount / net_amount / net_rate / amount_rate / float_values / reason / snap_ts`

---

### 4.20 `get_top_inst` — 龙虎榜机构/营业部明细

**用途**:上榜股票对应的买卖机构/营业部明细。

**签名**:
```python
def get_top_inst(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    exalter: Optional[str] = None,
    side: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:
- `exalter`:营业部名称过滤
- `side`:买卖方向过滤

**online 模式约束**:`ts_code` 可选(`tushare.top_inst` 支持 trade_date 单日查全市场)。

**返回列(11 列)**:`trade_date / ts_code / exalter / side / buy / buy_rate / sell / sell_rate / net_buy / snap_ts`

---

### 4.21 `get_block_trade` — 大宗交易

**用途**:大宗交易成交记录。

**签名**:
```python
def get_block_trade(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:**必传 ts_code**(`tushare.block_trade` 不支持全市场)+ 日期必传。

**返回列**:`ts_code / trade_date / price / vol / amount / buyer / seller / snap_ts`

---

### 4.22 `get_ggt_daily` — 港股通每日成交

**用途**:沪深港通南向资金每日成交(港股通)。

**签名**:
```python
def get_ggt_daily(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:必传日期(无 ts_code 参数)。

**返回列**:`trade_date / buy_amount / buy_volume / sell_amount / sell_volume / snap_ts`

---

### 4.23 `get_hsgt_top10` — 沪深港通 Top10(北向资金)

**用途**:沪股通/深股通每日前十大成交股(北向资金)。

**签名**:
```python
def get_hsgt_top10(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    market_type: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:
- `market_type`:`"HK"`(北向:沪股通+深股通)/ `"SZ"`(深股通)/ `"SH"`(沪股通)

**online 模式约束**:**必传 ts_code** + 日期必传。

**返回列**:`trade_date / ts_code / name / close / change / rank / market_type / amount / net_amount / buy / sell / snap_ts`

---

### 4.24 `get_limit_list` — 涨停/跌停清单

**用途**:每日涨停/跌停清单(每只上榜股的连板数 / 封单金额 / 首次涨停时间 等)。

**签名**:
```python
def get_limit_list(
    ts_code: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:
- `limit`:`"U"`(涨停)/ `"D"`(跌停)/ `"Z"`(炸板)

**online 模式约束**:`ts_code` 可选(`tushare.limit_list_d` 支持 trade_date 单日查全市场)+ 日期必传。

**返回列(11 列)**:`trade_date / ts_code / name / industry / close / pct_chg / amount / limit_amount / board_count / limit / snap_ts`

---

### 4.25 `get_margin` — 融资融券汇总(交易所维度)

**用途**:上交所/深交所/北交所每日融资融券汇总数据。

**签名**:
```python
def get_margin(
    exchange_id: Optional[str] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**参数**:
- `exchange_id`:`"SSE"`(上交所)/ `"SZSE"`(深交所)/ `"BSE"`(北交所)

**online 模式约束**:必传日期(`tushare.margin` 支持 trade_date 单日查全交易所)。

**返回列(8 列)**:`trade_date / exchange_id / rzye(融资余额) / rzmre(融资买入) / rzche(融资偿还) / rqye(融券余额) / rqmcl(融券卖出) / rzrqye(融资融券余额) / snap_ts`

---

### 4.26 `get_margin_detail` — 融资融券明细(个股维度)

**用途**:每日每只股票的融资融券明细。

**签名**:
```python
def get_margin_detail(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:**必传 ts_code** + 日期必传(循环 ts_codes 多个股票)。

**返回列(10 列)**:`trade_date / ts_code / name / rzye / rqye / rzmre / rqyl / rzche / rqchl / rqmcl / snap_ts`

---

### 4.27 `get_cyq_perf` — 筹码胜率(每日)

**用途**:每日每只股票的**历史成本分布 + 胜率**(用于看套牢盘 / 获利盘比例)。

**签名**:
```python
def get_cyq_perf(
    ts_code: Optional[str] = None,
    ts_codes: Optional[list] = None,
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_winner_rate: Optional[float] = None,
    max_winner_rate: Optional[float] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:**必传 ts_code** + 日期必传。

**返回列(6 列)**:`ts_code / trade_date / winner_rate(胜率%) / cost_50 / cost_85(85% 持仓成本) / snap_ts`

---

### 4.28 `get_major_news` — 头条/重要新闻

**用途**:与 `get_news`(普通 9 源聚合)区分,头条新闻是**重大事件级**(政策/财报/突发)。

**签名**:
```python
def get_major_news(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    trade_date: Optional[str] = None,
    src: Optional[Union[str, list]] = None,
    limit: int = 5000,
    offset: int = 0,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式约束**:必传日期(单日 / 多日);`src` 支持 list 循环(同 `get_news`)。

**返回列**:`datetime / src / title / content / channels / score / md5 / snap_ts`(同 `get_news`)

---

### 4.29 `get_cctv_news` — CCTV 新闻

**用途**:CCTV 联播 / 朝闻天下等官方新闻内容。

**签名**:
```python
def get_cctv_news(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    trade_date: Optional[str] = None,
    content: Optional[str] = None,
    source: str = "database",
) -> pd.DataFrame:
```

**online 模式**:**当前无 tushare / ths 在线源**,`source='online'` 返回空 DataFrame + WARN 日志(同 `get_week / get_month`)。

**返回列**:`trade_date / date / title / content / snap_ts`

---

## 5. 设计原则与约定

### 5.1 接口 1:1 镜像 offline_db_client 签名

data_provider 的 outer 函数签名**完全镜像** `offline_db_client` 的对应函数,**只加一个 `source` 参数**。这样上层代码从 db 切到 online 不需要改函数名/参数名。

| 例子 | offline_db_client | data_provider |
|---|---|---|
| 函数名 | `get_day` | `get_day`(同名) |
| 参数 | `(ts_code, ts_codes, start_date, end_date, trade_date, qfq, columns, conn)` | `(ts_code, ts_codes, start_date, end_date, trade_date, qfq, source)` |
| 返回值 | `pd.DataFrame` | `pd.DataFrame` |

**`columns / conn` 参数**没暴露给 data_provider(内部使用固定列 + 默认连接)。

### 5.2 online 模式不支持的能力(明确返回空/报错)

| 接口 | 限制 | 行为 |
|---|---|---|
| `get_week / get_month` | tushare.weekly/monthly 需高积分,未启用 | `source="online"` → 空 DataFrame + WARN 日志 |
| `get_day online qfq=True` | tushare.daily 不支持复权 | 用 `daily_qfq_range`(全市场拉 + 客户端过滤)|
| `get_adj_factor / stk_limit / moneyflow online` | tushare **不支持全市场查询** | 无 ts_code → ValueError |
| `get_kpl_list online` | 必传日期 | 无日期 → ValueError |
| `get_kpl_limit_performance online` | 必传日期 + ≤ 30 个交易日 | 超 30 → ValueError |

### 5.3 schema 对齐:`_align_cols`

db 和 online 返回的列可能有差异(比如 online 缺 `snap_ts`),`_align_cols(df, target_cols)` 自动:
- online 多的列保留(可能有数据)
- online 少的列填 `None`(保持 schema 一致,避免上层代码 KeyError)

### 5.4 单例 client

`_get_tushare() / _get_kpl() / _get_tdx()` 都是**全局 lazy 单例**(整个进程共享一个 client 实例)。第一次调用时初始化(读 token / 创建 session / 设置限流器),后续直接复用。

---

## 6. 错误处理

| 场景 | 行为 |
|---|---|
| source 未知值 | `ValueError(未知 source: 'xxx',只支持 'database' / 'online')` |
| db 模式没数据 → fallback online | **2026-09-18 新行为**:正常 fallback;但若 online 校验失败(如无日期),报清晰错误 |
| online 必传参数缺失 | `ValueError(<接口名> 必须传 <参数> 或 <参数>)` |
| online 多日 + 多 ts_code(笛卡尔积) | `ValueError(多 ts_code 跟多 trade_date 不能同时)` |
| online 调用次数 > 30 | `ValueError(超过限制 30)` |
| online 接口 HTTP 失败 | `Exception` 上抛(由 `_get_xxx().call` 内部 with_retry 处理)|

---

## 7. 测试与验证

`data_provider.py` 自身无单元测试,但通过以下方式验证:
- **回归测试**:`test_full_matrix.py`(已并入 git 历史)—— 19 接口列名 + 行数对比
- **多日测试**:4 个 online 接口(分钟/指数分钟/ticks/涨停表现)的拆日循环行为
- **today 路由测试**:`get_kpl_limit_performance` 今日走实时接口
- **多值测试**:`get_day / get_kpl_list / get_news` 的 ts_codes / tags / src 列表循环

---

## 8. 相关文档

| 文档 | 作用 |
|---|---|
| `offline_db_client.md`(在 `offlineDataManager/代码详细设计/`)| db 端 70 个接口详细说明(每个函数的参数、含义、SQL 实现) |
| `tushare_client.md` | online Tushare 21 个方法 |
| `kpl_client.md` | online KPL 23 元素数组 + 限流 + IP fallback |
| `tdx_client.md` | online 通达信 pytdx(分钟 / ticks / 5 档) |

---

## 9. 变更历史

| 日期 | 版本 | 变更 |
|---|---|---|
| 2026-09-17 | v0.1 | 初创,19 个接口 + 19 接口测试矩阵 + 8 个 bug 修复 |
| 2026-09-18 | v0.5 | `get_minute / get_minute_index / get_ticks / get_kpl_limit_performance` 加 online 多日支持(拆日循环 + ≤ 30 限制 + 交易日历过滤) |
| 2026-09-18 | v0.6 | `get_kpl_limit_performance` today 走实时接口(用户自加 `fetch_realtime_limit_performance`) |
| 2026-09-18 | v0.7 | `get_basic / get_index_basic` 透传 `exchange / market / publisher`;`get_kpl_list / get_news` 支持 list 参数;`get_day` online ts_codes 走逗号分隔;`get_adj_factor / stk_limit / moneyflow / kpl_concept_cons` 循环 ts_codes;**online 不支持 ts_code 必传**守卫;`get_week / get_month` online 模式直接返空 |
| 2026-09-18 | v1.0 | 写本文档(18 个接口) |
| 2026-09-18 | v1.1 | **新增 12 个扩展接口**:`get_suspend / get_top_list / get_top_inst / get_block_trade / get_ggt_daily / get_hsgt_top10 / get_limit_list / get_margin / get_margin_detail / get_cyq_perf / get_major_news / get_cctv_news`;tushare_client 加 `major_news` wrapper;文档加章节 3.1/3.2/4.18~4.29;测试套件扩到 **199 个断言** |
