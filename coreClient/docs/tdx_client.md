# tdx_client.md

> **版本**:v1.2(2026-09-26 新增:原生历史15分钟OHLC + 专用服务器failover)
> **v1.1**(2026-09-19 更新:buyorsell 字段权威定义 + seqId 字段语义变更)
> **v1.0**(2026-09-15 新增)
> **文件路径**:`~/TradingAgent/coreClient/tdx_client.py`
> **配置**:`~/TradingAgent/coreClient/tdx_config.py`
> **目的**:通达信 pytdx 客户端封装 + 批量 + IP 池 failover,跨业务复用

---

## 1. 功能

**为什么需要这个**:
- hithink-finance 没有 5档盘口、没有内/外盘
- pytdx 原生支持 `get_security_quotes`(完整 5档盘口 + s_vol/b_vol)
- pytdx 还支持 `get_minute_time_data`(实时 1分钟 K,不需要传 date)
- pytdx 还支持指数 5档(替代 hithink-finance `index.snapshot`)

**提供**:
- **类类**:`TdxClient`(单 IP 连接 + IP 池 failover + 5档盘口 / 1分钟 K / 历史 ticks)
- **模块级 helpers**:`market_of / code_of / _date_int_to_str / _minute_index_to_datetime / _ticks_time_to_datetime`
- **涨停/炸板判断**:`is_sealed(quote) / is_broken_seal(quote)`
- **数据时间戳打**:`_stamp_data_timestamp(items, field=...)`

---

## 2. 批量限制与性能(实测 2026-09-09)

| 接口 | 批量限制 | 单 IP 性能 |
|---|---|---|
| `get_security_quotes`(5档盘口) | **最多 80 只/次**(超限截断) | 80 只 = 15-25ms |
| `get_minute_time_data`(1分钟K) | **1 只/次**(无批量) | 1 只 = 13ms(~70 票/秒串行) |
| `get_security_bars`(历史15分钟OHLC) | **800 根/页、1只/次** | 约50个交易日/页 |
| 指数 5档 | **80 只/次** | 4 指数 = 20ms |
| `get_history_transaction_data`(分笔) | 2000 笔/页 | 自动分页(逆序拼接 + 保险排序) |

**可用 IP 池**(2026-09-09 实测 TCP 通):见 `tdx_config.TDX_IP_POOL`(6 个 IP,顺序随机)

**单 IP 失败**:自动 `try_failover()` 切下一个,最多试 `TDX_RETRY_MAX=3` 个 IP。

---

## 3. 接口

### 3.1 模块级 helper:ts_code 解析 + datetime 计算

#### `market_of(ts_code: str) -> int`

ts_code → pytdx market 编码(SH=1, SZ=0, BJ=2)。

```python
market_of("000001.SZ")   # → 0
market_of("600519.SH")   # → 1
market_of("920001.BJ")   # → 2
```
**Raises**:`ValueError`(无 `.` 后缀 / 未知市场)。

---

#### `code_of(ts_code: str) -> str`

ts_code → 6 位数字代码。

```python
code_of("000001.SZ")   # → "000001"
```

---

#### `_date_int_to_str(date_int: int) -> str`

`YYYYMMDD int` → `'YYYY-MM-DD'` 字符串。

```python
_date_int_to_str(20260512)   # → "2026-05-12"
```

---

#### `_minute_index_to_datetime(idx: int, date_int: int) -> str`

1分钟 K 线索引 0-239 → `'YYYY-MM-DD HH:MM:SS'`。

**索引规则**(2026-09-12 用户确认):
- `0-119` = `09:31-11:30`(上午 120 根)
- `120-239` = `13:01-15:00`(下午 120 根)

pytdx 不返回时间,客户端按序号推算。**不完整的 K 线(数据缺失)只填实有的索引,不会补"缺失"的时间。**

```python
_minute_index_to_datetime(0, 20260512)    # → "2026-05-12 09:31:00"
_minute_index_to_datetime(120, 20260512)  # → "2026-05-12 13:01:00"
```

---

#### `_ticks_time_to_datetime(time_str: str, date_int: int) -> str`

ticks `time` 字段 `'HH:MM'` → `'YYYY-MM-DD HH:MM:00'`。

pytdx tick time 只到分钟,秒钟补 `00`。

```python
_ticks_time_to_datetime("10:31", 20260512)   # → "2026-05-12 10:31:00"
```

---

### 3.2 类:`TdxClient`

#### `__init__(ip=None, port=TDX_DEFAULT_PORT)`

连接一个 IP。`ip=None` 时从池里随机挑一个起点。

```python
client = TdxClient()                        # 随机 IP
client = TdxClient(ip="180.153.18.170")      # 指定 IP(测试用)
```

---

#### `try_failover() -> bool`

当前 IP 失败时换下一个 IP 重连。成功返回 `True`。

**调用方约定**:不要手动 try/except 切 IP,直接调此方法。

---

#### `disconnect() -> None`

断开连接(异常吞掉)。

---

### 3.3 5档盘口

#### `get_orderbook(codes: List[Tuple[int, str]]) -> list[dict]`

批量拉 5档盘口。**最多 `TDX_QUOTE_BATCH_MAX=80` 只/批**,超过截断。

**2026-09-11**:
- 返回空时自动 failover 到下一个 IP 重试
- 每条 quote 都打上 `data_timestamp = orderbook_timestamp`(数据时间戳)

**输入**:`[(market, code), ...]`,例:`[(0, "000001"), (1, "600519")]`。

**返回**:list of dict(pytdx quote 字段,已打 `orderbook_timestamp` 时间戳)。

---

#### `get_orderbook_batched(codes: List[Tuple[int, str]], batch_size=TDX_QUOTE_BATCH_MAX) -> list[dict]`

自动分批拉 5档盘口,处理 > 80 只的情况。

```python
quotes = client.get_orderbook_batched([(0, "000001"), (1, "600519")] * 200, batch_size=80)
# 内部自动分 3 批拉
```

---

#### `get_index_orderbook(index_codes: List[Tuple[int, str]]) -> list[dict]`

拉指数 5档盘口(本质上是 `get_orderbook` 的别名)。

**已验证的指数代码**(见 `tdx_config.INDEX_CODES`):
- `(1, '000001')` 上证指数
- `(0, '399001')` 深圳成指
- `(0, '399006')` 创业板指
- `(1, '000688')` 科创50

---

### 3.4 1分钟 K 线

#### `get_minute_kline(ts_code: str) -> list[dict]`

**拉 1 只 1 天的 1分钟 K 线**(共 240 根)。

**2026-09-13 v4 改造**:改用 pytdx 实时分时接口 `get_minute_time_data(market, code)`,**不需要传 date**(用户原话:"实时分时图不需要传 trade date")。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `ts_code` | str | 必 | `"000006.SZ"` 形式 |

**返回字段**:
```
ts_code        str   "000006.SZ"
trade_date     str   "2026-09-13"(今天)
datetime       str   "2026-09-13 09:31:00"
time_idx       int   0-239
price          float
vol            int
data_timestamp str   "2026-09-13T01:25:15.847" (毫秒,2026-09-15 改造)
```

**盘中**:已走的分钟数(0..N,N < 240);**收盘后**:全 4 小时约 240 根。

---

#### `get_history_minute(ts_code: str, date: int) -> list[dict]`

**拉 1 只 1 天的历史 1分钟 K 线**(共 240 根)。

**2026-09-13 新增**:对齐 `policy_minute.db` 落盘表的 6 列字段(不带 `data_timestamp`,`data_gen.py` 自己加 `created_at`)。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `ts_code` | str | 必 | 6 位代码 + 市场后缀 |
| `date` | int | 必 | `YYYYMMDD` 整数,例 `20260911` |

**失败时返回空 list**。

---

#### `download_one_minute(ts_code, trade_date=None, start_date=None, end_date=None, max_pages=40) -> list[dict]`

离线完整OHLC数据库专用入口。调用
`get_security_bars(KLINE_TYPE_1MIN, ...)`按800根分页，返回：

`ts_code / trade_date / datetime / time_idx / open / high / low / close / vol / amount`

标准时段为09:31—11:30、13:01—15:00，共240根，`time_idx=0..239`；
价格不复权，成交量单位为股，成交额单位为元。服务端历史深度没有协议保证，
2026-09-26在三台历史服务器实测最近约24,000根，即100个交易日。

该接口不同于`get_history_minute()`：后者只有每分钟价格和成交量，不能提供完整
OHLC；完整1分钟Parquet的TDX替换和日更必须使用`download_one_minute()`。

---

### 3.5 历史15分钟OHLC

离线数据库下载入口使用`download_fifteen_minute()`；它与
`get_history_15min()`返回相同字段和时间口径，仅用于明确区分“联网下载”与
“数据库查询”。

#### `get_history_15min(ts_code, trade_date=None, start_date=None, end_date=None, max_pages=20) -> list[dict]`

调用 pytdx 原生 `get_security_bars(KLINE_TYPE_15MIN, ...)`，按800根一页向历史
翻页。`trade_date` 与日期范围互斥；不传日期时返回最近一页。

```python
client = TdxClient()
rows = client.get_history_15min("000001.SZ", trade_date=20260924)
# 16根：09:45、10:00……11:30、13:15……15:00
```

返回字段：`ts_code / trade_date / datetime / time_idx / open / high / low /
close / vol / amount`。价格是不复权口径，`vol`单位为股，`amount`单位为元。
09:45第一根包含09:30集合竞价。

旧行情IP可能能连接但返回空K线。第一页为空时，客户端会自动切换到
`TDX_HISTORY_BAR_IP_POOL`中经过实测的历史K线服务器。服务端保留深度没有协议
保证；2026-09-26对平安银行实测最早到2024-09-04，调用方必须检查是否覆盖请求
起点。

### 3.6 历史分笔成交(ticks)

#### `get_history_ticks(ts_code: str, date: int, max_pages=TDX_TICKS_MAX_PAGES) -> list[dict]`

**拉 1 只 1 天的全部分笔成交**(自动分页 + 去重)。

**pytdx 限制**:
- 每次最多 2000 笔(从 `start` 开始)
- 1 只 1 天,无批量
- 客户端按 `start` 循环分页拉

**2026-09-13 修正**:pytdx 服务器返回的 ticks 是**逆序分页**——
- `start=0`    → 最新成交(下午/收盘前)
- `start=2000` → 较早成交(上午)
- `start=4000` → 0 笔(超出范围)

每页**内部**是时间正序,但**页与页之间**是逆序(先拼 `start=大`,再拼 `start=小`)。最后做一次保险排序(防止个别页面内部出现乱序)。

**字段自动补全**:
```
ts_code              str    "000006.SZ"
trade_date           str    "2026-05-12"
datetime             str    "2026-05-12 10:31:00"(time 字段补 ":00")
seqId_in_minute      int    同 (ts_code, trade_date, time) 分钟内自增(0, 1, 2, ...)
time                 str    pytdx 原始 "HH:MM"
price                float
vol                  int
buyorsell            int    0=买盘 / 1=卖盘 / 2=中性或撮合 / 5=未知(待挖掘) / 8=集合竞价
```

> **字段语义变更**:
> - `seqId` → `seqId_in_minute`(2026-09-19):含义从"全局当日序号"改为"同 (ts_code, trade_date, time) 分钟内单股序号"。
>   旧调用方若需要"当日全局序号",请用 `enumerate(ticks)` 或 DB 端 `ROWID`。
>   详细背景见 `docs/落库方案_v2.md`。
>
> - `buyorsell` 取值集合(2026-09-19 更新):0/1/2/8 来自 pytdx 生态一手文档;5 至今未找到一手来源,标记为"未知,待挖掘"(详见 § 9)。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `ts_code` | str | 必 | 6 位代码 + 市场后缀 |
| `date` | int | 必 | `YYYYMMDD` 整数 |
| `max_pages` | int | `TDX_TICKS_MAX_PAGES=50` | 分页上限(50 = 100k 笔,极端涨停日防呆) |

**失败时返回空 list**(已打 `logger.warning`)。

---

### 3.7 涨停/炸板判断

#### `is_sealed(quote: dict) -> bool`

判断是否**封板**(`ask1=0` 且 `bid1>0`)。

```python
if is_sealed(quote):
    # 封死涨停
```

---

#### `is_broken_seal(quote: dict) -> bool`

判断是否**炸板**(涨停价位但有卖盘)。

```python
if is_broken_seal(quote):
    # 涨停后开板
```

---

### 3.8 数据时间戳打:`_stamp_data_timestamp(items, *, field="orderbook_timestamp") -> list`

给 pytdx 返回的每条数据打上数据时间戳(unix 秒 float + ISO 字符串)。

**简化原则**(用户 2026-09-11 决定):
- 数据源自带的字段原样保留,**不解析**
- 统一在 client 拿到数据的瞬间打 `<field>`(wall clock)

**输出字段**:
- `<field>`              浮点秒(unix,client 拿到数据的瞬间)
- `<field>_iso`          ISO 格式("2026-09-15T13:36:17.847")

---

## 4. 用法示例

### 4.1 拉 5档盘口(单只)

```python
from coreClient.tdx_client import TdxClient, market_of, code_of

client = TdxClient()
codes = [(market_of("000001.SZ"), code_of("000001.SZ"))]
quotes = client.get_orderbook(codes)
print(quotes[0]["bid1"], quotes[0]["ask1"])
client.disconnect()
```

### 4.2 批量拉 5档盘口

```python
ts_codes = ["000001.SZ", "600519.SH", "002594.SZ"]
codes = [(market_of(c), code_of(c)) for c in ts_codes]
quotes = client.get_orderbook_batched(codes)  # 自动分批(每 80 只一批)
```

### 4.3 实时 1分钟 K 线

```python
bars = client.get_minute_kline("000006.SZ")
print(f"已走 {len(bars)} 分钟,最新一根: {bars[-1]['datetime']} price={bars[-1]['price']}")
```

### 4.4 历史15分钟OHLC

```python
from coreClient.tdx_client import TdxClient

client = TdxClient()
bars = client.get_history_15min(
    "000001.SZ", start_date="2026-09-01", end_date="2026-09-24"
)
client.disconnect()
```

### 4.5 历史 ticks(分笔成交)

```python
ticks = client.get_history_ticks("000006.SZ", date=20260512)
print(f"共 {len(ticks)} 笔分笔成交")
print(f"首笔: {ticks[0]['datetime']} {ticks[0]['price']}")
print(f"末笔: {ticks[-1]['datetime']} {ticks[-1]['price']}")
```

### 4.6 4 大指数 5档

```python
from coreClient.tdx_client import INDEX_CODES
quotes = client.get_index_orderbook(INDEX_CODES)
for q in quotes:
    print(q["code"], q["price"], q["vol"])
```

### 4.7 涨停/炸板判断

```python
quote = client.get_orderbook([(0, "000006")])[0]
if is_sealed(quote):
    print("封板!")
elif is_broken_seal(quote):
    print("炸板!")
```

---

## 5. config 文件说明:tdx_config.py

**位置**:`~/TradingAgent/coreClient/tdx_config.py`

| 配置项 | 值 | 说明 |
|---|---|---|
| `TDX_IP_POOL` | `[(ip, port), ...]` 6 个 IP | pytdx 服务器 IP 池(2026-09-11 实测),failover 顺序随机 |
| `TDX_DEFAULT_PORT` | `7709` | pytdx 默认端口 |
| `INDEX_CODES` | `[(1, "000001"), (0, "399001"), (0, "399006"), (1, "000688")]` | 4 大指数代码(用户指定) |
| `INDEX_NAMES` | dict | 4 大指数中文名 |
| `TDX_RETRY_MAX` | `3` | 连接/拉数据失败重试次数 |
| `TDX_RETRY_SLEEP` | `0.5` | 重试基础间隔(秒,实际 `sleep = TDX_RETRY_SLEEP * (attempt + 1)`) |
| `TDX_TIMEOUT` | `30` | socket 超时(秒) |
| `TDX_QUOTE_BATCH_MAX` | `80` | `get_security_quotes` 批量上限 |
| `TDX_MINUTE_BATCH_MAX` | `1` | `get_minute_time_data` 批量上限(单只) |
| `TDX_TICKS_PAGE_SIZE` | `2000` | ticks 分页大小 |
| `TDX_TICKS_MAX_PAGES` | `50` | ticks 最大分页数(防呆,极端涨停日 100k 笔足够) |
| `TDX_TICKS_RETRY_BACKOFF` | `[1, 2, 4]` | ticks 分页失败退避(秒) |
| `TDX_MINUTE_BARS_PER_DAY` | `240` | 每天 1分钟 K 总根数 |
| `TDX_MORNING_START / END` | `"09:31" / "11:30"` | 上午开盘/收盘时间 |
| `TDX_AFTERNOON_START / END` | `"13:01" / "15:00"` | 下午开盘/收盘时间 |
| `TDX_MORNING_BARS / AFTERNOON_BARS` | `120 / 120` | 上午/下午 K 线根数 |

---

## 6. 使用方

| 业务 | 文件 | 用途 |
|---|---|---|
| **onlineDataManager** | `scripts/service/service_writeredis_orderbook.py` | 拉 5档盘口 → 写 Redis(`orderbook_window`) |
| **onlineDataManager** | `scripts/service/service_writeredis_minute.py` | 拉 1分钟 K 线 → 写 Redis(`minute_window`) |
| **onlineDataManager** | `scripts/core/watchlist_fetch.py` | 拉指数 5档 + 个股 5档 → 写 watchlist |
| **onlineDataManager** | `scripts/core/ths_client.py`(_stamp_snapshot_timestamp 借鉴)| 复用数据时间戳打模式 |
| **offlineDataManager**(历史 ticks 任务) | `scripts/...agent4_ticks.py` | 拉历史分笔成交(2026-09-12 起封装到此) |

---

## 7. 关键设计原则

1. **IP 池 + 自动 failover**:单 IP 连不上/不出数据时自动切下一个
2. **逆序分页 + 保险排序**:ticks 处理 pytdx 逆序分页的特殊行为
3. **时间戳字段命名**:每种数据有自己的字段(`orderbook_timestamp` / `minute_timestamp`),`_stamp_data_timestamp` 支持自定义
4. **批量自动分页**:`get_orderbook_batched` 处理 > 80 只的情况,调用方不用关心
6. **2026-09-13 v4 实时分时去 date**:实时分时不需要传 trade date(用户原话)

---

## 9. `buyorsell` 字段权威定义(2026-09-19 新增)

### 9.1 背景

`ticks` 接口返回的每条记录带一个 `buyorsell` 整数,原 doc 仅写"0/1/2"三个值。
2026-09-19 矩阵验收实测发现:服务端**实际还会返回 `5` 和 `8`**(详见 `docs/落库方案_v2.md` / `scripts/tdx_matrix_validation_v1.json`)。
之前的文档不完整,本节给出**有据可查**的权威定义。

### 9.2 取值含义(权威)

| 值 | 含义 | 来源等级 |
|---|---|---|
| `0` | 买盘(主动买入) | 一手文档:`xmtdx` + `easy_tdx` README |
| `1` | 卖盘(主动卖出) | 一手文档:`xmtdx` + `easy_tdx` README |
| `2` | 中性 / 撮合(同价位同方向合并显示) | 一手文档:`xmtdx` + `easy_tdx` README |
| `8` | 集合竞价(开盘或收盘前撮合) | 一手文档:`xmtdx` + `easy_tdx` README |
| `5` | **未知,待挖掘**(本项目实测:盘后 15:00 后出现 20 条,vol>0,疑似收盘后协议场成交,但**无一手文档背书**) | 推断 |

**一手来源**:
- [`xmtdx` PyPI README](https://pypi.org/project/xmtdx/)(pytdx 的 Rust 重写版,字段定义同 pytdx)
- [`easy_tdx` GitHub examples](https://github.com/handsomejustin/easy_tdx/blob/main/examples/05_transaction/transaction_data.py)

**为什么一手来源可信**:
- `xmtdx` 是 pytdx 协议的"兼容性逆向实现",基于"抓包和真实服务器交叉验证"(其 README 原话)
- `easy_tdx` 是 pytdx 的 Python 衍生库,直接引用 pytdx 协议语义
- 两个独立项目给出**完全一致**的 4 个值(0/1/2/8)

### 9.3 服务端分组差异(2026-09-19 实测)

**TDX 后端有两套服务器,行为不同**:

| 组 | 服务器 | buyorsell 返回 |
|---|---|---|
| **A 组**(原样给) | `180.153.18.170`、`60.12.136.250`、`115.238.56.198` | `{0, 1, 2, 5, 8}`(5 个值全给) |
| **B 组**(清洗后给) | `123.125.108.14`、`218.6.170.47`、`123.125.108.90` | `{0, 1, 2, 5}`(**过滤 8**)+ 盘前盘后零量条目一并过滤 |

**含义**:
- B 组维护者认为 `8` 是"集合竞价条目"(开盘前零量),不该进正常成交统计
- B 组也过滤了**盘前零量**(vol=0, time < 09:30)— 同样视为"非主成交"
- **两组的累计成交量(只看 vol>0)完全一致**,所以业务策略如只关心"主成交",走 B 组更省清洗
- A 组保留了**所有**服务端原始条目,适合需要审计/回放的场景

### 9.4 当前 wrapper 行为

- **不映射、不删除**任何 `buyorsell` 值 — 原样透传
- 落库侧由 `tbl_tick_v2` 的 PK `(ts_code, trade_date, time, seqId_in_minute)` 保证幂等,不同 `buyorsell` 的同分钟成交不会被合并
- 旧 `tbl_tick` 表因 PK 包含 wrapper 自产 `seqId`,有"重抓错位"风险,详见 `docs/落库方案_v2.md`

### 9.5 `buyorsell=5` 待挖掘事项(TODO)

| 事项 | 状态 |
|---|---|
| 找一手文档(通达信官方 / pytdx 源码注释 / 通达信客户端反编译) | **未做** |
| 在 pytdx 1.72 源码搜 `buyorsell` 上下文 | **未做** |
| 跨交易日/跨股票验证 5 的出现规律(全是盘后?大宗交易?) | 部分:本机 2026-08-27 / 2026-09-17 × 000001.SZ / 000006.SZ × 6 主机矩阵显示 5 全在 time > 15:00,且 vol>0 — 但样本太小不能下结论 |
| 给 buyorsell=5 加标注字段 `buyorsell_note='unknown_5'` | **不做**(保持现状 = 原样透传) |
| 像 B 组那样直接过滤 buyorsell=5 | **不做**(未确认语义前擅自过滤可能误删) |

**挖掘原则**: 找到一手文档前,**不擅自映射、不擅自删除、不擅自标注**。

---

## 8. 验证清单

- [x] `tdx_client.py` 现有代码 + 文档完整对齐(2026-09-15)
- [x] 5个接口族(5档盘口 / 1分钟 K / 历史 ticks / 指数 / 涨停判断)全部文档化
- [x] 4 个 module级 helper(market_of / code_of / _date_int_to_str / _minute_index_to_datetime / _ticks_time_to_datetime)完整说明
- [x] 16 项 config 配置全部说明
- [x] onlineDataManager / offlineDataManager 使用方文档化
- [x] 批量限制 + 性能数据(2026-09-09 实测)有说明
- [x] ticks 逆序分页特殊行为有文档化
