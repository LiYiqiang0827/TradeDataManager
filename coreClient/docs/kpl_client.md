# kpl_client.md

> **版本**:v1.0(2026-09-15 新增)
> **文件路径**:`~/TradingAgent/coreClient/kpl_client.py`
> **配置**:`~/TradingAgent/coreClient/kpl_config.py`
> **目的**:开盘啦(kpl)HTTP API 客户端封装,单例模式 + 限流 + 重试 + IP 直连 fallback

---

## 1. 功能

**为什么需要这个**:
- 开盘啦 App 内部使用的 HTTP API(`apphwhq / apphis / applhb / apphq` 4 个子域名)
- 提供 A 股独有的特色数据:**涨停基因 / 涨停表现 / 龙虎榜 / 涨跌统计**(同花顺没有)
- 单例模式(整个进程共享一个 `pro_api` / `requests.Session`)
- 限流(30 次/分钟,保守值)
- 网络错误自动重试(指数退避)
- **IP 直连 fallback**:国内 DNS 屏蔽场景(`aphis / applhb` 域名常被屏蔽)→ 用预解析的 IP + Host 头直连

**最常用的 6 个方法**(用户场景):
1. `market_sentiment()`             涨跌统计
2. `limit_up_performance(date, board_type)`  涨停表现详情(1-5 板)
3. `daily_limit_index()`            涨停板数(实时/历史)
4. `lhb_stock_list(date)`           龙虎榜股票
5. `stock_trend(stock_id, day)`     个股分时
6. `zt_gene(stock_id)`              涨停基因

**底层通用调用**:`.call(c, a, host=..., **params)` 可访问任何 endpoint(参考 trading:kpl-api skill)

---

## 2. 接口

### 2.1 工具函数(模块级)

#### `add_exchange_suffix(ts_code: str) -> str`

给 6 位股票代码补全交易所后缀(无后缀 → `.SH / .SZ / .BJ`)。

**规则**(2026-09-13 验证,数据源 `db_cn_basic.db:tbl_cn_basic` 5562 只 L 状态股票全量扫描):

| 前缀 | 后缀 | 板块 |
|---|---|---|
| `000 / 001 / 002 / 003` | SZ | 深主板 |
| `300 / 301 / 302` | SZ | 创业板 |
| `600 / 601 / 603 / 605` | SH | 上主板 |
| `688 / 689` | SH | 科创板 |
| `920` | BJ | 北交所 |

**示例**:
```python
add_exchange_suffix("000978")       # → "000978.SZ"
add_exchange_suffix("688783.SH")    # → "688783.SH"(不变)
add_exchange_suffix("920001")       # → "920001.BJ"
```

**Raises**:`ValueError`(无法识别前缀时,可能是 ETF/退市股票)

---

#### `add_exchange_suffix_series(series: pd.Series) -> pd.Series`

批量给 `pd.Series` 补全后缀(向量化)。

```python
df["ts_code"] = add_exchange_suffix_series(df["code"])
```

---

#### `EXCHANGE_SUFFIX_RULES: dict[str, str]`

完整规则表(13 条规则)。

---

### 2.2 类:`KPLClient`(单例)

#### `__new__()` — 单例模式

```python
client = KPLClient()     # 第一次调用 → 创建实例 + _init()
client2 = KPLClient()    # 第二次调用 → 返回同一个实例
assert client is client2
```

#### 通用调用:`call(c, a, host=KPL_HOST_APPHWHQ, **params) -> Union[dict, list]`

**通用调用入口**(带限流 + 重试)。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `c` | str | 必 | controller(模块名),服务器靠这个路由 |
| `a` | str | 必 | action(具体操作) |
| `host` | str | `KPL_HOST_APPHWHQ` | 子域名 |
| `**params` | dict | — | 业务参数(`Token + UserID` 自动加 + 公共参数自动加) |

**返回值**:`dict / list`(API 返回的 JSON;关键字段是 `info` 不是 `list`)

**示例**:
```python
client.call("HomeDingPan", "MarketStockZDNum")     # 涨跌统计
client.call("HisHomeDingPan", "DailyLimitPerformance", Day="2026-09-11", PidType=1)  # 历史涨停表现
```

**备注**:`aphis / applhb` 等子域名在中国大陆常被 DNS 屏蔽或被代理干扰,会自动回退到 IP 直连 + Host 头(见 `_call_via_ip`)。

---

#### 涨跌统计:`market_sentiment() -> dict`

endpoint:`HomeDingPan/MarketStockZDNum`(apphwhq)

返回:涨跌统计 dict(涨停/跌停家数、涨家数/跌家数)。

---

#### 涨停天梯:`limit_ladder() -> dict`

endpoint:`HomeDingPan/DailyLimitIndex`(apphwhq, 实时)

**返回**:
```python
{
    "errcode": "0",
    "data": {
        "一板": 33,
        "二板": 4,
        "三板": 2,
        "四板": 1,
        "更高板": 0,
        "总计": 40,
    },
    "raw_info": [33, 4, 2, 1, 0],
}
```

---

#### 涨停板数:`daily_limit_index(date=None) -> dict`

endpoint:`HomeDingPan/DailyLimitIndex`(实时,apphwhq) / `HisHomeDingPan/DailyLimitIndex`(历史,apphis)

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `date` | str \| None | `None` | `None` = 实时(默认);`YYYY-MM-DD` = 历史某日 |

---

#### 涨停表现详情:`limit_up_performance(date, board_type=1) -> pd.DataFrame`

endpoint:`HisHomeDingPan/DailyLimitPerformance`(apphis, 历史)

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `date` | str | 必 | `YYYY-MM-DD`(例 `'2026-09-11'`) |
| `board_type` | int | `1` | 板数(1-5) |

**返回**:`pd.DataFrame`,23 列(见下)。

**特殊解析**:API 返回嵌套数组结构(`[[s1, s2, ...]]`),需要 `_parse_limit_performance` 解析。

---

#### 涨停表现(实时):`fetch_realtime_limit_performance(...)`

实时版本的涨停表现(如果 API 提供)。

---

#### 涨停表现(批量):`get_daily_limit_performance(...)`

批量版涨停表现,按日期循环。

---

#### 龙虎榜:`lhb_stock_list(date) -> pd.DataFrame`

endpoint:`LongHuBang/GetStockList`(applhb)

返回:龙虎榜股票 DataFrame(列:`ts_code / name / buy_amount / sell_amount / net_amount / ...`)。

---

#### 个股分时:`stock_trend(stock_id, day) -> pd.DataFrame`

endpoint:`StockL2Data/GetStockTrend`(apphwhq)

| 参数 | 类型 | 说明 |
|---|---|---|
| `stock_id` | str | 6 位股票代码(**无后缀**),例 `'600104'` |
| `day` | str | `YYYY-MM-DD` |

返回:DataFrame(列:`time / price / volume / amount / ...`)。

---

#### 涨停基因:`zt_gene(stock_id) -> dict`

endpoint:`StockL2Data/GetZhangTingGene`(apphwhq)

返回:dict(题材/概念/涨停原因)。

---

### 2.3 LIMIT_PERFORMANCE 23 元素映射

`limit_up_performance` 返回的嵌套数组每个元素是 23 元素 list,对应字段如下:

| 索引 | 字段 | 类型 | 说明 |
|---|---|---|---|
| `[0]` | `ts_code` | str | 股票代码 |
| `[1]` | `name` | str | 股票简称 |
| `[2]` | `tag` | int | 标记(恒为 0,可能是保留字段) |
| `[3]` | `remark` | str | 备注 |
| `[4]` | **`lu_time`** | int | **涨停时间(unix timestamp;离线库 "13:06:24")** |
| `[5]` | `theme` | str | 主题材简称 |
| `[6]` | `limit_order` | float | 涨停封单金额(元) |
| `[7]` | `lu_limit_order` | float | 涨停板封单金额(元) |
| `[8]` | `net_change` | float | 净额(元) |
| `[9]` | `main_in` | float | 主力流入(元) |
| `[10]` | `main_out` | float | 主力流出(元) |
| `[11]` | `amount` | float | 成交额(元) |
| `[12]` | `limit_reason` | str | 涨停原因全文(多个题材逗号分隔) |
| `[13]` | `free_float` | float | 流通市值(元) |
| `[14]` | `turnover_rate` | float | 换手率(%) |
| `[15]` | `board_count` | int | 板数(1-5) |
| `[16]` | `is_break` | int | 是否炸板(0=封死 1=曾炸开) |
| `[17]` | `amplitude` | float | 振幅(%) |
| `[18]` | `board_period` | str | `"X天Y板"` 字符串(一板时为空) |
| `[19]` | `theme_id` | str | 主题材 ID(6位字符串) |
| `[20]` | `sector_id` | int | 板块/分类 ID(数值) |
| `[21]` | `close_price` | float | 收盘价(元) |
| `[22]` | `pct_chg` | float | 涨幅(%) |

**注意**:
- `lu_time` 是 unix timestamp,**不能删**(用户原话"涨停时间非常重要的数据")
- 解析方法:`_parse_limit_performance(raw_info, date, board_type) -> DataFrame`

---

### 2.4 限流类:`RateLimiter`

**滑动窗口限流器**。

```python
RateLimiter(max_per_min=30)
acquire()  # 超限自动 sleep 到下个窗口起点
```

---

### 2.5 重试装饰器:`@with_retry`

**网络错误重试装饰器**(不重试 `JSONDecodeError` 等数据问题)。

**只对以下异常重试**:
- `requests.exceptions.ConnectionError`
- `requests.exceptions.Timeout`
- `requests.exceptions.HTTPError`

**退避**:`KPL_RETRY_SLEEP * (attempt + 1)`(第 1 次 2s,第 2 次 4s,第 3 次 6s)

---

### 2.6 IP 直连 fallback:`_call_via_ip(host, payload, headers)`

**国内 DNS 屏蔽场景的 fallback**(`aphis / applhb` 域名被屏蔽时)。

**预解析的 IP 表**(`_HOST_IP_FALLBACK`):

| 域名 | IP |
|---|---|
| `aphis.longhuvip.com` | `124.71.63.67` |
| `applhb.longhuvip.com` | `113.45.200.207` |
| `apphwhq.longhuvip.com` | `139.159.142.175` |
| `apphq.longhuvip.com` | `139.159.142.175` |

**实现**:用 IP 直连 URL + `Host` 头模拟域名 + 关掉 SSL 验证。

**调用方约定**:不要手动调用,`call()` 自动 fallback。

---

## 3. 用法示例

### 3.1 涨跌统计(市场情绪快照)

```python
from coreClient.kpl_client import KPLClient

client = KPLClient()
sentiment = client.market_sentiment()
print(f"涨停家数: {sentiment.get('info', [])[:5]}")
```

### 3.2 涨停天梯

```python
ladder = client.limit_ladder()
print(f"今日涨停天梯: {ladder['data']}")  # {"一板": 33, "二板": 4, ...}
```

### 3.3 涨停表现详情(2026-09-11 一板)

```python
df = client.limit_up_performance("2026-09-11", board_type=1)
print(f"一板 {len(df)} 只")
print(df[["ts_code", "name", "lu_time", "theme", "limit_order"]].head())
```

### 3.4 龙虎榜

```python
df = client.lhb_stock_list("2026-09-11")
print(f"龙虎榜 {len(df)} 只")
print(df[["ts_code", "name", "buy_amount", "sell_amount", "net_amount"]].head())
```

### 3.5 个股分时

```python
df = client.stock_trend("600104", "2026-09-11")
print(f"分时 {len(df)} 个数据点")
```

### 3.6 涨停基因(为什么涨停)

```python
gene = client.zt_gene("600104")
print(gene)  # {"题材": "AI", "概念": [...], "涨停原因": "..."}
```

### 3.7 补全股票代码后缀

```python
from coreClient.kpl_client import add_exchange_suffix

codes = ["000978", "600519", "688783", "920001"]
fixed = [add_exchange_suffix(c) for c in codes]
# → ["000978.SZ", "600519.SH", "688783.SH", "920001.BJ"]
```

---

## 4. config 文件说明:kpl_config.py

**位置**:`~/TradingAgent/coreClient/kpl_config.py`

### 4.1 认证

```python
KPL_AUTH_TOKEN = "0"   # TODO: 替换为真实 token(未登录时用 0 占位)
KPL_USER_ID    = "0"   # TODO: 替换为真实 user_id
```

**自动读取**:`get_kpl_auth() -> tuple[str, str]` 优先从 `~/.kpl-api/.auth.json` 读,失败则用 `kpl_config` 硬编码。

**修改方式**:改 `~/.kpl-api/.auth.json` 即可生效(改 token 不需要改代码)。

### 4.2 API hosts

| 配置项 | 值 | 用途 |
|---|---|---|
| `KPL_HOST_APPHWHQ` | `"apphwhq.longhuvip.com"` | 实时盯盘 |
| `KPL_HOST_APPHIS` | `"apphis.longhuvip.com"` | 历史 |
| `KPL_HOST_APPLHB` | `"applhb.longhuvip.com"` | 龙虎榜 |
| `KPL_HOST_APPPHQ` | `"apphq.longhuvip.com"` | 行情/复盘 |

### 4.3 限流与重试

| 配置项 | 值 | 说明 |
|---|---|---|
| `KPL_RATE_LIMIT_PER_MIN` | `30` | 个人账号保守值(每分钟最多 30 次) |
| `KPL_RETRY_MAX` | `3` | 网络错误重试次数 |
| `KPL_RETRY_SLEEP` | `2` | 基础间隔(秒,实际 `sleep = KPL_RETRY_SLEEP * (attempt + 1)`) |

---

## 5. 使用方

| 业务 | 文件 | 用途 |
|---|---|---|
| **onlineDataManager** | `scripts/service/service_writeredis_zt.py` | 涨停表现详情 + 涨停基因 + 涨停天梯 → 写 Redis(`zt_detail` 等) |
| **onlineDataManager** | `scripts/service/service_writeredis_limitperformance.py` | 实时涨停表现详情 → 写 Redis |
| **onlineDataManager** | `scripts/service/service_writeredis_anomaly.py` | 涨跌统计 + 异动背景 → 写 Redis |
| **offlineDataManager** | 各类离线数据补齐脚本 | 涨停基因 / 涨停表现(历史) → 写 `db_cn_kpl.db` |
| **quant-strategy**(未来) | 题材轮动策略 | 涨停基因 + 题材成分 |

---

## 6. 关键设计原则

1. **单例模式**:整个进程共享一个 `requests.Session` + `RateLimiter`,避免重复初始化
2. **限流 + 重试分离**:`@with_retry` 只重试网络错误,不重试数据错误
3. **IP 直连 fallback**:国内 DNS 屏蔽是常态,fallback 机制自动透明处理
4. **`lu_time` 必须保留**:涨停时间 unix timestamp 是用户最高优先级数据
5. **后缀补全自动**:`add_exchange_suffix` + `add_exchange_suffix_series` 覆盖 5562 只股票(全量验证)
6. **23 元素数组映射**:`LIMIT_PERFORMANCE_FIELDS` 类常量,字段顺序固定
7. **实时 + 历史双接口**:`call(c, a, host=...)` 同一签名支持实时和历史(切 host)

---

## 7. Pitfall 备注

- **pitfall #13**:`aphis / applhb` 域名被 DNS 屏蔽时,自动 IP 直连 fallback(见 `_call_via_ip`)
- **pitfall #14**:`lu_time` 是 unix timestamp,**绝对不能删除或转字符串**(用户原话)
- **pitfall #15**:`limit_up_performance` 返回嵌套数组,必须用 `_parse_limit_performance` 解析,不能直接 `pd.DataFrame(result)`
- **pitfall #16**:DailyLimitPerformance 字段顺序固定(23 元素),不能用 `keys()` 假设

---

## 8. 验证清单

- [x] `kpl_client.py` 现有代码 + 文档完整对齐(2026-09-15)
- [x] 6 个最常用方法 + 5 个底层方法全部文档化
- [x] 23 元素数组字段映射完整(关键字段 `lu_time` 高亮标注)
- [x] 限流 + 重试 + IP fallback 三大机制有说明
- [x] 自动读取 `~/.kpl-api/.auth.json` 认证机制有说明
- [x] 13 条 EXCHANGE_SUFFIX_RULES 表格化
- [x] onlineDataManager / offlineDataManager / quant-strategy 使用方文档化
- [x] 4 个 pitfall 有备注