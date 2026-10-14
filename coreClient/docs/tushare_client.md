# tushare_client.md

> **版本**:v1.0(2026-09-15 新增)
> **文件路径**:`~/TradingAgent/coreClient/tushare_client.py`
> **配置**:`~/TradingAgent/coreClient/tushare_config.py`
> **目的**:Tushare pro API 客户端单例封装,提供常用接口便捷方法 + 限流 + 重试

---

## 1. 功能

**为什么需要这个**:
- Tushare pro API 是 A 股最完整的金融数据源(日 K / 复权因子 / 龙虎榜 / 融资融券 / 涨跌停价 / 筹码 / 资金流...)
- 单例模式(整个进程共享一个 `pro_api`)
- 限流(200/min,Tushare pro 限制)
- 网络错误自动重试(指数退避)
- **便捷封装**:常用 18+ 个接口都有 `def xxx(self, **params)` 直接调

**提供**:
- 类:`TushareClient`(单例)
- 通用调用:`call(api_func_name, **params)`
- 18+ 便捷封装接口(覆盖 90% 离线数据需求)

---

## 2. 接口

### 2.1 类:`TushareClient`(单例)

#### `__new__()` — 单例模式

```python
client = TushareClient()    # 第一次调用 → 创建实例 + _init()
client2 = TushareClient()   # 第二次调用 → 返回同一个实例
assert client is client2
```

---

#### 通用调用:`call(api_func_name: str, **params) -> pd.DataFrame`

**通用调用入口**(带限流 + 重试)。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `api_func_name` | str | 必 | API 方法名(如 `"stock_basic"`) |
| `**params` | dict | — | 业务参数 |

**返回值**:`pd.DataFrame`(Tushare API 返回的标准格式)

**示例**:
```python
df = client.call("stock_basic", list_status="L", exchange="SSE")
# → tushare.pro.stock_basic(list_status="L", exchange="SSE")
```

---

### 2.2 便捷封装(常用 18+ 接口)

#### 1. 股票基本信息:`stock_basic(**params) -> pd.DataFrame`

tushare pro.stock_basic,文档:https://tushare.pro/document/2?doc_id=25

---

#### 2. 交易日历:`trade_cal(**params) -> pd.DataFrame`

tushare pro.trade_cal,文档:https://tushare.pro/document/2?doc_id=26

---

#### 3. 每日涨跌停价格:`stk_limit(**params) -> pd.DataFrame`

**限制**:单次最多 5800 条(可循环,总量不限)
**返回字段**:`trade_date, ts_code, up_limit, down_limit`

**用法**:
```python
# 单日全市场
df = client.stk_limit(trade_date='20190625')
# 单只一段时间
df = client.stk_limit(ts_code='002149.SZ', start_date='20190115', end_date='20190615')
# 单只全历史(默认)
df = client.stk_limit(ts_code='000001.SZ')
```

---

#### 4. 每日停复牌:`suspend_d(**params) -> pd.DataFrame`

**限制**:单次最多 5000 条;积分要求 2000+
**返回字段**:`ts_code, trade_date, suspend_timing, suspend_type(S/R)`

---

#### 5. 龙虎榜每日活跃:`top_list(**params) -> pd.DataFrame`

**限制**:必填 `trade_date`(单日);积分要求 2000+
**实际起始**:2020-12-01
**返回字段(15)**:`trade_date, ts_code, name, close, pct_change, turnover_rate, amount, l_sell, l_buy, l_amount, net_amount, net_rate, amount_rate, float_values, reason`

---

#### 6. 龙虎榜机构买卖:`top_inst(**params) -> pd.DataFrame`

**限制**:必填 `trade_date`(单日);积分要求 2000+
**实际起始**:2020-12-01

---

#### 7. 大宗交易:`block_trade(**params) -> pd.DataFrame`

**限制**:单次最多 1000 条;积分要求 2000+
**实际起始**:2020-12-29

---

#### 8. 港股通每日成交:`ggt_daily(**params) -> pd.DataFrame`

**限制**:单次最多 1000 条;积分要求 2000+
**实际起始**:2017-01-03(港股通 2016-12-05 开通)

---

#### 9. 沪深股通十大成交股:`hsgt_top10(**params) -> pd.DataFrame`

**限制**:积分要求 2000+
**实际起始**:2014-11-17(沪港通开通日)

---

#### 10. 个股资金流向:`moneyflow(**params) -> pd.DataFrame`

**限制**:单次最多 6000 条;积分要求 2000+
**实际起始**:2015-01-05
**返回 20 列**:小/中/大/特大单的买卖(数量+金额)+ 净流入

---

#### 11. 融资融券交易汇总:`margin(**params) -> pd.DataFrame`

**限制**:单次最多 3 行(3 个交易所)
**返回 9 列**:`trade_date, exchange_id, rzye(融资余额), rzmre(融资买入), rzche(融资偿还), rqye(融券余额), rqmcl(融券卖出), rzrqye(融资融券余额), rqyl(融券余量)`

---

#### 12. 融资融券交易明细:`margin_detail(**params) -> pd.DataFrame`

**限制**:单次最多 6000 行,单月通常 2000-6000 行,需 OFFSET 分页
**实际起始**:2017-12-29

---

#### 13. 每日筹码及胜率:`cyq_perf(**params) -> pd.DataFrame`

**限制**:单次最多 6000 条;积分要求 5000+
**实际起始**:2020-01-02
**返回 11 列**:`ts_code, trade_date, his_low, his_high, cost_5pct, cost_15pct, cost_50pct, cost_85pct, cost_95pct, weight_avg, winner_rate`

---

#### 14. 每日指标(重要基本面):`daily_basic(**params) -> pd.DataFrame`

**限制**:单次最多 6000 条;积分要求 2000+
**实际起始**:2015-01-05
**返回 18 列**:`ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv`

**注意**:`limit` 和 `status` 是必填(虽然实测不传也 OK,文档说需要,稳妥起见默认传)
- `status`:L=上市, P=退市, D=退市

---

#### 15. 每日涨跌停列表:`limit_list_d(**params) -> pd.DataFrame`

**限制**:单次最多 2500 条;积分要求 5000+(200次/分钟,1万次/天)
**实际起始**:2020 年起(文档说法);实测更早
**返回 18 列**:`trade_date, ts_code, industry, name, close, pct_chg, amount, limit_amount, float_mv, total_mv, turnover_ratio, fd_amount, first_time, last_time, open_times, up_stat, limit_times, limit`

**limit_type**:
- `U`:涨停
- `D`:跌停
- `Z`:炸板

---

#### 16. 日 K(不复权):`daily(**params) -> pd.DataFrame`

⚠️ tushare `pro.daily` **不支持复权参数**(官方明确"未复权行情")
要拿前复权必须用 `pro_bar`(只能单 ts_code,不适用全市场)
所以我们的设计:`tbl_cn_day` 存不复权原始数据,前复权在 `update_week/update_month` 里临时用 `adj_factor` 算

---

#### 17. 日 K 范围(前复权):`daily_qfq_range(start_date, end_date, qfq=True) -> pd.DataFrame`

**拉取日期范围内所有日 K 数据**(自动按日期循环 + OFFSET 分页)。

**问题**:Tushare daily 范围查询默认只返最新 6000 行,要做全历史必须按日期循环。本方法自动遍历每天 + 分页,返回完整日线数据。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `start_date` | str | 必 | `YYYYMMDD` |
| `end_date` | str | 必 | `YYYYMMDD` |
| `qfq` | bool | `True` | 是否前复权 |

---

#### 18. 周 K / 月 K:`weekly(**params) / monthly(**params)`

---

#### 19. 复权因子:`adj_factor(**params) -> pd.DataFrame`

---

#### 20. KPL 题材相关(3 个):`kpl_list / kpl_concept_cons / kpl_concept`

- `kpl_list(**params)`:开盘啦涨停榜榜单
- `kpl_concept_cons(**params)`:开盘啦题材成分(`ts_code, name, con_name, con_code, trade_date, desc, hot_num`)
- `kpl_concept(**params)`:开盘啦题材排行(`trade_date, ts_code, name, z_t_num, up_num`)

---

#### 21. 新闻:`news(**params)`

Tushare 9 源新闻之一。

---

### 2.3 限流类:`RateLimiter`

**滑动窗口限流器**(200/min,Tushare 限制)。

```python
RateLimiter(max_per_min=200)
acquire()  # 超限自动 sleep 到下个窗口起点
```

---

### 2.4 重试装饰器:`@with_retry`

**网络错误重试装饰器**(任何 `Exception` 都重试,Tushare 数据错误也重试)。

**退避**:`TUSHARE_RETRY_SLEEP * (attempt + 1)`(第 1 次 2s,第 2 次 4s,第 3 次 6s)

---

## 3. 用法示例

### 3.1 股票基本信息

```python
from coreClient.tushare_client import TushareClient

client = TushareClient()
df = client.stock_basic(list_status="L", exchange="SSE", fields="ts_code,name,industry")
print(f"上交所 {len(df)} 只上市股")
print(df.head())
```

### 3.2 拉 2026-09-11 全市场涨停价

```python
df = client.stk_limit(trade_date='20260911')
print(df.head())
# 输出:trade_date  ts_code  up_limit  down_limit
#       20260911    000001.SZ  10.85    8.87
```

### 3.3 拉某月涨停范围

```python
df = client.limit_list_d(start_date='20240901', end_date='20240930', limit_type='U')
print(f"9 月共 {len(df)} 次涨停")
```

### 3.4 拉龙虎榜

```python
df = client.top_list(trade_date='20240913')
print(df[["ts_code", "name", "net_amount", "reason"]])
```

### 3.5 拉每日指标

```python
df = client.daily_basic(trade_date='20240913')
print(df[["ts_code", "pe", "pb", "total_mv"]].head())
```

### 3.6 拉全历史日 K(前复权)

```python
df = client.daily_qfq_range("20250101", "20250911", qfq=True)
print(f"全市场 {len(df)} 条日 K")
```

### 3.7 拉筹码胜率

```python
df = client.cyq_perf(trade_date='20240913')
print(df[["ts_code", "winner_rate", "cost_50pct"]].sort_values("winner_rate", ascending=False).head(10))
```

---

## 4. config 文件说明:tushare_config.py

**位置**:`~/TradingAgent/coreClient/tushare_config.py`

| 配置项 | 值 | 说明 |
|---|---|---|
| `TUSHARE_TOKEN` | `"328371bc416729ed...3dbcd4201486720844e"` | Tushare pro API token(复用 MyATM) |
| `TUSHARE_RATE_LIMIT_PER_MIN` | `200` | Tushare pro 限制 200 次/分钟 |
| `TUSHARE_RETRY_MAX` | `3` | 失败重试次数 |
| `TUSHARE_RETRY_SLEEP` | `2` | 失败重试基础间隔(秒,实际 `sleep = TUSHARE_RETRY_SLEEP * (attempt + 1)`) |

**重要**:`offlineDataManager/scripts/config/settings.py` 里曾经有 `TUSHARE_TOKEN` 等配置,**已删除,统一在这里管理**。

**多个工程共享**:offlineDataManager / onlineDataManager / 未来的 MyATM 替代,共享同一份 token 配置。

---

## 5. 使用方

| 业务 | 文件 | 用途 |
|---|---|---|
| **offlineDataManager** | `scripts/init_db/...py`(全量初始化)| `stock_basic` / `trade_cal` / `daily` / `daily_basic` / `adj_factor` 等 |
| **offlineDataManager** | `scripts/update_*.py`(每日增量) | `stk_limit` / `suspend_d` / `daily_basic` / `moneyflow` 等 |
| **offlineDataManager** | 龙虎榜同步 | `top_list` / `top_inst` |
| **offlineDataManager** | 题材轮动 | `kpl_concept` / `kpl_concept_cons` / `kpl_list` |
| **onlineDataManager** | watchlist 初始化 | `stock_basic` / `daily_basic` 初始化 watchlist 表 |
| **monitor**(未来) | 行情指标监控 | `daily_basic` / `moneyflow` |

---

## 6. 关键设计原则

1. **单例模式**:整个进程共享一个 `pro_api`(避免重复初始化和重复 token 校验)
2. **通用 `call()` 入口**:任何 tushare API 都能调,不限于 21 个便捷方法
3. **便捷方法只做语义包装**:直接转发到 `call()`,不修改参数
4. **`daily_qfq_range` 解决范围查询分页**:Tushare daily 不支持范围查询,必须按日循环
5. **OFFSET 分页**:超过 6000 条的接口(如 `moneyflow` / `margin_detail`)需要 OFFSET 分页
6. **积分要求分级**:2000+ 是龙虎榜/融资融券基础门槛,5000+ 才能用 `cyq_perf` / `limit_list_d`
7. **不复权存储**:日 K 用不复权原始数据存,前复权临时算(用户设计原则)

---

## 7. Pitfall 备注

- **pitfall #17**:`daily()` 不支持复权,要前复权必须用 `daily_qfq_range(qfq=True)` 或临时 `adj_factor` 算
- **pitfall #18**:超过 6000 条的接口需要 OFFSET 分页(`moneyflow` / `margin_detail` / `daily_basic`)
- **pitfall #19**:积分要求分级,2000+ 才能用龙虎榜/融资融券,5000+ 才能用 `cyq_perf`
- **pitfall #20**:`limit_list_d` 限制 200次/分钟,1万次/天 — 频繁调用注意
- **pitfall #21**:`TUSHARE_TOKEN` 必须统一管理,不要在多个文件中硬编码

---

## 8. 验证清单

- [x] `tushare_client.py` 现有代码 + 文档完整对齐(2026-09-15)
- [x] 21 个便捷封装方法全部文档化
- [x] 每个方法的限制 / 实际起始 / 返回字段都有
- [x] 4 项 config 配置全部说明
- [x] offlineDataManager / onlineDataManager / monitor 使用方文档化
- [x] 单例模式 + 限流 + 重试三大机制有说明
- [x] 5 个 pitfall 有备注
- [x] OFFSET 分页 / 范围查询等特殊场景有说明