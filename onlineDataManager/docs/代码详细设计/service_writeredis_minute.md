# service/service_writeredis_minute.py 详细设计

> **1 分钟 K 线 service(分时图)**— **v6.15:30s/轮**

> **v6.15 更新(2026-09-16)**:频率 60s → **30s**(用户最新统一规定,普通 kind 频率表)
> **数据源**:`coreClient/tdx_client.get_minute_kline(ts_code)`
> **周期**:`60s/轮`
> **批量**:`全 watchlist`
> **v6 特殊性**:`v4 改用 tdx_client 实时接口(trade_date 不用传)`

---

## §1 职责

- 拉取 `coreClient/tdx_client.get_minute_kline(ts_code)` 数据
- watchlist = 在线 `online:watchlist` SET(由 `service_writeredis_watchlist` 维护)
- 写 Redis `online:minute:*` 一整套(详见 §4)
- **只写 Redis,不落盘**(落盘由 `service_savedata_minute` 独立 daemon)

## §1.5 设计意图(2026-09-15 第 18 轮体检补)


### §1.5. 设计意图(2026-09-15 第 18 轮体检补)

#### §1.5..1 业务背景
- **minute** 是 1 分钟 K 线(分时图)——盘中 240 根/股的分钟 K,基本复盘
- 不复用其它 kind 的业务动机:minute 是 tick 级(每 1 分钟 1 根),不复用 snapshot(6s 一次)

#### §1.5..2 存储结构
- **`online:minute:{ts_code}` ZSET**(86400s/1 天):每只股一个 ZSET,**不走 STREAM**
- **score = time_idx 整数(0-239)**:而不是 unix 时间戳,`ZRANGE 0 -1` 一次取全量
- **member = JSON-encoded minute bar**:price / vol / data_timestamp
- **没有 STREAM / 没有 hash / 没有 `:bars:` 中缀 key**
- **每次 put 前 DEL 全清**:pytdx 历史接口每次返全日 0..N bars 会跟旧数据重叠

#### §1.5..3 TTL / 周期 推导
- **WINDOW_TTL_MINUTE = 86400s(1 天)**:1 个交易日 = 4h,留 24h 容错(午休跨日不退)
- **writeredis 周期 6s**:pytdx 实时接口每 6s 推一批
- **savedata DEFAULT_INTERVAL = 900s(15min)**:124 只 × 240 根/天 ≈ 30k 条/天,15min batch 节约 SQLite

#### §1.5..4 v5 / v6 MyATM 改造(2026-09-14)
- **不走 STREAM 的设计意图**:数据量大(124 只 × 240 根 ≈ 30k 条/天),STREAM MAXLEN 会丢数据
- **score 整数排序**:`ZRANGE 0 -1` 一次取全量 240 根,不需要 ZRANGEBYSCORE 时间过滤
- **每次 DEL + ZADD 重写**:pytdx 每次返全日 bars,会跟旧数据重叠,所以 DEL 全清再 ZADD
- **v4 schema 精简**(12 列 → 8 列):分时图只关心当前价 + 量,OHLCV 全量不要


---

## §2 生命周期

scheduler 在 09:25 auction 阶段末尾 spawn:
- 一轮:拉数据 + 写 Redis
- 之后 sleep `interval` 秒等下一轮
- 15:35 切到 closed 时 SIGTERM 杀掉

## §3 调用链

```
service_writeredis_minute
  └─ OnlineRedis()
  └─ r.get_watchlist()                  # 读 online:watchlist
  └─ <数据源>.fetch_xxx(codes=...)
  └─ for ts_code, data: r.put_minute(ts_code, data)   # 批量 pipeline
  └─ (MyATM 风格无 commit,EXPIRE 在 put_xxx_pool 内部处理)
  └─ sleep 60s
```

## §4 Redis 数据结构

| key | 类型 | EXPIRE | 备注 |
|---|---|---:|---|
| `online:minute:{ts_code}` | ZSET | **86400s(1 天)** = `WINDOW_TTL_MINUTE` | 每次 put 前 DEL 全清 + ZADD(score=time_idx)|

> ⚠️ **没有 `online:minute:stream` / `online:minute:hash` / `online:minute:bars:{ts_code}` 这些 key**。
> 真值只有一个 ZSET:`online:minute:{ts_code}`,member = `{time_idx, datetime, price, vol, data_timestamp}` 编码后的 JSON,score = time_idx。
> doc 早期写"stream EXPIRE 12h / hash EXPIRE 86400 / minute:bars EXPIRE 86400"全错。
> v4(2026-09-13)后,minute 落盘走 savedata 直接读 ZSET,**不经 STREAM**(STREAM 体系不通用 minute)。

## §5 启动方式

```bash
python3 -m service.service_writeredis_minute --interval <N>
python3 -m service.service_writeredis_minute --once      # 单次(测试用)
```

## §6 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_minute** | 读 STREAM `online:minute:stream` 落盘 |

## §7 历史变更

- **2026-09-13 v4** 改 tdx_client 实时接口,trade_date 不再传
- **2026-09-14 17:25** bug 修复:--once 模式重写,不再死循环
