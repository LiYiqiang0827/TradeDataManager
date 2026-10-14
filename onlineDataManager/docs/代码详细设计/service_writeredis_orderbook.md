# service/service_writeredis_orderbook.py 详细设计

> **5 档盘口监控 service** — 🔴 **v6.15 起停用**
> **数据源**:`同花顺 fetch_orderbook_quotes`
> **周期**:`10s/轮`
> **批量**:`80 只/批(pytdx 限制)`
> **v6 特殊性**:`v6 保持原有 STREAM + ZSET 模式,无 timeline/archive`
> **v6.15(2026-09-16)**:用户原话"orderbook不再抓取了,这个对应的rediswrite和savedata服务停掉不再运行了"。scheduler_onlineData.py:
> - `WRITER_DAEMONS_MORNING_AFTERNOON` 删除 `service_writeredis_orderbook`
> - `SAVEDATA_DAEMONS_MORNING_AFTERNOON` 删除 `service_savedata_orderbook`
> - `ALL_TRADE_DAEMONS` 同步删除
> - **service 文件保留**(代码不动),scheduler 不再调用
> - **pytdx 5 档接口实测整体失效**(2026-09-16 测试 6 个 IP 全 0 数据,pytdx 1.72 archived quotes协议下线)

---

## §1 职责

- 拉取 `同花顺 fetch_orderbook_quotes` 数据
- watchlist = 在线 `online:watchlist` SET(由 `service_writeredis_watchlist` 维护)
- 写 Redis `online:orderbook:*` 一整套(详见 §4)
- **只写 Redis,不落盘**(落盘由 `service_savedata_orderbook` 独立 daemon)

## §1.5 设计意图(2026-09-15 第 18 轮体检补)


### §1.5. 设计意图(2026-09-15 第 18 轮体检补)

#### §1.5..1 业务背景
- **orderbook** 是 5 档盘口快照——盘口 tick 级行情,监控买卖盘变化
- 不复用其它 kind 的业务动机:盘口是高频 tick(snapshot 是 6s/轮),需要独立的 10s/轮高频流

#### §1.5..2 存储结构
- **STREAM `online:orderbook:stream`**(21600s):每次 tick 进 STREAM
- **window ZSET `online:orderbook:window:{ts_code}`**(210s/3.5min):每只股近 18 个 tick 滑窗
- **没有 archive / timeline**:v6 保持原 STREAM + window 双结构,**不上 MyATM**
- **没有 HSET key**:v3 精简后只保留 STREAM + window

#### §1.5..3 TTL / 周期 推导
- **WINDOW_TTL_ORDERBOOK = 210s(3.5min = 3×60+30)**:3 分钟滑窗 + 30s 余量。30s 余量是为了让 savedata 来不及消费时数据不丢
- **STREAM TTL = 21600s(6h)**:标准
- **writeredis 周期 10s**:盘口高频,10s/轮
- **savedata DEFAULT_INTERVAL = 900s(15min)**:5 档盘口高频,15min 落盘足够复盘
- **pytdx 限制 80 只/批**:pytdx 单次最多返回 80 只股,大于 80 要分批

#### §1.5..4 v5 / v6 MyATM 改造(2026-09-14)
- **v6 保持原模式**(不上 MyATM):盘口只对 watchlist 单股有用,不需要全市场聚合 archive
- **v3 精简**:删除 HSET key,只保留 STREAM + window


---

## §2 生命周期

scheduler 在 09:25 auction 阶段末尾 spawn:
- 一轮:拉数据 + 写 Redis
- 之后 sleep `interval` 秒等下一轮
- 15:35 切到 closed 时 SIGTERM 杀掉

## §3 调用链

```
service_writeredis_orderbook
  └─ OnlineRedis()
  └─ r.get_watchlist()                  # 读 online:watchlist
  └─ <数据源>.fetch_xxx(codes=...)
  └─ for ts_code, data: r.put_orderbook(ts_code, data)   # 批量 pipeline
  └─ (MyATM 风格无 commit,EXPIRE 在 put_xxx_pool 内部处理)
  └─ sleep 10s
```

## §4 Redis 数据结构

| key | 类型 | EXPIRE | 备注 |
|---|---|---:|---|
| `online:orderbook:stream` | STREAM | **21600s(6h)**(= `STREAM_TTL`= `DEFAULT_STREAM_TTL`= `3600*6`)| MAXLEN 100000 |
| `online:orderbook:window:{ts_code}` | ZSET | **210s(3.5 分钟)** = `WINDOW_TTL_ORDERBOOK = 3*60+30` | 每次 put(3 分钟滑窗 + 30s 余量)|

> ⚠️ **没有 `online:orderbook:hash:{ts_code}` 这个 key** — v3 精简后只保留 STREAM + window ZSET。
> ⚠️ doc 早期写"stream EXPIRE 12h / hash EXPIRE 86400 / window EXPIRE 12h"是错误的,真值是 STREAM 6h / 无 hash / window 210s。

## §5 启动方式

```bash
python3 -m service.service_writeredis_orderbook --interval <N>
python3 -m service.service_writeredis_orderbook --once      # 单次(测试用)
```

## §6 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_orderbook** | 读 STREAM `online:orderbook:stream` 落盘 |

## §7 历史变更

- **2026-09-12 v3** 精简字段 + 批量 80 只/批
- **2026-09-14** watchlist 改用 service_writeredis_watchlist 维护的 online:watchlist SET
