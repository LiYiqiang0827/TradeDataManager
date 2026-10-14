# service/service_writeredis_zt.py 详细设计

> **涨停池写入 service** — **v6.15:60s/轮**

> **v6.15 更新(2026-09-16)**:频率 30s → **60s**(用户最新统一规定,普通 kind zt/break 频率)
> **数据源**:`同花顺 fetch_limitup_pool()`
> **周期**:`30s/轮`
> **批量**:`全市场`
> **v6 特殊性**:`v6 MyATM 风格:HSET archive + ZSET timeline + STREAM`

---

## §1 职责

- 拉取 `同花顺 fetch_limitup_pool()` 数据
- watchlist = 在线 `online:watchlist` SET(由 `service_writeredis_watchlist` 维护)
- 写 Redis `online:zt:*` 一整套(详见 §4)
- **只写 Redis,不落盘**(落盘由 `service_savedata_zt` 独立 daemon)

## §1.5 设计意图(2026-09-15 第 18 轮体检补)


### §1.5. 设计意图(2026-09-15 第 18 轮体检补)

#### §1.5..1 业务背景
- **zt** 是 涨停池(沪深 A 股今日涨停的所有股票)——涨停事件集合,监控涨停封板/开板
- 不复用其它 kind 的业务动机:涨停是事件级(瞬时),不复用 snapshot(连续 tick)

#### §1.5..2 存储结构
- **STREAM `online:zt:stream`**:每条涨停 tick 进 STREAM,**字段展开**(`lu_time` 永不丢)
- **timeline ZSET `online:zt:timeline`**(43200s):MyATM 索引
- **archive HSET `online:zt:archive:{unix_ts}`**(300s/5min):全市场涨停聚合

#### §1.5..3 TTL / 周期 推导
- **archive TTL = 300s(5min)**:涨停瞬时事件,5min 内必变化
- **timeline TTL = 43200s(12h)**:标准
- **STREAM TTL = 21600s(6h)**:标准
- **writeredis 周期 30s**:抓涨停瞬时窗口
- **lu_time 字段**:用户最高优先级约束,**涨停时间,字段展开永不丢**(复盘涨停决定性)

#### §1.5..4 v5 / v6 MyATM 改造(2026-09-14)
- 改造前:用 pool SET 存涨停股
- 改造后(v6):删 pool SET,改 MyATM 风格(STREAM + timeline + archive)
- 改造动机:SET 不支持按 ts_code 索引


---

## §2 生命周期

scheduler 在 09:25 auction 阶段末尾 spawn:
- 一轮:拉数据 + 写 Redis
- 之后 sleep `interval` 秒等下一轮
- 15:35 切到 closed 时 SIGTERM 杀掉

## §3 调用链

```
service_writeredis_zt
  └─ OnlineRedis()
  └─ r.get_watchlist()                  # 读 online:watchlist
  └─ <数据源>.fetch_xxx(codes=...)
  └─ r.put_zt_pool(zt_list, data_timestamp=now_unix)   # **批量 pipeline,不是 put_zt**(zt 是一次写整池,不是按 ts_code 循环)
  └─ (MyATM 风格无 commit,EXPIRE 在 put_zt_pool 内部处理)
  └─ sleep 30s
```

## §4 Redis 数据结构

| key | 类型 | EXPIRE | 备注 |
|---|---|---:|---|
| `online:zt:archive:{unix_ts}` | HSET | 300s | 每个 snapshot 各自过期 |
| `online:zt:timeline` | ZSET | 43200s(12h) = `ZT_TIMELINE_TTL` | 12h 兜底 + 写入时 prune 10min 前 |
| `online:zt:stream` | STREAM | **21600s(6h)**(= `STREAM_TTL` = `DEFAULT_STREAM_TTL` = `3600*6`)| 落盘用,字段展开(lu_time 永不丢)|

> ⚠️ doc 早期写 STREAM EXPIRE "1d"(86400)错,2026-09-12 v3 改 6h。
> ⚠️ 真值方法名 `put_zt_pool`,doc 早期写 `put_zt` 错(单方法不存在)。

## §5 启动方式

```bash
python3 -m service.service_writeredis_zt --interval <N>
python3 -m service.service_writeredis_zt --once      # 单次(测试用)
```

## §6 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_zt** | 读 STREAM `online:zt:stream` 落盘 |

## §7 历史变更

- **2026-09-11 v3 重构** 原子化,1 service = 1 kind
- **2026-09-14 v6** 改 MyATM 风格 + timeline/archive,删除 pool SET
