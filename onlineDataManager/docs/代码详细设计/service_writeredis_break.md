# service/service_writeredis_break.py 详细设计

> **炸板池写入 service**
> **数据源**:`同花顺 fetch_limitbreak_pool()`
> **周期**:`60s/轮`
> **批量**:`全市场`
> **v6 特殊性**:`v6 MyATM 风格(同 zt)`

---

## §1 职责

- 拉取 `同花顺 fetch_limitbreak_pool()` 数据
- watchlist = 在线 `online:watchlist` SET(由 `service_writeredis_watchlist` 维护)
- 写 Redis `online:break:*` 一整套(详见 §4)
- **只写 Redis,不落盘**(落盘由 `service_savedata_break` 独立 daemon)

## §1.5 设计意图(2026-09-15 第 18 轮体检补)


### §1.5. 设计意图(2026-09-15 第 18 轮体检补)

#### §1.5..1 业务背景
- **break** 是 炸板池——涨停板打开瞬间的股票集合,监控盘中开板风险
- 不复用其它 kind 的业务动机:炸板事件比涨停低频,单独 kind 便于 sent 报警

#### §1.5..2 存储结构
- **STREAM `online:break:stream`**:每次炸板事件进 STREAM
- **timeline ZSET `online:break:timeline`**(43200s):MyATM 索引
- **archive HSET `online:break:archive:{unix_ts}`**(300s/5min):全市场聚合

#### §1.5..3 TTL / 周期 推导
- **archive TTL = 300s(5min)**:炸板是瞬时事件,5min 内必变化
- **timeline TTL = 43200s(12h)**:标准 12h
- **STREAM TTL = 21600s(6h)**:标准
- **writeredis 周期 60s**:炸板事件比涨停(30s/轮)低频,慢一倍

#### §1.5..4 v5 / v6 MyATM 改造(2026-09-14)
- 改造前:用 pool SET 存炸板股
- 改造后(v6):删 pool SET,改 MyATM 风格(STREAM + timeline + archive)
- 改造动机:SET 不支持按 ts_code 索引 + 需要 timeline 索引


---

## §2 生命周期

scheduler 在 09:25 auction 阶段末尾 spawn:
- 一轮:拉数据 + 写 Redis
- 之后 sleep `interval` 秒等下一轮
- 15:35 切到 closed 时 SIGTERM 杀掉

## §3 调用链

```
service_writeredis_break
  └─ OnlineRedis()
  └─ r.get_watchlist()                  # 读 online:watchlist
  └─ <数据源>.fetch_xxx(codes=...)
  └─ r.put_break_pool(break_list, data_timestamp=now_unix)   # **批量 pipeline,不是 put_break**
  └─ (MyATM 风格无 commit,EXPIRE 在 put_break_pool 内部处理)
  └─ sleep 60s
```

## §4 Redis 数据结构

| key | 类型 | EXPIRE | 备注 |
|---|---|---:|---|
| `online:break:archive:{unix_ts}` | HSET | 300s |  |
| `online:break:timeline` | ZSET | 43200s(12h) = `BREAK_TIMELINE_TTL` |  |
| `online:break:stream` | STREAM | **21600s(6h)**(= `STREAM_TTL` = `DEFAULT_STREAM_TTL` = `3600*6`)|  |

> ⚠️ doc 早期写 STREAM EXPIRE "1d" 错,v3 改 6h。
> ⚠️ 真值方法名 `put_break_pool`,doc 早期写 `put_break` 错(单方法不存在)。

## §5 启动方式

```bash
python3 -m service.service_writeredis_break --interval <N>
python3 -m service.service_writeredis_break --once      # 单次(测试用)
```

## §6 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_savedata_break** | 读 STREAM `online:break:stream` 落盘 |

## §7 历史变更

- **2026-09-11 新建** 
- **2026-09-14 v6** 改 MyATM 风格,删除 pool SET
