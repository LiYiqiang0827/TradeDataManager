# service/service_savedata_snapshot.py 详细设计

> **snapshot 落盘 daemon**
> **运行模式**:`daemon(while True)`
> **周期**:`900s(15min)`

---

## §1 职责

- 读 Redis `online:snapshot:stream (STREAM)`
- 调 `core.persist_client.persist_kind(r, kind="snapshot", trade_date=trade_date)`
- 落盘 SQLite 表 `snapshot_YYYYMMDD`
- **DEFAULT_INTERVAL** = `900s`(代码真值,2026-09-14 体检校正)
- **只落盘,不写 Redis**(写由对应 writeredis service 负责)


## §2 设计意图(2026-09-15 第 18 轮体检补)

### §2.1 业务背景
- snapshot 是 快照行情(全市场 6s/轮)——盘中任意时刻全市场快照,基本面复盘
- 不复用其它 kind 的业务动机:6s 高频 vs zt/break 是事件级,snapshot 是连续行情

### §2.2 存储结构
- **STREAM `online:snapshot:stream`**:每条 tick 进 STREAM,savedata 靠 XREAD > last_id cursor 增量读
- **timeline ZSET**(`43200s (12h)`):MyATM v6 改造后加的快照索引,支持「某时刻全市场 snapshot 快照」查询
- **archive HSET**(`动态计算(11:00-11:30→7200s,其余→1800s)`):全市场聚合 `snapshot:archive:{unix_ts}`,savedata 兜底时 N→1 一次取齐
- **window ZSET**(`43200s (12h)`):每只股近 N 个 tick 滑窗,下游实时渲染

### §2.3 TTL 推导
- **archive TTL = 动态计算(11:00-11:30→7200s,其余→1800s)**:v6.3 跨午休补偿,30min 业务 + 90min 午休 = 120min
- **timeline TTL = 43200s(12h)**:业务日内 12h 全覆盖 + 跨日兜底
- **STREAM TTL = 21600s(6h)** = `STREAM_TTL`:标准 `DEFAULT_STREAM_TTL`,不是特殊
- **window TTL = 43200s (12h)**:单股 12h 兜底,下游能查任一时刻单股快照

### §2.4 落盘节奏
- **DEFAULT_INTERVAL = 900**(15min):盘中 4h × 4 = 16 次,数据量大节约 SQLite
- **daemon 模式**(非 `--once`):因为盘中持续有新 snapshot 事件,需要持续消费
- **失败重试语义**:`savedata_loop.py:60-73` 包裹 try/except,**daemon 永不退出**;**STREAM cursor 持久化**到 SQLite `online_stream_cursor`,**重启不丢**

### §2.5 双时间戳原则
- **`snapshot_timestamp`**(数据时间):snapshot_timestamp 来源同花顺数据源头 wall-clock,**用户最高优先级约束**——复盘时区分「数据几点产生」与「系统几点记录」
- **`created_at`**(落盘时间):SQLite DEFAULT `datetime('now','localtime')` 自动加

### §2.6 特殊字段(lu_time 永不丢)
- 无涨停时间字段
## §2 生命周期

**scheduler morning_savedata / afternoon_savedata 阶段 spawn,15:35 post 阶段 SIGTERM**

## §3 调用链

```
service_savedata_snapshot
  └─ OnlineRedis()
  └─ (无独立 connect_db 子类层调用)
  └─ SavedataDaemon._do_persist()
     └─ persist_kind(r, kind="snapshot", trade_date=trade_date)        # ← core.persist_client
     └─ INSERT INTO snapshot_YYYYMMDD ...
  └─ sleep DEFAULT_INTERVAL                  # 仅 daemon 模式
```

## §4 表结构

```sql
-- snapshot_YYYYMMDD
-- (详细 schema 见 03_数据详细设计.md §<X>)
```

## §5 启动方式

```bash
python3 -m service.service_savedata_snapshot             # daemon
python3 -m service.service_savedata_snapshot --once      # 单次
```

## §6 备注

v6.3 加 timeline + archive 是查询层,落盘逻辑不变。STREAM 游标机制自然兼容午休无数据。

## §7 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_writeredis_snapshot** | 上游,写 Redis |
| **scheduler_onlineData.py** | spawn 触发 |

## §8 历史变更

- **2026-09-11** 重构为独立 daemon
- **2026-09-14 v6.3** 落盘逻辑不变
