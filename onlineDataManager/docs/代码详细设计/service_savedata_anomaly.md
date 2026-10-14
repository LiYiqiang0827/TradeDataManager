# service/service_savedata_anomaly.py 详细设计

> **anomaly 异动清单落盘 daemon**
> **运行模式**:`daemon`
> **周期**:`1800s(30min)`

---

## §1 职责

- 读 Redis `online:anomaly:stream (STREAM)`
- 调 `core.persist_client.persist_kind(r, kind="anomaly", trade_date=trade_date)`
- 落盘 SQLite 表 `anomaly_YYYYMMDD`
- **DEFAULT_INTERVAL** = `1800s`(代码真值,2026-09-14 体检校正)
- **只落盘,不写 Redis**(写由对应 writeredis service 负责)

## §2 生命周期

**scheduler morning_savedata / afternoon_savedata 阶段 spawn,15:35 closed 阶段 SIGTERM**

## §3 调用链

```
service_savedata_anomaly
  └─ OnlineRedis()
  └─ (无独立 connect_db 子类层调用)
  └─ SavedataDaemon._do_persist()
     └─ persist_kind(r, kind="anomaly", trade_date=trade_date)        # ← core.persist_client
     └─ persist_anomaly(r, *, trade_date=trade_date)                # line 461 双表写
        ├─ INSERT OR IGNORE INTO anomaly_YYYYMMDD ...               # 顶层 15 字段
        └─ INSERT INTO anomaly_keywords_YYYYMMDD ...                # keyword_list 拆 keyword_idx
  └─ sleep DEFAULT_INTERVAL                  # 仅 daemon 模式
```

**anomaly 双表设计意图(用户最关心)**:同花顺的异动清单 `keyword_list` 是个 list 字段(一个异动事件可能挂多个关键词,如"涨停 + 业绩预增 + 5G"),存顶层 anomaly 表不利于下游按 keyword 检索。
v3(2026-09-12)拆成两张表:
- **anomaly_YYYYMMDD** 顶层 15 字段(`anomaly_timestamp` ISO + ts_code + trade_date + 异动标题 + 涨跌幅等元数据)
- **anomaly_keywords_YYYYMMDD** 长表 4 字段(trade_date + ts_code + anomaly_timestamp + keyword_idx + keyword),通过 (ts_code, anomaly_timestamp, keyword_idx) 定位

下游用 `SELECT * FROM anomaly_keywords_<date> WHERE keyword = '涨停'` 即可查所有涨停异动事件。

## §4 表结构

```sql
-- anomaly_YYYYMMDD
-- (详细 schema 见 03_数据详细设计.md §<X>)
```

## §5 启动方式

```bash
python3 -m service.service_savedata_anomaly             # daemon
python3 -m service.service_savedata_anomaly --once      # 单次
```

## §6 备注

基类 SavedataDaemon 流程

## §7 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_writeredis_anomaly** | 上游,写 Redis |
| **scheduler_onlineData.py** | spawn 触发 |

## §8 历史变更

- **2026-09-11 新建** 新增 service
