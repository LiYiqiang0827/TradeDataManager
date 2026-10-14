# service/service_savedata_auction.py 详细设计

> **auction 落盘 service**
> **运行模式**:`--once`
> **周期**:`N/A(--once)`

---

## §1 职责

- 读 Redis `online:auction:stream (STREAM)`
- 调 `core.persist_client.persist_kind(r, kind="auction", trade_date=trade_date)`
- 落盘 SQLite 表 `auction_YYYYMMDD`
- **DEFAULT_INTERVAL** = `99999s`(代码真值,2026-09-14 体检校正)
- **只落盘,不写 Redis**(写由对应 writeredis service 负责)

## §2 生命周期

**scheduler 09:29 + 15:10 各一次**

## §3 调用链

```
service_savedata_auction
  └─ OnlineRedis()
  └─ (无独立 connect_db 子类层调用)
  └─ SavedataDaemon._do_persist()
     └─ persist_kind(r, kind="auction", trade_date=trade_date)        # ← core.persist_client
     └─ INSERT INTO auction_YYYYMMDD ...
  └─ sys.exit(main(SavedataAuction))      # 强制 --once(sys.argv 注入),跑一次就退出
```

## §4 表结构

```sql
-- auction_YYYYMMDD
-- (详细 schema 见 03_数据详细设计.md §<X>)
```

## §5 启动方式

```bash
python3 -m service.service_savedata_auction             # daemon
python3 -m service.service_savedata_auction --once      # 单次
```

## §6 备注

为什么不在 morning 阶段跑 daemon?因为 auction 数据只有 9:15-9:30 才有,15:00 后不会有新数据。两次 --once 比一个 6h daemon 更准确。

## §7 与其他模块的关系

| 谁会用 | 关系 |
|---|---|
| **service_writeredis_auction** | 上游,写 Redis |
| **scheduler_onlineData.py** | spawn 触发 |

## §8 历史变更

- **2026-09-11** 改为 --once 模式
- **2026-09-14 v6.2** 落盘逻辑不变,只是上游加了 timeline + archive
