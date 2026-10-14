# 代码详细设计/service_block_trade.md

`scripts/service/service_block_trade.py` — 大宗交易更新 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_block_trade(start_date, end_date)`
4. 写日志到 `logs/service_block_trade.log`

## 入口

```bash
python3 -m service.service_block_trade  # 增量(从 ctrl 到今天)
python3 -m service.service_block_trade --start-date 20201229 --end-date 20210131
```

## 关键点

- **CTRL_KEY = "cn_block_trade"**
- **`update_block_trade` 写 `tbl_cn_block_trade`**(kpl DB)
- **断点**:`tbl_kpl_ctrl.cn_block_trade`
- **按月循环 + OFFSET 分页**(单次最多 1000 行)
- 起始 2020-12-29(tushare 接口上线时间)

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update 第 4 阶段 → service_block_trade
```

**只挂一个 task**(2026-09-15):`task_full_update` 第 4 阶段

## 数据流

```
service_block_trade.main()
  ↓
resolve_date_range(conn_basic, "cn_block_trade", args)
  ↓
down.update_block_trade(start_date=sd, end_date=ed)
  ↓
按月循环 cur=sd..ed
  对每月 [month_start, month_end]:
    offset = 0
    while True:
      df = self.client.block_trade(start_date, end_date, limit=1000, offset=offset)
      if df is empty: break
      upsert_df(conn_kpl, df, "tbl_cn_block_trade", key_cols=[...])
      if len(df) < 1000: break  # 已拉完
      offset += 1000
  ↓
update_ctrl(conn_basic, "cn_block_trade", last_success_date)
  ↓
return total_inserted
```

## 注意事项

- **单次最大 1000 行**,所以按月 + OFFSET 分页
- **大宗交易数据稀疏**,可能某些月份 0 行(没有大宗)
- **失败跳过**(不中止整个 task)

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单月 | 1,637 行 | 0.4s |
| 全量 5 年 | 估算 ~50K 行 | ~3 分钟 |
