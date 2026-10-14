# 代码详细设计/service_moneyflow.md

`scripts/service/service_moneyflow.py` — 个股资金流向 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_moneyflow(start_date, end_date)`
4. 写日志到 `logs/service_moneyflow.log`

## 入口

```bash
python3 -m service.service_moneyflow  # 增量
python3 -m service.service_moneyflow --start-date 20150101 --end-date 20151231
```

## 关键点

- **CTRL_KEY = "cn_moneyflow"**
- **`update_moneyflow` 写 `tbl_cn_moneyflow`**(basic DB)
- **断点**:`tbl_basic_ctrl.cn_moneyflow`
- **按月循环 + OFFSET 分页**(单月 > 6000,需分页)
- 起始 2015-01-05(实测)
- 全量 141 个月,每 200 次/分钟限频,约 1-2 分钟跑完

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update 第 4 阶段 → service_moneyflow
```

**只挂一个 task**(2026-09-15):`task_full_update` 第 4 阶段

## 数据流

```
service_moneyflow.main()
  ↓
resolve_date_range(conn_basic, "cn_moneyflow", args)
  ↓
down.update_moneyflow(start_date, end_date)
  ↓
按月循环
  对每月 [month_start, month_end]:
    offset = 0
    while True:
      df = self.client.moneyflow(start_date, end_date, limit=6000, offset=offset)
      if df is empty: break
      upsert_df(conn_basic, df, "tbl_cn_moneyflow", key_cols=["trade_date", "ts_code"])
      if len(df) < 6000: break  # 已拉完
      offset += 6000
  ↓
update_ctrl(conn_basic, "cn_moneyflow", last_success_date)
```

## 注意事项

- **单月行数 > 6000**,必须 OFFSET 分页
- 单日 ~5088 行(2024-09-13 实测)
- 字段 20 列:4 类单(小/中/大/特大)买卖 + 净流入
- **实测起始 2015-01-05**(文档说 2010,但实测 2014 年也没数据)

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单月 2024-09 | 96,676 行 | 12.8s |
| 单日 2024-09-13 | 5088 行 | < 1s |
| 全量 2015-01-05 ~ 2026-09-15(~141 个月) | ~1.7 亿行(估算)| 1-2 分钟 |
