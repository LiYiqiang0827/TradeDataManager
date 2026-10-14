# 代码详细设计/service_ggt_daily.md

`scripts/service/service_ggt_daily.py` — 港股通每日成交更新 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_ggt_daily(start_date, end_date)`
4. 写日志到 `logs/service_ggt_daily.log`

## 入口

```bash
python3 -m service.service_ggt_daily  # 增量
python3 -m service.service_ggt_daily --start-date 20240101 --end-date 20240930
```

## 关键点

- **CTRL_KEY = "cn_ggt_daily"**
- **`update_ggt_daily` 写 `tbl_cn_ggt_daily`**(kpl DB)
- **断点**:`tbl_kpl_ctrl.cn_ggt_daily`
- **按月循环**(实测范围参数有效)
- 单月 ~30 行,**远低于 1000 限制**

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update 第 4 阶段 → service_ggt_daily
```

## 数据流

```
service_ggt_daily.main()
  ↓
resolve_date_range(conn_basic, "cn_ggt_daily", args)
  ↓
down.update_ggt_daily(start_date, end_date)
  ↓
按月循环,每天 1 行汇总
```

## 注意事项

- **每天只有 1 行**(汇总买/卖金额和笔数)
- 数据量小,跑全量很快
- 注意:实测发现起始日期是 **2015-01-05**(不是我之前以为的 2017-01-03)

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单月 | 17 行 | 0.1s |
| 全量 11.7 年 | ~2,300 行 | ~30 秒 |
