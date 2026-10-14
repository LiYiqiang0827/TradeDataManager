# 代码详细设计/service_cyq_perf.md

`scripts/service/service_cyq_perf.py` — 每日筹码及胜率 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date` / `--trade-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_cyq_perf(start_date, end_date)`
4. 写日志到 `logs/service_cyq_perf.log`

## 入口

```bash
python3 -m service.service_cyq_perf  # 增量
python3 -m service.service_cyq_perf --start-date 20200102 --end-date 20200131
python3 -m service.service_cyq_perf --trade-date 20240913  # 单日全市场
```

## 关键点

- **CTRL_KEY = "cn_cyq_perf"**
- **`update_cyq_perf` 写 `tbl_cn_cyq_perf`**(basic DB)
- **断点**:`tbl_basic_ctrl.cn_cyq_perf`
- **按日循环**(逐日 `trade_date`)
- 主键:`(trade_date, ts_code)`
- **接口要求**:`ts_code` 或 `trade_date` 至少传 1 个

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update → service_cyq_perf
```

**挂一个 task**(2026-09-15):`task_full_update` 阶段 4

**为什么挂 full_update 而非 morning**:
- 数据 18-21 点更新,晚上才有数据
- 用户要求"每日"更新

## 数据流

```
service_cyq_perf.main()
  ↓
resolve_date_range(conn_basic, "cn_cyq_perf", args)
  ↓
down.update_cyq_perf(start_date, end_date)
  ↓
按日循环:
  对每天:
    df = self.client.cyq_perf(trade_date=cur_sd)  # 5578 行
    if df is not None and len(df) > 0:
      upsert_df(conn_basic, df, "tbl_cn_cyq_perf", key_cols=["trade_date", "ts_code"])
  ↓
update_ctrl(conn_basic, "cn_cyq_perf", last_success_date)
```

## 注意事项

- **单日全市场 ~5578 行**(< 6000 限制,无需 OFFSET)
- **接口必填校验**:`ts_code` 或 `trade_date` 至少 1 个
- **每日 18-21 点更新**,早上拉还是昨天的旧数据
- **起始 2020-01-02**(用户要求 2020-01-01 起,实测 2020-01-02 最早)

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单日 | 5578 行 | 0.5s,1 次调用 |
| 全量 2020-01-02 ~ 2026-09-15(~1660 天) | ~920 万行 | ~15-25 分钟(限流影响) |