# 代码详细设计/service_daily_basic.md

`scripts/service/service_daily_basic.py` — 每日指标 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date` / `--trade-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_daily_basic(start_date, end_date)`
4. 写日志到 `logs/service_daily_basic.log`

## 入口

```bash
python3 -m service.service_daily_basic  # 增量
python3 -m service.service_daily_basic --start-date 20150105 --end-date 20151231
python3 -m service.service_daily_basic --trade-date 20240913  # 单日
```

## 关键点

- **CTRL_KEY = "cn_daily_basic"**
- **`update_daily_basic` 写 `tbl_cn_daily_basic`**(basic DB)
- **断点**:`tbl_basic_ctrl.cn_daily_basic`
- **按日循环**(单日全市场 status='L' ~5341 行)
- 主键:`(trade_date, ts_code)`
- **接口必填**:`limit=6000 + status='L'`(用户提示,文档说必填,稳妥起见默认传)
- **用户要求**:从 **2015-01-01 开始**(实测 2015-01-05 最早)

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update → service_daily_basic
```

**挂一个 task**(2026-09-15):`task_full_update` 阶段 4

**为什么挂 full_update 而非 morning**:
- 数据 15-17 点才更新,早上数据是昨天的
- 用户要求"每日更新",18:00 跑能拿到当天数据

## 数据流

```
service_daily_basic.main()
  ↓
resolve_date_range(conn_basic, "cn_daily_basic", args, default_start="20150105")
  ↓
down.update_daily_basic(start_date, end_date)
  ↓
按日循环:
  对每天:
    df = self.client.daily_basic(
      trade_date=cur_sd,
      limit=6000,
      status="L",  # L=上市,P=退市,D=退市
    )
    if df is not None and len(df) > 0:
      upsert_df(conn_basic, df, "tbl_cn_daily_basic", key_cols=["trade_date", "ts_code"])
  ↓
update_ctrl(conn_basic, "cn_daily_basic", last_success_date)
```

## 注意事项

- **单日全市场 ~5341 行**(status='L'),< 6000 限制
- **接口必填** `limit=6000 + status='L'`(实测不传也 OK,但稳妥起见默认传)
- **数据每日 15-17 点更新**
- **实际起始 2015-01-05**(用户要求 2015-01-01,实测最早)

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单日 | 5341 行 | 0.4s,1 次调用 |
| 全量 2015-01-05 ~ 2026-09-15(~2850 天) | ~1520 万行 | ~2-3 小时(限流影响) |