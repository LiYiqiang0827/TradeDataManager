# 代码详细设计/service_suspend.md

`scripts/service/service_suspend.py` — 每日停复牌信息更新 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_suspend(start_date, end_date)`
4. 写日志到 `logs/service_suspend.log`

## 入口

```bash
python3 -m service.service_suspend --start-date 20200101 --end-date 20200131
python3 -m service.service_suspend  # 增量(ctrl)
```

## 关键流程

```python
CTRL_KEY = "cn_suspend"

def main():
    parser = argparse.ArgumentParser(description="Update 每日停复牌信息")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期 YYYYMMDD")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_suspend.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_suspend(start_date=sd, end_date=ed)
        logger.info(f"[service_suspend] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_suspend] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_suspend"**
- **传 `down.conn_basic`** 给 `resolve_date_range`
- **`update_suspend` 写 `tbl_cn_suspend`**(basic DB)
- **断点**:`tbl_basic_ctrl.cn_suspend`
- **log rotation=20 MB**
- **按月循环**(实测范围参数有效,~30 秒全量)

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update 第 4 阶段 → service_suspend
```

**只挂一个 task**(2026-09-15):`task_full_update` 第 4 阶段

## 数据流

```
service_suspend.main()
  ↓
resolve_date_range(conn_basic, "cn_suspend", args) → (sd, ed, desc)
  ↓
down.update_suspend(start_date=sd, end_date=ed)
  ↓
按月循环 cur=sd..ed(每次 ~1 个月)
  对每月 [month_start, month_end]:
    df = self.client.suspend_d(start_date=..., end_date=...)  # ~200 行/月
    if df is None or empty: continue
    df["snap_ts"] = snap_ts()
    upsert_df(conn_basic, df, "tbl_cn_suspend", key_cols=["trade_date", "ts_code"])
  ↓
update_ctrl(conn_basic, "cn_suspend", last_success_date)
  ↓
return total_inserted
```

## 注意事项

- **按月循环**:503 个月 ≈ 503 次调用,远低于 200/分钟限流
- **停牌期间每天一行**:覆盖从停牌第一天到复牌前一天
- **`suspend_timing` 仅日内停牌有值**

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单月拉取 | ~200 行 | 0.2s |
| 全量 11.7 年 | 281,758 行 | ~30 秒 |
| 1 天数据占用 | < 1 MB | — |
