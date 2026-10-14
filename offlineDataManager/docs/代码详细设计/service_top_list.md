# 代码详细设计/service_top_list.md

`scripts/service/service_top_list.py` — 龙虎榜每日活跃 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_top_list(start_date, end_date)`
4. 写日志到 `logs/service_top_list.log`

## 入口

```bash
python3 -m service.service_top_list  # 增量
python3 -m service.service_top_list --start-date 20201201 --end-date 20240930
```

## 关键流程

```python
CTRL_KEY = "cn_top_list"

def main():
    parser = argparse.ArgumentParser(description="Update 龙虎榜每日活跃")
    parser.add_argument("--start_date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_top_list.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_top_list(start_date=sd, end_date=ed)
        logger.info(f"[service_top_list] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_top_list] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_top_list"**
- **`update_top_list` 写 `tbl_cn_top_list`**(kpl DB,2026-09-15 用户要求)
- **断点**:`tbl_kpl_ctrl.cn_top_list`
- **log rotation=20 MB**
- **按日循环**(实测无范围参数支持,跳过周六周日)
- **1404 次调用 + 触发限流**(实测 ~7 分钟)

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update 第 4 阶段 → service_top_list
```

**只挂一个 task**(2026-09-15):`task_full_update` 第 4 阶段

## 数据流

```
service_top_list.main()
  ↓
resolve_date_range(conn_basic, "cn_top_list", args) → (sd, ed, desc)
  ↓
down.update_top_list(start_date=sd, end_date=ed)
  ↓
按日循环 cur=sd..ed
  对每天 td:
    if weekday >= 5: skip  # 跳过周末
    df = self.client.top_list(trade_date=td)  # ~65 行/日
    if df is None or empty: continue
    df["snap_ts"] = snap_ts()
    upsert_df(conn_kpl, df, "tbl_cn_top_list", key_cols=["trade_date", "ts_code"])
  ↓
update_ctrl(conn_basic, "cn_top_list", last_success_date)
  ↓
return total_inserted
```

## 注意事项

- **跳过周六周日**(周末不调)
- **限流频繁**(200次/分钟),RateLimiter 自动 sleep ~30s
- **单日失败不中止**,继续下一天(写入 last_success_date)

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 | 调用 |
|---|---|---|---|
| 单日 | ~65 行 | 0.1s | 1 |
| 全量 11.7 年 | 103,941 行 | ~7 分钟 | 1404 |
| 增量(空跑) | 0 行 | 0.5s | 0 |
