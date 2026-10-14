# 代码详细设计/service_top_inst.md

`scripts/service/service_top_inst.py` — 龙虎榜机构买卖明细 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_top_inst(start_date, end_date)`
4. 写日志到 `logs/service_top_inst.log`

## 入口

```bash
python3 -m service.service_top_inst  # 增量
python3 -m service.service_top_inst --start-date 20201201 --end-date 20240930
```

## 关键流程

```python
CTRL_KEY = "cn_top_inst"

def main():
    parser = argparse.ArgumentParser(description="Update 龙虎榜机构买卖明细")
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_top_inst.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_top_inst(start_date=sd, end_date=ed)
        logger.info(f"[service_top_inst] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_top_inst] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_top_inst"**
- **`update_top_inst` 写 `tbl_cn_top_inst`**(kpl DB)
- **断点**:`tbl_kpl_ctrl.cn_top_inst`
- **主键**:`(trade_date, ts_code, exalter, side)` 4 列
- **单日 ~533 行**,数据量大(全量 ~300 万行)
- **按日循环 + 跳过周末**(跟 top_list 共用 `_update_daily_top` 实现)

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update 第 4 阶段 → service_top_inst
```

**只挂一个 task**(2026-09-15):`task_full_update` 第 4 阶段

## 数据流

```
service_top_inst.main()
  ↓
resolve_date_range(conn_basic, "cn_top_inst", args) → (sd, ed, desc)
  ↓
down.update_top_inst(start_date=sd, end_date=ed)
  ↓
按日循环 cur=sd..ed
  对每天 td:
    if weekday >= 5: skip
    df = self.client.top_inst(trade_date=td)  # ~533 行/日
    if df is None or empty: continue
    df["snap_ts"] = snap_ts()
    upsert_df(conn_kpl, df, "tbl_cn_top_inst", key_cols=["trade_date", "ts_code", "exalter", "side"])
  ↓
update_ctrl(conn_basic, "cn_top_inst", last_success_date)
  ↓
return total_inserted
```

## 注意事项

- **跳过周六周日**
- **限流密集**(200次/分钟),RateLimiter 自动 sleep ~30s
- **单日 ~533 行 × 1404 天 ≈ 75 万行**(全量 11.7 年)
- **失败单日跳过**,继续下一天
- **跟 top_list 共用实现**:`_update_daily_top`

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 | 调用 |
|---|---|---|---|
| 单日 | ~533 行 | 0.2s | 1 |
| 全量 11.7 年 | 估算 ~300 万行 | ~30 分钟 | 1404 |
| 部分增量 | 884,239 行(2015-01~2021-09) | 几分钟 | ~1500 |
