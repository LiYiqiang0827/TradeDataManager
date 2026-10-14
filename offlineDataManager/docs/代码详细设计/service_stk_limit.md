# 代码详细设计/service_stk_limit.md

`scripts/service/service_stk_limit.py` — 每日涨跌停价格更新 service(2026-09-15 新增)。

## 职责

1. 解析 `--trade-date` / `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_stk_limit(start_date, end_date)`
4. 写日志到 `logs/service_stk_limit.log`

## 入口

```bash
python3 -m service.service_stk_limit --trade-date 20241008
python3 -m service.service_stk_limit --start-date 20240101 --end-date 20241008
python3 -m service.service_stk_limit  # 增量(ctrl)
```

## 关键流程

```python
CTRL_KEY = "cn_stk_limit"

def main():
    parser = argparse.ArgumentParser(description="Update 每日涨跌停价格")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()

    log_file = PROJECT_ROOT / "logs" / "service_stk_limit.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)

    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_stk_limit(start_date=sd, end_date=ed)
        logger.info(f"[service_stk_limit] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_stk_limit] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_stk_limit"**
- **传 `down.conn_basic`** 给 `resolve_date_range`
- **`update_stk_limit` 写 `tbl_cn_stk_limit`**,主键 `(trade_date, ts_code)`
- **断点**:`tbl_basic_ctrl.cn_stk_limit`
- **log rotation=20 MB**(数据量适中,2 年 ~260 万行,服务跑慢所以 20MB)
- **按日循环**(实测 tushare `pro.stk_limit` 的 `start_date` / `end_date` 范围参数不生效,只能 `trade_date` 单日拉)

## 依赖链位置

```
scheduler 触发时间表:
  - 09:05  task_morning_update → service_stk_limit + service_kpl_list
  - 18:00/20:00/22:00  task_full_update → ... + service_stk_limit(第 4 阶段)
```

**挂两个 task**(2026-09-15):
- `task_morning_update`:每天 09:05 跑一次(用户开盘前快速补)
- `task_full_update` 第 4 阶段:每天 18:00/20:00/22:00 跑三次(兜底)

## 数据流

```
service_stk_limit.main()
  ↓
resolve_date_range(conn_basic, "cn_stk_limit", args) → (sd, ed, desc)
  ↓
down.update_stk_limit(start_date=sd, end_date=ed)
  ↓
按日期循环 cur=sd..ed
  对每天 td:
    df = self.client.stk_limit(trade_date=td)  # ~5438 行,< 5800 限制
    if df is None or empty: continue  # 非交易日 / 没数据
    df["snap_ts"] = snap_ts()
    upsert_df(conn_basic, df, "tbl_cn_stk_limit", key_cols=["trade_date", "ts_code"])
    if inserted > 0: last_success_date = td
  ↓
update_ctrl(conn_basic, "cn_stk_limit", last_success_date)
  ↓
return total_inserted
```

## 注意事项

- **挂两个 task**:`task_morning_update` (09:05) + `task_full_update` 第 4 阶段 (18:00/20:00/22:00)
- **限流**:Tushare 200 次/分钟,跑 2 年约 503 次调用,~3.5 分钟(实测),触发限流时 `RateLimiter` 自动 sleep
- **包含 A 股 + 场内基金**:tushare `pro.stk_limit` 返回 A 股 + ETF/LOF,**不区分**,都存进同一张表
- **特殊数据**:`trade_date` 是 YYYYMMDD(8 位),`ts_code` 包含 `.SZ` / `.SH` / `.BJ` 等后缀
- **空日不推进断点**(节假日)

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单日拉取 | 5438 行 | 0.2s |
| 2 年增量(503 天) | 2,620,294 行 | 203.7s(~3.5 分钟) |
| 1 天数据占用 | ~ 6.4 MB(2,625,732 行 ≈ 100 MB) | — |

注:2 年增量占总 DB 增量 < 1%,因为数值字段少(4 列 REAL,2 个 TEXT)。
