# 代码详细设计/service_daily.md

`scripts/service/service_daily.py` — A 股日 K 更新 service。

## 职责

1. 解析 `--trade-date` / `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_daily(start_date, end_date)`
4. 写日志到 `logs/service_daily.log`

## 入口

```bash
python3 -m service.service_daily --trade-date 20240909
python3 -m service.service_daily --start-date 20240101 --end-date 20241231
python3 -m service.service_daily  # 增量
```

## 关键流程

```python
CTRL_KEY = "cn_daily"

def main():
    parser = argparse.ArgumentParser(description="Update 日 K")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()
    
    log_file = PROJECT_ROOT / "logs" / "service_daily.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_daily(start_date=sd, end_date=ed)
        logger.info(f"[service_daily] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_daily] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_daily"**
- **传 `down.conn_basic`** 给 `resolve_date_range`
- **`update_daily` 写 `tbl_cn_day`**,主键 `(ts_code, trade_date)`
- **断点**:`tbl_basic_ctrl.cn_daily`
- **log rotation=50 MB**

## 依赖链位置

```
service_basic  →  service_daily  →  service_adj_factor
                                     ↓
                               service_week
                                     ↓
                               service_month
```

**`service_daily` 必须在 `service_adj_factor` 之前跑**(service_adj_factor 内部读 `tbl_cn_adj_factor` 跟 `tbl_cn_day` 算前复权,如果 `tbl_cn_day` 还没今天的数据,前复权会算错。scheduler 调度顺序:basic → tradecal → daily → adj_factor → week → month)。

## 数据流

```
service_daily.main()
  ↓
resolve_date_range(conn_basic, "cn_daily", args) → (sd, ed, desc)
  ↓
down.update_daily(start_date=sd, end_date=ed)
  ↓
按日期循环 cur=sd..ed
  对每天 td:
    offset=0, while True:
      df = self.client.daily(trade_date=td, limit=6000, offset=offset)
      upsert_df(conn_basic, df, "tbl_cn_day", key_cols=["ts_code", "trade_date"])
      if len(df) < 6000: break  # 当天拉完
      offset += 6000
    cur += 1 day
  ↓
update_ctrl(conn_basic, "cn_daily", last_success_date)
  ↓
return total_inserted
```

## 注意事项

- **拉的是不复权原始数据**(`tbl_cn_day` 不存 qfq)
- **`update_daily` 单日 6000 LIMIT**,2024+ 部分日期可能超 6000(自动 OFFSET 分页)
- **`--trade-date` 优先级最高**:`resolve_date_range` 内部 if 优先级 `trade_date > start/end > 默认增量`,**不是 mutex**(同时传 `trade_date + start_date`,`trade_date` 生效,`start_date` 被忽略)
