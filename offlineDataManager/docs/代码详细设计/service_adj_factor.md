# 代码详细设计/service_adj_factor.md

`scripts/service/service_adj_factor.py` — 复权因子更新 service。

## 职责

1. 解析 `--trade-date` / `--start-date` / `--end-date`
2. 调 `resolve_date_range`
3. 调 `down.update_adj_factor(start_date, end_date)`
4. 写日志到 `logs/service_adj_factor.log`

## 入口

```bash
python3 -m service.service_adj_factor --trade-date 20240105
python3 -m service.service_adj_factor --start-date 20240101 --end-date 20241231
python3 -m service.service_adj_factor  # 增量
```

## 关键流程

```python
CTRL_KEY = "cn_adj_factor"

def main():
    parser = argparse.ArgumentParser(description="Update 复权因子")
    parser.add_argument("--trade-date", type=str, default=None, ...)
    parser.add_argument("--start-date", type=str, default=None, ...)
    parser.add_argument("--end-date", type=str, default=None, ...)
    args = parser.parse_args()
    
    log_file = PROJECT_ROOT / "logs" / "service_adj_factor.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_adj_factor(start_date=sd, end_date=ed)
        logger.info(f"[service_adj_factor] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_adj_factor] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_adj_factor"**
- **传 `down.conn_basic`**
- **`update_adj_factor` 写 `tbl_cn_adj_factor`**,主键 `(ts_code, trade_date)`
- **断点**:`tbl_basic_ctrl.cn_adj_factor`
- **log rotation=50 MB**

## 依赖链位置

```
service_daily  →  service_adj_factor  →  service_week
                                          ↓
                                    service_month
```

**`service_adj_factor` 必须在 `service_week` / `service_month` 之前跑**。这两个 service 启动时会校验 `ctrl.cn_daily == ctrl.cn_adj_factor == today`,否则返回 rc=2 业务中止。

## 数据流

```
service_adj_factor.main()
  ↓
resolve_date_range(conn_basic, "cn_adj_factor", args) → (sd, ed, desc)
  ↓
down.update_adj_factor(start_date=sd, end_date=ed)
  ↓
按日期循环 cur=sd..ed
  对每天 td:
    offset=0, while True:
      df = self.client.pro.adj_factor(trade_date=td, limit=6000, offset=offset)
      upsert_df(conn_basic, df, "tbl_cn_adj_factor", key_cols=["ts_code", "trade_date"])
      if len(df) < 6000: break
      offset += 6000
    cur += 1 day
  ↓
update_ctrl(conn_basic, "cn_adj_factor", last_success_date)
```

## 注意事项

- **用的是 `self.client.pro.adj_factor`**,不是 `self.client.adj_factor`(走 `pro.` 裸接口)
- **复权因子用于前复权计算**(`get_day(qfq=True)` 读时 join 此表)
- **断点 key 跟 `service_daily` 独立**,需要两个都跑完后才能跑 week / month
