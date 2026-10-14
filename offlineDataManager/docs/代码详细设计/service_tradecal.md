# 代码详细设计/service_tradecal.md

`scripts/service/service_tradecal.py` — 交易日历增量更新 service。

## 职责

1. 解析 `--trade-date` / `--start-date` / `--end-date` 三个日期参数
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_tradecal(start_date, end_date)`(3 个交易所各跑一遍)
4. 写日志到 `logs/service_tradecal.log`

## 入口

```bash
python3 -m service.service_tradecal --trade-date 20260915
python3 -m service.service_tradecal --start-date 20240101 --end-date 20241231
python3 -m service.service_tradecal  # 增量
```

## 关键流程

```python
from core.offline_downloader import CNDataDown
from service.common import resolve_date_range

def main():
    parser = argparse.ArgumentParser(description="Update 交易日历")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()
    
    log_file = PROJECT_ROOT / "logs" / "service_tradecal.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        # tradecal 有 3 个交易所,ctrl 分开,这里用 SSE 断点作为统一起点
        # update_tradecal 内部每个交易所仍会用各自的 ctrl 增量
        sd, ed, desc = resolve_date_range(down.conn_basic, "cn_tradecal_SSE", args)
        n = down.update_tradecal(start_date=sd, end_date=ed)
        logger.info(f"[service_tradecal] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_tradecal] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **3 个交易所的 ctrl key 不同**:`cn_tradecal_SSE` / `cn_tradecal_SZSE` / `cn_tradecal_BSE`
- **`service_tradecal` 调 `resolve_date_range` 只用 SSE 断点**作为 service 层的 start_date 起点
- **`update_tradecal` 内部每个交易所用各自 ctrl 增量**(SSE/SZSE/BSE 分别 `get_ctrl(self.conn_basic, f"cn_tradecal_{exch}")`)
- **`update_tradecal` 接受 start_date 参数时,3 个交易所都从同一起点拉**(绕过各自 ctrl 增量)
- **断点 key**:`tbl_basic_ctrl.cn_tradecal_<SSE/SZSE/BSE>`
- **log rotation=20 MB**

## 数据流

```
service_tradecal.main()
  ↓
resolve_date_range(conn_basic, "cn_tradecal_SSE", args) → (sd, ed, desc)
  ↓
down.update_tradecal(start_date=sd, end_date=ed)
  ↓
for exch in ["SSE", "SZSE", "BSE"]:
    last = get_ctrl(self.conn_basic, f"cn_tradecal_{exch}")
    actual_sd = sd if start_date 参数 else (last if last else "20180101")
    df = client.trade_cal(exchange=exch, start_date=actual_sd, end_date=ed)
    upsert_df(conn_basic, df, "tbl_cn_tradecal", key_cols=["cal_date", "exchange"])
    update_ctrl(conn_basic, f"cn_tradecal_{exch}", max_date)
  ↓
return 总行数
```

## 注意事项

- **3 个交易所的断点是独立的**,但 service 传的 `start_date` 会同时影响 3 个交易所
- **BSE 实际从未拉到数据**(DB 里只有 SSE+ SZSE),但代码仍会尝试拉
- **如果 3 个交易所 ctrl 进度不一致**(如 SSE 拉到 20250901,SZSE 还在 20250801),service 传 start_date 时会强制 3 个都从同一天起,可能拉重数据(主键去重保证幂等)
