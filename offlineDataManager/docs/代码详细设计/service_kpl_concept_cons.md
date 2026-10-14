# 代码详细设计/service_kpl_concept_cons.md

`scripts/service/service_kpl_concept_cons.py` — 开盘啦题材成分增量更新 service。

## 职责

1. 解析日期参数
2. 调 `resolve_date_range`
3. 调 `down.update_kpl_concept_cons(start_date, end_date)`
4. 写日志到 `logs/service_kpl_concept_cons.log`

## 入口

```bash
python3 -m service.service_kpl_concept_cons --trade-date 20240909
python3 -m service.service_kpl_concept_cons --start-date 20240101 --end-date 20241231
python3 -m service.service_kpl_concept_cons  # 增量
```

## 关键流程

```python
CTRL_KEY = "cn_kpl_concept_cons"

def main():
    parser = argparse.ArgumentParser(description="Update 题材成分")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()
    
    log_file = PROJECT_ROOT / "logs" / "service_kpl_concept_cons.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_kpl, CTRL_KEY, args)
        n = down.update_kpl_concept_cons(start_date=sd, end_date=ed)
        logger.info(f"[service_kpl_concept_cons] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_kpl_concept_cons] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_kpl_concept_cons"**
- **传 `down.conn_kpl`**
- **`update_kpl_concept_cons` 写 `tbl_cn_kpl_concept_cons`**,主键 `(ts_code, con_code, trade_date)`
  - `ts_code` 是**题材代码**(不是股票代码)
  - `con_code` 才是成分股代码
- **断点**:`tbl_kpl_ctrl.cn_kpl_concept_cons`
- **log rotation=50 MB**

## 数据流

```
service_kpl_concept_cons.main()
  ↓
resolve_date_range(conn_kpl, "cn_kpl_concept_cons", args)
  → tbl_kpl_ctrl 已删 → fallback 到 20180101
  ↓
down.update_kpl_concept_cons(start_date=sd, end_date=ed)
  ↓
按日期循环,每天 tushare pro.kpl_concept_cons(trade_date=td, limit=3000, offset=offset)
  ↓
upsert_df(conn_kpl, df, "tbl_cn_kpl_concept_cons", key_cols=["ts_code", "con_code", "trade_date"])
  ↓
update_ctrl(conn_basic, "cn_kpl_concept_cons", last_success_date)
```

**注意**(跟 `service_kpl_list` 同问题):
- `tbl_kpl_ctrl` 已删,`get_ctrl` 走 fallback 返回 None
- service 传 `conn_kpl` 给 `resolve_date_range` 读到空
- 然后 service 把 `sd=20180101` 传给 `update_kpl_concept_cons`,函数内部 `if start_date: sd = _fmt_yyyymmdd(start_date)` —— **直接采用外部 sd,不再读 ctrl**
- **结果**:`service_kpl_concept_cons` 每次都从 20180101 全量重拉(主键去重保证幂等,但跑得慢)

## 注意事项

- **跟 `service_kpl_list` 同样的 read/write 错位**:`tbl_kpl_ctrl` 已删,实际断点在 `tbl_basic_ctrl`
- **tushare 拉的是题材-成分股关系**,不是题材-股票映射
- **表里 `ts_code` 是题材代码**(`000009.KP` 风格),`con_code` 是成分股代码(`600104.SH`)
- **`get_kpl_concept_cons.ts_codes` 实际查 `con_code`**(API 设计为方便查询,API 名保留)
