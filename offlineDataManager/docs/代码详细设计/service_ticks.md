# 代码详细设计/service_ticks.md

`scripts/service/service_ticks.py` — 个股分笔成交更新 service(2026-09-17 新增)。

## 职责

1. 解析 `--watchlist` / `--ts-codes` + 日期参数
2. 调 `resolve_market_pairs` 算出待下载的 (ts_code, trade_date) 对
3. 调 `CNDataDown.update_ticks(...)` 循环下载 + 落库
4. 写日志到 `logs/service_ticks.log`

## 入口

```bash
# 1. 用 watchlist
python3 service_ticks.py --watchlist watchlist_tczt_20260101_20260831.csv

# 2. 单只单日
python3 service_ticks.py --ts-codes 000006.SZ --trade-date 2026-08-28

# 3. 多只 + 日期段
python3 service_ticks.py --ts-codes "000006.SZ,000636.SZ" --start-date 2026-08-26 --end-date 2026-08-28

# 4. 强制重拉 / 限速
python3 service_ticks.py --watchlist x.csv --force
python3 service_ticks.py --watchlist x.csv --rate 0.3
```

## 关键点

- **不接 watchlist_dir**:watchlist 路径由 caller 解析
- **数据源:tdx**(`get_history_ticks`),依赖 `coreClient/tdx_client.TdxClient`
- **落库:policy_ticks.db / tbl_tick + tbl_tick_ctrl**(跟 minute 是**不同的 db**)
- **schema 由 `core.offline_db_client.ensure_schema("ticks")` 在启动时建**
- **断点机制**:同 service_intraday,`CNDataDown.update_ticks` 内部调 `has_data("ticks", ts, td)` 查 ctrl

## 依赖链位置

```
policyStudy/policy/题材涨停研究/scripts/data_gen.py  v3.7
  └─ subprocess.run([sys.executable, service_ticks.py, "--watchlist", temp_csv, ...])
       └─ service_ticks.main()
            └─ resolve_market_pairs(args)
            └─ CNDataDown().update_ticks(ts, td, ...)
```

**没挂 scheduler**:用户手动跑 `data_gen.py` 或单独跑 service

## 数据流

```
service_ticks.main()
  │
  ├─ argparse 7 个参数(--watchlist/--ts-codes/--trade-date/--start-date/--end-date/--rate/--force)
  │
  ├─ resolve_market_pairs(args)  # 详见 service_intraday.md
  │
  ├─ ensure_schema(db_kind="ticks")  # 建 tbl_tick + tbl_tick_ctrl
  │
  └─ run_market_loop(
       down, pairs,
       update_method="update_ticks",
       client=TdxClient(),
       ...
     )
     │
     └─ for (ts, td) in pairs:
          inserted = down.update_ticks(ts, td, client=client, rate=rate, force=force)
          ├─ has_data("ticks", ts, td) 查 ctrl
          ├─ client.get_history_ticks(ts, date_int) 拉(单日 1000-20000 行,涨停日 1-3 万)
          ├─ upsert_rows("ticks", df) + mark_downloaded()
          └─ time.sleep(rate)
```

## 与 service_intraday 的差异

| 维度 | service_intraday | service_ticks |
|---|---|---|
| tdx 接口 | `get_history_minute` | `get_history_ticks` |
| 单日数据量 | 240 行(分钟 K) | 1000-20000 行(分笔) |
| 落库 db | policy_minute.db | **policy_ticks.db**(独立 db) |
| update_method | `update_minute` | `update_ticks` |

ticks 数据量比 minute 大 1-2 个数量级,跑全量时**总耗时明显更长**;但调用模式完全对称。

## 注意事项

- **ticks 单日数据量大**(涨停日可达 1-3 万笔),拉取耗时主要在 tdx 网络传输
- **极端单日上限 5 万行**(`TDX_TICKS_MAX_PAGES = 50`,每页 2000 条)
- **拉取失败不要紧**:tdx_client 内部已有 retry + backoff,本 service 不重复处理
- **ctrl 表 `(ts_code, trade_date)`** 是 PK,upsert 用 (ts_code, trade_date, seqId) 三键,保证分笔不重复

## 性能数据(2026-09-17 实测)

| 场景 | 数据量 | 耗时 | 备注 |
|---|---|---|---|
| 1 对(000006.SZ 2026-08-28) | 2572 行 | 0.4s | TdxClient 0.1s + tdx 拉 0.2s + upsert 0.1s |
| 全量 13738 对(原 ticks 任务) | 30,507,851 行(已下载) | 4-5 小时(估算) | 1 涨停日 ≈ 0.5-1s,网络瓶颈 |

## 修改记录

- 2026-09-17 v1.0:创建(从 `service_market_ticks.py` 重命名)
