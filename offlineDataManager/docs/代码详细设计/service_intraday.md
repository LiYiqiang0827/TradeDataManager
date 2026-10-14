# 代码详细设计/service_intraday.md

`scripts/service/service_intraday.py` — 个股分钟 K 更新 service(2026-09-17 新增)。

## 职责

1. 解析 `--watchlist` / `--ts-codes` + 日期参数
2. 调 `resolve_market_pairs` 算出待下载的 (ts_code, trade_date) 对
3. 调 `CNDataDown.update_minute(...)` 循环下载 + 落库
4. 写日志到 `logs/service_intraday.log`

## 入口

```bash
# 1. 用 watchlist(传名字或绝对路径)
python3 service_intraday.py --watchlist watchlist_tczt_20260101_20260831.csv
python3 service_intraday.py --watchlist /tmp/my_watchlist.csv

# 2. 单只单日
python3 service_intraday.py --ts-codes 000006.SZ --trade-date 2026-08-28

# 3. 多只 + 日期段
python3 service_intraday.py --ts-codes "000006.SZ,000636.SZ" --start-date 2026-08-26 --end-date 2026-08-28

# 4. 强制重拉(忽略 ctrl)
python3 service_intraday.py --watchlist x.csv --force

# 5. 调限速
python3 service_intraday.py --watchlist x.csv --rate 0.3
```

## 关键点

- **不接 watchlist_dir**(2026-09-17 v3.7 设计决定):watchlist 路径由 caller 解析,service 只接受完整路径
- **数据源:tdx**(不是 tushare),依赖 `coreClient/tdx_client.TdxClient`
- **落库:policy_minute.db / tbl_minute + tbl_minute_ctrl**
- **schema 由 `core.offline_db_client.ensure_schema("minute")` 在启动时建**(本 service 不管)
- **断点机制**:`CNDataDown.update_minute` 内部调 `has_data("minute", ts, td)` 查 ctrl,已下载则 skip

## 依赖链位置

```
policyStudy/policy/题材涨停研究/scripts/data_gen.py  v3.7
  └─ subprocess.run([sys.executable, service_intraday.py, "--watchlist", temp_csv, ...])
       └─ service_intraday.main()
            └─ resolve_market_pairs(args)            # 算 (ts_code, trade_date) 对
            └─ CNDataDown().update_minute(ts, td, ...)  # 调 tdx + 写 db
```

**没挂 scheduler**(2026-09-17 设计决定):用户手动跑 `data_gen.py watchlist xxx`,或单独跑 service

## 数据流

```
service_intraday.main()
  │
  ├─ argparse 解析 7 个参数(--watchlist/--ts-codes/--trade-date/--start-date/--end-date/--rate/--force)
  │
  ├─ resolve_market_pairs(args)  # 三种模式自动选:
  │    ├─ --watchlist → load_watchlist_pairs() 读 csv
  │    ├─ --ts-codes  → build_ts_codes_pairs() 笛卡尔积
  │    └─ 都没有     → ([], 'no_pairs') 返回 1
  │
  ├─ ensure_schema(db_kind="minute")  # 幂等建表
  │
  └─ run_market_loop(
       down, pairs,
       update_method="update_minute",  # 关键:对应 CNDataDown.update_minute
       client=TdxClient(),
       rate=args.rate,
       force=args.force,
     )
     │
     └─ for (ts, td) in pairs:
          inserted = down.update_minute(ts, td, client=client, rate=rate, force=force)
          ├─ force=False 时 has_data("minute", ts, td) 查 ctrl
          ├─ 调 client.get_history_minute(ts, date_int) 拉 240 行
          ├─ upsert_rows("minute", df) + mark_downloaded()
          └─ time.sleep(rate) 限速
```

## 与其他 service 的差异

| 维度 | service_intraday(本) | service_ticks | service_intraday_index |
|---|---|---|---|
| 数据源 | tdx `get_history_minute` | tdx `get_history_ticks` | tdx `get_history_minute`(指数代码) |
| 落库 | policy_minute.db / tbl_minute | policy_ticks.db / tbl_tick | policy_minute.db / tbl_minute_index |
| 接收 watchlist | ✅ | ✅ | ❌(指数固定 5 个,只接日期) |
| update_method | `update_minute` | `update_ticks` | `update_minute_index` |

## 注意事项

- **window 算法在 caller(data_gen.py)**,本 service 不算窗口 —— 只接受完整 (ts, td) 对
- **限速默认 0.15s/对**(pytdx ~70 req/s,实测安全值)
- **失败隔离**:`run_market_loop` 内 try/except,单对失败不中断后续
- **幂等**:force=False 时 ctrl 已 mark 跳过;ctrl 状态保留在 db 文件里,mv 文件后仍有效

## 性能数据(2026-09-17 实测)

| 场景 | 数据量 | 耗时 | 备注 |
|---|---|---|---|
| 1 对(000006.SZ 2026-08-28) | 240 行 | 0.3s | 包含 TdxClient 初始化(0.1s)+ tdx 拉取(0.1s)+ upsert(0.05s) |
| 800 对(5 指数 × 160 交易日) | 192,000 行 | 216s ≈ 3.6 分钟 | speed ~3.7 对/s,瓶颈是 tdx 网络延迟 |
| 13738 对(涨停 watchlist × 涨停日前 1 后 2 4 天) | ~35000 对 ctrl skip 后 | ~待测 | 取决于网络 |

## 修改记录

- 2026-09-17 v1.0:创建(从 `service_market_minute.py` 重命名,内部 `update_method` 保持 `update_minute`)
