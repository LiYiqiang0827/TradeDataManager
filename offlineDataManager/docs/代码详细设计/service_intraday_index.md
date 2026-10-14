# 代码详细设计/service_intraday_index.md

`scripts/service/service_intraday_index.py` — 大盘指数分钟 K 更新 service(2026-09-17 新增)。

## 职责

1. 解析 `--start-date` / `--end-date` / `--trade-date`(只接日期,**不接 watchlist**)
2. 调 `build_index_pairs(args)` 算出 5 指数 × N 交易日的 (ts_code, trade_date) 对
3. 调 `CNDataDown.update_minute_index(...)` 循环下载 + 落库
4. 写日志到 `logs/service_intraday_index.log`

## 入口

```bash
# 1. 单日 5 指数
python3 service_intraday_index.py --trade-date 2026-08-28

# 2. 日期段 5 指数 × N 个交易日
python3 service_intraday_index.py --start-date 2026-08-26 --end-date 2026-08-28

# 3. 强制重拉
python3 service_intraday_index.py --start-date 2026-08-26 --end-date 2026-08-28 --force
```

**显式拒绝** watchlist 和 ts-codes(指数是固定 5 个,跟具体股票无关):

```bash
python3 service_intraday_index.py --watchlist xxx.csv
# → ERROR: 指数 service 不接受 --watchlist / --ts-codes,只需日期参数
```

## 关键点

- **不接 watchlist**(2026-09-17 设计决定):指数列表固定 5 个,跟个股 watchlist 无关
- **不接 --ts-codes**:同上,不能让 caller 自选指数
- **指数列表**:`offline_downloader.INDEX_CODES_HERE` = `["000001.SH", "399001.SZ", "399006.SZ", "000688.SH", "000016.SH"]`
- **日期过交易日历过滤**:`get_tradecal(market="SSE", is_open=True)` 去掉周末/节假日
- **数据源:tdx**(指数代码也直接支持,实测 5 指数都能拉到 240 行)
- **落库:policy_minute.db / tbl_minute_index + tbl_minute_index_ctrl**(跟个股 minute **同 db,不同表**)

## 依赖链位置

```
policyStudy/policy/题材涨停研究/scripts/data_gen.py  v3.7
  │  watchlist 的 min/max trade_date 作为 --start-date / --end-date
  └─ subprocess.run([sys.executable, service_intraday_index.py, "--start-date", ..., "--end-date", ...])
       └─ service_intraday_index.main()
            ├─ build_index_pairs(args)         # 5 指数 × 区间内交易日
            ├─ ensure_schema(db_kind="minute_index")
            └─ CNDataDown().update_minute_index(ts, td, ...)
```

**没挂 scheduler**:用户手动跑 `data_gen.py` 或单独跑 service

## 数据流

```
service_intraday_index.main()
  │
  ├─ argparse 4 个参数(--start-date/--end-date/--trade-date/--rate/--force)
  │  显式检查 --watchlist/--ts-codes → ERROR 拒绝
  │
  ├─ build_index_pairs(args)
  │   ├─ --trade-date      → 5 × 1 对
  │   ├─ --start-date/--end-date → 5 × N 对(N = 区间内交易日数)
  │   └─ 都没有             → [] 返回 1
  │
  ├─ ensure_schema(db_kind="minute_index")  # 建 tbl_minute_index + ctrl
  │
  └─ run_market_loop(
       down, pairs,
       update_method="update_minute_index",
       client=TdxClient(),
       ...
     )
     │
     └─ for (ts, td) in pairs:   # ts 是指数代码
          down.update_minute_index(ts, td, ...)
          ├─ has_data("minute_index", ts, td)
          ├─ client.get_history_minute(ts, date_int)  # 指数代码直接支持
          ├─ upsert_rows("minute_index", df) + mark
          └─ time.sleep(rate)
```

## 与 service_intraday 的差异

| 维度 | service_intraday | service_intraday_index |
|---|---|---|
| ts_code 范围 | 任意个股(从 watchlist 来) | 固定 5 指数(`INDEX_CODES_HERE`) |
| 接 watchlist | ✅ | ❌(拒绝) |
| 接 --ts-codes | ✅ | ❌(拒绝) |
| 接日期 | --trade-date / --start-end | --trade-date / --start-end(语义同) |
| update_method | `update_minute` | `update_minute_index` |
| 落库表 | tbl_minute | tbl_minute_index(同 db) |
| 用途 | 涨停个股分钟研究 | 涨停 vs 大盘指数对比 |

## 注意事项

- **指数代码也走 pytdx**,跟个股 minute 用同一个接口 `get_history_minute`,只是 ts_code 是指数
- **`market_of` 自动识别**:`market_of("000001.SH")` → 1,`market_of("399001.SZ")` → 0,跟个股规则一样
- **限速 0.15s/对**(5 指数 × 160 交易日 = 800 对,约 3-4 分钟跑完)
- **跟 service_intraday 完全独立**:不依赖 watchlist,可以直接手动跑(比如补一天的指数数据)

## 性能数据(2026-09-17 实测)

| 场景 | 数据量 | 耗时 | 备注 |
|---|---|---|---|
| 1 日 × 5 指数 = 5 对 | 1,200 行 | 0.4s | 含 TdxClient 初始化 |
| 160 交易日 × 5 指数 = 800 对(全量) | 192,000 行 | 216s ≈ 3.6 分钟 | ctrl 全 skip 时 0.6s(只校验 ctrl) |
| 1 对新插 240 行 | 240 行 | ~0.1s | 单次拉取 |

## 5 指数列表(2026-09-17)

```python
INDEX_CODES_HERE = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创50
    "000016.SH",  # 上证50
]
```

区别于 `tdx_config.INDEX_CODES`(4 个,不含 000016.SH 上证50);这里扩展到 5 个,因为 policyStudy 研究需要全 5 指数。

## 修改记录

- 2026-09-17 v1.0:创建(从 `service_market_index.py` 重命名)
