# 代码详细设计/service_margin.md

`scripts/service/service_margin.py` — 融资融券交易汇总 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date` / `--trade-date`
2. 调服务层 `_resolve_margin_date_range` 决定拉取范围，不修改全局 `common.resolve_date_range`
3. 调 `down.update_margin(start_date, end_date)`
4. 写日志到 `logs/service_margin.log`

## 入口

```bash
python3 -m service.service_margin  # 增量：ctrl 往前回看10个自然日，到今天
python3 -m service.service_margin --start-date 20100401 --end-date 20100430
python3 -m service.service_margin --trade-date 20260924  # 历史缺市场修复
```

## 关键点

- **CTRL_KEY = "cn_margin"**
- **默认增量回看10个自然日**：起点为 `max(20150101, min(ctrl, 今天) - 10日)`，终点为今天；没有断点时从20150101开始。覆盖 T+1、假期和交易所分批晚到的数据，重复主键由入库层去重。
- **显式历史日期不受 ctrl 抬升**：传 `--trade-date` 或 `--start-date/--end-date` 原样执行。只传开始日期时结束为今天；只传结束日期时开始为20150101。单日与范围参数互斥，日期倒置报错。
- **`update_margin` 写 `tbl_cn_margin`**(basic DB)
- **断点**:`tbl_basic_ctrl.cn_margin`
- **按日循环**(无 range 参数,必须单日)
- 跳过周末(融资融券仅交易日)
- 单日 3 行(SSE + SZSE + BSE)
- **用户要求:挂 task_morning_update(09:05)**

## 依赖链位置

```
scheduler 触发时间表:
  - 09:05  task_morning_update → service_margin
```

**只挂一个 task**(2026-09-15):`task_morning_update` 阶段

## 数据流

```
service_margin.main()
  ↓
_resolve_margin_date_range(conn_basic, "cn_margin", args)
  无日期参数：从 ctrl 往前10自然日重试
  显式日期：按请求范围执行，不抬到 ctrl
  ↓
down.update_margin(start_date, end_date)
  ↓
按日循环(跳过周末)
  对每天:
    df = self.client.margin(trade_date=td)  # 3 行
    if df is not None and len(df) > 0:
      upsert_df(conn_basic, df, "tbl_cn_margin", key_cols=["trade_date", "exchange_id"])
  ↓
update_ctrl(conn_basic, "cn_margin", last_success_date)
```

## 注意事项

- **单日最多 3 行**(SSE + SZSE + BSE)
- **BSE 2021 才开业**,2021 之前只有 2 行(SSE + SZSE)
- **实际起始 2015-01-05**(用户要求 2015-01-01 开始,实测 2015-01-05 最早有数据)
- 融资融券数据盘后才出,挂 task_morning 抓"昨天"
- 回看是晚到重试机制，不代表断点之前已全部完整；超过10自然日的历史缺口需显式回补，并逐日核对 SSE/SZSE/BSE 覆盖。当前 downloader 的断点语义尚不等于“各市场完整水位”。
- 无 range 参数,必须按日循环

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单日 | 3 行 | 0.4s,1 次调用 |
| 全量 2015-01-05 ~ 2026-09-15(~12 年) | ~1.1 万行 | ~5-7 分钟 |

## 验证

`scripts/service/test_margin_date_ranges.py` 使用内存 SQLite 验证默认回看、显式历史日期不被抬升及错误参数；不调用外部 API，不写生产数据库。
