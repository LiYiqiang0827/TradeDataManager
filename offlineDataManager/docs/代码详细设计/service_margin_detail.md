# 代码详细设计/service_margin_detail.md

`scripts/service/service_margin_detail.py` — 融资融券交易明细 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date` / `--trade-date`
2. 复用 `service_margin._resolve_margin_date_range`，默认回看10个自然日；显式历史范围不受 ctrl 抬升
3. 调 `down.update_margin_detail(start_date, end_date)`
4. 写日志到 `logs/service_margin_detail.log`

## 入口

```bash
python3 -m service.service_margin_detail  # 增量：ctrl 往前10自然日到今天
python3 -m service.service_margin_detail --start-date 20171229 --end-date 20180131
python3 -m service.service_margin_detail --trade-date 20240913  # 单日
```

## 关键点

- **CTRL_KEY = "cn_margin_detail"**
- 默认起点为 `max(20150101, min(ctrl, 今天) - 10日)`，终点今天；没有 ctrl 则从20150101开始。
- `--trade-date` 或显式范围直接执行，不经过全局 `resolve_date_range` 的 ctrl 抬升。只传开始日期时结束为今天；只传结束日期时开始为20150101。单日与范围互斥，倒置范围报错。
- **`update_margin_detail` 写 `tbl_cn_margin_detail`**(basic DB)
- **断点**:`tbl_basic_ctrl.cn_margin_detail`
- **按月循环 + OFFSET 分页**(单月 2000-6000 行,需 OFFSET 0-1 次)
- 主键:`(trade_date, ts_code)`
- **用户要求:挂 task_morning_update(09:05)**(跟 `service_margin` 一起)

## 依赖链位置

```
scheduler 触发时间表:
  - 09:05  task_morning_update → service_margin_detail
```

**只挂一个 task**(2026-09-15):`task_morning_update` 阶段

## 数据流

```
service_margin_detail.main()
  ↓
_resolve_margin_date_range(conn_basic, "cn_margin_detail", args)
  无日期参数：从 ctrl 往前10自然日重试
  显式日期：按请求范围执行，不抬到 ctrl
  ↓
down.update_margin_detail(start_date, end_date)
  ↓
按月循环:
  对每月:
    offset = 0
    while True:
      df = self.client.margin_detail(
        start_date=cur_sd, end_date=cur_ed, limit=6000, offset=offset,
      )
      if df is None or len(df) == 0: break
      upsert_df(conn_basic, df, "tbl_cn_margin_detail", key_cols=["trade_date", "ts_code"])
      if len(df) < 6000: break  # 拉完了
      offset += 6000
      if offset > 100000: break  # 安全上限
  ↓
update_ctrl(conn_basic, "cn_margin_detail", last_success_date)
```

## 注意事项

- **单次最多 6000 条**(Tushare 限制)
- **单月 2000-6000 行**(2018 早期约 1000,2024 约 4000)
- **OFFSET 分页**: 单月可能 1-2 页
- **实际起始 2014-12-31**(用户要求 2015-01-01 开始,实测 2014-12-31 最早有数据)
- 融资融券数据盘后才出,挂 task_morning 抓"昨天"
- 10自然日回看重试晚到的 SH/SZ/BJ 数据，不保证更早缺口自动补齐。历史缺口用显式日期修复，并按市场逐日复核；当前 downloader 的 ctrl 不表示各市场完整水位。

## 验证

`scripts/service/test_margin_date_ranges.py` 同时覆盖汇总和明细服务的日期解析，使用内存数据库；不下载、不写生产库。

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单日 | 3923 行 | 0.5s,1 次调用 |
| 单月 (2024-09) | ~80,000 行 | ~2s,1 次调用(不满 6000) |
| 全量 2014-12-31 ~ 2026-09-15(141 个月) | ~1100 万行 | ~30-45 分钟 |
