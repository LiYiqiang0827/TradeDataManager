# 代码详细设计/service_hsgt_top10.md

`scripts/service/service_hsgt_top10.py` — 沪深股通十大成交股更新 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_hsgt_top10(start_date, end_date)`
4. 写日志到 `logs/service_hsgt_top10.log`

## 入口

```bash
python3 -m service.service_hsgt_top10  # 增量
python3 -m service.service_hsgt_top10 --start-date 20240101 --end-date 20240930
```

## 关键点

- **CTRL_KEY = "cn_hsgt_top10"**
- **`update_hsgt_top10` 写 `tbl_cn_hsgt_top10`**(kpl DB)
- **断点**:`tbl_kpl_ctrl.cn_hsgt_top10`
- **按月循环**(实测范围参数有效)
- 单月 ~600 行(沪 10 + 深 10 × 30 天)

## ⚠️ 重要:market_type 是数字不是字符串

| market_type | 含义 |
|---|---|
| `1` | 沪股通(SH) |
| `3` | 深股通(SZ) |

查询示例:
```python
# 沪股通
df = get_hsgt_top10(trade_date='20240930', market_type='1')
# 深股通
df = get_hsgt_top10(trade_date='20240930', market_type='3')
```

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update 第 4 阶段 → service_hsgt_top10
```

## 数据流

```
service_hsgt_top10.main()
  ↓
resolve_date_range(conn_basic, "cn_hsgt_top10", args)
  ↓
down.update_hsgt_top10(start_date, end_date)
  ↓
按月循环
```

## 注意事项

- `market_type` 是数字 `1` / `3`,**不是** SH/SZ
- 沪港通 2014-11-17 开通,但 tushare 早期就有数据(实测从 2015-01 起)
- 起始日期建议:`ctrl` 默认 20140101,空数据自动跳过

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单月 | 300 行 | 0.1s |
| 全量 11.7 年 | 39,660 行 | ~2 分钟 |
