# 代码详细设计/service_limit_list.md

`scripts/service/service_limit_list.py` — 每日涨跌停列表 service(2026-09-15 新增)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `resolve_date_range` 决定拉取范围
3. 调 `down.update_limit_list(start_date, end_date)`
4. 写日志到 `logs/service_limit_list.log`

## 入口

```bash
python3 -m service.service_limit_list  # 增量
python3 -m service.service_limit_list --start-date 20240901 --end-date 20240930
```

## 关键点

- **CTRL_KEY = "cn_limit_list"**
- **`update_limit_list` 写 `tbl_cn_limit_list`**(kpl DB)
- **断点**:`tbl_kpl_ctrl.cn_limit_list`
- **按月循环**,3 个 `limit_type`(U/D/Z)各跑一次
- 单月 ~1700 行(U+D+Z 合计)

## ⚠️ 重要:`limit` 是 SQLite 保留字

| 问题 | 解决方案 |
|---|---|
| 字段名 `limit` | 用反引号 `` ` `` 引用 |
| 主键 / 索引 | `PRIMARY KEY (trade_date, ts_code, "limit")` |
| WHERE 子句 | `` wheres.append('`limit` = ?') `` |
| upsert_df | 已在 `offline_db_client.py` 改造为自动加反引号 |

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update 第 4 阶段 → service_limit_list
```

## 数据流

```
service_limit_list.main()
  ↓
resolve_date_range(conn_basic, "cn_limit_list", args)
  ↓
down.update_limit_list(start_date, end_date)
  ↓
按月循环
  对每月:
    for lt in ("U", "D", "Z"):
      df = self.client.limit_list_d(start_date, end_date, limit_type=lt)
      if df is not None and len(df) > 0:
        upsert_df(conn_kpl, df, "tbl_cn_limit_list", key_cols=["trade_date", "ts_code", "limit"])
  ↓
update_ctrl(conn_basic, "cn_limit_list", last_success_date)
```

## 注意事项

- **`limit` 是 SQLite 保留字** — SQL 必须用反引号
- 单日 < 2500 行,无需 OFFSET 分页
- tushare 文档 `doc_id=298`(原以为是 ggt_daily,实际是 limit_list_d)
- **实测起始日期:2019-11-28**(不是 2020 年,也不是 2024-09)
- 7 年数据 ~30 万行,25 秒跑完

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单月 | 2,231 行 | 0.4s |
| 全量 2019-11-28 ~ 2026-09-15 | **165,451 行**(涨停 U 102,169 + 跌停 D 25,653 + 炸板 Z 37,629) | ~25 秒,357 次调用 |
