# 代码详细设计/service_index_daily.md

`scripts/service/service_index_daily.py` — 12 只指数日线行情 service(2026-09-15 新增,用户要求)。

## 职责

1. 解析 `--start-date` / `--end-date`
2. 调 `down.update_index_daily(start_date, end_date)`
3. 写日志到 `logs/service_index_daily.log`

## 入口

```bash
python3 -m service.service_index_daily  # 默认 19930101 ~ 今天
python3 -m service.service_index_daily --start-date 20240101 --end-date 20240913
python3 -m service.service_index_daily --start-date 19930101  # 全量
```

## 关键流程

```python
def main():
    from core.offline_db_client import init_db
    init_db("index")
    down = CNDataDown()
    n = down.update_index_daily(start_date="19930101", end_date=None)
```

## 关键点

- **CTRL_KEY = "cn_index_daily"**(basic DB)
- **`update_index_daily` 写 `tbl_cn_index_daily`**(index DB)
- **12 只指数固定列表**(`CNDataDown.INDEX_DAILY_CODES` 类属性):
  - 上证综指 / 上证50 / 中证500 / 中证1000 / 沪深300
  - 深证成指 / 深证综指 / 创业板指 / 创业板综
  - 上证科创板50 / 科创50 / 北证50
- **按 ts_code 循环 + 每只 ts_code 内 limit + offset 分页**
- **断点**:`tbl_index_ctrl.cn_index_daily`
- 起始日期:**1993-01-01**(用户要求)
- 单只指数失败不阻塞其他指数,继续跑

## 依赖链位置

```
scheduler 触发时间表:
  - 18:00/20:00/22:00  task_full_update → service_index_daily
```

**挂 task_full_update 阶段 4**(每天 18:00 后跑)

## 数据流

```
service_index_daily.main()
  ↓
init_db("index")  # 建 db_cn_index.db
  ↓
down.update_index_daily(start_date="19930101", end_date=None)
  ↓
for ts_code in [000001.SH, 000016.SH, ..., 899050.BJ]:  # 12 只
    offset = 0
    while True:
      df = self.client.index_daily(
        ts_code=ts_code, start_date=sd, end_date=ed,
        limit=6000, offset=offset,
      )
      if df is None or empty: break  # 该指数无数据(未成立)或拉完
      upsert_df(conn_index, df, "tbl_cn_index_daily", key_cols=["trade_date", "ts_code"])
      if len(df) < 6000: break  # 拉完了
      offset += 6000
  ↓
update_ctrl(conn_basic, "cn_index_daily", last_success_date)
```

## 12 只指数列表(实测起始)

| ts_code | 名称 | 实际起始 | 行数 |
|---|---|---|---|
| 000001.SH | 上证综指 | 1993-01-04 | ~8200 |
| 399001.SZ | 深证成指 | 1993-01-04 | ~8200 |
| 399106.SZ | 深证综指 | 1993-01-03 | 8196 |
| 000016.SH | 上证50 | 2004-01-02 | ~5500 |
| 000300.SH | 沪深300 | 2005-04-08 | ~5300 |
| 000905.SH | 中证500 | 2005-01-04 | ~5300 |
| 000852.SH | 中证1000 | 2005-01-04 | ~5300 |
| 399006.SZ | 创业板指 | 2010-06-01 | 3959 |
| 399102.SZ | 创业板综 | 2010-06-01 | 3959 |
| 000680.SH | 上证科创板50 | 2019-07-22 | 1627 |
| 000688.SH | 科创50 | 2019-07-22 | 1627 |
| 899050.BJ | 北证50 | 2022-04-29 | 1064 |

**部分指数晚于 1993 年成立,代码自动适配**(从该指数实际起始日期拉取)。

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 单只 1 月 | 22 行 | ~0.2s,1 次调用 |
| **全量 12 只 1993-2026** | **58,889 行** | **5.1s,15 次调用** |