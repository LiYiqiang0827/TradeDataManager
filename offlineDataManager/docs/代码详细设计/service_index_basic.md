# 代码详细设计/service_index_basic.md

`scripts/service/service_index_basic.py` — 指数基本信息 service(2026-09-15 新增)。

## 职责

1. 调 `init_db("index")` 建表(新库 `db_cn_index.db`)
2. 调 `down.update_index_basic()` 全量覆盖
3. 写日志到 `logs/service_index_basic.log`

## 入口

```bash
python3 -m service.service_index_basic
```

## 关键流程

```python
def main():
    init_logger(SERVICE_NAME)
    init_db("index")  # 确保 index DB 存在并建表

    down = CNDataDown()
    n = down.update_index_basic()  # 全量覆盖,~30 秒
```

## 关键点

- **CTRL_KEY = "cn_index_basic"**(basic DB, max_date="all")
- **写入方式:replace_table**(用户要求每次覆盖更新)
- **无日期参数**(指数清单是静态的)
- **数据量:~8000 行**(SW 950 + 其它市场 ~7050)
- **挂一个 task**(2026-09-15):`task_full_update` 阶段 4
- **耗时:~0.4 秒**(1 次调用 + 1 次 replace_table)

## 数据流

```
service_index_basic.main()
  ↓
init_db("index")  # 建 db_cn_index.db
  ↓
down.update_index_basic()
  ├─ df = client.index_basic()  # 1 次调用,~8000 行
  ├─ df["snap_ts"] = snap_ts()
  ├─ replace_table(conn_index, df, "tbl_cn_index_basic")  # 全量覆盖
  └─ update_ctrl(conn_basic, "cn_index_basic", "all")
```

## 性能数据(2026-09-15 实测)

| 场景 | 数据量 | 耗时 |
|---|---|---|
| 首次全量覆盖 | 8000 行 | **0.4s** |
| 第二次覆盖 | 8000 行 | **0.3s** |