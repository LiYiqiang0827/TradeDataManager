# 代码详细设计/service_kpl_list.md

`scripts/service/service_kpl_list.py` — 开盘啦涨停榜增量更新 service。

## ⚠️ 数据来源

| 项 | 值 |
|---|---|
| **数据源平台** | **tushare(pro.tushare.pro 平台)** |
| **API 客户端** | `coreClient.tushare_client.TushareClient`(**不是** 同花顺 KPL) |
| **接口 endpoint** | `pro.kpl_list`(支持 `tag` 参数:'涨停' / '炸板') |
| **跟同花顺 KPL 的区别** | tushare 走 `pro_api()`;同花顺 KPL 走 `KPLClient`(`HisHomeDingPan` 等) |

> ⚠️ 此 service 用 **tushare**,**不是** 同花顺。`tbl_cn_kpl_limit_performance` 才是同花顺。

## 职责

1. 解析 `--trade-date` / `--start-date` / `--end-date`
2. 调 `resolve_date_range`
3. 调 `down.update_kpl_list(start_date, end_date)`(**2026-09-15 改造:默认只下炸板,涨停沿用原始默认下载**)
4. 写日志到 `logs/service_kpl_list.log`

## 入口

```bash
python3 -m service.service_kpl_list --trade-date 20240909
python3 -m service.service_kpl_list --start-date 20201009 --end-date 20201031
python3 -m service.service_kpl_list  # 增量(默认从 ctrl 取,默认下炸板)
```

## 关键流程

```python
CTRL_KEY = "cn_kpl_list"

def main():
    parser = argparse.ArgumentParser(description="Update 涨停榜")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()
    
    log_file = PROJECT_ROOT / "logs" / "service_kpl_list.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        sd, ed, desc = resolve_date_range(down.conn_basic, CTRL_KEY, args)
        n = down.update_kpl_list(start_date=sd, end_date=ed)
        logger.info(f"[service_kpl_list] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_kpl_list] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_kpl_list"**
- **2026-09-15 用户策略**:
  - **涨停**: 用 tushare 默认下载(不传 tag),主键 `(ts_code, trade_date, tag='涨停')` — 已在原始下载里覆盖
  - **炸板**: 单独再下 1 次,`tag='炸板'`,从 **2020-10-09 起**(2020-10-09 之前未下载)
  - **取数**:`get_kpl_list(tags=...)` 支持 3 种筛选(单独涨停/单独炸板/两者都有)
- **调用 `update_kpl_list(tag=None)`**:
  - 默认只下载 `tag='炸板'`
  - 显式 `tag='涨停'` → 只下载以涨停收盘的股票
  - 显式 `tag='炸板'` → 只下载炸板股票
- **传 `down.conn_basic` 给 `resolve_date_range`**:实际读 `tbl_basic_ctrl`(2026-09-15 修复版统一),fallback 到 `TRADECAL_FULL_START=20180101`
- **`update_kpl_list` 内部把断点写到 `tbl_kpl_ctrl.cn_kpl_list`**(2026-09-15 修复版统一)
- **log rotation=20 MB**

## 关键设计:read/write 错位

- **service 读 ctrl** → `tbl_kpl_ctrl`(已删,fallback 到 20180101)
- **downloader 写 ctrl** → `tbl_kpl_ctrl.cn_kpl_list`(实际有效)

**原因**:2026-09-15 修复版删了 `tbl_kpl_ctrl`,download 端统一写 `tbl_basic_ctrl`。service 这边用 `conn_kpl` 是历史遗留,**实际断点读取无效**:因为 `resolve_date_range` 传 `down.conn_kpl` 但 `tbl_kpl_ctrl` 已删 → `get_ctrl` 返回 None → 走 fallback `TRADECAL_FULL_START=20180101`。然后 service 把 `sd=20180101` 传给 `update_kpl_list(start_date=sd, ...)`,函数内部 `if start_date: sd = _fmt_yyyymmdd(start_date)` —— **直接采用外部 sd,不再读 ctrl**。**结果**:`service_kpl_list` 每次都从 20180101 拉一遍(主键去重保证幂等,但跑得慢)。

## 数据流

```
service_kpl_list.main()
  ↓
resolve_date_range(conn_kpl, "cn_kpl_list", args)
  → tbl_kpl_ctrl 已删 → get_ctrl 返回 None
  → sd = TRADECAL_FULL_START = "20180101"
  ↓
down.update_kpl_list(start_date=sd, end_date=ed)
  ↓
update_kpl_list 内部:
  - 检测到外部传了 start_date(20180101),直接采用
  - 按日期循环拉 tushare pro.kpl_list
  - upsert + 推进 ctrl(写到 tbl_kpl_ctrl.cn_kpl_list)
```

## 注意事项

- **`resolve_date_range` 传的 conn 不重要**,因为 `tbl_kpl_ctrl` 不存在
- **`update_kpl_list` 内部才是真正的增量逻辑**
- **kpl_list 数据 T+1 滞后**(tushare 第二天早上才更新),当天数据靠 `service_kpl_limit_performance` 补
