# 代码详细设计/service_kpl_limit_performance.md

`scripts/service/service_kpl_limit_performance.py` — 开盘啦涨停表现详情更新 service。

## ⚠️ 数据来源

| 项 | 值 |
|---|---|
| **数据源平台** | **同花顺(10jqka / iFinD / 长华 aphis.longhuvip.com)** |
| **API 客户端** | `coreClient.kpl_client.KPLClient`(**不是** tushare) |
| **Host** | `apphis` / `apphwhq` / `applhb` / `appphq` |
| **端点(endpoint)** | `HisHomeDingPan/DailyLimitPerformance`(历史 host apphis)<br>`HomeDingPan/DailyLimitPerformance`(实时 host apphwhq) |
| **跟 tushare 的区别** | tushare 走 `pro.kpl_list`(只接 tushare);KPL 走同花顺私有 API |

> ⚠️ 注意:此表 `tbl_cn_kpl_limit_performance` 数据来自**同花顺 KPL**,**不是** tushare。
> 同花顺 KPL 接口盘中(today)有数据,历史接口盘中无数据;tushare 接口盘中无数据,第二天早上才有。

## 职责

1. 解析日期参数
2. 调 `resolve_date_range`
3. 调 `down.update_kpl_limit_performance(start_date, end_date)`(**走同花顺 KPLClient**)
4. 写日志到 `logs/service_kpl_limit_performance.log`

## 入口

```bash
python3 -m service.service_kpl_limit_performance --trade-date 20260911
python3 -m service.service_kpl_limit_performance --start-date 20260907 --end-date 20260911
python3 -m service.service_kpl_limit_performance  # 增量(本周一+ctrl.max_date 取大)
```

## 关键流程

```python
CTRL_KEY = "cn_kpl_limit_performance"

def main():
    parser = argparse.ArgumentParser(description="Update 涨停表现详情(实时补 kpl_list 滞后)")
    parser.add_argument("--trade-date", type=str, default=None, help="单日(YYYYMMDD)")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期")
    args = parser.parse_args()
    
    log_file = PROJECT_ROOT / "logs" / "service_kpl_limit_performance.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        # KPL 自己的断点库
        sd, ed, desc = resolve_date_range(down.conn_kpl, CTRL_KEY, args, ctrl_store="kpl")
        n = down.update_kpl_limit_performance(start_date=sd, end_date=ed)
        logger.info(f"[service_kpl_limit_performance] 完成({desc}): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_kpl_limit_performance] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **CTRL_KEY = "cn_kpl_limit_performance"**
- **传 `down.conn_kpl` 与 `ctrl_store="kpl"` 给 `resolve_date_range`**，断点位于 KPL 库的 `tbl_kpl_ctrl`。
- **`update_kpl_limit_performance` 内部**:
  - 在懒加载客户端或请求 API 之前，验证请求范围中每个自然日均有本地 SSE 日历且 `is_open` 为0或1。日历缺失或未知即报错，整个请求不下载、不入库。
  - 仅对 `is_open=1` 的日期请求。休市日不请求、不入库、不推进断点，不能把假期的旧实时快照重标为假期数据。
  - 懒加载 `KPLClient`(首次调时 `from coreClient.kpl_client import KPLClient`)
  - 按日期循环,每天:
    - `td == today` → 用实时接口 `fetch_realtime_limit_performance()`(同花顺 HomeDingPan 实时 host,盘中可用)
    - `td <  today` → 用历史接口 `get_daily_limit_performance(date_str)`(HisHomeDingPan 历史 host,便宜)
  - 默认起始日期 = `max(本周周一, ctrl.max_date)`(只补本周的)
  - 字段映射:保留表 24 列,其他丢弃
  - 若 `lu_time` 仍为 Unix 秒/毫秒时间戳，按北京时间提取源日期；任何一条与请求日期不一致或无法解析即拒绝整日入库。历史接口的 `HH:MM:SS` 没有日期，不能用于验证源日期，也不会因旧版 UTC 时钟少8小时被误判为日期错误。
  - **断点**:`tbl_kpl_ctrl.cn_kpl_limit_performance`
- **log rotation=50 MB**

## 关键设计

### 数据源双接口策略(2026-09-15 实现)

| 日期 | 接口 | endpoint | 用途 |
|---|---|---|---|
| **当天(`td == today`)** | **同花顺实时接口** `fetch_realtime_limit_performance()` | `HomeDingPan/DailyLimitPerformance` on apphwhq | 盘中可用,返回当天 32+ 行涨停明细 |
| **历史(`td < today`)** | **同花顺历史接口** `get_daily_limit_performance(date_str)` | `HisHomeDingPan/DailyLimitPerformance` on apphis | 便宜,完整历史 |

**代码逻辑**:
```python
today = datetime.now().strftime("%Y%m%d")
if td == today:
    df = self._kpl_client.fetch_realtime_limit_performance()  # 实时接口
else:
    df = self._kpl_client.get_daily_limit_performance(date_str)  # 历史接口
```

**为什么需要两个接口**:
- 同花顺历史接口(HisHomeDingPan)盘中拉 today **返 0 行**(数据库 T+1 同步)
- 同花顺实时接口(HomeDingPan)盘中可用,返回当天涨停明细
- 用分支判断而非 fallback,确保准确性

### 跟 tbl_cn_kpl_list 的对比

| 表 | 数据来源 | 接口平台 | endpoint |
|---|---|---|---|
| `tbl_cn_kpl_list` | **tushare `pro.kpl_list`** | **tushare**(tushare.pro) | pro_api 平台 |
| `tbl_cn_kpl_limit_performance` | **同花顺 KPL** `HisHomeDingPan/DailyLimitPerformance` | **同花顺**(10jqka / iFinD / 长华 aphis) | 私有 HTTP API |

**两表数据源完全不同,互补不冗余**:

| 字段 | tbl_cn_kpl_list(tushare) | tbl_cn_kpl_limit_performance(同花顺 KPL) |
|---|---|---|
| 数据源 | tushare pro.kpl_list | 同花顺 KPL HisHomeDingPan/DailyLimitPerformance |
| 更新延迟 | T+1 滞后(第二天早上) | 实时(16:30+,走同花顺实时接口) |
| 主键 | (ts_code, trade_date) | (trade_date, ts_code) |
| 字段 | 25 列,通用 | 24 列,细粒(amplitude / is_break / close_price) |
| 题材字段 | `theme`(逗号组合) | `theme`(单题材)+ `limit_reason`(多题材全文) |
| 调用客户端 | `TushareClient` | `KPLClient` |

**两表互补,不是冗余**。

## 数据流

```
service_kpl_limit_performance.main()
  ↓
resolve_date_range(conn_kpl, "cn_kpl_limit_performance", args, ctrl_store="kpl")
  → sd 来自 tbl_kpl_ctrl.cn_kpl_limit_performance
  ↓
down.update_kpl_limit_performance(start_date=sd, end_date=ed)
  ↓
先验证整个范围的 SSE 日历，未知即失败；筛出开市日
  ↓
lazy import KPLClient  # 全部休市时不初始化客户端
  ↓
按已确认的开市日循环
  对每个开市日 td:
    date_str = "{td[:4]}-{td[4:6]}-{td[6:]}"  # YYYY-MM-DD 给 KPLClient
    if td == today:
      df = self._kpl_client.fetch_realtime_limit_performance()       # 同花顺实时接口
    else:
      df = self._kpl_client.get_daily_limit_performance(date_str)      # 同花顺历史接口
    if df is None or empty: continue  # 开市日空返回不推进
    检查可核验的 lu_time Unix 源日期；错配拒绝整日
    df["trade_date"] = td  # 覆盖为 YYYYMMDD
    df["snap_ts"] = snap_ts()
    df = df[keep_cols]  # 只保留 24 列
    upsert_df(conn_kpl, df, "tbl_cn_kpl_limit_performance", key_cols=["trade_date", "ts_code"])
  ↓
update_ctrl_kpl(conn_kpl, "cn_kpl_limit_performance", last_success_date)
```

## 调度位置

- **16:30 / 20:00 单独调度**(`task_kpl_limit_performance` 在 scheduler)
- **完整更新链路第 4 阶段兜底**(`task_full_update` 也跑一次)

## 注意事项

- **KPLClient 懒加载**:避免启动时强依赖
- **last_success_date 推进规则**:只有当日有 INSERT 才推进,空日不推进
- **默认起始日期 = 本周一**:`update_kpl_limit_performance` 内部 `today - timedelta(days=today.weekday())`
- 当前规则只阻止新的休市日污染，不自动删除库中已有的假期记录。历史污染需要先备份/隔离，再单独修复。
- 当前客户端仍会吞掉单个板数接口的错误，非空返回可能不完整；本次日历保护不能替代逐板数请求状态审计。历史 `HH:MM:SS` 也不能独立证明源交易日。

## 验证

`scripts/core/test_kpl_limit_performance_calendar.py` 使用内存 SQLite 和 mock 客户端，覆盖开市、休市、未知日历、混合区间、源日期错配、毫秒时间戳、历史纯时钟及空返回；测试不下载、不写生产库。
