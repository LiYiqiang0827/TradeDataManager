# 代码详细设计/service_month.md

`scripts/service/service_month.py` — A 股月 K 重算 service，同时生成前复权和原始不复权表。

## 职责

1. **强校验**(复用 `service_week.check_daily_adj_consistency`)
2. 调 `down.update_month()`，原子覆盖 `tbl_cn_month` 和 `tbl_cn_month_origin`
3. 写日志到 `logs/service_month.log`

## 入口

```bash
python3 -m service.service_month  # 无日期参数
```

## 关键流程

```python
from service.service_week import check_daily_adj_consistency  # 复用

def main():
    log_file = PROJECT_ROOT / "logs" / "service_month.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        if not check_daily_adj_consistency(down):
            logger.error("[service_month] 中止:cn_daily / cn_adj_factor 未就绪")
            return 2  # 业务中止
        n = down.update_month()
        logger.info(f"[service_month] 完成(全量): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_month] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **复用 `service_week.check_daily_adj_consistency`**:从 `service.service_week` import
- **校验失败 → return 2**
- **`update_month()` 无日期参数**,全量重算
- **log rotation=50 MB**

## 依赖链位置

```
service_daily  →  service_adj_factor  →  service_week
                                          ↓
                                    service_month
```

`service_month` 是依赖链最末端。

## 数据流

```
service_month.main()
  ↓
down = CNDataDown()
  ↓
check_daily_adj_consistency(down)  # 复用 service_week 的校验
  ↓
down.update_month()
  ↓
core.periodic_kline_rebuild.rebuild_period_tables(...)
  ↓
开启源数据读事务,按 ts_code 每批 64 只取完整日线与因子历史
  ↓
过滤 open>0 & close>0
  ↓
沿用 _aggregate_daily_to_freq 聚合 origin 和 qfq
  qfq 价格 = 原价 × 当日因子 / 该股全历史最新非空因子
  qfq 成交量 = 原量 ÷ 同一系数;成交额不变
  ↓
每批结果写入私有磁盘 SQLite staging 文件,输出批次进度
  ↓
全部聚合成功后,同一事务 DELETE + INSERT SELECT
  成对覆盖 tbl_cn_month 与 tbl_cn_month_origin
  ↓
return inserted
```

## 注意事项

- **`update_month` 跟 `update_week` 区别只在 `freq="monthly"` vs `freq="weekly"`**
- 前复权表和 origin 表使用同一个事务，全成功或全回滚
- **聚合粒度**:monthly 按 YYYYMM 自然月,weekly 按 ISO year-week
- **trade_date**:monthly 是月内最后交易日,weekly 是周内最后交易日
- **`check_daily_adj_consistency` 不能并行调用**:`service_week` / `service_month` 必须串行(已在 scheduler 第 3 阶段顺序)

## 分批全量重建与回滚

- 仍然重建全部股票的完整历史，不是仅重算最近一周或一月；前复权基准变化时历史价格也必须更新。
- `update_week(start_date=None, end_date=None)` / `update_month(...)` 的签名不变。只有起止参数都提供时才筛选日线，并以该范围覆盖整个目标表；只给一个边界时沿用旧行为读取全历史。即使指定历史范围，前复权基准仍取该股全历史最新非空因子。
- 完整股票始终属于同一批，跨周/月的 `pre_close` 不会因批次重置；ISO 周、OHLC、成交量、成交额、涨跌幅四位小数均沿用原聚合器。
- 每批仅保留约 64 只股票的日线、因子和聚合结果，不再同时读取千万行日线及千万行因子。staging 使用私有磁盘文件，避免调用连接设置 `temp_store=MEMORY` 时重新占满内存。
- 构建期间保留旧表；仅在所有批次成功后成对覆盖。构建、复制或约束失败会回滚整个源事务，两张生产表均保持旧值。WAL 下已有读者继续看到旧版本，提交后新读者看到完整新版本。
- 源日线范围为空时返回 0 并保留旧表；源数据存在但全部是无效价格时抛错并保留旧表。临时文件、附加数据库在正常完成或 Python 异常时自动清理。
- 不需改变 service 或 scheduler 的执行命令，也不靠加长超时掩盖内存问题。
