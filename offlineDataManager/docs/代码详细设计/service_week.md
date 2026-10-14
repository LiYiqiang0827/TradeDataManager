# 代码详细设计/service_week.md

`scripts/service/service_week.py` — A 股周 K 重算 service，同时生成前复权和原始不复权表。

## 职责

1. **强校验**:`cn_daily` 和 `cn_adj_factor` 的 ctrl 必须一致且 ≥ 最近已收盘交易日
2. 实例化 `CNDataDown`
3. 调 `down.update_week()`，原子覆盖 `tbl_cn_week` 和 `tbl_cn_week_origin`
4. 写日志到 `logs/service_week.log`

## 入口

```bash
python3 -m service.service_week  # 无日期参数
```

**不接受日期参数**——周 K 是派生表,每次跑都是全量重算。

## 关键流程

```python
def check_daily_adj_consistency(down: CNDataDown) -> bool:
    """校验 tbl_ctrl 中 cn_daily 和 cn_adj_factor 的日期是否一致"""
    today = latest_completed_trade_date(down.conn_basic)
    ctrl_daily = get_ctrl(down.conn_basic, "cn_daily")
    ctrl_adj = get_ctrl(down.conn_basic, "cn_adj_factor")
    
    if not ctrl_daily or not ctrl_adj: return False
    if ctrl_daily != ctrl_adj: return False
    if ctrl_daily < today: return False
    return True

def main():
    log_file = PROJECT_ROOT / "logs" / "service_week.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        if not check_daily_adj_consistency(down):
            logger.error("[service_week] 中止:cn_daily / cn_adj_factor 未就绪")
            return 2  # 业务中止
        n = down.update_week()
        logger.info(f"[service_week] 完成(全量): +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_week] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **`check_daily_adj_consistency(down)`** 强校验:
  - `cn_daily` 跟 `cn_adj_factor` 必须都存在
  - 两者的 max_date 必须**完全相等**(YYYYMMDD 字符串比较)
  - 必须**≥ 最近已收盘交易日**(节假日和盘中不会错误要求未完成的日线)
  - 任一不满足 → return False
- **校验失败 → return 2 业务中止**(scheduler 会捕获 rc=2 并整体 abort 第 3 阶段)
- **`update_week()` 无日期参数**,全量重算
- **log rotation=50 MB**

## 依赖链位置

```
service_daily  →  service_adj_factor  →  service_week
                                          ↓
                                    service_month
```

**`service_week` 跟 `service_month` 都用 `check_daily_adj_consistency`**,逻辑复用。

## 数据流

```
service_week.main()
  ↓
down = CNDataDown()
  ↓
check_daily_adj_consistency(down)
  ↓
  ctrl_daily = get_ctrl(conn_basic, "cn_daily")
  ctrl_adj = get_ctrl(conn_basic, "cn_adj_factor")
  today = latest_completed_trade_date(down.conn_basic)
  校验:ctrl_daily == ctrl_adj >= today
  ↓
down.update_week()
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
  成对覆盖 tbl_cn_week 与 tbl_cn_week_origin
  ↓
return inserted 行数
```

## 注意事项

- 前复权表和 origin 表使用同一个事务，全成功或全回滚
- **scheduler 串行调** `service_week` / `service_month`(代码无 mutex,但 scheduler 第 3 阶段顺序调用保证不并发)
- **`check_daily_adj_consistency` 是 service 间的契约**:必须先有 daily + adj_factor 才能算 week
- **校验失败返回 rc=2**(不是 rc=1),scheduler 知道这是"业务中止"而非异常

## 分批全量重建与回滚

- 仍然重建全部股票的完整历史，不是仅重算最近一周或一月；前复权基准变化时历史价格也必须更新。
- `update_week(start_date=None, end_date=None)` / `update_month(...)` 的签名不变。只有起止参数都提供时才筛选日线，并以该范围覆盖整个目标表；只给一个边界时沿用旧行为读取全历史。即使指定历史范围，前复权基准仍取该股全历史最新非空因子。
- 完整股票始终属于同一批，跨周/月的 `pre_close` 不会因批次重置；ISO 周、OHLC、成交量、成交额、涨跌幅四位小数均沿用原聚合器。
- 每批仅保留约 64 只股票的日线、因子和聚合结果，不再同时读取千万行日线及千万行因子。staging 使用私有磁盘文件，避免调用连接设置 `temp_store=MEMORY` 时重新占满内存。
- 构建期间保留旧表；仅在所有批次成功后成对覆盖。构建、复制或约束失败会回滚整个源事务，两张生产表均保持旧值。WAL 下已有读者继续看到旧版本，提交后新读者看到完整新版本。
- 源日线范围为空时返回 0 并保留旧表；源数据存在但全部是无效价格时抛错并保留旧表。临时文件、附加数据库在正常完成或 Python 异常时自动清理。
- 不需改变 service 或 scheduler 的执行命令，也不靠加长超时掩盖内存问题。
