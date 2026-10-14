# 代码详细设计/service_basic.md

`scripts/service/service_basic.py` — 股票基本信息全量更新 service。

## 职责

1. 解析 CLI args
2. 实例化 `CNDataDown`
3. 调 `down.update_basic(bFull=args.full)`
4. 写日志到 `logs/service_basic.log`

## 入口

```bash
python3 -m service.service_basic [--full]
```

**无默认参数**,如果不传 `--full` 走增量模式(实际是全量刷,`update_basic` 内部 bFull=False 也是拉全部 L+P)。

## 关键流程

```python
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from core.offline_downloader import CNDataDown
from loguru import logger

def main():
    parser = argparse.ArgumentParser(description="Update basic stock info")
    parser.add_argument("--full", action="store_true", help="force full reload")
    args = parser.parse_args()
    
    log_file = PROJECT_ROOT / "logs" / "service_basic.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        n = down.update_basic(bFull=args.full)
        logger.info(f"[service_basic] 完成: +{n:,} 行, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_basic] 失败: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

## 关键点

- **不调 `resolve_date_range`**(无日期参数,全量表)
- **断点写入**:`tbl_basic_ctrl.cn_basic = snap_ts()`(snap_ts 不是日期,是"这次拉过"的标记)
- **返回 0 / 1**(成功 / 异常,不会 rc=2)
- **log rotation=20 MB**

## 依赖

- `core/offline_downloader.CNDataDown`
- `coreClient.tushare_client.TushareClient`(CNDataDown 内部)
- `loguru`

## 数据流

```
scheduler spawn service_basic
  ↓
service_basic.main() 解析 --full
  ↓
CNDataDown() 初始化(3 DB conn)
  ↓
update_basic(bFull)
  ↓
client.stock_basic(list_status="L") + list_status="P"
  ↓
concat + 去重 + upsert(conn_basic, df, "tbl_cn_basic", key_cols=["ts_code"])
  ↓
update_ctrl(conn_basic, "cn_basic", snap_ts())
  ↓
return inserted 行数
  ↓
service_basic 退出 rc=0
```

## 修改指南

- 加新参数:在 `parser.add_argument` 加,然后传给 `update_basic`
- 改日志大小:调 `logger.add` 的 `rotation` 参数

## 注意事项

- **`update_basic` 拉 L + P 两个状态合并去重**,不拉 D(退市)
- **`bFull=True` 会先 `DELETE FROM tbl_cn_basic` 再写**,谨慎使用(主键 ts_code 唯一,不会丢历史)
