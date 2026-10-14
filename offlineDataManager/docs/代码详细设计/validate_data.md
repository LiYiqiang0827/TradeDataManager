# 代码详细设计/validate_data.md

`scripts/test/validate_data.py` — 数据完整性验证(对比 DB 行数和 Tushare 实时行数)。

## 职责

抽样几个日期,对 4 张表(daily / kpl_list / kpl_concept_cons / news)拉 Tushare 实时行数,跟 DB 里的行数对比,输出 ✓/✗。

## 入口

```bash
cd /Users/nickzhang/TradingAgent/offlineDataManager/scripts
python3 test/validate_data.py
```

## 关键点

- **需要 Tushare API 配额**(会真实拉数据)
- **抽样 5 个日期**:`['20260908', '20260115', '20250115', '20241015', '20240615']`
- **抽样 5 个 news 源**:`['sina', 'cls', 'eastmoney', '10jqka', 'wallstreetcn']`
- **验证 4 张表**:daily / kpl_list / kpl_concept_cons / news(单源)

## 2026-09-15 修复版

**之前有 bug**:`conn = sqlite3.connect(DB_PATH)` 用 basic conn 查 kpl/news 表,会报 `no such table`。
**修复后**:
```python
from config.settings import DB_PATH_BASIC, DB_PATH_KPL, DB_PATH_NEWS
# ...
conn_basic = sqlite3.connect(str(DB_PATH_BASIC))
conn_kpl = sqlite3.connect(str(DB_PATH_KPL))
conn_news = sqlite3.connect(str(DB_PATH_NEWS))
```
每个 DB 用自己的 conn。

## 关键结构

```python
import sys
import os
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT.parent))  # 加 ~/TradingAgent/

import sqlite3
from coreClient.tushare_client import TushareClient
from config.settings import DB_PATH_BASIC, DB_PATH_KPL, DB_PATH_NEWS


def count_daily(client, trade_date) -> int:
    """用 OFFSET 分页拿某天 daily 总行数"""
    total = 0; offset = 0
    while True:
        df = client.daily(trade_date=td, limit=6000, offset=offset)
        if df is None or len(df) == 0: break
        total += len(df)
        if len(df) < 6000: break
        offset += 6000
    return total


def count_kpl_list(client, trade_date) -> int: ...
def count_kpl_concept_cons(client, trade_date) -> int: ...
def count_news(client, src, trade_date) -> int: ...


def validate():
    client = TushareClient()
    conn_basic = sqlite3.connect(str(DB_PATH_BASIC))
    conn_kpl = sqlite3.connect(str(DB_PATH_KPL))
    conn_news = sqlite3.connect(str(DB_PATH_NEWS))
    
    SAMPLE_DATES = ['20260908', '20260115', '20250115', '20241015', '20240615']
    
    # 1. daily
    for td in SAMPLE_DATES:
        expected = count_daily(client, td)
        actual = conn_basic.execute("SELECT COUNT(*) FROM tbl_cn_day WHERE trade_date = ?", (td,)).fetchone()[0]
        print(f"  {'✓' if actual >= expected else '✗'} {td}: 期望 {expected}, 实际 {actual}")
    
    # 2. kpl_list(conn_kpl)
    # 3. kpl_concept_cons(conn_kpl)
    # 4. news(conn_news, 每源)
```

## 数据流

```
python3 test/validate_data.py
  ↓
TushareClient() 实例化
  ↓
3 个 sqlite3 conn
  ↓
抽样日期 × 4 张表
  ↓
对每个 (td, table):
  expected = count_xxx(client, td)  # OFFSET 分页拿 Tushare 全部
  actual = conn_xxx.execute("SELECT COUNT(*) FROM ... WHERE trade_date = ?")
  print ✓/✗
```

## 注意事项

- **会消耗 Tushare API 配额**:按需跑,不要 cron 自动跑
- **sample 日期是写死的**:如果数据有缺失会显示 ✗,需要看是采样日期问题还是数据漏拉
- **2026-09-15 修复版**:用 conn_basic/conn_kpl/conn_news 三个独立 conn
- **不是单元测试**:没有 assert,只看输出
- **`actual >= expected` 才算 ✓**:DB 行数可能比 Tushare 多(主键去重不会丢,但可能因 Tushare 限流少了)
- **不走 service / scheduler**:直接 `from coreClient.tushare_client import TushareClient` 调 Tushare
- **sys.path 加 `PROJECT_ROOT.parent`**:让 `coreClient` 能 import
