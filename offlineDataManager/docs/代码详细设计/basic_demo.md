# 代码详细设计/basic_demo.md

`scripts/test/basic_demo.py` — basic 库(db_cn_basic.db)读取接口演示。

## 职责

演示 7 个 `get_*` 接口的典型用法,作为文档和测试用。

## 入口

```bash
cd /Users/nickzhang/TradingAgent/offlineDataManager/scripts
python3 test/basic_demo.py
```

## 覆盖接口

| 接口 | 演示 case |
|---|---|
| `get_day` | 单股单日 / 单股区间 / 不复权 vs 前复权对比 / 多股同日 / 自定义列(**6 个 case**) |
| `get_week` | 单股 + ISO 周归属 / 区间(2 个 case) |
| `get_month` | 单股 + 自然月归属 / 区间(2 个 case) |
| `get_tradecal` | 区间 / market=None / offset 模式(跳过周末) / offset+market=SZSE(4 个 case) |
| `get_basic` | 全表 / 单 exchange / 单 market / 多值组合 / 沪市主板+科创板 / 暂停股(6 个 case) |
| `get_adj_factor` | 单股区间 / 多股同日(2 个 case) |
| `get_ctrl_basic` | 全部 / 单个 / 多个(3 个 case) |

**总 25 个 case**(6+2+2+4+6+2+3),每个 case print 行数 + 关键字段。

## 关键结构

```python
import sys
sys.path.insert(0, '/Users/nickzhang/TradingAgent/offlineDataManager/scripts')

import pandas as pd
pd.set_option('display.width', 250)
pd.set_option('display.max_columns', 12)
pd.set_option('display.max_colwidth', 50)

from core.offline_db_client import (
    get_day, get_week, get_month,
    get_tradecal, get_basic, get_adj_factor, get_ctrl_basic,
)


def section(title: str):
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")
```

## 数据流

```
python3 test/basic_demo.py
  ↓
import 7 个 get_* 接口
  ↓
逐个 case 调用
  ↓
每个接口走 offline_db_client 内部 get_conn("basic")
  ↓
返回 DataFrame
  ↓
print 行数 + head()
```

## 注意事项

- **只读演示**,不改数据
- **sys.path 硬编码绝对路径**:不依赖 cwd
- **pandas display options**:用 250 宽,12 列,50 字符
- **演示用 `002579.SZ`**:实际存在的股票,确保 demo 不会跑出 0 行
- **不传 `qfq=False` 测试不复权**:2020-05 段不复权 vs 前复权对比,验证 qfq 公式正确
- **会真实拉取数据**:走 `get_conn("basic")` 打开 db_cn_basic.db
