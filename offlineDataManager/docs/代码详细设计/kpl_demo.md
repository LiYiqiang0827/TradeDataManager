# 代码详细设计/kpl_demo.md

`scripts/test/kpl_demo.py` — kpl 库(db_cn_kpl.db)读取接口演示。

## 职责

演示 4 个接口(3 个数据 + 1 个断点)的典型用法。

## 入口

```bash
cd /Users/nickzhang/TradingAgent/offlineDataManager/scripts
python3 test/kpl_demo.py
```

## 覆盖接口

| 接口 | 演示 case |
|---|---|
| `get_kpl_list` | 单日全部 / themes 模糊 / themes 多值 / status 精确 / status 关键字(非首板/N板以上/天2板/3天)/ lu_descs 模糊 / themes+status 组合(10 个 case) |
| `get_kpl_concept_cons` | 单股反向查题材 / 单题材查成分股 / 组合 / descs 模糊 / 多值+min_hot_num / 多股同日(6 个 case) |
| `get_kpl_limit_performance` | 单日全部 / board_counts 过滤 / only_broken=True / themes 过滤 / min_amplitude / 区间+自定义列 / 多股 / 与 get_kpl_list 数据源对比(8 个 case) |
| `get_ctrl_basic`(替代已删的 `get_kpl_ctrl`) | 3 个 kpl 断点 / 单个 / 多个(3 个 case) |

**总 27 个 case**。

## 关键结构

```python
import sys
sys.path.insert(0, '/Users/nickzhang/TradingAgent/offlineDataManager/scripts')

import pandas as pd
pd.set_option('display.width', 250)
pd.set_option('display.max_columns', 10)
pd.set_option('display.max_colwidth', 50)

from core.offline_db_client import (
    get_kpl_list, get_kpl_concept_cons, get_kpl_limit_performance, get_ctrl_basic,
)
```

**2026-09-15 修复版**:
- 顶部 `from ... get_kpl_ctrl` 改成 `get_ctrl_basic`
- 移除第 3 节"get_kpl_ctrl"演示,改为"kpl 库断点(查 tbl_basic_ctrl)"
- `get_ctrl_basic(key=['cn_kpl_list', 'cn_kpl_concept_cons', 'cn_kpl_limit_performance'])` 3 行

## 关键概念演示

### status 关键字展开
```python
# 非首板
df = get_kpl_list(status='非首板')
# 2板以上
df = get_kpl_list(status='2板以上')
# 模糊后缀 '天2板'
df = get_kpl_list(status='天2板')
# 模糊前缀 '3天'
df = get_kpl_list(status='3天')
```

### kpl_list vs kpl_limit_performance 数据源对比
```python
df_lp = get_kpl_limit_performance(trade_date='20260911')
df_list = get_kpl_list(trade_date='20260911')
# limit_performance: kpl API 实时(16:30+)
# kpl_list: tushare 第二天早上更新
# 字段差异:lp 多 amplitude/is_break/close_price/board_period
```

### con_code 反向查
```python
# 查 600104.SH(上汽集团)属于哪些题材
# ts_codes 参数实际查 con_code
df = get_kpl_concept_cons(ts_codes='600104.SH', trade_date='20250310')
```

## 数据流

```
python3 test/kpl_demo.py
  ↓
import 4 个接口
  ↓
逐个 case 调用
  ↓
get_kpl_* 走 get_conn("kpl")
get_ctrl_basic 走 get_conn("basic")
  ↓
返回 DataFrame
  ↓
print
```

## 注意事项

- **只读演示**
- **kpl_demo 用 20240913 / 20250310 / 20260911 几个固定日期**:根据 `tbl_cn_kpl_list` / `tbl_cn_kpl_limit_performance` 的实际数据范围
- **2026-09-15 修复版后**:`get_kpl_ctrl` 已删,改用 `get_ctrl_basic(key='cn_kpl_*')`
- **`get_kpl_concept_cons.ts_codes` 查 con_code**:演示了这点
- **status 关键字展开是动态的**:每次从 DB 读 `DISTINCT status`,正则解析
