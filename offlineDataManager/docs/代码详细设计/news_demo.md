# 代码详细设计/news_demo.md

`scripts/test/news_demo.py` — news 库(db_cn_news.db)读取接口演示。

## 职责

演示 4 个接口的典型用法,展示 news / major_news / cctv_news 的 datetime 格式差异和分页。

## 入口

```bash
cd /Users/nickzhang/TradingAgent/offlineDataManager/scripts
python3 test/news_demo.py
```

## 覆盖接口

| 接口 | 演示 case |
|---|---|
| `get_news` | 默认 limit=5000 / trade_date 单日 / start_date+end_date 区间 / start_datetime 精确 / offset 分页 / src 单/多 / title 模糊 / content 模糊 / title+content 组合 / 全功能(11 个 case) |
| `get_major_news` | 默认 / trade_date+content / 精确 datetime(3 个 case) |
| `get_cctv_news` | trade_date 单日 / start_date+end_date 区间 / content 模糊 1 年 / 单日+content(4 个 case) |
| `get_news_ctrl` | 全部(11 行) / 单 src / 多 src / src=[] 空显式 0 行(4 个 case) |

**总 22 个 case**。

## 关键结构

```python
import sys
sys.path.insert(0, '/Users/nickzhang/TradingAgent/offlineDataManager/scripts')

import pandas as pd
pd.set_option('display.width', 250)
pd.set_option('display.max_columns', 10)
pd.set_option('display.max_colwidth', 50)

from core.offline_db_client import (
    get_news, get_major_news, get_cctv_news, get_news_ctrl,
)
```

## 关键概念演示

### 三种 datetime 格式
- `get_news` / `get_major_news`:`datetime` 是 `YYYY-MM-DD HH:MM:SS` 带时间戳
  - `start_date` 自动补 00:00:00
  - `start_datetime` 是精确时间
  - `trade_date` 自动 [00:00:00, 23:59:59]
- `get_cctv_news`:`datetime` 是 `YYYYMMDD` 无时间

### 分页演示
```python
# 第一页(默认 limit=5000)
df = get_news()
# 第二页
df = get_news(limit=5000, offset=5000)
# 首行 datetime 应该是第一页最后一行之前
```

### 互斥校验
- `trade_date` 跟 `*_datetime` 互斥
- `start_date` + `end_date` 自动补全,跟 `start_datetime` 行为不同

### 11 个 src 演示
- `get_news_ctrl()` 返回 11 行(9 news 源 + cctv_news + major_news)
- 9 news 源: sina / wallstreetcn / 10jqka / eastmoney / yuncaijing / fenghuang / jinrongjie / cls / yicai
- 2 disabled(2026-09-11): yuncaijing / fenghuang

## 数据流

```
python3 test/news_demo.py
  ↓
import 4 个接口
  ↓
逐个 case 调用
  ↓
get_news / get_major_news / get_cctv_news 走 get_conn("news")
get_news_ctrl 走 get_conn("news")
  ↓
返回 DataFrame(可能很大,默认 limit=5000)
  ↓
print head / 行数
```

## 注意事项

- **只读演示**
- **`get_news` 默认 limit=5000**:全表 945 万行,不限制会很慢
- **datetime 格式差异是教学重点**:`demo` 顶部 docstring 强调这一点
- **`src=[]` 显式空 0 行**:演示 None vs [] 语义
- **演示用 2026-09-11 / 20240911**:实际有数据的日期
- **content 模糊匹配 `习近平` / `新能源`**:跟 `LIKE '%关键词%'` 行为一致
