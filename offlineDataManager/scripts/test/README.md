# test/ — 接口调用示例

每个 `*_demo.py` 是一个**独立可运行**的演示脚本,展示该库所有 `get_*` 接口的典型用法。

| 脚本 | 覆盖接口 | 数据库 |
|---|---|---|
| `basic_demo.py` | `get_day`, `get_week`, `get_month`, `get_tradecal`, `get_basic`, `get_adj_factor`, `get_ctrl_basic` | `db_cn_basic.db` |
| `kpl_demo.py` | `get_kpl_list`, `get_kpl_concept_cons`, `get_kpl_ctrl` | `db_cn_kpl.db` |
| `news_demo.py` | `get_news`, `get_major_news`, `get_cctv_news`, `get_news_ctrl` | `db_cn_news.db` |

## 运行

```bash
cd /Users/nickzhang/TradingAgent/offlineDataManager/scripts
python3 test/basic_demo.py
python3 test/kpl_demo.py
python3 test/news_demo.py
```

## 用法速查

```python
from core.offline_db_client import get_day, get_kpl_list, get_news

# 日 K(前复权)
df = get_day(ts_code='002579.SZ', start_date='20240101', end_date='20241231')

# 题材涨停榜
df = get_kpl_list(trade_date='20240913', status='2板以上')

# 新闻快讯(分页)
df = get_news(src='sina', limit=5000, offset=0)
```

## 接口速查表

| 接口 | 库 | 主要参数 | 默认 |
|---|---|---|---|
| `get_day` | basic | `ts_code`/`ts_codes`, `start_date`, `end_date`, `trade_date`, `qfq` | qfq=True |
| `get_week` | basic | 同上,无 `qfq` (已前复权) | — |
| `get_month` | basic | 同 `get_week` | — |
| `get_tradecal` | basic | `start_date`/`end_date`, `trade_date`+`day_num`, `market` | market='SSE' |
| `get_basic` | basic | `exchange`(列表), `market`(列表), `list_status` | 全部 None |
| `get_adj_factor` | basic | `ts_code`/`ts_codes`, 日期 | — |
| `get_ctrl_basic` | basic | `key`(列表) | None=全部 |
| `get_kpl_list` | kpl | 日期, `ts_codes`, `themes`(模糊), `lu_descs`(模糊), `status`(精确+关键字) | — |
| `get_kpl_concept_cons` | kpl | 日期, `ts_codes`(查 con_code), `themes`, `descs`(模糊), `min_hot_num` | — |
| `get_kpl_ctrl` | kpl | `key` | None=全部 |
| `get_news` | news | 日期, `start_datetime`/`end_datetime`, `title`/`content`, `src`, `limit`, `offset` | limit=5000 |
| `get_major_news` | news | 同 `get_news` | limit=5000 |
| `get_cctv_news` | news | 日期, `content`(模糊) | — |
| `get_news_ctrl` | news | `src` | None=全部 |

## 几个常见陷阱

1. **`get_kpl_concept_cons` 的 `ts_codes` 实际查 `con_code`**(成分股代码),因为表里 `ts_code` 是题材代码。
2. **`get_news` 默认 `limit=5000`**(全表 940 万行太慢),分页用 `offset`。
3. **`get_news` 的 `datetime` 是 `YYYY-MM-DD HH:MM:SS`**,`start_date` 自动补 `00:00:00`,`start_datetime` 才是精确时间。
4. **`get_cctv_news` 的 `datetime` 是 `YYYYMMDD`**(cctv 接口没时间字段),跟 news 表不一样。
5. **`get_kpl_list` 的 `status` 支持关键字**:`'非首板'` / `'N板以上'` / `'N天'` / `'天M板'`。
