# 代码详细设计/settings.md

`scripts/config/settings.py` — 项目配置文件,声明 **4 个** DB 路径 + **4 套** schema(2026-09-15 加 index DB)。

## 职责

1. **4 个 SQLite 库路径**(`db_cn_basic.db` / `db_cn_kpl.db` / `db_cn_news.db` / `db_cn_index.db`)
2. **4 套表 schema**(`SCHEMA_SQL_BASIC` / `SCHEMA_SQL_KPL` / `SCHEMA_SQL_NEWS` / `SCHEMA_SQL_INDEX`,2026-09-15 加)
3. **Tushare token 来源说明**(实际 token 在 `~/TradingAgent/coreClient/tushare_config.py`,本文件不持有)
4. **日志 / 增量窗口 / 默认数据源**等运行时常量

## 入口

- **被谁 import**:`core/offline_db_client.py` 第 22-25 行(`init_db` / `get_conn` 用)
- **被谁 import**:`core/offline_downloader.py` 第 26-35 行(`CNDataDown` 用所有路径 + schema)
- **被谁 import**:`test/validate_data.py` 第 16 行(只读 `DB_PATH_BASIC/KPL/NEWS`)

**不直接被 service import**(service 通过 `CNDataDown` 间接用)。

## 关键常量

### 路径(7 个)

| 常量 | 值 | 用途 |
|---|---|---|
| `PROJECT_ROOT` | `Path(__file__).resolve().parent.parent.parent` = `~/TradingAgent/offlineDataManager/` | 项目根 |
| `DATA_DIR` | `PROJECT_ROOT / "data"` | 3 个 DB 文件所在目录 |
| `LOG_DIR` | `PROJECT_ROOT / "logs"` | 日志目录 |
| `DB_PATH_BASIC` | `DATA_DIR / "db_cn_basic.db"` | 基础数据 DB |
| `DB_PATH_KPL` | `DATA_DIR / "db_cn_kpl.db"` | 开盘啦 DB |
| `DB_PATH_NEWS` | `DATA_DIR / "db_cn_news.db"` | 新闻 DB |
| `DB_PATH` | `DB_PATH_BASIC`(兼容旧代码) | 旧代码用,新代码用 `DB_PATH_BASIC` |

`DATA_DIR.mkdir(parents=True, exist_ok=True)` + `LOG_DIR.mkdir(parents=True, exist_ok=True)` 在 import 时自动建目录(确保 `init_db` 不会因目录不存在而失败)。

### 运行时常量(4 个)

| 常量 | 值 | 用途 |
|---|---|---|
| `DEFAULT_SOURCE` | `"tushare"` | 数据源标识(目前只有 tushare,留扩展位) |
| `DEFAULT_LOOKBACK_DAYS` | `7` | 增量更新多往前看 7 天(避免漏数据,主键去重保证幂等) |
| `LOG_FORMAT` | loguru 格式串 | 通用日志格式 |
| `LOG_LEVEL` | `"INFO"` | 全局日志级别 |

### Schema(3 套)

| 变量 | 包含的表 | 行数 |
|---|---|---|
| `SCHEMA_SQL_BASIC` | `tbl_cn_basic` + `tbl_cn_tradecal` + `tbl_cn_day` + `tbl_cn_adj_factor` + `tbl_cn_week` + `tbl_cn_month` + `tbl_basic_ctrl` | 7 张 |
| `SCHEMA_SQL_KPL` | `tbl_cn_kpl_list` + `tbl_cn_kpl_concept_cons` + `tbl_cn_kpl_limit_performance` | 3 张 |
| `SCHEMA_SQL_NEWS` | `tbl_news` + `tbl_major_news` + `tbl_cctv_news` + `tbl_news_ctrl` | 4 张 |
| `SCHEMA_SQL` | = `SCHEMA_SQL_BASIC` | 兼容旧代码 |

**完整字段定义见** [../01_数据详细设计文档.md](../01_数据详细设计文档.md)。

## 关键设计

### 路径计算用 `__file__`,不依赖 cwd
```python
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
```
- `__file__` = `scripts/config/settings.py`
- `.parent` × 3 = `~/TradingAgent/offlineDataManager/`
- **不依赖 `os.getcwd()`**,从任何目录跑都能找到 DB

### 兼容旧代码的别名
- `DB_PATH` 是 `DB_PATH_BASIC` 的别名,旧 `init_db()` 无参版本默认用 basic
- `SCHEMA_SQL` 是 `SCHEMA_SQL_BASIC` 的别名,同上

## 数据流

```
import config.settings
       ↓
PROJECT_ROOT / DATA_DIR / LOG_DIR  被算出(并 mkdir)
       ↓
DB_PATH_BASIC/KPL/NEWS  路径常量
       ↓
SCHEMA_SQL_BASIC/KPL/NEWS  schema 字符串
       ↓
被 offline_db_client.init_db() / get_conn() 读取
被 offline_downloader.CNDataDown.__init__() 读取
```

## 修改指南

### 加新表
1. 在对应 `SCHEMA_SQL_*` 字符串里加 `CREATE TABLE IF NOT EXISTS`
2. 在 `init_db` 自动生效(`CREATE TABLE IF NOT EXISTS` 幂等)
3. **如果加 ctrl 表**:在 `tbl_basic_ctrl` 加 key,不新建独立 ctrl 表

### 改路径
**不要直接改 `PROJECT_ROOT`**,改 `__file__` 路径或重命名目录,`Path(__file__).resolve().parent.parent.parent` 自动跟进。

## 注意事项

- **不要在 settings.py 里持有 Tushare token**。所有 token 读取走 `coreClient.tushare_client.TushareClient`
- **不要 import loguru / pandas / tushare 等重依赖**。settings.py 必须保持轻量,只 import `pathlib`
- **`mkdir(exist_ok=True)` 是 idempotent 的**,多次 import 不会报错
