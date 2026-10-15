"""
core/sqlite_client.py
=====================

SQLite 数据库操作层。

数据库命名约定(2026-09-10 重构后,按用户规则):
    ~/TradingAgent/onlineDataManager/data/online_data_YYYYMM.db
    一个月一个 db 文件

表命名约定:
    <kind>_<YYYYMMDD>
    例如:
        auction_20260910  - 2026-09-10 的集合竞价快照
        snapshot_20260910 - 2026-09-10 的连续竞价快照
        orderbook_20260910 - 2026-09-10 的 5 档盘口
        minute_20260910   - 2026-09-10 的分时图
        zt_20260910       - 2026-09-10 的涨停池快照

每个 kind 一张表,字段全拆列(2026-09-11 重构后,用户要求):
- 原本所有 kind 都塞 payload_json TEXT → 拆成实际字段列(snapshot 15 列、zt 17 列等)
- 数组字段单独长表:minute.bars → minute_bars_<date>(其它数组型字段如 anomaly.keyword_list 已废弃,不另存长表)
- 通用列:trade_date, ts_code, data_timestamp, save_timestamp, created_at

watchlist 表(2026-09-11 新增,2026-09-12 v3 精简):
    与其他 8 个 kind 不同 — watchlist 用"覆盖落盘"模式,不是增量
    表名:watchlist_YYYYMMDD
    字段:ts_code(PK) + source + priority + reason + watchlist_timestamp + created_at
    每次落盘前 DELETE 当日全部 rows,再 INSERT 新 rows(等同整体替换)
    v3 精简:删 data_timestamp/save_timestamp,改 watchlist_timestamp(ISO 字符串)+ created_at(SQLite DEFAULT 自动)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


from coreClient._paths import online_data_dir

# 数据库路径与代码目录分离；旧 TRADE_AGENT_ROOT_PATH 不参与库位置解析。
DATA_DIR = online_data_dir()


# ============================================================
# 路径推导
# ============================================================
def db_path_for_month(year_month: str) -> Path:
    """根据 YYYYMM 返回 db 路径

    例:db_path_for_month("202609") → online_data_202609.db
    """
    if len(year_month) != 6 or not year_month.isdigit():
        raise ValueError(f"year_month 必须是 YYYYMM 6位数字,实际 {year_month!r}")
    return DATA_DIR / f"online_data_{year_month}.db"


def db_path_for_date(trade_date: str) -> Path:
    """根据 YYYYMMDD 返回 db 路径(自动取月份)"""
    if len(trade_date) != 8 or not trade_date.isdigit():
        raise ValueError(f"trade_date 必须是 YYYYMMDD 8位数字,实际 {trade_date!r}")
    return db_path_for_month(trade_date[:6])


def current_year_month() -> str:
    """返回当前 YYYYMM"""
    return datetime.now().strftime("%Y%m")


# ============================================================
# Schema(2026-09-11 重构:每 kind 独立 schema,字段全拆列)
# ============================================================
# 重构动机:
#   - 旧设计:所有 kind 用 COMMON_TABLE_SCHEMA(payload_json TEXT)
#   - 问题:1) 读时必须 json.loads 每行 2) 体积大(5-6x) 3) 不能 SELECT 列
#   - 新设计:每 kind 用自己的 schema,所有字段拆成独立 REAL/INTEGER/TEXT 列
#
# 通用列(每张表都有):
#   - trade_date     YYYYMMDD
#   - ts_code        600519.SH
#   - data_timestamp 数据时间(ISO T 字符串)
#   - save_timestamp 落盘时间(ISO T 字符串)
#   - created_at     SQLite DEFAULT (datetime('now','localtime'))
#
# 去重:UNIQUE(ts_code, data_timestamp) 避免重复落盘


# --- snapshot:连续竞价快照(15 字段,价格 + 量 + 涨跌 + 时序) ---
# 2026-09-12 v3 精简:只保留 2 个时间戳
#   - snapshot_timestamp(数据时间戳,同花顺给的毫秒;落盘时转 ISO 字符串)
#   - created_at(落盘时间戳,SQLite DEFAULT 自动加,本地时区)
SNAPSHOT_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 股票代码(含后缀,如 600519.SH)
    volume                      INTEGER,
    turnover                    INTEGER,
    last_price                  REAL,
    price_change                REAL,
    price_change_ratio_pct      REAL,
    open_price                  REAL,
    high_price                  REAL,
    low_price                   REAL,
    prev_price                  REAL,
    snapshot_timestamp          TEXT,       -- 数据时间戳:存盘时从 int 毫秒转 ISO 字符串(本地时区)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, snapshot_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code);
CREATE INDEX IF NOT EXISTS idx_{table_name}_snapshot_ts ON {table_name}(snapshot_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# --- snapshot_index:指数行情快照(8 只指数,2026-09-16 v6.10 新增) ---
# 数据源:ths_client.fetch_index_snapshot → hithink-finance index snapshot
# 11 字段:thscode/ticker/volume/turnover/last_price/price_change/price_change_ratio_pct/open_price/high_price/low_price/prev_price
# 与 snapshot 同构,但只 8 只指数,30s/轮
# v6.10 设计决策:
#   - ts_code 即指数代码(同花顺 thscode,基类 _flat_record 已重命名)
#   - ticker:同花顺内部代号(如 1A0001),保留用于对账
#   - snapshot_unix:数据时间戳的 int 毫秒形式(用户最高优先级:任何表都要存 unix int64)
#    ※指数无涨停时间,不能叫 lu_time(语义错),改用 snapshot_unix 表达"数据时间戳的 unix 形式"
SNAPSHOT_INDEX_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 指数代码(如 000001.SH,同 thscode)
    ticker                      TEXT,                  -- 同花顺内部 ticker(如 1A0001)
    volume                      INTEGER,
    turnover                    INTEGER,
    last_price                  REAL,
    price_change                REAL,
    price_change_ratio_pct      REAL,
    open_price                  REAL,
    high_price                  REAL,
    low_price                   REAL,
    prev_price                  REAL,
    snapshot_timestamp          TEXT NOT NULL,         -- 数据时间戳(ISO 字符串,本地时区)
    snapshot_unix               INTEGER NOT NULL,      -- ★ 数据时间戳的 int 毫秒形式(用户最高优先级:必须存 unix int64)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, snapshot_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code);
CREATE INDEX IF NOT EXISTS idx_{table_name}_snapshot_ts ON {table_name}(snapshot_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# --- auction:集合竞价快照(2026-09-12 v3:独立 schema,数据时间戳改 auction_timestamp) ---
AUCTION_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 唯一标识(v3 精简:删 thscode/ticker)
    volume                      INTEGER,
    turnover                    INTEGER,
    last_price                  REAL,
    price_change                REAL,
    price_change_ratio_pct      REAL,
    open_price                  REAL,
    high_price                  REAL,
    low_price                   REAL,
    prev_price                  REAL,
    auction_timestamp           TEXT NOT NULL,         -- 接口响应组装时间(不是行情事件时间)
    auction_timestamp_source    TEXT,                  -- api_response_assembly / client_response_time_fallback
    auction_request_stage       TEXT,                  -- CLI 请求阶段: live / final
    auction_phase               TEXT,                  -- 同花顺原始 phase
    data_status                 TEXT,                  -- 同花顺原始状态
    name                        TEXT,                  -- 同花顺源名称
    auction_price               REAL,                  -- 同花顺原生字段；不映射 last_price
    auction_pct                 REAL,                  -- 同花顺原生字段
    auction_volume              REAL,                  -- 同花顺原生值；单位未在接口文档中说明
    auction_amount              REAL,                  -- 同花顺原生值；单位未推断
    auction_unmatched           REAL,                  -- 同花顺原生值
    auction_turnover_pct        REAL,                  -- 同花顺原生换手率字段
    auction_yesterday_ratio_pct REAL,                  -- 同花顺原生字段
    auction_volume_ratio        REAL,                  -- 同花顺原生字段
    pre_close_price             REAL,                  -- 同花顺原生字段
    float_market_cap            REAL,                  -- 同花顺原生值；单位按来源保留
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, auction_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code);
CREATE INDEX IF NOT EXISTS idx_{table_name}_auction_ts ON {table_name}(auction_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# --- orderbook:5 档盘口(2026-09-12 v3:数据时间戳改 orderbook_timestamp) ---
ORDERBOOK_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 唯一标识(v3 精简:删 thscode/ticker)
    bid_price_1                 REAL,    -- 买一价
    bid_volume_1                INTEGER, -- 买一量
    ask_price_1                 REAL,    -- 卖一价
    ask_volume_1                INTEGER, -- 卖一量
    last_price                  REAL,
    volume                      INTEGER,
    orderbook_timestamp         TEXT NOT NULL,         -- ★ v3 数据时间戳(ISO 字符串)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, orderbook_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_orderbook_ts ON {table_name}(ts_code, orderbook_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# --- minute:分时图(2026-09-13 v4:展开存,每根 K 线 1 行,彻底废弃 minute_bars 长表)
# 字段对齐 coreClient/tdx_client.get_minute_kline(ts_code, date) 输出:
#   ts_code / trade_date / time_idx / datetime / price / vol / data_timestamp
# + 我们自己加的 created_at(落盘时间)
# PK = (ts_code, trade_date, time_idx) 天然去重
MINUTE_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,
    time_idx                    INTEGER NOT NULL,    -- 该股第几根 K 线(0-239)
    datetime                    TEXT NOT NULL,       -- 'YYYY-MM-DD HH:MM:SS'
    price                       REAL,
    vol                         INTEGER,
    data_timestamp              TEXT,                -- ISO 字符串(client 源头 wall-clock)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, trade_date, time_idx)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
CREATE INDEX IF NOT EXISTS idx_{table_name}_datetime ON {table_name}(datetime);
"""


# minute_bars 长表在 2026-09-13 v4 彻底废弃——合并到 minute_<date> 表
# (保留 schema 占位符以兼容旧代码 import,但保证不再创建新表)
MINUTE_BARS_TABLE_SCHEMA = "-- deprecated since 2026-09-13 v4: merged into MINUTE_TABLE_SCHEMA"


# --- zt:涨停池(17 字段,封单金额 + 连板信息) ---
ZT_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,
    name                        TEXT,
    is_st                       INTEGER,             -- bool→0/1
    is_new                      INTEGER,
    last_price                  REAL,
    price_change_ratio_pct      REAL,
    limit_up_time               TEXT,                -- '09:39'
    limit_up_reason             TEXT,
    continue_day_text           TEXT,                -- '首板'/'3 连板'
    continue_day_cnt            INTEGER,
    seal_money                  INTEGER,             -- 封单金额
    max_seal_money              REAL,                -- 最大封单金额
    first_time_unix             REAL,                -- 首次涨停时间(unix 秒)
    zt_timestamp                TEXT NOT NULL,       -- ★ v3 数据时间戳(ISO 字符串)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, zt_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_data_ts ON {table_name}(ts_code, zt_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# --- break:炸板池(11 字段) ---
BREAK_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 唯一标识(2026-09-12 v3 精简:删 thscode/ticker)
    name                        TEXT,
    last_price                  REAL,
    price_change_ratio_pct      REAL,
    open_times                  INTEGER,
    turnover_ratio_pct          REAL,
    turnover                    INTEGER,
    break_timestamp             TEXT NOT NULL,         -- ★ v3 数据时间戳(ISO 字符串)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, break_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_break_ts ON {table_name}(ts_code, break_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# --- anomaly:异动清单(2026-09-12 v3:顶层精简,keyword_list 拆到 anomaly_keywords 长表) ---
# 2026-09-16 v6.12:用户要求 keyword_list 不再拆长表(冗余,丢);anomaly_keywords 整套废弃
ANOMALY_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 唯一标识(v3 精简:删 thscode/ticker)
    stock_name                  TEXT,
    analysis_content            TEXT,
    tag_name                    TEXT,
    anomaly_timestamp           TEXT NOT NULL,         -- ★ v3 数据时间戳(ISO 字符串)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, anomaly_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code);
CREATE INDEX IF NOT EXISTS idx_{table_name}_anomaly_ts ON {table_name}(anomaly_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# --- anomaly_keywords:anomaly 的 keyword_list 长表 —— 2026-09-16 v6.12 整套废弃 ---
# 原本存在:LONG_TABLE_SCHEMAS["anomaly_keywords"] + insert_anomaly_keywords_batch,
# 用户原话"anomaly 数据落盘时没必要存 anomaly_keywords,keyword_list 信息已在 analysis_content 里包含"
# 现已彻底删除 SCHEMA / 插入函数 / persist_client 调用链。历史 anomaly_keywords_<date> 表保留不删(只读),后续用迁移脚本清理。


# --- hot:热股榜(9 字段) ---
HOT_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 唯一标识(2026-09-12 v3 精简:删 thscode/ticker)
    name                        TEXT,
    rank                        INTEGER,
    heat                        TEXT,                -- 注意:同花顺 heat 是 str(float)
    rank_change                 INTEGER,
    rank_trend                  TEXT,                -- 'up'/'down'/'flat'
    hot_timestamp               TEXT NOT NULL,         -- ★ v3 数据时间戳(ISO 字符串)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, hot_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_hot_ts ON {table_name}(ts_code, hot_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_rank ON {table_name}(rank);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# --- limitperformance:涨停表现详情(2026-09-14 v5 新增,23 字段) ---
#  数据源:coreClient/kpl_client.KPLClient.fetch_realtime_limit_performance()
#          (apphwhq 实时盯盘 host,盘中可用)
#  盘中 30s/轮写入 STREAM;15 min/轮 savedata 落盘
# 落盘 key:UNIQUE(ts_code, limitperformance_timestamp)
#           同一只股多次拉的快照都会落(保留涨停列表演化轨迹)
# 数据时间戳:limitperformance_timestamp(ISO 字符串;落盘时 persist 客户端将 unix int 转 ISO)
# lu_time:    涨停时间(**秒级字符串 `YYYY-MM-DD HH:MM:SS`**,与 limitperformance_timestamp 等记录时间不同;原 unix int 由 extractor 转;Redis STREAM 里仍存原始 int 便于二次处理)
LIMITPERFORMANCE_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date                  TEXT NOT NULL,
    ts_code                     TEXT NOT NULL,         -- 002790.SZ
    name                        TEXT,
    board_type                  INTEGER,              -- 1..5(连板数)
    board_count                 INTEGER,              -- 实际连板数(可能跟 board_type 不一致,比如新股)
    lu_time                     TEXT,                 -- 涨停时间(`YYYY-MM-DD HH:MM:SS` 秒级字符串)
    theme                       TEXT,                 -- 题材
    limit_reason                TEXT,                 -- 涨停原因
    is_break                    INTEGER,              -- 0/1 炸板标志
    amplitude                   REAL,                 -- 振幅
    turnover_rate               REAL,                 -- 换手率
    limit_order                 INTEGER,              -- 封单金额
    lu_limit_order              INTEGER,              -- 涨停封单
    net_change                  INTEGER,              -- 净买入
    main_in                     INTEGER,              -- 主力流入
    main_out                    INTEGER,              -- 主力流出
    amount                      INTEGER,              -- 总成交额
    free_float                  INTEGER,              -- 流通市值
    close_price                 REAL,                 -- 收盘价
    pct_chg                     REAL,                 -- 涨跌幅
    board_period                TEXT,                 -- '3天3板' / '首板'
    theme_id                    TEXT,
    sector_id                   INTEGER,
    limitperformance_timestamp  TEXT NOT NULL,        -- ★ 数据时间戳(ISO 字符串)
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
    UNIQUE(ts_code, limitperformance_timestamp)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_lp_ts ON {table_name}(ts_code, limitperformance_timestamp);
CREATE INDEX IF NOT EXISTS idx_{table_name}_board ON {table_name}(board_type, board_count);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
"""


# 9 个 kind → schema 映射
KIND_SCHEMAS = {
    "snapshot": SNAPSHOT_TABLE_SCHEMA,
    "auction": AUCTION_TABLE_SCHEMA,
    "orderbook": ORDERBOOK_TABLE_SCHEMA,
    "minute": MINUTE_TABLE_SCHEMA,
    "zt": ZT_TABLE_SCHEMA,
    "break": BREAK_TABLE_SCHEMA,
    "anomaly": ANOMALY_TABLE_SCHEMA,
    "hot": HOT_TABLE_SCHEMA,
    "limitperformance": LIMITPERFORMANCE_TABLE_SCHEMA,
}

# 长表:kind + "_<child>" → schema(子表逻辑独立,不在 KIND_SCHEMAS 里)
# 2026-09-16 v6.12:anomaly_keywords 已废弃,只剩 minute_bars
LONG_TABLE_SCHEMAS = {
    "minute_bars": MINUTE_BARS_TABLE_SCHEMA,
}


# 2026-09-11 新增:STREAM 游标持久化表
# 每个 stream key 一行,记录上次落盘的 last_id
# 启动时 read_cursor 取 last_id,落盘成功后 update_cursor
CURSOR_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS online_stream_cursor (
    stream_key      TEXT PRIMARY KEY,        -- 如 online:auction:stream
    last_id         TEXT NOT NULL,           -- Redis STREAM 的 last_id,如 "1789089985092-5"
    last_trade_date TEXT NOT NULL,           -- 落盘对应的交易日
    last_persist_ts TEXT NOT NULL,           -- 本次落盘的 wall clock
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime'))
);
"""


def table_name_for(kind: str, trade_date: str) -> str:
    """生成表名:kind_YYYYMMDD"""
    if kind not in (
        "auction", "snapshot", "orderbook", "minute", "zt",
        "break", "anomaly", "hot",  # 2026-09-11 新增 3 种
        "watchlist",                  # 2026-09-11 新增:覆盖落盘表
        "limitperformance",           # 2026-09-14 v5 新增:涨停表现详情
        "snapshot_index",             # 2026-09-16 v6.10 新增:8 只指数快照
    ):
        raise ValueError(
            f"kind 必须是 auction/snapshot/orderbook/minute/zt/break/anomaly/hot/watchlist/limitperformance/snapshot_index,实际 {kind!r}"
        )
    return f"{kind}_{trade_date}"


# ============================================================
# 核心:连接 + 建表
# ============================================================
def connect(year_month: str, *, readonly: bool = False) -> sqlite3.Connection:
    """打开某月的 db,自动 mkdir

    readonly: True 用于校验(多进程并发读取时用)
    """
    db_path = db_path_for_month(year_month)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if readonly:
        # 只读 URI 模式
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10)
    else:
        conn = sqlite3.connect(str(db_path), timeout=10)

    conn.row_factory = sqlite3.Row
    # WAL 模式允许多进程并发读写
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    if not readonly:
        # 2026-09-11 新增:创建 STREAM 游标持久化表
        conn.executescript(CURSOR_TABLE_SCHEMA)
        conn.commit()
    return conn


# ============================================================
# 2026-09-11 新增:STREAM 游标读/写
# ============================================================
def read_cursor(conn: sqlite3.Connection, stream_key: str) -> str | None:
    """读某 stream 的 last_id(没有则返回 None,调用方应 fallback 到 '0')"""
    cur = conn.execute(
        "SELECT last_id FROM online_stream_cursor WHERE stream_key=?",
        (stream_key,),
    )
    row = cur.fetchone()
    return row["last_id"] if row else None


def update_cursor(
    conn: sqlite3.Connection,
    *,
    stream_key: str,
    last_id: str,
    trade_date: str,
) -> None:
    """写/更新某 stream 的 last_id(upsert)"""
    now_iso = datetime.now().isoformat(timespec="milliseconds")
    conn.execute(
        """
        INSERT INTO online_stream_cursor
            (stream_key, last_id, last_trade_date, last_persist_ts, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(stream_key) DO UPDATE SET
            last_id=excluded.last_id,
            last_trade_date=excluded.last_trade_date,
            last_persist_ts=excluded.last_persist_ts,
            updated_at=excluded.updated_at
        """,
        (stream_key, last_id, trade_date, now_iso, now_iso),
    )
    conn.commit()


def list_cursors(conn: sqlite3.Connection) -> list[dict]:
    """列出所有 cursor(调试用)"""
    rows = conn.execute(
        "SELECT * FROM online_stream_cursor ORDER BY stream_key"
    ).fetchall()
    return [dict(r) for r in rows]


def ensure_table(conn: sqlite3.Connection, kind: str, trade_date: str) -> str:
    """确保某张表存在,返回表名

    2026-09-11 重构:按 kind 分发到对应 schema(全拆列)
    """
    table_name = table_name_for(kind, trade_date)
    schema = KIND_SCHEMAS.get(kind)
    if schema is None:
        raise ValueError(
            f"kind {kind!r} 不在 KIND_SCHEMAS 里,可选: {list(KIND_SCHEMAS.keys())}"
        )
    conn.executescript(schema.format(table_name=table_name))
    if kind == "auction":
        # Additive per-table compatibility migration. Existing history remains
        # untouched and is never rewritten; new source fields start NULL there.
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}
        for column, sql_type in AUCTION_ADDITIVE_COLUMNS.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {sql_type}")
    conn.commit()
    return table_name


def ensure_long_table(conn: sqlite3.Connection, long_kind: str, trade_date: str) -> str:
    """确保长表存在(2026-09-16 v6.12:仅 long_kind = 'minute_bars')

    长表名 = <long_kind>_<YYYYMMDD>
    """
    if long_kind not in LONG_TABLE_SCHEMAS:
        raise ValueError(
            f"long_kind 必须是 {list(LONG_TABLE_SCHEMAS.keys())},实际 {long_kind!r}"
        )
    table_name = f"{long_kind}_{trade_date}"
    schema = LONG_TABLE_SCHEMAS[long_kind]
    conn.executescript(schema.format(table_name=table_name))
    conn.commit()
    return table_name


# ============================================================
# 写入(2026-09-11 重构:按 kind 拆列)
# ============================================================

# 每个 kind 的列提取函数(persist_client 仍传旧 dict 格式,我们内部拆)
def _field(s: dict, key: str, default=None):
    """从 s 取字段,优先顶层(2026-09-11 v2 源头展开),fallback 到 s['payload'] 子 dict(兼容旧数据)

    这样两种来源都能正常工作:
        - v2 源头展开:persist_client 传 record 整体作为 payload 嵌套
        - 旧数据:字段在 payload 子 dict
    """
    if key in s and s[key] is not None:
        return s[key]
    p = s.get("payload")
    if isinstance(p, dict):
        return p.get(key, default)
    return default


def _extract_snapshot_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """snapshot 顶层字段提取(2026-09-12 v3 精简):
      - 删 thscode(同 ts_code)
      - 删 ticker(ts_code 去掉后缀即可)
      - 只保留 snapshot_timestamp(数据时间戳,存盘时已转 ISO)
      - created_at 由 SQLite DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')) 自动加,不入参
    """
    return (
        trade_date,
        s["ts_code"],
        _field(s, "volume"),
        _field(s, "turnover"),
        _field(s, "last_price"),
        _field(s, "price_change"),
        _field(s, "price_change_ratio_pct"),
        _field(s, "open_price"),
        _field(s, "high_price"),
        _field(s, "low_price"),
        _field(s, "prev_price"),
        _field(s, "snapshot_timestamp"),  # 已是 ISO 字符串(persist 端转)
    )


def _extract_snapshot_index_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """snapshot_index 顶层字段提取(2026-09-16 v6.10 新增):8 只指数,30s/轮

    数据源:ths_client.fetch_index_snapshot → 11 字段 + snapshot_timestamp(int unix ms)
    v6.10 设计决策:
      - ts_code 即指数代码(同花顺 thscode,基类 _flat_record 已重命名)
      - ticker:同花顺内部代号,保留用于对账
      - snapshot_unix:int unix ms(从 snapshot_timestamp 直接拿,ths_client 已经处理)
      - snapshot_timestamp 列存 ISO 字符串(兼容其它 snapshot 表)
      - created_at 由 SQLite DEFAULT 自动加,不入参
    """
    # snapshot_unix:从 ths_client 拿到的 int unix ms(单位:毫秒)
    snap_ts_raw = s.get("snapshot_timestamp", 0)
    try:
        snap_unix = int(snap_ts_raw) if snap_ts_raw else None
    except (ValueError, TypeError):
        snap_unix = None
    # 兜底:用当前 wall clock ms
    if snap_unix is None or snap_unix == 0:
        snap_unix = int(datetime.now().timestamp() * 1000)

    # snapshot_timestamp ISO 字符串:从 int ms 转换(供 SQL 排序 / 兼容其它 snapshot 表)
    try:
        snap_ts_iso = datetime.fromtimestamp(snap_unix / 1000).isoformat(timespec="milliseconds")
    except (ValueError, TypeError, OSError):
        snap_ts_iso = ""

    # ts_code:基类 _flat_record 已把 thscode 重命名为 ts_code
    ts_code = s.get("ts_code") or s.get("thscode") or ""

    return (
        trade_date,
        ts_code,                  # ts_code(指数代码,如 000001.SH)
        _field(s, "ticker"),
        _field(s, "volume"),
        _field(s, "turnover"),
        _field(s, "last_price"),
        _field(s, "price_change"),
        _field(s, "price_change_ratio_pct"),
        _field(s, "open_price"),
        _field(s, "high_price"),
        _field(s, "low_price"),
        _field(s, "prev_price"),
        snap_ts_iso,              # ISO 字符串(给 SQL)
        snap_unix,                # int ms(用户最高优先级:必须存 unix int64)
    )


def _extract_auction_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """auction 顶层字段提取(2026-09-12 v3:独立 schema,字段对齐 snapshot 但时间戳改 auction_timestamp)

    v3 字段对齐 redis_client.put_auction(STREAM 字段展开)
    """
    return (
        trade_date,
        s.get("ts_code", ""),
        _field(s, "volume"),
        _field(s, "turnover"),
        _field(s, "last_price"),
        _field(s, "price_change"),
        _field(s, "price_change_ratio_pct"),
        _field(s, "open_price"),
        _field(s, "high_price"),
        _field(s, "low_price"),
        _field(s, "prev_price"),
        _field(s, "auction_timestamp") or "",    # v6.13:auction 同花顺接口必返回,无需 snapshot_timestamp 兜底
        _field(s, "auction_timestamp_source"),
        _field(s, "auction_request_stage"),
        _field(s, "auction_phase"),
        _field(s, "data_status"),
        _field(s, "name"),
        _field(s, "auction_price"),
        _field(s, "auction_pct"),
        _field(s, "auction_volume"),
        _field(s, "auction_amount"),
        _field(s, "auction_unmatched"),
        _field(s, "auction_turnover_pct"),
        _field(s, "auction_yesterday_ratio_pct"),
        _field(s, "auction_volume_ratio"),
        _field(s, "pre_close_price"),
        _field(s, "float_market_cap"),
    )


def _extract_orderbook_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """orderbook 顶层字段提取(2026-09-12 v3:数据时间戳改 orderbook_timestamp)

    v3 字段对齐 redis_client.put_orderbook(STREAM 字段展开)
    """
    return (
        trade_date,
        s.get("ts_code", ""),
        _field(s, "bid_price_1"),
        _field(s, "bid_volume_1"),
        _field(s, "ask_price_1"),
        _field(s, "ask_volume_1"),
        _field(s, "last_price"),
        _field(s, "volume"),
        _field(s, "orderbook_timestamp") or _field(s, "data_timestamp") or "",
    )


def _extract_minute_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """minute 字段提取(2026-09-13 v4:展开存,每根 K 线 1 行)

    对齐 tdx_client.get_minute_kline 输出:
      ts_code / trade_date / time_idx / datetime / price / vol / data_timestamp
    created_at 由 SQLite DEFAULT 自动加
    """
    return (
        trade_date,
        s.get("ts_code", ""),
        int(s.get("time_idx", s.get("bar_idx", 0))),
        s.get("datetime") or s.get("bar_time") or "",
        s.get("price"),
        s.get("vol"),
        s.get("data_timestamp") or "",
    )


def _normalize_zt_flag(value: Any, *, field: str) -> int | None:
    """Normalize Redis scalar encodings of zt flags without truth-testing strings."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        if value in (0, 1):
            return int(value)
        logger.warning("zt flag %s has unsupported numeric value type=%s; storing NULL", field, type(value).__name__)
        return None
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "y"}:
            return 1
        if normalized in {"0", "false", "no", "n", ""}:
            return 0 if normalized else None
        logger.warning("zt flag %s has unsupported string representation; storing NULL", field)
        return None
    logger.warning("zt flag %s has unsupported value type=%s; storing NULL", field, type(value).__name__)
    return None


def _extract_zt_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """zt 提取(2026-09-12 v3 精简):只保留 zt_timestamp,删除 thscode/ticker/snap_ts_unix/data_timestamp/save_timestamp

    Args:
        s: 含 ts_code / payload(嵌套,fallback)的 dict
        trade_date: 交易日期
        now_save: 兼容参数(不再用,created_at 由 SQLite 自动)
    """
    return (
        trade_date,
        s["ts_code"],
        _field(s, "name"),
        _normalize_zt_flag(_field(s, "is_st"), field="is_st"),
        _normalize_zt_flag(_field(s, "is_new"), field="is_new"),
        _field(s, "last_price"),
        _field(s, "price_change_ratio_pct"),
        _field(s, "limit_up_time"),
        _field(s, "limit_up_reason"),
        _field(s, "continue_day_text"),
        _field(s, "continue_day_cnt"),
        _field(s, "seal_money"),
        _field(s, "max_seal_money"),
        _field(s, "first_time_unix"),
        s["zt_timestamp"],
    )


def _extract_break_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """break 提取(2026-09-12 v3 精简):只保留 break_timestamp,删除 thscode/ticker/snap_ts_unix/data_timestamp/save_timestamp

    v3 字段对齐 redis_client.put_break_pool(STREAM 字段展开)
    persist_client 已把 break_timestamp 转 ISO 字符串
    """
    return (
        trade_date,
        s.get("ts_code", ""),
        _field(s, "name"),
        _field(s, "last_price"),
        _field(s, "price_change_ratio_pct"),
        _field(s, "open_times"),
        _field(s, "turnover_ratio_pct"),
        _field(s, "turnover"),
        s.get("break_timestamp") or s.get("data_timestamp", ""),
    )


def _extract_anomaly_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """anomaly 顶层字段提取(2026-09-12 v3 精简):
      - 删 thscode(同 ts_code)
      - 删 ticker(可从 ts_code 推导)
      - 删 snap_ts_unix / data_timestamp / save_timestamp
      - 只保留 anomaly_timestamp(数据时间戳,存盘时已转 ISO)
      - created_at 由 SQLite DEFAULT 自动加
    """
    return (
        trade_date,
        s.get("ts_code", ""),
        _field(s, "stock_name"),
        _field(s, "analysis_content"),
        _field(s, "tag_name"),
        _field(s, "anomaly_timestamp") or _field(s, "data_timestamp") or "",
    )


def _extract_hot_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """hot 提取(2026-09-12 v3 精简):只保留 hot_timestamp,删除 thscode/ticker/snap_ts_unix/data_timestamp/save_timestamp

    v3 字段对齐 redis_client.put_hot_rank(STREAM 字段展开)
    persist_client 已把 hot_timestamp 转 ISO 字符串
    """
    heat_val = _field(s, "heat")
    return (
        trade_date,
        s.get("ts_code", ""),
        _field(s, "name"),
        _field(s, "rank"),
        str(heat_val) if heat_val is not None else None,
        _field(s, "rank_change"),
        _field(s, "rank_trend"),
        s.get("hot_timestamp") or s.get("data_timestamp", ""),
    )


def _extract_watchlist_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """watchlist 提取(2026-09-15 v6.7 新增)

    数据时间戳 watchlist_timestamp:
      - STREAM 里是 ISO 字符串(新 v6.7 写)或 unix(老数据)
      - persist_watchlist 已统一转 ISO 字符串
    source / priority / reason:
      - source 必有
      - priority 默认 0(没有 STREAM 字段兜底)
      - reason 默认 = source(STREAM 没有 reason,跟 v6 旧 schema 对齐)
    """
    return (
        trade_date,
        s.get("ts_code", ""),
        _field(s, "source"),
        int(s.get("priority", 0) or 0),
        s.get("reason", "") or _field(s, "source"),
        s.get("watchlist_timestamp", ""),
    )


def _extract_limitperformance_row(s: dict, *, trade_date: str, now_save: str = "") -> tuple:
    """limitperformance 提取(2026-09-14 v5 新增,2026-09-16 v6.11 lu_time 转字符串):
       涨停表现详情 23 字段,源头展开存。

    数据时间戳 limitperformance_timestamp:
      - 写入 STREAM 时是 float unix int
      - persist 落盘前会转 ISO 字符串(通用模板 _persist_stream_to_sqlite 自动处理)
      - 字段名 = f"{kind}_timestamp" = "limitperformance_timestamp",跟其它 8 kind 对齐
    lu_time:
      - 涨停时间(原始 unix int → 落盘为 `YYYY-MM-DD HH:MM:SS` 秒级字符串)
      - 与 limitperformance_timestamp / created_at 等"记录时间"完全无关;lu_time 表示"这只股涨停封板的那一秒"
      - Redis STREAM 里仍保留原始 int(便于二次处理,如秒级比对)
      - 转秒失败时落空字符串(不抛错)
    """
    lu_raw = _field(s, "lu_time")
    lu_str = ""
    if lu_raw not in (None, ""):
        try:
            lu_str = datetime.fromtimestamp(int(lu_raw)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            lu_str = ""

    return (
        trade_date,
        s.get("ts_code", ""),
        _field(s, "name"),
        _field(s, "board_type"),
        _field(s, "board_count"),
        lu_str,                            # ← 秒级字符串
        _field(s, "theme"),
        _field(s, "limit_reason"),
        _field(s, "is_break"),
        _field(s, "amplitude"),
        _field(s, "turnover_rate"),
        _field(s, "limit_order"),
        _field(s, "lu_limit_order"),
        _field(s, "net_change"),
        _field(s, "main_in"),
        _field(s, "main_out"),
        _field(s, "amount"),
        _field(s, "free_float"),
        _field(s, "close_price"),
        _field(s, "pct_chg"),
        _field(s, "board_period"),
        _field(s, "theme_id"),
        _field(s, "sector_id"),
        s.get("limitperformance_timestamp", ""),
    )


_KIND_EXTRACTORS = {
    "snapshot": _extract_snapshot_row,
    "snapshot_index": _extract_snapshot_index_row,    # 2026-09-16 v6.10 新增
    "auction": _extract_auction_row,            # 2026-09-12 v3:独立 schema + 独立 extractor
    "orderbook": _extract_orderbook_row,        # 2026-09-12 v3:独立 schema + 独立 extractor
    "minute": _extract_minute_row,
    "zt": _extract_zt_row,
    "break": _extract_break_row,
    "anomaly": _extract_anomaly_row,
    "hot": _extract_hot_row,
    "limitperformance": _extract_limitperformance_row,    # 2026-09-14 v5 新增
    "watchlist": _extract_watchlist_row,                  # 2026-09-15 v6.7 新增
}


def insert_snapshot(
    conn: sqlite3.Connection,
    *,
    kind: str,
    trade_date: str,
    ts_code: str,
    data_timestamp: str,
    save_timestamp: str | None = None,
    payload: dict,
) -> bool:
    """插入一条快照(主键冲突时 IGNORE)

    2026-09-11 重构:按 kind 拆列,payload 不再存 JSON,字段全展开到独立列

    Returns:
        True  = 插入成功
        False = 已存在(IGNORE 跳过)
    """
    if save_timestamp is None:
        save_timestamp = datetime.now().isoformat(timespec="milliseconds")
    table_name = ensure_table(conn, kind, trade_date)
    extractor = _KIND_EXTRACTORS[kind]
    s = {
        "ts_code": ts_code,
        "data_timestamp": data_timestamp,
        "save_timestamp": save_timestamp,
        "payload": payload,
    }
    row = extractor(s, trade_date=trade_date, now_save=save_timestamp)
    # 用 schema 里的列名拼 SQL(简单可靠,不用 f-string 注入风险)
    cols = _columns_for_kind(kind)
    placeholders = ",".join("?" for _ in cols)
    col_names = ",".join(cols)
    try:
        conn.execute(
            f"INSERT OR IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})",
            row,
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def _columns_for_kind(kind: str) -> list[str]:
    """返回某 kind 表的所有列名(不含 id),用于 INSERT"""
    return _KIND_COLUMNS[kind]


# 各 kind 的列定义顺序(与 _extract_*_row 一一对应)
# 2026-09-12 v3 精简:snapshot/auction 只保留 ts_code + snapshot_timestamp
_KIND_COLUMNS = {
    "snapshot": [
        "trade_date", "ts_code",
        "volume", "turnover", "last_price", "price_change",
        "price_change_ratio_pct", "open_price", "high_price", "low_price",
        "prev_price", "snapshot_timestamp",
    ],
    "snapshot_index": [    # 2026-09-16 v6.10 新增:8 只指数,字段顺序与 _extract_snapshot_index_row 一一对应
        "trade_date", "ts_code", "ticker",
        "volume", "turnover", "last_price", "price_change",
        "price_change_ratio_pct", "open_price", "high_price", "low_price",
        "prev_price", "snapshot_timestamp", "snapshot_unix",
    ],
    "auction": [  # 2026-09-12 v3:独立 schema,字段对齐 snapshot + 加 auction_phase / data_status,时间戳改 auction_timestamp
        "trade_date", "ts_code",
        "volume", "turnover", "last_price", "price_change",
        "price_change_ratio_pct", "open_price", "high_price", "low_price",
        "prev_price", "auction_timestamp", "auction_timestamp_source", "auction_request_stage",
        "auction_phase", "data_status", "name", "auction_price", "auction_pct",
        "auction_volume", "auction_amount", "auction_unmatched", "auction_turnover_pct",
        "auction_yesterday_ratio_pct", "auction_volume_ratio", "pre_close_price", "float_market_cap",
    ],
    "orderbook": [  # 2026-09-12 v3:删 thscode/ticker/data_timestamp/save_timestamp,时间戳改 orderbook_timestamp
        "trade_date", "ts_code",
        "bid_price_1", "bid_volume_1", "ask_price_1", "ask_volume_1",
        "last_price", "volume", "orderbook_timestamp",
    ],
    "minute": [  # 2026-09-13 v4:展开存,每根 K 线 1 行,字段对齐 tdx_client.get_minute_kline
        "trade_date", "ts_code", "time_idx", "datetime",
        "price", "vol", "data_timestamp",
    ],
    "zt": [  # 2026-09-12 v3 精简:删 thscode/ticker/snap_ts_unix/data_timestamp/save_timestamp
        "trade_date", "ts_code", "name",
        "is_st", "is_new", "last_price", "price_change_ratio_pct",
        "limit_up_time", "limit_up_reason", "continue_day_text",
        "continue_day_cnt", "seal_money", "max_seal_money",
        "first_time_unix", "zt_timestamp",
    ],
    "break": [  # 2026-09-12 v3 精简:删 thscode/ticker/snap_ts_unix/data_timestamp/save_timestamp
        "trade_date", "ts_code", "name",
        "last_price", "price_change_ratio_pct", "open_times",
        "turnover_ratio_pct", "turnover", "break_timestamp",
    ],
    "anomaly": [  # 2026-09-12 v3 精简:删 thscode/ticker/snap_ts_unix/data_timestamp/save_timestamp,时间戳改 anomaly_timestamp
        "trade_date", "ts_code", "stock_name",
        "analysis_content", "tag_name", "anomaly_timestamp",
    ],
    "hot": [  # 2026-09-12 v3 精简:删 thscode/ticker/snap_ts_unix/data_timestamp/save_timestamp
        "trade_date", "ts_code", "name",
        "rank", "heat", "rank_change", "rank_trend",
        "hot_timestamp",
    ],
    "limitperformance": [  # 2026-09-14 v5 新增:涨停表现详情 23 字段,UNIQUE(ts_code, limitperformance_timestamp)
        "trade_date", "ts_code", "name",
        "board_type", "board_count", "lu_time",
        "theme", "limit_reason", "is_break",
        "amplitude", "turnover_rate",
        "limit_order", "lu_limit_order",
        "net_change", "main_in", "main_out",
        "amount", "free_float",
        "close_price", "pct_chg",
        "board_period", "theme_id", "sector_id",
        "limitperformance_timestamp",
    ],
    "watchlist": [   # 2026-09-15 v6.7 新增:与 WATCHLIST_TABLE_SCHEMA 列对齐
        "trade_date", "ts_code",
        "source", "priority", "reason", "watchlist_timestamp",
    ],
}

# Columns added to existing daily auction tables without rewriting historical rows.
AUCTION_ADDITIVE_COLUMNS = {
    "auction_timestamp_source": "TEXT",
    "auction_request_stage": "TEXT",
    "name": "TEXT",
    "auction_price": "REAL",
    "auction_pct": "REAL",
    "auction_volume": "REAL",
    "auction_amount": "REAL",
    "auction_unmatched": "REAL",
    "auction_turnover_pct": "REAL",
    "auction_yesterday_ratio_pct": "REAL",
    "auction_volume_ratio": "REAL",
    "pre_close_price": "REAL",
    "float_market_cap": "REAL",
}


def insert_snapshots_batch(
    conn: sqlite3.Connection,
    *,
    kind: str,
    trade_date: str,
    snapshots: list[dict],
) -> int:
    """批量插入快照,返回成功条数

    2026-09-11 重构:按 kind 拆列

    snapshots: [{
        'ts_code': str,
        'data_timestamp': str,    # 数据时间(ISO)
        'save_timestamp': str,    # 落盘时间(ISO,可选,默认 wall clock)
        'payload': dict,          # 含 kind 特有字段
    }, ...]
    """
    if not snapshots:
        return 0

    table_name = ensure_table(conn, kind, trade_date)
    cols = _columns_for_kind(kind)
    extractor = _KIND_EXTRACTORS[kind]
    now_save = datetime.now().isoformat(timespec="milliseconds")
    rows = [extractor(s, trade_date=trade_date, now_save=now_save) for s in snapshots]

    placeholders = ",".join("?" for _ in cols)
    col_names = ",".join(cols)
    try:
        cur = conn.executemany(
            f"INSERT OR IGNORE INTO {table_name} ({col_names}) VALUES ({placeholders})",
            rows,
        )
        conn.commit()
        # SQLite 的 executemany 不返回精确 rowcount(rowcount=-1 或 0 都常见)
        # 2026-09-15 v6.7:用实际 rowcount(可能 -1)兜底为 len(rows)
        rc = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(rows)
        return rc
    except sqlite3.IntegrityError as e:
        logger.error(f"批量插入失败: {e}")
        return -1   # 2026-09-15 v6.7:用 -1 标识真失败,区别于 INSERT OR IGNORE 全去重(返回 0)
    except sqlite3.Error as e:
        logger.error(f"SQLite 错误: {e}")
        return -1


def insert_minute_source_quality(conn: sqlite3.Connection, records: list[dict]) -> int:
    """Idempotently persist source-quality observations beside minute rows.

    Soft flags never alter a bar's OHLC, volume, or amount. The separate table
    lets callers retain the legacy minute schema while volume-sensitive readers
    can identify affected keys and inspect the exact decoded source payload.
    """
    if not records:
        return 0
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS minute_source_quality (
            trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            time_idx INTEGER NOT NULL,
            datetime TEXT NOT NULL,
            quality_code TEXT NOT NULL,
            source TEXT NOT NULL,
            source_payload_json TEXT NOT NULL,
            source_timestamp TEXT,
            detected_at TEXT NOT NULL,
            PRIMARY KEY (trade_date, ts_code, time_idx, quality_code)
        )
        """
    )
    now = datetime.now().isoformat(timespec="milliseconds")
    rows = []
    for record in records:
        payload = {
            "price": record.get("price"),
            "vol": record.get("vol"),
            "amount": record.get("source_amount"),
            "ohlc": record.get("source_ohlc"),
            "decoder_corrections": record.get("decoder_corrections"),
        }
        for quality_code in sorted(set(record.get("quality_flags") or ())):
            rows.append((
                record["trade_date"],
                record["ts_code"],
                int(record["time_idx"]),
                record["datetime"],
                quality_code,
                record.get("source", "tdx_native_1min"),
                json.dumps(payload, ensure_ascii=False, allow_nan=False, sort_keys=True),
                record.get("data_timestamp"),
                now,
            ))
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT INTO minute_source_quality (
            trade_date, ts_code, time_idx, datetime, quality_code, source,
            source_payload_json, source_timestamp, detected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trade_date, ts_code, time_idx, quality_code) DO UPDATE SET
            datetime=excluded.datetime,
            source=excluded.source,
            source_payload_json=excluded.source_payload_json,
            source_timestamp=excluded.source_timestamp,
            detected_at=excluded.detected_at
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def insert_minute_bars_batch(
    conn: sqlite3.Connection,
    *,
    trade_date: str,
    bars: list[dict],
) -> int:
    """批量插入 minute_bars 长表

    2026-09-13 v4:已废弃——minute 数据合并到 minute_<date> 表
    保留此函数为兼容旧代码 import,但调用会直接报错
    改用 insert_snapshots_batch(conn, kind="minute", ...) 代替
    """
    raise NotImplementedError(
        "insert_minute_bars_batch 在 v4 已废弃:"
        "minute 数据合并到 minute_<date> 表,改用 insert_snapshots_batch(kind='minute')"
    )


# 旧函数保留占位以避免外部 import 失败
_ = insert_minute_bars_batch


# 2026-09-16 v6.12:insert_anomaly_keywords_batch 已废弃(用户原话"没必要存"),
# 保留 schema 常量引用删除,这里直接删函数,persist_client 不再调用;历史 anomaly_keywords_<date> 表保留不删(只读),后续用迁移脚本清理。
# 如果未来需要重建,参考 git history commit 前版本。


# ============================================================
# 查询
# ============================================================
def query_snapshots(
    conn: sqlite3.Connection,
    *,
    kind: str,
    trade_date: str,
    ts_code: str | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    start_data_ts: str | None = None,
    end_data_ts: str | None = None,
    start_save_ts: str | None = None,
    end_save_ts: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    """查询快照

    2026-09-11 改造:
        - snap_ts → data_timestamp(数据时间)+ save_timestamp(落盘时间)双字段
        - start_ts/end_ts 兼容旧调用,等价于 start_data_ts/end_data_ts

    Args:
        kind: 'auction' / 'snapshot' / 'orderbook' / 'minute' / 'zt' / 'break' / 'anomaly' / 'hot'
        trade_date: YYYYMMDD
        ts_code: 可选,只查某只股票
        start_ts / end_ts: 旧 API,等价 start_data_ts/end_data_ts
        start_data_ts / end_data_ts: 数据时间区间(ISO)
        start_save_ts / end_save_ts: 落盘时间区间(ISO)
        limit: 最多返回多少条
    """
    # 兼容旧 API
    if start_ts and not start_data_ts:
        start_data_ts = start_ts
    if end_ts and not end_data_ts:
        end_data_ts = end_ts

    table_name = table_name_for(kind, trade_date)
    # 表可能不存在
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    if cur.fetchone() is None:
        return []

    # Data time columns differ by physical table schema. Minute bars expose
    # data_timestamp (and datetime/time_idx for bar ordering); snapshot_index
    # shares snapshot_timestamp rather than a snapshot_index_timestamp column.
    kind_ts_field = {
        "minute": "data_timestamp",
        "snapshot_index": "snapshot_timestamp",
    }.get(kind, f"{kind}_timestamp")

    sql = f"SELECT * FROM {table_name} WHERE 1=1"
    params = []
    if ts_code:
        sql += " AND ts_code=?"
        params.append(ts_code)
    if start_data_ts:
        sql += f" AND {kind_ts_field}>=?"
        params.append(start_data_ts)
    if end_data_ts:
        sql += f" AND {kind_ts_field}<=?"
        params.append(end_data_ts)
    if start_save_ts:
        sql += " AND created_at>=?"
        params.append(start_save_ts)
    if end_save_ts:
        sql += " AND created_at<=?"
        params.append(end_save_ts)
    if kind == "minute":
        sql += " ORDER BY datetime ASC, time_idx ASC"
    else:
        sql += f" ORDER BY {kind_ts_field} ASC"
    if limit:
        sql += f" LIMIT {int(limit)}"

    rows = conn.execute(sql, params).fetchall()
    # 2026-09-11 重构:直接返列(不 json.loads payload_json)
    return [dict(r) for r in rows]


def count_snapshots(
    conn: sqlite3.Connection,
    *,
    kind: str,
    trade_date: str,
) -> int:
    """某张表的行数(不存在返回 0)"""
    table_name = table_name_for(kind, trade_date)
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    if cur.fetchone() is None:
        return 0
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def list_tables(conn: sqlite3.Connection) -> list[str]:
    """列出 db 里的所有表"""
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


# ============================================================
# watchlist 表(2026-09-11 新增,2026-09-15 v6.7 改为增量落盘)
# ============================================================
WATCHLIST_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    source TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    watchlist_timestamp TEXT NOT NULL,   -- ★ v3 数据时间戳(ISO 字符串)
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),  -- ★ v3 落盘时间戳(SQLite 自动)
    UNIQUE(ts_code, watchlist_timestamp)  -- 2026-09-15 v6.7:允许多份并存(增量 INSERT 去重)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_priority ON {table_name}(priority DESC);
CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
CREATE INDEX IF NOT EXISTS idx_{table_name}_watchlist_ts ON {table_name}(watchlist_timestamp);
"""


def ensure_watchlist_table(conn: sqlite3.Connection, trade_date: str) -> str:
    """确保 watchlist_<YYYYMMDD> 表存在,返回表名

    2026-09-11 新增 — 2026-09-15 v6.7 改为增量落盘(每天允许多份)
    """
    table_name = table_name_for("watchlist", trade_date)
    conn.executescript(WATCHLIST_TABLE_SCHEMA.format(table_name=table_name))
    conn.commit()
    return table_name


# 注册 watchlist schema 到 KIND_SCHEMAS(L348 定义在文件上方,Python 模块加载顺序要求 schema 常量先定义)
KIND_SCHEMAS["watchlist"] = WATCHLIST_TABLE_SCHEMA    # 2026-09-15 v6.7 新增

# 注册 snapshot_index schema 到 KIND_SCHEMAS
KIND_SCHEMAS["snapshot_index"] = SNAPSHOT_INDEX_TABLE_SCHEMA    # 2026-09-16 v6.10 新增


def replace_watchlist(
    conn: sqlite3.Connection,
    *,
    trade_date: str,
    rows: list[dict],
) -> int:
    """覆盖落盘 watchlist_<YYYYMMDD>(2026-09-11 新增)

    流程(单事务):
        1. DELETE 当 trade_date 的所有行
        2. INSERT 新 rows
        3. COMMIT

    rows 格式:[{
        'ts_code'             : str,
        'source'              : str,    # 'yest' / 'prev_lianban' / ...
        'priority'            : int,    # 0-3,默认 0
        'reason'              : str,    # 来源说明
        'watchlist_timestamp' : str,    # ★ v3 数据时间戳(ISO 字符串)
                                        # 落盘时间戳 created_at 由 SQLite DEFAULT 自动加
    }, ...]

    Returns:
        写入条数(rows 数,DELETE 不计)
    """
    if not rows:
        # 没数据也要清表
        table_name = ensure_watchlist_table(conn, trade_date)
        conn.execute(f"DELETE FROM {table_name} WHERE trade_date = ?", (trade_date,))
        conn.commit()
        logger.info(f"[watchlist] 覆盖落盘:0 条,trade_date={trade_date}")
        return 0

    table_name = ensure_watchlist_table(conn, trade_date)

    payload_rows = [
        (
            r["ts_code"],
            trade_date,
            r["source"],
            int(r.get("priority", 0)),
            r.get("reason", ""),
            r.get("watchlist_timestamp") or r.get("data_timestamp") or "",  # ★ v3 兼容旧
        )
        for r in rows
    ]

    try:
        conn.execute("BEGIN")
        conn.execute(f"DELETE FROM {table_name} WHERE trade_date = ?", (trade_date,))
        conn.executemany(
            f"""
            INSERT INTO {table_name}
                (ts_code, trade_date, source, priority, reason, watchlist_timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            payload_rows,
        )
        conn.commit()
        logger.info(
            f"[watchlist] 覆盖落盘 {len(rows)}/{len(rows)} 条到 {table_name},"
            f"watchlist_ts={payload_rows[0][5]}"
        )
        return len(rows)
    except sqlite3.IntegrityError as e:
        conn.rollback()
        logger.error(f"[watchlist] 覆盖落盘失败: {e}")
        return 0


def query_watchlist(
    conn: sqlite3.Connection,
    *,
    trade_date: str,
) -> list[dict]:
    """查询 watchlist_<YYYYMMDD> 当日所有行,按 priority DESC, watchlist_timestamp DESC

    2026-09-12 v3 精简:删 save_timestamp,时间戳改 watchlist_timestamp,新增 created_at
    """
    table_name = table_name_for("watchlist", trade_date)
    if conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone() is None:
        return []
    rows = conn.execute(
        f"""
        SELECT ts_code, trade_date, source, priority, reason,
               watchlist_timestamp, created_at
        FROM {table_name}
        WHERE trade_date = ?
        ORDER BY priority DESC, watchlist_timestamp DESC
        """,
        (trade_date,),
    ).fetchall()
    return [dict(r) for r in rows]


# ============================================================
# CLI 测试
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 用当前年月测试
    ym = current_year_month()
    td = datetime.now().strftime("%Y%m%d")
    print(f"测试月份: {ym}, 日期: {td}")

    conn = connect(ym)
    print(f"连接: {db_path_for_month(ym)}")
    print(f"现有表: {list_tables(conn)}")

    # 插一条
    ok = insert_snapshot(
        conn,
        kind="snapshot",
        trade_date=td,
        ts_code="000001.SZ",
        data_timestamp=datetime.now().isoformat(timespec="milliseconds"),
        payload={"price": 10.5, "vol": 1000, "pct_chg": 1.5},
    )
    print(f"插入结果: {ok}")
    print(f"插后行数: {count_snapshots(conn, kind='snapshot', trade_date=td)}")

    # 查
    rows = query_snapshots(conn, kind="snapshot", trade_date=td, ts_code="000001.SZ")
    print(f"查询: {rows}")

    conn.close()
    print(f"✅ sqlite_client.py 测试通过")
