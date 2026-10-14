"""
~/TradingAgent/scripts/migrate_tbl_tick_v1_to_v2.py
tdx ticks 落库 schema 迁移脚本(2026-09-19 新增)

目的:
- 把 tbl_tick (PK: ts_code, trade_date, seqId) 重命名为 tbl_tick_legacy(只读保留)
- 新建 tbl_tick_v2 (PK: ts_code, trade_date, time, seqId_in_minute)
- 不迁移数据(数据从 v2 起重新抓取)
- 不动 v1 的 ctrl 表(tbl_tick_ctrl);新建 tbl_tick_v2_ctrl

参考:docs/落库方案_v2.md

⚠️ 重要: 本脚本**不**自动执行,需手动:
    python3 scripts/migrate_tbl_tick_v1_to_v2.py --execute

不带 --execute 只做 dry-run,打印计划,不修改 db。

回滚:
    ALTER TABLE tbl_tick_legacy RENAME TO tbl_tick;
    DROP TABLE tbl_tick_v2;
    DROP TABLE tbl_tick_v2_ctrl;

注意:
    - 旧表改名后,所有读 tbl_tick 的旧代码会报错
    - offline_downloader.update_ticks 已双写兼容(v2 表存在才走新逻辑)
    - 旧读路径(data_provider.get_ticks 等)暂未改,需后续 PR
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "offlineDataManager" / "scripts"))

from core.offline_db_client import TICKS_DB, ensure_schema  # noqa: E402

V2_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tbl_tick_v2 (
    ts_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    time TEXT NOT NULL,
    seqId_in_minute INTEGER NOT NULL,
    datetime TEXT NOT NULL,
    price REAL NOT NULL,
    vol INTEGER NOT NULL,
    buyorsell INTEGER,
    fetch_seq INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    fetched_host TEXT NOT NULL,
    fetch_complete INTEGER NOT NULL,
    PRIMARY KEY (ts_code, trade_date, time, seqId_in_minute)
);
CREATE INDEX IF NOT EXISTS idx_tbl_tick_v2_ts_date ON tbl_tick_v2(ts_code, trade_date);

CREATE TABLE IF NOT EXISTS tbl_tick_v2_ctrl (
    ts_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    PRIMARY KEY (ts_code, trade_date)
);
"""


def check_state(conn: sqlite3.Connection) -> dict:
    """查看 db 当前状态:旧表在不在、新表在不在、待迁移数据量"""
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    return {
        "has_tbl_tick": "tbl_tick" in tables,
        "has_tbl_tick_legacy": "tbl_tick_legacy" in tables,
        "has_tbl_tick_v2": "tbl_tick_v2" in tables,
        "has_tbl_tick_v2_ctrl": "tbl_tick_v2_ctrl" in tables,
        "tbl_tick_rows": (
            conn.execute("SELECT COUNT(*) FROM tbl_tick").fetchone()[0]
            if "tbl_tick" in tables else 0
        ),
    }


def dry_run() -> int:
    print(f"db: {TICKS_DB}")
    conn = sqlite3.connect(str(TICKS_DB))
    try:
        state = check_state(conn)
        print(f"  tbl_tick 存在: {state['has_tbl_tick']} ({state['tbl_tick_rows']} 行将被冻结)")
        print(f"  tbl_tick_legacy 存在: {state['has_tbl_tick_legacy']} (需要旧表改名)")
        print(f"  tbl_tick_v2 存在: {state['has_tbl_tick_v2']}")
        print(f"  tbl_tick_v2_ctrl 存在: {state['has_tbl_tick_v2_ctrl']}")

        if state["has_tbl_tick_v2"]:
            print()
            print("⚠️ tbl_tick_v2 已存在,本次运行不会覆盖")
            print("   如想重建:先 DROP TABLE tbl_tick_v2 和 tbl_tick_v2_ctrl")
            return 1

        print()
        print("迁移计划:")
        print("  1. ALTER TABLE tbl_tick RENAME TO tbl_tick_legacy")
        print(f"     → {state['tbl_tick_rows']} 行只读保留")
        print("  2. CREATE TABLE tbl_tick_v2 + tbl_tick_v2_ctrl + idx")
        print()
        print("⚠️ 不会迁移数据(数据从 v2 起重新抓取)")
        print("⚠️ 不会动 tbl_tick_ctrl")
        print()
        print("确认执行请加 --execute")
        return 0
    finally:
        conn.close()


def execute() -> int:
    conn = sqlite3.connect(str(TICKS_DB))
    try:
        state = check_state(conn)
        if state["has_tbl_tick_v2"]:
            print("✗ tbl_tick_v2 已存在,拒绝迁移。先 DROP 再说。")
            return 1
        if not state["has_tbl_tick"]:
            print("✗ tbl_tick 不存在,无需迁移。")
            return 1
        # 1. rename
        conn.executescript("ALTER TABLE tbl_tick RENAME TO tbl_tick_legacy;")
        print("✓ tbl_tick → tbl_tick_legacy")
        # 2. create v2
        conn.executescript(V2_SCHEMA_SQL)
        print("✓ tbl_tick_v2 + tbl_tick_v2_ctrl + idx 创建完成")
        conn.commit()
        print()
        print("迁移完成。后续:")
        print("  - offline_downloader.update_ticks 会自动走 v2 路径(检测到 v2 表)")
        print("  - 旧读路径需要后续 PR 改造")
        print("  - 旧数据(在 tbl_tick_legacy)继续可读")
        return 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true",
                        help="实际执行(默认 dry-run)")
    args = parser.parse_args()
    if args.execute:
        return execute()
    return dry_run()


if __name__ == "__main__":
    sys.exit(main())
