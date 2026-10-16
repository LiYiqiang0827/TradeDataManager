#!/usr/bin/env python3
"""
repair_snapshot_index_20261016.py
=================================

一次性补回脚本(2026-10-17 v3):
    10/16 snapshot_index stream 3800 条数据因 stream 字段名不匹配(旧名 snapshot_timestamp,
    persist 端要 snapshot_index_timestamp)被全部丢进 online_stream_timestamp_quarantine 表,
    表 snapshot_index_20261016 未建。本脚本从 quarantine 提取 payload_json,直接用
    _extract_snapshot_index_row 落回当日表。

用法:
    # 默认补 10/16 一天(历史 trade_date 可通过 --trade-date 指定)
    python3 service/repair_snapshot_index_20261016.py
    python3 service/repair_snapshot_index_20261016.py --trade-date 20261016 --dry-run

退出码:
    0  全部成功
    1  有失败 / 缺数据 / 数据库异常
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

# 允许直接执行:与同级 service 脚本一致,只把 SCRIPTS_DIR 加进 sys.path
# (sqlite_client / coreClient 都被拉到 sys.path;coreClient 路径由 _paths 处理)
_HERE = Path(__file__).resolve()
SERVICE_DIR = _HERE.parent
SCRIPTS_DIR = SERVICE_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from core import sqlite_client  # noqa: E402


REPAIR_KIND = "snapshot_index"


def _db_path_for_month(year_month: str) -> Path:
    """与 sqlite_client.db_path_for_month 一致,这里复制避免 import 副作用"""
    # 默认配置 ~/TradingDatabase/online/online_data_YYYYMM.db
    base = Path.home() / "TradingDatabase" / "online"
    return base / f"online_data_{year_month}.db"


def _fetch_quarantine(conn: sqlite3.Connection, trade_date: int) -> tuple[list[dict], int]:
    """从 online_stream_timestamp_quarantine 提取指定 trade_date 的 payload_json"""
    cur = conn.execute(
        """
        SELECT payload_json, stream_id, source_timestamp
        FROM online_stream_timestamp_quarantine
        WHERE stream_key = ? AND target_trade_date = ? AND kind = ?
        ORDER BY stream_id ASC
        """,
        (f"online:{REPAIR_KIND}:stream", trade_date, REPAIR_KIND),
    )
    rows = []
    invalid_payload_rows = 0
    for payload_json, stream_id, src_ts in cur.fetchall():
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError):
            invalid_payload_rows += 1
            continue
        rows.append({"payload": payload, "stream_id": stream_id, "src_ts": src_ts})
    return rows, invalid_payload_rows


def _table_stats(db_path: Path, table_name: str) -> tuple[int, int, set[tuple[str, str]]]:
    """读取目标表行数、代码数和业务键，用于验证回填并支持幂等重跑。"""
    conn = sqlite3.connect(str(db_path))
    try:
        total_rows, code_count = conn.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT ts_code) FROM {table_name}"
        ).fetchone()
        keys = {
            (str(ts_code), str(snapshot_timestamp))
            for ts_code, snapshot_timestamp in conn.execute(
                f"SELECT ts_code, snapshot_timestamp FROM {table_name}"
            )
        }
        return int(total_rows), int(code_count), keys
    finally:
        conn.close()


def _insert_rows(db_path: Path, table_name: str, columns: list[str], rows: list[tuple]) -> int:
    """批量 INSERT,使用 OR IGNORE 避免 UNIQUE(ts_code, snapshot_timestamp) 冲突"""
    if not rows:
        return 0
    placeholders = ",".join(["?"] * len(columns))
    col_list = ",".join(columns)
    sql = f"INSERT OR IGNORE INTO {table_name} ({col_list}) VALUES ({placeholders})"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        cur = conn.cursor()
        # 拆批 500 行/批,避免大事务
        BATCH = 500
        total = 0
        for i in range(0, len(rows), BATCH):
            batch = rows[i:i + BATCH]
            cur.executemany(sql, batch)
            total += cur.rowcount
        conn.commit()
        return total
    finally:
        conn.close()


def _ensure_table(db_path: Path, table_name: str) -> None:
    """用 sqlite_client.SNAPSHOT_INDEX_TABLE_SCHEMA 建表(幂等)"""
    schema = sqlite_client.SNAPSHOT_INDEX_TABLE_SCHEMA.format(table_name=table_name)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(schema)
        # 索引补建(schema 里已含,这里幂等保险)
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_snapshot_ts ON {table_name}(snapshot_timestamp)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code)"
        )
        conn.commit()
    finally:
        conn.close()


def repair(trade_date: int, *, dry_run: bool = False, verbose: bool = True) -> dict:
    year_month = f"{str(trade_date)[:4]}{str(trade_date)[4:6]}"  # 20261016 → "202610"
    db_path = _db_path_for_month(year_month)
    table_name = f"{REPAIR_KIND}_{trade_date}"
    result = {
        "trade_date": trade_date,
        "db_path": str(db_path),
        "table_name": table_name,
        "quarantine_rows": 0,
        "invalid_payload_rows": 0,
        "extracted_rows": 0,
        "expected_unique_rows": 0,
        "inserted_rows": 0,
        "verified_rows": 0,
        "table_rows": 0,
        "distinct_codes": 0,
        "errors": [],
    }

    try:
        parsed_date = datetime.strptime(str(trade_date), "%Y%m%d")
        if parsed_date.strftime("%Y%m%d") != str(trade_date):
            raise ValueError("trade_date 必须是有效的 YYYYMMDD")
    except ValueError:
        result["errors"].append(f"无效 trade_date: {trade_date},应为有效 YYYYMMDD")
        return result

    if not db_path.exists():
        result["errors"].append(f"db 不存在: {db_path}")
        return result

    # 1) 从 quarantine 读 payload_json
    conn = sqlite3.connect(str(db_path))
    try:
        quarantine_rows, invalid_payload_rows = _fetch_quarantine(conn, trade_date)
    finally:
        conn.close()
    result["quarantine_rows"] = len(quarantine_rows)
    result["invalid_payload_rows"] = invalid_payload_rows
    if invalid_payload_rows:
        result["errors"].append(f"有 {invalid_payload_rows} 条 quarantine payload 不是有效 JSON")

    if not quarantine_rows:
        result["errors"].append(f"quarantine 表无 trade_date={trade_date} 的数据")
        return result

    # 2) 用 _extract_snapshot_index_row 解析成表行
    trade_date_str = str(trade_date)
    columns = sqlite_client._KIND_COLUMNS[REPAIR_KIND]
    extract = sqlite_client._KIND_EXTRACTORS[REPAIR_KIND]
    rows = []
    for r in quarantine_rows:
        try:
            row = extract(r["payload"], trade_date=trade_date_str)
            rows.append(row)
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"extract 失败 stream_id={r['stream_id']}: {e}")
    result["extracted_rows"] = len(rows)
    ts_code_idx = columns.index("ts_code")
    timestamp_idx = columns.index("snapshot_timestamp")
    expected_keys = {
        (str(row[ts_code_idx]), str(row[timestamp_idx]))
        for row in rows
    }
    result["expected_unique_rows"] = len(expected_keys)

    if not rows:
        result["errors"].append("没有可回填的有效行")
        return result

    if dry_run:
        if verbose:
            print(f"[DRY-RUN] trade_date={trade_date} 拟补 {len(rows)} 行 → {db_path} / {table_name}")
        return result

    # 3) 建表(幂等)+ 批量 INSERT
    try:
        _ensure_table(db_path, table_name)
    except Exception as e:  # noqa: BLE001
        result["errors"].append(f"建表失败: {e}")
        return result

    t0 = time.time()
    try:
        result["inserted_rows"] = _insert_rows(db_path, table_name, columns, rows)
    except Exception as e:  # noqa: BLE001
        result["errors"].append(f"INSERT 失败: {e}")
    result["insert_duration_s"] = round(time.time() - t0, 2)

    # 检查所有来源业务键都存在于目标表。重复运行时 inserted_rows=0 仍可成功，
    # 只要此前写入的行完整；若只写入一部分，则明确报告缺失数并失败。
    try:
        table_rows, distinct_codes, table_keys = _table_stats(db_path, table_name)
        verified_rows = len(expected_keys & table_keys)
        result["table_rows"] = table_rows
        result["distinct_codes"] = distinct_codes
        result["verified_rows"] = verified_rows
        result["missing_rows"] = len(expected_keys - table_keys)
        if result["missing_rows"]:
            result["errors"].append(
                f"回填核验缺少 {result['missing_rows']} 个来源业务键"
            )
    except Exception as e:  # noqa: BLE001
        result["errors"].append(f"回填后核验失败: {e}")

    return result


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="从 online_stream_timestamp_quarantine 补回 snapshot_index_<trade_date> 表"
    )
    p.add_argument(
        "--trade-date",
        type=int,
        default=20261016,
        help="目标 trade_date,默认 20261016(本次 bug 受影响日)",
    )
    p.add_argument("--dry-run", action="store_true", help="只统计,不写库")
    p.add_argument("--quiet", action="store_true", help="静默(成功不打印)")
    args = p.parse_args(argv)

    r = repair(args.trade_date, dry_run=args.dry_run, verbose=not args.quiet)
    if not args.quiet:
        print(json.dumps(r, ensure_ascii=False, indent=2))

    # 退出码:提取/写入/核验有错误才失败；幂等重跑允许 inserted_rows 为 0。
    if r["errors"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
