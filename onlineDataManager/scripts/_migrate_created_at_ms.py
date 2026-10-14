#!/usr/bin/env python3
"""
一次性迁移脚本:把今日 10 张表的 created_at DEFAULT 从秒级
`datetime('now','localtime')` 改成毫秒级
`strftime('%Y-%m-%d %H:%M:%f','now','localtime')`。

SQLite 不支持 ALTER COLUMN,所以用 12 步标准重建法:
  1. 取旧 schema + 索引
  2. PRAGMA foreign_keys=OFF
  3. BEGIN TRANSACTION
  4. CREATE TABLE new_xxx (...DEFAULT strftime...)
  5. CREATE INDEX ...
  6. INSERT INTO new_xxx SELECT * FROM old_xxx
  7. DROP TABLE old_xxx
  8. ALTER TABLE new_xxx RENAME TO xxx
  9. COMMIT
  10. VACUUM (可选,回收空间)
  11. 验证数据条数
  12. 验证 DEFAULT 是毫秒版

2026-09-15 第 23 轮 · 时间戳全部毫秒化
"""
import sqlite3
import sys
import shutil
import os
from datetime import datetime
from pathlib import Path

from coreClient._paths import online_data_dir

DB = str(online_data_dir() / "online_data_202609.db")
TRADE_DATE = "20260915"
NEW_DEFAULT = "strftime('%Y-%m-%d %H:%M:%f','now','localtime')"

# 11 张当日表(11 因有 minute 是 data_timestamp 不是 minute_timestamp)
TABLES = [
    "snapshot_20260915",
    "auction_20260915",
    "orderbook_20260915",
    "minute_20260915",
    "zt_20260915",
    "break_20260915",
    "anomaly_20260915",
    "hot_20260915",
    "limitperformance_20260915",
    "watchlist_20260915",
]


def migrate_table(conn, table_name: str) -> bool:
    """重建单张表,把 created_at DEFAULT 改为毫秒版。"""
    cur = conn.cursor()

    # 0. 清理上次失败的残留 new_ 表和索引(SQLite DDL 不受事务控制,失败会留垃圾)
    new_table = f"new_{table_name}"
    cur.execute(f'DROP TABLE IF EXISTS "{new_table}"')
    # 清理可能留下的 new_ 索引(auto-index / explicit 都清)
    leftover_idxs = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND (tbl_name=? OR tbl_name=?)",
        (new_table, table_name),
    ).fetchall()
    # 索引 RENAME 表后不会自动变名,只清残留的 new_xxx 索引
    for (idx_name,) in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'new_%'"
    ).fetchall():
        try:
            cur.execute(f'DROP INDEX IF EXISTS "{idx_name}"')
        except Exception:
            pass

    # 1. 检查表是否存在
    exists = cur.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()[0]
    if not exists:
        print(f"  ✗ {table_name}: 不存在,跳过")
        return False

    # 2. 检查 created_at 列是否存在
    cols = cur.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    col_names = [c[1] for c in cols]
    if "created_at" not in col_names:
        print(f"  ✗ {table_name}: 无 created_at 列,跳过")
        return False

    # 3. 取旧 schema(SQL 字符串)
    schema_sql = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()[0]

    # 检查是否已经是毫秒版
    if NEW_DEFAULT in schema_sql:
        print(f"  ✓ {table_name}: 已经是毫秒版,跳过")
        return True

    # 4. 替换 DEFAULT 字符串
    new_schema = schema_sql.replace(
        "datetime('now', 'localtime')", NEW_DEFAULT
    ).replace(
        "datetime('now','localtime')", NEW_DEFAULT
    )
    if NEW_DEFAULT not in new_schema:
        print(f"  ✗ {table_name}: schema 里找不到旧 DEFAULT,可能不需要改")
        return False

    # 5. 取所有索引(原表上的)
    indexes = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table_name,),
    ).fetchall()
    index_sqls = [i[0] for i in indexes]

    # 6. 数据条数(验证用)
    old_count = cur.execute(f"SELECT COUNT(*) FROM '{table_name}'").fetchone()[0]

    # 7. 12 步表重建
    new_table = f"new_{table_name}"

    cur.execute("PRAGMA foreign_keys = OFF")
    cur.execute("BEGIN IMMEDIATE")

    try:
        # a. 删旧表的索引(避免 new_ 表创建时同名冲突)
        for (idx_name,) in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name NOT LIKE 'sqlite_%'",
            (table_name,),
        ).fetchall():
            cur.execute(f'DROP INDEX IF EXISTS "{idx_name}"')

        # b. 新建表(用新 schema,表名带 new_ 前缀)
        cur.execute(new_schema.replace(table_name, new_table))

        # c. 重建索引(只改表名,索引名保持原名 — 但旧索引已删,new_ 表无同名)
        #    RENAME TABLE 后索引自动跟随
        for idx_sql in index_sqls:
            new_idx = idx_sql.replace(
                f"ON {table_name}(", f"ON {new_table}("
            ).replace(
                f"ON {table_name} (", f"ON {new_table} ("
            )
            cur.execute(new_idx)

        # d. 复制数据(保持列顺序)
        col_list = ",".join(f'"{c}"' for c in col_names)
        cur.execute(f'INSERT INTO "{new_table}" ({col_list}) SELECT {col_list} FROM "{table_name}"')
        new_count = cur.execute(f'SELECT COUNT(*) FROM "{new_table}"').fetchone()[0]

        if new_count != old_count:
            raise RuntimeError(
                f"数据复制失败 {old_count} -> {new_count},回滚"
            )

        # e. 删旧表
        cur.execute(f'DROP TABLE "{table_name}"')

        # f. 改新表名为旧名(索引自动跟随)
        cur.execute(f'ALTER TABLE "{new_table}" RENAME TO "{table_name}"')

        conn.commit()
        print(f"  ✓ {table_name}: {old_count} 条数据,DEFAULT 改毫秒")
        return True

    except Exception as e:
        conn.rollback()
        print(f"  ✗ {table_name}: 失败 {e},已回滚")
        return False
    finally:
        cur.execute("PRAGMA foreign_keys = ON")


def verify(conn, table_name: str):
    """验证表 schema DEFAULT 是毫秒版"""
    cur = conn.cursor()
    row = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if not row:
        return False, 0
    schema_sql = row[0]
    is_ms = NEW_DEFAULT in schema_sql
    total = cur.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    return is_ms, total


def main():
    # 1. 备份 DB(防万一)
    bak = DB + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"=== 备份 {DB} → {bak} ===")
    shutil.copy2(DB, bak)
    print(f"  ✓ 备份完成 ({os.path.getsize(bak)} bytes)")

    # 2. 连接 DB
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")  # 加速大批 INSERT

    print(f"\n=== 重建 {len(TABLES)} 张表 ===")
    success = 0
    for t in TABLES:
        if migrate_table(conn, t):
            success += 1

    # 3. 验证
    print(f"\n=== 验证 ({success}/{len(TABLES)}) ===")
    for t in TABLES:
        is_ms, total = verify(conn, t)
        flag = "✓ ms" if is_ms else "✗ s"
        print(f"  {t:30}: {total:>7} 条, DEFAULT = {flag}")

    conn.close()

    # 4. VACUUM(回收空间,单独连接,需要排他锁)
    print("\n=== VACUUM 回收空间 ===")
    try:
        v_conn = sqlite3.connect(DB)
        v_conn.execute("VACUUM")
        v_conn.close()
        print("  ✓ VACUUM 完成")
    except Exception as e:
        print(f"  ! VACUUM 失败(可忽略): {e}")

    print(f"\n=== 完工 {success}/{len(TABLES)} ===")
    print(f"备份文件: {bak}")
    return 0 if success == len(TABLES) else 1


if __name__ == "__main__":
    sys.exit(main())