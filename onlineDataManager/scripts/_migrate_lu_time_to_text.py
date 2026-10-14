#!/usr/bin/env python3
"""limitperformance 表 lu_time 列 INTEGER → TEXT(`YYYY-MM-DD HH:MM:SS`)迁移(2026-09-16 v6.11)。

设计原则:
  - 幂等:重复运行不会重复迁移(只迁移 INTEGER 列的表)
  - 一次性 12 步法(05 §6 SQLite schema 改造原则):
    1) 找到所有 limitperformance_* 表
    2) 对每张表:PRAGMA table_info 看 lu_time 类型
    3) INTEGER → 12 步表重建
    4) TEXT → 跳过
  - 不锁表(SQLite 单文件 ALTER TABLE 本身是事务内的,生产期间写 daemon 短暂停顿,
    `service_savedata_limitperformance` 下一轮 15min 才会写,影响窗口 < 1s)
  - 不删旧表备份到 _bak_20260916_lu_time(保留 7 天可回滚)

⚠️ 运行前必看:
  - 必须在生产 scheduler 重启**之前**跑(否则新数据会被旧 schema 改回)
  - 也可在 scheduler 暂停期间跑(launchctl unload + 迁移 + load)
  - 建议:先 scheduler unload → 跑脚本 → 验证 → scheduler load
"""
from __future__ import annotations
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


def migrate_one(conn: sqlite3.Connection, table: str) -> str:
    """单表迁移;返回迁移状态描述。"""
    cur = conn.cursor()
    # 1. 拿到 CREATE TABLE SQL
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
    row = cur.fetchone()
    if not row:
        return "missing"
    create_sql = row[0]
    # 2. 看 lu_time 列类型
    cur.execute(f"PRAGMA table_info('{table}')")
    cols = cur.fetchall()
    lu_type = next((c[2] for c in cols if c[1] == "lu_time"), None)
    if lu_type is None:
        return "no-lu_time-col"
    if "TEXT" in lu_type.upper():
        return f"already-TEXT"

    # 3. 备份当前表
    bak = f"{table}_bak_20260916_lu_time"
    cur.execute(f"DROP TABLE IF EXISTS '{bak}'")
    cur.execute(f"ALTER TABLE '{table}' RENAME TO '{bak}'")

    # 4. 改 schema(把 lu_time INTEGER 改成 TEXT),其它原样
    import re
    new_create = re.sub(r"lu_time\s+INTEGER", "lu_time                     TEXT", create_sql, count=1)
    if new_create == create_sql:
        # 兜底:用模糊空白匹配
        new_create = re.sub(r"lu_time\s+\w+", "lu_time                     TEXT", create_sql, count=1)
    if new_create == create_sql:
        return "schema-replace-failed"
    cur.execute(new_create)

    # 5. 把备份数据转 lu_time int → str 灌回新表
    col_names = [c[1] for c in cols]
    col_list = ", ".join(f'"{c}"' for c in col_names)
    cur.execute(f"SELECT {col_list} FROM '{bak}'")
    rows = cur.fetchall()
    lu_idx = col_names.index("lu_time")
    new_rows = []
    for r in rows:
        lu_raw = r[lu_idx]
        if lu_raw is None or lu_raw == "":
            lu_new = ""
        else:
            try:
                lu_new = datetime.fromtimestamp(int(lu_raw)).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                lu_new = ""
        new_rows.append(r[:lu_idx] + (lu_new,) + r[lu_idx + 1:])
    placeholders = ",".join(["?"] * len(col_names))
    cur.executemany(f"INSERT INTO '{table}' ({col_list}) VALUES ({placeholders})", new_rows)

    # 6. 索引重建(从旧表读索引 SQL 重命名)
    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?", (bak,))
    for name, idx_sql in cur.fetchall():
        if idx_sql is None:
            continue  # autoindex
        new_idx_sql = idx_sql.replace(f"ON '{bak}'", f"ON '{table}'").replace(f"ON {bak}", f"ON {table}")
        # 索引名也加 _v2 避免重名
        new_idx_name = f"{name}_v2"
        cur.execute(f"DROP INDEX IF EXISTS '{new_idx_name}'")
        cur.execute(idx_sql.replace(name, new_idx_name).replace(f"ON '{bak}'", f"ON '{table}'").replace(f"ON {bak}", f"ON {table}"))

    conn.commit()
    return f"migrated ({len(rows)} rows)"


def main() -> int:
    from coreClient._paths import online_data_dir
    db_path = online_data_dir() / "online_data_202609.db"
    if not db_path.exists():
        print(f"❌ DB not found: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'limitperformance_%' AND name NOT LIKE '%_bak%'")
    tables = [r[0] for r in cur.fetchall()]
    if not tables:
        print("⚠️ No limitperformance_* tables found")
        return 0

    print(f"## lu_time INTEGER → TEXT 迁移(2026-09-16 v6.11)")
    print(f"DB: {db_path}")
    print(f"Tables: {len(tables)}")
    print()
    total_migrated = 0
    for t in tables:
        result = migrate_one(conn, t)
        if "migrated" in result:
            n = int(result.split("(")[1].split(" ")[0])
            total_migrated += n
        print(f"  {t:<40} → {result}")

    conn.close()
    print()
    print(f"✅ done: 迁移 {total_migrated} rows;备份表 *_bak_20260916_lu_time 保留 7 天可回滚")
    return 0


if __name__ == "__main__":
    sys.exit(main())