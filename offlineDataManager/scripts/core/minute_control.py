"""分钟正式存储的轻量更新控制表；不得用下载覆盖范围代替实际落库日期。"""
from datetime import datetime, timezone

CONTROLS = {
    "one_minute": ("tbl_one_minute_ctrl", "one_min_bars",
        "SELECT MAX(trade_date), MAX(imported_at) FROM one_min_ingest_catalog WHERE status='imported'"),
    "fifteen_minute": ("tbl_fifteen_minute_ctrl", "fifteen_min_bars",
        "SELECT MAX(trade_date), MAX(updated_at) FROM fifteen_min_days WHERE row_count > 0"),
}


def _timestamp(value=None):
    value = value or datetime.now(timezone.utc)
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    # The existing DuckDB metadata timestamps are UTC without tzinfo.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat(timespec="seconds")


def connect_minute_writer(path, *, timeout=30.0):
    """短暂只读连接也会挡住DuckDB跨进程写锁；等待后重试，不丢下载断点。"""
    import time
    import duckdb
    deadline = time.monotonic() + timeout
    delay = 0.05
    while True:
        try:
            return duckdb.connect(str(path))
        except duckdb.IOException as exc:
            if 'lock' not in str(exc).lower() or time.monotonic() >= deadline:
                raise
            time.sleep(min(delay, max(0, deadline - time.monotonic())))
            delay = min(0.5, delay * 1.5)


def ensure_minute_control(conn, kind):
    table, key, seed_sql = CONTROLS[kind]
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (key VARCHAR PRIMARY KEY, max_date VARCHAR, updated_at VARCHAR)")
    if not conn.execute(f"SELECT 1 FROM {table} WHERE key=?", [key]).fetchone():
        # Only published catalog/day metadata, never scan the bars/Parquet.
        date, updated = conn.execute(seed_sql).fetchone()
        if date is not None:
            conn.execute(f"INSERT INTO {table} VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                         [key, str(date), _timestamp(updated) if updated else None])


def record_minute_update(conn, kind, trade_date, *, updated_at=None):
    """调用方在数据发布之后、目录/事实表同一个事务内调用。"""
    table, key, _ = CONTROLS[kind]
    date = str(trade_date).replace("-", "")
    datetime.strptime(date, "%Y%m%d")
    conn.execute(f"""
        INSERT INTO {table} VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            max_date=GREATEST({table}.max_date, excluded.max_date),
            updated_at=excluded.updated_at
    """, [key, date, _timestamp(updated_at)])


def touch_minute_update(conn, kind):
    """因子等原地修复只刷新写入时间，不改变行情日期。"""
    table, key, _ = CONTROLS[kind]
    conn.execute(f"UPDATE {table} SET updated_at=? WHERE key=?", [_timestamp(), key])


def main():
    """为已有正式库补建控制表，只读取既有目录/日清单，不扫描分钟行情。"""
    import argparse
    import json
    from pathlib import Path
    import duckdb
    from config.settings import ONE_MIN_CATALOG_PATH, FIFTEEN_MIN_DB_PATH
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--one-minute', default=str(ONE_MIN_CATALOG_PATH))
    parser.add_argument('--fifteen-minute', default=str(FIFTEEN_MIN_DB_PATH))
    args = parser.parse_args()
    for kind, path in (('one_minute', args.one_minute), ('fifteen_minute', args.fifteen_minute)):
        if not Path(path).is_file():
            raise FileNotFoundError(path)
        with connect_minute_writer(path) as conn:
            conn.execute('BEGIN')
            try:
                ensure_minute_control(conn, kind)
                conn.execute('COMMIT')
            except Exception:
                conn.execute('ROLLBACK')
                raise
            table = CONTROLS[kind][0]
            print(json.dumps({'table': table, 'rows': conn.execute(f'SELECT * FROM {table}').fetchall()}, ensure_ascii=False))


if __name__ == '__main__':
    main()
