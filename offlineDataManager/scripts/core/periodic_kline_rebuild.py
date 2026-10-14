"""Bounded-memory rebuild of the paired weekly/monthly derived tables.

Each batch contains complete stock histories, so period boundaries and the
latest full-history adjustment-factor baseline match the original rebuild.
Only staging data are written until both results are ready.  The final copy
replaces the adjusted/original pair in one SQLite transaction.
"""

import sqlite3
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import pandas as pd
from loguru import logger


def _stage_frame(stage, table, frame, columns):
    selected = [column for column in frame.columns if column in columns]
    if not selected:
        raise ValueError(f"周/月 K staging 没有可写列: {table}")
    names = ",".join(f'"{column}"' for column in selected)
    placeholders = ",".join("?" for _ in selected)
    # A generator avoids materializing a second Python list of every output row.
    rows = (
        tuple(None if pd.isna(value) else value for value in row)
        for row in frame[selected].itertuples(index=False, name=None)
    )
    stage.executemany(
        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})', rows
    )


def rebuild_period_tables(
    conn,
    *,
    freq,
    aggregate,
    start_date=None,
    end_date=None,
    batch_size=64,
):
    """Rebuild a full adjusted/original pair without loading all daily rows.

    Date semantics deliberately remain compatible with ``update_week/month``:
    filter daily rows only when *both* bounds are provided, replace the entire
    target tables with that range, and use the latest non-null adjustment factor
    over each stock's *full* factor history even for a historical date range.

    ``aggregate`` is the existing pandas period aggregator.  Using complete
    stocks per batch preserves its OHLC, sum, rounding and pre_close behavior.
    A private on-disk SQLite staging file bounds memory independently of the
    caller's ``temp_store`` setting.  No production table is touched until the
    entire source snapshot has been processed successfully.
    """
    if freq not in {"weekly", "monthly"}:
        raise ValueError(f"Unsupported period frequency: {freq}")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if conn.in_transaction:
        raise ValueError("周/月 K 重建要求连接没有未提交事务")

    period = "week" if freq == "weekly" else "month"
    label = "周K" if freq == "weekly" else "月K"
    tables = (f"tbl_cn_{period}", f"tbl_cn_{period}_origin")
    columns_by_table = {}
    for table in tables:
        columns = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]
        if not columns:
            raise ValueError(f"周/月 K 目标表不存在: {table}")
        columns_by_table[table] = columns

    date_where = "WHERE trade_date >= ? AND trade_date <= ?" if start_date and end_date else ""
    date_params = (start_date, end_date) if date_where else ()
    counts = dict.fromkeys(tables, 0)
    source_rows = valid_rows = factor_rows = 0
    t0 = time.monotonic()
    alias = "kline_stage_" + uuid4().hex
    attached = False

    with tempfile.TemporaryDirectory(prefix="tradingagent-period-") as directory:
        stage_path = Path(directory) / "period_staging.sqlite"
        stage = sqlite3.connect(stage_path)
        try:
            # The private staging file needs no crash recovery: main tables are
            # still untouched, and a failed build is always discarded.
            stage.execute("PRAGMA journal_mode=OFF")
            for table, columns in columns_by_table.items():
                definitions = ",".join(f'"{column}"' for column in columns)
                stage.execute(f'CREATE TABLE "{table}" ({definitions})')

            # One source snapshot for every batch and its adjustment baseline.
            # Other WAL readers continue seeing the old pair until COMMIT.
            conn.execute("BEGIN")
            codes = [
                row[0] for row in conn.execute(
                    f"SELECT DISTINCT ts_code FROM tbl_cn_day {date_where} ORDER BY ts_code",
                    date_params,
                )
            ]
            if not codes:
                conn.rollback()
                logger.warning(f"[{label}] tbl_cn_day 指定范围为空,跳过")
                return counts

            logger.info(f"[{label}] 按股票分批全量重建: {len(codes):,} 只,每批 {batch_size} 只")
            for offset in range(0, len(codes), batch_size):
                batch_codes = codes[offset:offset + batch_size]
                placeholders = ",".join("?" for _ in batch_codes)
                bounds = " AND trade_date >= ? AND trade_date <= ?" if date_where else ""
                day = pd.read_sql_query(
                    "SELECT ts_code, trade_date, open, high, low, close, vol, amount "
                    f"FROM tbl_cn_day WHERE ts_code IN ({placeholders}){bounds} "
                    "ORDER BY ts_code, trade_date",
                    conn,
                    params=(*batch_codes, *date_params),
                )
                source_rows += len(day)
                day = day[(day["open"] > 0) & (day["close"] > 0)].copy()
                valid_rows += len(day)
                if not day.empty:
                    origin = aggregate(day, freq=freq)
                    _stage_frame(stage, tables[1], origin, columns_by_table[tables[1]])
                    counts[tables[1]] += len(origin)
                    del origin

                    factors = pd.read_sql_query(
                        "SELECT ts_code, trade_date, adj_factor FROM tbl_cn_adj_factor "
                        f"WHERE ts_code IN ({placeholders}) ORDER BY ts_code, trade_date",
                        conn,
                        params=batch_codes,
                    )
                    factor_rows += len(factors)
                    latest = factors.groupby("ts_code")["adj_factor"].last().reset_index()
                    latest.columns = ["ts_code", "adj_factor_latest"]
                    day = day.merge(factors, on=["ts_code", "trade_date"], how="left")
                    day = day.merge(latest, on="ts_code", how="left")
                    day["adj_factor"] = pd.to_numeric(day["adj_factor"]).fillna(1.0)
                    day["adj_factor_latest"] = pd.to_numeric(day["adj_factor_latest"]).fillna(1.0)
                    qfq_factor = day["adj_factor"] / day["adj_factor_latest"]
                    for column in ["open", "high", "low", "close"]:
                        day[column] = day[column] * qfq_factor
                    day["vol"] = day["vol"] / qfq_factor
                    day = day.drop(columns=["adj_factor", "adj_factor_latest"])
                    adjusted = aggregate(day, freq=freq)
                    _stage_frame(stage, tables[0], adjusted, columns_by_table[tables[0]])
                    counts[tables[0]] += len(adjusted)
                    del adjusted, factors, latest, qfq_factor
                del day
                stage.commit()
                completed = min(offset + batch_size, len(codes))
                logger.info(
                    f"[{label}] staging {completed:,}/{len(codes):,} 只; "
                    f"日线 {source_rows:,},复权因子 {factor_rows:,}; "
                    f"qfq/origin {counts[tables[0]]:,}/{counts[tables[1]]:,} 行; "
                    f"耗时 {time.monotonic() - t0:.1f}s"
                )

            if not all(counts.values()):
                raise ValueError(f"[{label}] 没有有效聚合数据,保留原表")
            stage.close()
            stage = None
            conn.execute(f'ATTACH DATABASE ? AS "{alias}"', (str(stage_path),))
            attached = True
            logger.info(f"[{label}] staging 完成,开始成对原子覆盖; 过滤无效价格 {source_rows - valid_rows:,} 行")
            for table, columns in columns_by_table.items():
                names = ",".join(f'"{column}"' for column in columns)
                conn.execute(f'DELETE FROM main."{table}"')
                cursor = conn.execute(
                    f'INSERT INTO main."{table}" ({names}) '
                    f'SELECT {names} FROM "{alias}"."{table}"'
                )
                if cursor.rowcount != counts[table]:
                    raise RuntimeError(f"[{label}] 写入行数不一致: {table}")
            conn.commit()
            logger.info(
                f"[完成] {tables[0]} +{counts[tables[0]]:,} 行; "
                f"{tables[1]} +{counts[tables[1]]:,} 行; 耗时 {time.monotonic() - t0:.1f}s"
            )
            return counts
        except BaseException:
            conn.rollback()
            raise
        finally:
            if stage is not None:
                stage.close()
            if attached:
                conn.execute(f'DETACH DATABASE "{alias}"')
