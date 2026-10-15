from __future__ import annotations

import duckdb

from tools.data_checks.audit_duckdb_tables import (
    _audit_catalog_links,
    _audit_table,
    _classify_limit_event_stock_orphans,
    _derived_checks,
)


def test_table_metrics_keep_null_nan_inf_and_date_counts_on_their_columns():
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE fixture (
              row_id INTEGER,
              price DOUBLE,
              quantity DOUBLE,
              note VARCHAR,
              trade_date VARCHAR,
              valid_at VARCHAR,
              changed_dates_json VARCHAR
            )
        """)
        con.executemany(
            "INSERT INTO fixture VALUES (?,?,?,?,?,?,?)",
            [
                (1, float("nan"), 7.0, "first", "20261014", "20261014 23:59:59", '{"kpl_list":[]}'),
                (2, float("inf"), None, None, "2026-02-30", "2026-10-15 12:00:00", '{"kpl_list":[]}'),
                (3, None, float("inf"), "third", "20261015 12:30:00", None, '{"kpl_list":[]}'),
            ],
        )
        report = _audit_table(con, "fixture", "main", "fixture", None, None)
        assert report["rows"] == 3
        assert report["null_counts"] == {
            "row_id": 0, "price": 1, "quantity": 1, "note": 1, "trade_date": 0,
            "valid_at": 1, "changed_dates_json": 0,
        }
        assert report["nonfinite_numeric"] == {"price": 2, "quantity": 1}
        assert report["date_checks"]["trade_date"]["invalid_nonnull"] == 1
        assert report["date_checks"]["valid_at"]["invalid_nonnull"] == 0
        assert "changed_dates_json" not in report["date_checks"]
        assert report["invalid_json_counts"]["changed_dates_json"] == 0
    finally:
        con.close()


def test_fifteen_min_catalog_compares_total_bars_not_stock_count():
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE fifteen_min_bars(ts_code VARCHAR, trade_date INTEGER, time_idx INTEGER)")
        con.execute("""
            INSERT INTO fifteen_min_bars
            SELECT code, 20261014, idx
            FROM (VALUES ('000001.SZ'), ('600000.SH')) codes(code), range(16) r(idx)
        """)
        con.execute("""
            CREATE TABLE fifteen_min_ingest_catalog(
              trade_date VARCHAR, rows BIGINT, stock_count INTEGER,
              bars_per_stock_min INTEGER, bars_per_stock_max INTEGER
            )
        """)
        con.execute("INSERT INTO fifteen_min_ingest_catalog VALUES ('20261014',32,2,16,16)")
        assert _audit_catalog_links(con, "fifteen_min") == {
            "daily_catalog_bar_count_mismatches": 0,
        }
    finally:
        con.close()


def test_empty_daily_source_is_zero_when_documented_daily_row_exists():
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE fact_limit_event(event_id VARCHAR, trade_date VARCHAR, tag VARCHAR)")
        con.execute("CREATE TABLE rel_limit_theme(event_id VARCHAR, theme_id VARCHAR, attribution_role VARCHAR)")
        con.execute("""
            CREATE TABLE fact_theme_daily(
              trade_date VARCHAR, theme_id VARCHAR, limit_up_count INTEGER, break_count INTEGER
            )
        """)
        con.execute("INSERT INTO fact_theme_daily VALUES ('20261014','T1',0,0)")
        result = _derived_checks(con, "theme_graph")
        assert result["daily_groups_compared"] == 1
        assert result["limit_up_count_mismatches"] == 0
        assert result["break_count_mismatches"] == 0
    finally:
        con.close()


def test_missing_theme_stock_identity_separates_blown_only_from_limitup():
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE dim_stock(ts_code VARCHAR)")
        con.execute("CREATE TABLE fact_limit_event(event_id VARCHAR, ts_code VARCHAR, tag VARCHAR)")
        con.executemany("INSERT INTO dim_stock VALUES (?)", [("000004.SZ",)])
        con.executemany(
            "INSERT INTO fact_limit_event VALUES (?,?,?)",
            [
                ("b1", "000001.SZ", "炸板"),
                ("b2", "000001.SZ", "炸板"),
                ("l1", "000002.SZ", "涨停"),
                ("x1", "000003.SZ", "其他"),
                ("ok", "000004.SZ", "涨停"),
            ],
        )
        report = _classify_limit_event_stock_orphans(con)
        assert report["status"] == "checked"
        assert report["orphan_codes"] == 3
        assert report["expected_blown_only_codes"] == 1
        assert report["expected_blown_only_event_rows"] == 2
        assert report["true_missing_limitup_dim_codes"] == 1
        assert report["true_missing_limitup_event_rows"] == 1
        assert report["other_or_unknown_tag_codes"] == 1
        assert report["samples"]["expected_blown_only"][0]["ts_code"] == "000001.SZ"
        assert report["samples"]["true_missing_limitup_dim"][0]["ts_code"] == "000002.SZ"
    finally:
        con.close()
