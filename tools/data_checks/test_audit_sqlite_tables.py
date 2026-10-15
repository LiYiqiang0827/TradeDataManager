from datetime import date
import sqlite3

from tools.data_checks.audit_sqlite_tables import _date_value, _profile, _timestamp_profile, run


def test_profile_respects_text_heat_signed_values_and_trade_date_calendar():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("""
            CREATE TABLE tbl_cn_kpl_list(
              ts_code TEXT, trade_date TEXT, tag TEXT, heat TEXT, pe REAL,
              amount REAL, snap_ts TEXT, PRIMARY KEY(ts_code,trade_date,tag)
            )
        """)
        conn.executemany(
            "INSERT INTO tbl_cn_kpl_list VALUES (?,?,?,?,?,?,?)",
            [
                ("000001.SZ", "20261014", "limit", "高热", -2.5, 12.0, "2099-01-01 00:00:00"),
                ("000002.SZ", "20261017", "limit", "7.1", -1.0, 0.0, "2099-01-01 00:00:00"),
                ("000003.SZ", "not-a-date", "limit", "中", None, float("inf"), "2099-01-01 00:00:00"),
            ],
        )
        result = _profile(
            conn,
            "tbl_cn_kpl_list",
            {date(2026, 10, 14), date(2026, 10, 15)},
            (date(2026, 10, 1), date(2026, 10, 31)),
        )
        assert result["status"] == "scanned"
        assert result["row_count"] == 3
        assert result["primary_key_check"]["null_key_rows"] == 0
        assert result["primary_key_check"]["duplicate_groups"] == 0
        assert result["business_date_checks"]["trade_date"]["invalid_nonnull_values"] == [
            {"value": "not-a-date", "rows": 1}
        ]
        assert result["business_date_checks"]["trade_date"]["calendar_comparison"][
            "not_open_date_rows"
        ] == 1
        assert result["column_storage_and_null_counts"]["heat"]["text"] == 3
        assert result["column_storage_and_null_counts"]["pe"]["real"] == 2
        assert result["column_storage_and_null_counts"]["amount"]["declared_numeric_nonfinite"] == 1
        assert result["domain_check_status"] == "not_evaluated_no_domain_rule"
        assert "snap_ts" not in result["business_date_checks"]
    finally:
        conn.close()


def test_date_parser_accepts_compact_dates_and_news_timestamps():
    assert _date_value("20261015") == date(2026, 10, 15)
    assert _date_value("2026-10-15T12:34:56+08:00", timestamp=True) == date(2026, 10, 15)
    assert _date_value("20261015123456", timestamp=True) == date(2026, 10, 15)


def test_mixed_storage_null_nonfinite_counts_are_mapped_to_their_own_columns():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE mixed(i INTEGER, f REAL, label TEXT, n NUMERIC)")
        conn.executemany("INSERT INTO mixed VALUES (?,?,?,?)", [
            (1, 1.5, "finite", None),
            (None, float("inf"), None, float("inf")),
            (3, None, "NaN", float("-inf")),
        ])
        result = _profile(conn, "mixed", set(), None)
        stats = result["column_storage_and_null_counts"]
        assert stats["i"]["null"] == 1
        assert stats["i"]["integer"] == 2
        assert stats["f"]["null"] == 1
        assert stats["f"]["real"] == 2
        assert stats["f"]["declared_numeric_nonfinite"] == 1
        assert stats["label"]["null"] == 1
        assert stats["label"]["text"] == 2
        assert stats["n"]["null"] == 1
        assert stats["n"]["declared_numeric_nonfinite"] == 2
        # SQLite stores textual NaN as text; it is not misreported as a float.
        assert stats["label"]["text"] == 2
    finally:
        conn.close()


def test_news_timestamp_audit_separates_event_date_and_full_timestamp_shape():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE tbl_news(datetime TEXT, content TEXT)")
        conn.executemany(
            "INSERT INTO tbl_news VALUES (?,?)",
            [
                ("20200101", "not inspected"),
                ("2026-10-15 12:30:00", "not inspected"),
                ("2026-10-15 12:30", "not inspected"),
                ("2024-02-29T23:59", "not inspected"),
                ("2026-02-30 12:30", "not inspected"),
                ("2026-10-15 24:00", "not inspected"),
                ("2026-10-15 12:60", "not inspected"),
                ("20261015123100", "not inspected"),
                ("20261015 12:30:00", "not inspected"),
                ("2026-10-15 25:61:00", "not inspected"),
                ("20261015256100", "not inspected"),
                ("2026-13-99 12:30:00", "not inspected"),
                ("garbage", "not inspected"),
            ],
        )
        result = _profile(conn, "tbl_news", set(), None)
        dates = result["business_date_checks"]["datetime"]
        assert dates["distinct_event_date_groups"] == 7
        assert dates["invalid_event_date_rows"] == 3
        assert dates["timestamp_format_checks"]["valid_iso_timestamp_rows"] == 1
        assert dates["timestamp_format_checks"]["valid_iso_minute_timestamp_rows"] == 2
        assert dates["timestamp_format_checks"]["compact_timestamp_rows"] == 3
        assert dates["timestamp_format_checks"]["invalid_iso_timestamp_rows"] == 5
        assert dates["timestamp_format_checks"]["invalid_compact_event_time_rows"] == 1
        assert dates["timestamp_format_checks"]["unknown_format_rows"] == 1
        assert len(dates["invalid_nonnull_values"]) == 3
        assert dates["calendar_comparison"]["status"] == "not_applicable"
    finally:
        conn.close()


def test_timestamp_only_resume_reuses_existing_profile_and_table_whitelist(tmp_path):
    root = tmp_path / "database"
    offline = root / "offline"
    offline.mkdir(parents=True)
    with sqlite3.connect(offline / "db_cn_basic.db") as conn:
        conn.execute("CREATE TABLE tbl_cn_tradecal(cal_date TEXT, exchange TEXT, is_open INTEGER)")
    with sqlite3.connect(offline / "db_cn_news.db") as conn:
        conn.execute("CREATE TABLE tbl_news(datetime TEXT, body TEXT)")
        conn.executemany("INSERT INTO tbl_news VALUES (?,?)", [
            ("20261015 12:30:00", "x"),
            ("2026-10-15 99:99:99", "y"),
        ])
    output = tmp_path / "audit.json"
    key = "db_cn_news.db:tbl_news"
    first = run(root, output, table_keys={key})
    old_profile = first["tables"][key]
    assert old_profile["status"] == "scanned"
    first_stats = old_profile["column_storage_and_null_counts"]

    updated = run(root, output, table_keys={key}, resume=True, timestamp_only=True)
    profile = updated["tables"][key]
    assert updated["schema_version"] == 3
    assert updated["writer_version"] == 3
    assert profile["status"] == "scanned"
    assert profile["column_storage_and_null_counts"] == first_stats
    assert profile["business_date_checks"]["datetime"]["timestamp_format_checks"]["invalid_iso_timestamp_rows"] == 1
    provenance = updated["audit_provenance"]
    assert provenance["mode"] == "timestamp_only_resume"
    assert provenance["rescan_status"] == "complete"
    assert provenance["selected_tables"] == [key]
    assert provenance["selected_timestamp_columns"] == {key: ["datetime"]}
    assert provenance["baseline_receipt"]["sha256"]
    assert provenance["composite_coverage_status"] == "partial_or_unverified"
