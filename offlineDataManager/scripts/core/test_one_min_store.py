from __future__ import annotations

import csv
from pathlib import Path
import sys

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from core.one_min_store import (
    get_one_min, get_one_min_catalog, get_one_min_source_files, import_one_min_csv, import_one_min_directory,
    list_one_min_dates, scan_one_min_sources,
)


HEADER = [
    "证券代码", "证券名称", "交易日期", "分钟时间", "开盘价（元）", "最高价（元）",
    "最低价（元）", "收盘价（元）", "成交量", "成交额", "复权因子",
]


def _write_csv(path: Path, day: str) -> None:
    dashed = f"{day[:4]}-{day[4:6]}-{day[6:8]}"
    rows = []
    for code, name, base in [("000001.SZ", "平安银行", 10.0), ("600000.SH", "浦发银行", 8.0)]:
        for stamp, bump in [("09:30:00", 0.0), ("09:31:00", 0.1), ("13:01:00", 0.2), ("15:00:00", 0.3)]:
            rows.append([code, name, day, f"{dashed} {stamp}", base+bump, base+bump+0.1,
                         base+bump-0.1, base+bump+0.05, 1000.0, 10000.0, 2.0])
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows)


def test_import_and_query_by_date_and_symbol(tmp_path: Path) -> None:
    source = tmp_path / "20260924_A股1分钟K线.csv"
    store = tmp_path / "store"
    _write_csv(source, "20260924")
    result = import_one_min_csv(source, root=store)
    assert result["rows"] == 6
    assert result["stock_count"] == 2
    assert list_one_min_dates(store) == ["20260924"]
    whole_day = get_one_min(trade_date="2026-09-24", root=store)
    assert len(whole_day) == 6
    stock = get_one_min("000001.SZ", start_date="20260924", end_date="20260924", root=store)
    assert stock["time_idx"].tolist() == [0, 120, 239]
    assert stock["datetime"].dt.strftime("%H:%M").tolist() == ["09:31", "13:01", "15:00"]
    assert set(stock.columns) >= {"open", "high", "low", "close", "amount", "adj_factor"}


def test_idempotent_and_no_sqlite_fallback(tmp_path: Path) -> None:
    source = tmp_path / "20260924_A股1分钟K线.csv"
    store = tmp_path / "store"
    _write_csv(source, "20260924")
    import_one_min_csv(source, root=store)
    again = import_one_min_csv(source, root=store)
    assert again["action"] == "skipped_existing"
    assert get_one_min("999999.SZ", trade_date="20260924", root=store).empty
    with pytest.raises(FileNotFoundError):
        get_one_min("000001.SZ", trade_date="20260924", root=tmp_path / "missing")


def test_full_market_without_date_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "20260924_A股1分钟K线.csv"
    store = tmp_path / "store"
    _write_csv(source, "20260924")
    import_one_min_csv(source, root=store)
    with pytest.raises(ValueError, match="全市场"):
        get_one_min(root=store)
    with pytest.raises(ValueError, match="单个trade_date"):
        get_one_min(start_date="20260923", end_date="20260924", root=store)


def test_duckdb_catalog_tracks_pending_imported_and_changed_source(tmp_path: Path) -> None:
    source_root = tmp_path / "csv"
    source_root.mkdir()
    source = source_root / "20260924_A股1分钟K线.csv"
    store = tmp_path / "store"
    catalog = tmp_path / "catalog.duckdb"
    _write_csv(source, "20260924")
    scan = scan_one_min_sources(source_root, root=store, catalog_path=catalog)
    assert scan["pending"] == 1
    assert get_one_min_catalog(catalog).iloc[0]["status"] == "pending"
    results = import_one_min_directory(source_root, root=store, catalog_path=catalog)
    assert results[0]["action"] == "imported"
    row = get_one_min_catalog(catalog).iloc[0]
    assert row["status"] == "imported"
    assert row["rows"] == 6
    with source.open("a", encoding="utf-8") as handle:
        handle.write("\n")
    scan = scan_one_min_sources(source_root, root=store, catalog_path=catalog)
    assert scan["pending"] == 1
    assert get_one_min_catalog(catalog).iloc[0]["status"] == "pending"


def test_data_provider_get_oneMin_is_the_public_query_api(tmp_path: Path) -> None:
    source = tmp_path / "20260924_A股1分钟K线.csv"
    store = tmp_path / "store"
    _write_csv(source, "20260924")
    import_one_min_csv(source, root=store)
    from coreClient.data_provider import get_oneMin

    result = get_oneMin("000001.SZ", trade_date="20260924", data_root=store)
    assert len(result) == 3
    assert result["close"].notna().all()


def test_identical_duplicate_source_files_are_recorded_once_per_date(tmp_path: Path) -> None:
    source_root = tmp_path / "csv"
    source_root.mkdir()
    original = source_root / "20260924_A股1分钟K线.csv"
    duplicate = source_root / "20260924_A股1分钟K线(1).csv"
    store = tmp_path / "store"
    catalog = tmp_path / "catalog.duckdb"
    _write_csv(original, "20260924")
    duplicate.write_bytes(original.read_bytes())
    summary = scan_one_min_sources(source_root, root=store, catalog_path=catalog)
    assert summary["scanned"] == 1
    assert summary["source_files"] == 2
    assert summary["duplicate_files"] == 1
    assert summary["source_conflicts"] == 0
    sources = get_one_min_source_files(catalog)
    assert len(sources) == 2
    assert sources["selected"].sum() == 1
    assert Path(sources.loc[sources["selected"], "source_path"].iloc[0]).name == original.name


def test_recursive_year_directory_and_source_move_do_not_reimport(tmp_path: Path) -> None:
    old_root = tmp_path / "old" / "2026"
    old_root.mkdir(parents=True)
    source = old_root / "20260924_A股1分钟K线.csv"
    store = tmp_path / "store"
    catalog = tmp_path / "catalog.duckdb"
    _write_csv(source, "20260924")
    first = import_one_min_directory(tmp_path / "old", root=store, catalog_path=catalog)
    assert first[0]["action"] == "imported"

    new_root = tmp_path / "分钟数据" / "1min" / "2026"
    new_root.mkdir(parents=True)
    moved = new_root / source.name
    source.replace(moved)
    summary = scan_one_min_sources(tmp_path / "分钟数据" / "1min", root=store, catalog_path=catalog)
    assert summary["pending"] == 0
    row = get_one_min_catalog(catalog).iloc[0]
    assert row["status"] == "imported"
    assert row["source_path"] == str(moved)


def test_bj_after_hours_minute_is_discarded(tmp_path: Path) -> None:
    source = tmp_path / "20250102_A股1分钟K线.csv"
    with source.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerow([
            "920000.BJ", "安徽凤凰", "20250102", "2025-01-02 15:00:00",
            10.0, 10.1, 9.9, 10.05, 1000.0, 10000.0, 2.0,
        ])
        writer.writerow([
            "920000.BJ", "安徽凤凰", "20250102", "2025-01-02 15:30:00",
            10.0, 10.1, 9.9, 10.05, 1000.0, 10000.0, 2.0,
        ])
    store = tmp_path / "store"
    imported = import_one_min_csv(source, root=store)
    result = get_one_min("920000.BJ", trade_date="20250102", root=store)
    assert imported["rows"] == 1
    assert imported["discarded_after_close_rows"] == 1
    assert result["time_idx"].tolist() == [239]
    assert result["datetime"].max().strftime("%H:%M") == "15:00"
