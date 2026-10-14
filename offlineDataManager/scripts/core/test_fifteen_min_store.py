from __future__ import annotations

import csv
from pathlib import Path
import sys

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from core.fifteen_min_store import (
    get_fifteen_min, get_fifteen_min_catalog, import_fifteen_min_csv,
    import_fifteen_min_directory, optimize_fifteen_min_database,
    scan_fifteen_min_sources,
)


HEADER = [
    "证券代码", "证券名称", "交易日期", "分钟时间", "开盘价（元）", "最高价（元）",
    "最低价（元）", "收盘价（元）", "成交量", "成交额", "复权因子",
]
BASE_TIMES = [
    "09:30:00", "09:45:00", "10:00:00", "10:15:00", "10:30:00",
    "10:45:00", "11:00:00", "11:15:00", "11:30:00", "13:15:00",
    "13:30:00", "13:45:00", "14:00:00", "14:15:00", "14:30:00",
    "14:45:00", "15:00:00",
]


def _write_csv(path: Path, day: str, *, invalid_time: bool = False) -> None:
    dashed = f"{day[:4]}-{day[4:6]}-{day[6:8]}"
    rows = []
    specs = [
        ("000001.SZ", "平安银行", BASE_TIMES),
        ("920000.BJ", "安徽凤凰", BASE_TIMES + ["15:15:00", "15:30:00"]),
    ]
    for code, name, times in reversed(specs):  # 刻意乱序，验证导入后的全局排序。
        for index, stamp in enumerate(times):
            price = 10 + index / 100
            rows.append([code, name, day, f"{dashed} {stamp}", price, price+0.1,
                         price-0.1, price+0.05, 1000.0, 10000.0, 2.0])
    if invalid_time:
        rows.append(["000001.SZ", "平安银行", day, f"{dashed} 12:00:00",
                     10, 10.1, 9.9, 10, 1000.0, 10000.0, 2.0])
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows)


def test_import_discards_open_marker_and_bj_after_hours(tmp_path: Path) -> None:
    source = tmp_path / "20240102_A股15分钟K线.csv"
    store = tmp_path / "fifteen_min.duckdb"
    _write_csv(source, "20240102")
    result = import_fifteen_min_csv(source, root=store)
    assert result["rows"] == 32
    assert result["discarded_opening_marker_rows"] == 2
    assert result["discarded_after_close_rows"] == 2
    assert result["discarded_outside_session_rows"] == 4
    sz = get_fifteen_min("000001.SZ", trade_date="20240102", root=store)
    bj = get_fifteen_min("920000.BJ", trade_date="20240102", root=store)
    assert sz["time_idx"].tolist() == list(range(16))
    assert bj["time_idx"].tolist() == list(range(16))
    assert sz["datetime"].min().strftime("%H:%M") == "09:45"
    assert bj["datetime"].max().strftime("%H:%M") == "15:00"


def test_catalog_increment_and_idempotence(tmp_path: Path) -> None:
    source_root = tmp_path / "15min" / "2024"
    source_root.mkdir(parents=True)
    source = source_root / "20240102_A股15分钟K线.csv"
    store = tmp_path / "fifteen_min.duckdb"
    catalog = store
    _write_csv(source, "20240102")
    summary = scan_fifteen_min_sources(source_root.parent, root=store, catalog_path=catalog)
    assert summary["pending"] == 1
    results = import_fifteen_min_directory(source_root.parent, root=store, catalog_path=catalog)
    assert results[0]["action"] == "imported"
    assert get_fifteen_min_catalog(catalog).iloc[0]["status"] == "imported"
    assert import_fifteen_min_directory(source_root.parent, root=store, catalog_path=catalog) == []


def test_data_provider_is_public_query_api(tmp_path: Path) -> None:
    source = tmp_path / "20240102_A股15分钟K线.csv"
    store = tmp_path / "fifteen_min.duckdb"
    _write_csv(source, "20240102")
    import_fifteen_min_csv(source, root=store)
    from coreClient.data_provider import get_fifteenMin

    result = get_fifteenMin("000001.SZ", trade_date="20240102", data_root=store)
    assert len(result) == 16
    assert set(result.columns) >= {"open", "high", "low", "close", "amount", "adj_factor"}


def test_invalid_fifteen_minute_timestamp_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "20240102_A股15分钟K线.csv"
    _write_csv(source, "20240102", invalid_time=True)
    with pytest.raises(ValueError, match="15分钟时间"):
        import_fifteen_min_csv(source, root=tmp_path / "fifteen_min.duckdb")


def test_optimize_keeps_rows_and_query_contract(tmp_path: Path) -> None:
    database = tmp_path / "fifteen_min.duckdb"
    for day in ("20240102", "20240103"):
        source = tmp_path / f"{day}_A股15分钟K线.csv"
        _write_csv(source, day)
        import_fifteen_min_csv(source, root=database)
    result = optimize_fifteen_min_database(database)
    assert result["rows"] == 64
    queried = get_fifteen_min(
        "000001.SZ", start_date="20240102", end_date="20240103", root=database,
    )
    assert len(queried) == 32
    assert queried["datetime"].is_monotonic_increasing
