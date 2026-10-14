from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SCRIPTS_DIR.parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.offline_db_client import (
    FifteenMinuteWriter,
    get_fifteen_min,
    get_fifteen_min_stats,
    get_fifteen_min_status,
    init_fifteen_min_db,
    mark_fifteen_min_status,
    repair_fifteen_min_adj_factors,
    upsert_fifteen_min_rows,
)


TIMES = (
    "09:45", "10:00", "10:15", "10:30", "10:45", "11:00", "11:15", "11:30",
    "13:15", "13:30", "13:45", "14:00", "14:15", "14:30", "14:45", "15:00",
)


def _rows(code: str, day: str, offset: float = 0.0) -> list[dict]:
    dashed = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    result = []
    for index, stamp in enumerate(TIMES):
        price = 10 + offset + index / 100
        result.append({
            "ts_code": code,
            "trade_date": dashed,
            "datetime": f"{dashed} {stamp}:00",
            "time_idx": index,
            "open": price,
            "high": price + 0.2,
            "low": price - 0.1,
            "close": price + 0.1,
            "vol": 1000 + index,
            "amount": 10000 + index,
        })
    return result


def test_tdx_fifteen_min_write_query_replace_and_stats(tmp_path: Path) -> None:
    database = tmp_path / "db_fifteenMinute.db"
    assert init_fifteen_min_db(database) == database
    result = upsert_fifteen_min_rows(
        _rows("000001.SZ", "20260923") + _rows("000001.SZ", "20260924"),
        requested_start="20260901",
        requested_end="20260924",
        name="平安银行",
        adj_factors={"20260923": 123.0, "20260924": 124.0},
        source_host="example:7709",
        data_root=database,
    )
    assert result["rows"] == 32
    frame = get_fifteen_min(
        "000001.SZ", start_date="20260923", end_date="20260924",
        data_root=database,
    )
    assert len(frame) == 32
    assert frame["time_idx"].tolist() == list(range(16)) * 2
    assert frame["adj_factor"].iloc[-1] == 124.0

    upsert_fifteen_min_rows(
        _rows("000001.SZ", "20260924", offset=2.0),
        requested_start="20260924", requested_end="20260924",
        name="平安银行", data_root=database,
    )
    replaced = get_fifteen_min(
        "000001.SZ", trade_date="20260924", data_root=database,
    )
    assert len(replaced) == 16
    assert replaced["open"].iloc[0] == 12.0
    sync = get_fifteen_min_status(database)
    assert int(sync.iloc[0]["covered_start"]) == 20260901
    assert int(sync.iloc[0]["covered_end"]) == 20260924
    stats = get_fifteen_min_stats(database)
    assert stats["rows"] == 32
    assert stats["symbols"] == 1
    assert stats["trade_dates"] == 2


def test_data_provider_reads_new_tdx_database(tmp_path: Path) -> None:
    database = tmp_path / "db_fifteenMinute.db"
    upsert_fifteen_min_rows(
        _rows("600000.SH", "20260924"),
        requested_start="20260924", requested_end="20260924",
        data_root=database,
    )
    from coreClient.data_provider import get_fifteenMin
    frame = get_fifteenMin("600000.SH", trade_date="20260924", data_root=database)
    assert len(frame) == 16
    assert frame["datetime"].dt.strftime("%H:%M").tolist() == list(TIMES)


def test_sync_end_never_advances_past_last_returned_bar(tmp_path: Path) -> None:
    database = tmp_path / "db_fifteenMinute.db"
    upsert_fifteen_min_rows(
        _rows("000001.SZ", "20260924"),
        requested_start="20240901", requested_end="20260925",
        data_root=database,
    )
    sync = get_fifteen_min_status(database)
    assert int(sync.iloc[0]["covered_start"]) == 20240901
    assert int(sync.iloc[0]["covered_end"]) == 20260924
    assert int(sync.iloc[0]["actual_end"]) == 20260924


def test_batch_writer_commits_multiple_symbols_together(tmp_path: Path) -> None:
    database = tmp_path / "db_fifteenMinute.db"
    with FifteenMinuteWriter(database) as writer:
        result = writer.upsert_batch([
            {
                "rows": _rows("000001.SZ", "20260924"),
                "requested_start": "20260924", "requested_end": "20260924",
                "name": "平安银行", "adj_factors": {"20260924": 1.2},
                "source_host": "one:7709",
            },
            {
                "rows": _rows("600000.SH", "20260924", offset=1.0),
                "requested_start": "20260924", "requested_end": "20260924",
                "name": "浦发银行", "adj_factors": {"20260924": 2.3},
                "source_host": "two:7709",
            },
        ], append_only=True)
    assert result["rows"] == 32
    assert result["symbols"] == 2
    frame = get_fifteen_min(
        ts_codes=["000001.SZ", "600000.SH"], trade_date="20260924",
        data_root=database,
    )
    assert len(frame) == 32
    assert frame.groupby("ts_code").size().to_dict() == {
        "000001.SZ": 16, "600000.SH": 16,
    }


def test_tdx_empty_does_not_advance_coverage(tmp_path: Path) -> None:
    database = tmp_path / "db_fifteenMinute.db"
    mark_fifteen_min_status(
        "000016.SZ", "20240901", "20260924",
        status="tdx_empty", error="TDX返回空", data_root=database,
    )
    sync = get_fifteen_min_status(database).iloc[0]
    assert sync["status"] == "tdx_empty"
    assert pd.isna(sync["covered_start"])
    assert pd.isna(sync["covered_end"])


def test_factor_repair_null_stale_missing_invalid_and_idempotent(tmp_path: Path) -> None:
    database = tmp_path / "fifteen.db"
    upsert_fifteen_min_rows(
        _rows("000001.SZ", "20260923") + _rows("000001.SZ", "20260924"),
        requested_start="20260923", requested_end="20260924",
        adj_factors={"20260924": 2.0}, data_root=database,
    )
    for code in ["600000.SH", "600001.SH", "600002.SH"]:
        upsert_fifteen_min_rows(
            _rows(code, "20260924"), requested_start="20260924", requested_end="20260924",
            adj_factors={"20260924": 3.0} if code == "600001.SH" else None,
            data_root=database,
        )
    codes = ["000001.SZ", "600000.SH", "600001.SH", "600002.SH"]
    before = get_fifteen_min(ts_codes=codes, start_date="20260923", end_date="20260924", data_root=database)
    factors = pd.DataFrame([
        {"ts_code": "000001.SZ", "trade_date": "20260923", "adj_factor": 1.1},
        {"ts_code": "000001.SZ", "trade_date": "20260924", "adj_factor": 2.1},
        {"ts_code": "000001.SZ", "trade_date": "20260924", "adj_factor": 2.1},
        {"ts_code": "000001.SZ", "trade_date": "20260925", "adj_factor": 9.9},
        {"ts_code": "600001.SH", "trade_date": "20260924", "adj_factor": 0.0},
        {"ts_code": "600002.SH", "trade_date": "20260924", "adj_factor": float("inf")},
    ])
    # Public write interface reuses a live writer connection without lock/config conflict.
    with FifteenMinuteWriter(database) as writer:
        result = repair_fifteen_min_adj_factors(
            factors, data_root=database, connection=writer.connection,
        )
        assert result["updated_rows"] == 32
        assert result["valid_factor_keys"] == 3
        assert result["ignored_invalid_factor_rows"] == 2
        assert writer.repair_adj_factors(factors)["updated_rows"] == 0
        assert writer.connection.execute("SELECT count(*) FROM fifteen_min_bars").fetchone()[0] == 80
    after = get_fifteen_min(ts_codes=codes, start_date="20260923", end_date="20260924", data_root=database)
    pd.testing.assert_frame_equal(before.drop(columns="adj_factor"), after.drop(columns="adj_factor"))
    a = after[after.ts_code == "000001.SZ"]
    assert a.groupby("trade_date").adj_factor.first().to_dict() == {20260923: 1.1, 20260924: 2.1}
    assert after[after.ts_code == "600000.SH"].adj_factor.isna().all()  # Missing source stays unknown.
    assert (after[after.ts_code == "600001.SH"].adj_factor == 3.0).all()  # Invalid source cannot replace old.
    assert after[after.ts_code == "600002.SH"].adj_factor.isna().all()  # Unknown never becomes 1.


def test_factor_repair_rejects_conflicting_same_day_factors(tmp_path: Path) -> None:
    database = tmp_path / "fifteen.db"
    upsert_fifteen_min_rows(
        _rows("000001.SZ", "20260924"), requested_start="20260924", requested_end="20260924",
        data_root=database,
    )
    with pytest.raises(ValueError, match="冲突因子"):
        repair_fifteen_min_adj_factors(pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": ["20260924", "20260924"], "adj_factor": [2.0, 3.0],
        }), data_root=database)
    assert get_fifteen_min("000001.SZ", trade_date="20260924", data_root=database).adj_factor.isna().all()


def test_service_repairs_factors_when_all_download_jobs_are_skipped(tmp_path: Path, monkeypatch) -> None:
    import sqlite3
    from service import service_fifteenMinute as service

    database = tmp_path / "fifteen.db"
    upsert_fifteen_min_rows(
        _rows("000001.SZ", "20260924"), requested_start="20260924", requested_end="20260924",
        data_root=database,
    )
    monkeypatch.setattr(service, "OFFLINE_ROOT", tmp_path)
    (tmp_path / "logs").mkdir()
    monkeypatch.setattr(service, "_load_universe", lambda *_a: pd.DataFrame({
        "ts_code": ["000001.SZ"], "name": ["平安银行"], "list_date": ["19910403"],
    }))
    monkeypatch.setattr(service, "TdxClient", lambda: pytest.fail("Covered stocks must not download again"))
    monkeypatch.setattr(service, "get_conn", lambda *_a: sqlite3.connect(":memory:"))
    calls = []
    def source_factors(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260924"], "adj_factor": [4.2]})
    monkeypatch.setattr(service, "get_adj_factor", source_factors)
    assert service.main(["--database", str(database), "--trade-date", "20260924"]) == 0
    assert len(calls) == 1 and calls[0]["start_date"] == calls[0]["end_date"] == "20260924"
    assert "conn" in calls[0]
    assert (get_fifteen_min("000001.SZ", trade_date="20260924", data_root=database).adj_factor == 4.2).all()


def test_service_factor_repair_reads_only_existing_dates_in_bounded_batches(tmp_path: Path, monkeypatch) -> None:
    import sqlite3
    from service import service_fifteenMinute as service

    database = tmp_path / "fifteen.db"
    upsert_fifteen_min_rows(
        _rows("000001.SZ", "20260923") + _rows("000001.SZ", "20260924"),
        requested_start="20260923", requested_end="20260924", data_root=database,
    )
    monkeypatch.setattr(service, "get_conn", lambda *_a: sqlite3.connect(":memory:"))
    calls = []
    def source_factors(**kwargs):
        calls.append((kwargs["start_date"], kwargs["end_date"]))
        return pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "trade_date": ["20260922", "20260923", "20260924"], "adj_factor": [9.0, 1.0, 2.0],
        })
    monkeypatch.setattr(service, "get_adj_factor", source_factors)
    with FifteenMinuteWriter(database) as writer:
        result = service._repair_local_adj_factors(writer, "20240901", "20261008", batch_days=1)
    assert calls == [("20260923", "20260923"), ("20260924", "20260924")]
    assert result["dates_checked"] == result["batches"] == result["source_rows"] == 2
    assert result["updated_rows"] == 32 and result["remaining_null_rows"] == 0
