import json
import hashlib
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coreClient.data_provider import _one_min_quality_summary, get_oneMin_quality


def _write_partition(root: Path, day: str) -> None:
    target = root / f"year={day[:4]}" / f"month={day[4:6]}" / f"trade_date={day}"
    target.mkdir(parents=True)
    table = pa.table({
        "trade_date": [int(day)] * 3,
        "ts_code": ["000001.SZ", "000001.SZ", "600000.SH"],
        "time_idx": [0, 1, 0],
        "datetime": [pd.Timestamp("2026-05-12 09:31:00"), pd.Timestamp("2026-05-12 09:32:00"), pd.Timestamp("2026-05-12 09:31:00")],
        "vol": [0, 0, 10],
        "amount": [12.5, 0.0, 25.0],
    })
    pq.write_table(table, target / "part-000.parquet")


def test_uncovered_day_reports_unknown_not_zero(tmp_path):
    summary = _one_min_quality_summary("20260512", pd.DataFrame(), data_root=tmp_path)
    assert summary["coverage_status"] == "unknown_not_audited"
    assert summary["soft_quality_warning_rows"] is None
    assert summary["stock_level_status"] == "unknown_not_audited"


def test_day_audit_counts_are_not_reused_as_per_stock_counts(tmp_path):
    manifest_dir = tmp_path / "_manifest"
    manifest_dir.mkdir()
    (manifest_dir / "trade_date=20260512.json").write_text(json.dumps({
        "historical_quality_audit": {
            "status": "audited",
            "informational_decoder_correction_rows": 1234,
            "zero_reported_volume_positive_amount_rows_after_correction": 5,
        },
        "source_price_validation_status": "not_audited",
    }))
    summary = _one_min_quality_summary("20260512", pd.DataFrame(), data_root=tmp_path, code_scoped=True)
    assert summary["coverage_status"] == "audited_numeric_domains_scope_unavailable"
    assert summary["informational_decoder_correction_rows"] is None
    assert summary["day_level_audit"]["informational_decoder_correction_rows"] == 1234
    assert summary["source_price_validation_status"] == "not_audited"


def test_historical_zero_volume_positive_amount_is_queryable_as_derived(tmp_path):
    _write_partition(tmp_path, "20260512")
    frame = get_oneMin_quality("20260512", ts_code="000001.SZ", data_root=tmp_path)
    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["quality_flags"] == '["amount_nonzero_with_zero_volume"]'
    assert row["source"] == "historical_stored_values"
    payload = json.loads(row["source_payload_json"])
    assert payload["vol"] == 0 and payload["amount"] == 12.5
    assert payload["provenance"] == "derived_from_current_canonical_stored_values_not_original_source_payload"


def test_existing_sidecar_warning_is_not_duplicated(tmp_path):
    _write_partition(tmp_path, "20260512")
    sidecar_dir = tmp_path / "_quality" / "trade_date=20260512"
    sidecar_dir.mkdir(parents=True)
    pq.write_table(pa.table({
        "trade_date": ["2026-05-12"], "ts_code": ["000001.SZ"], "time_idx": [0],
        "datetime": [pd.Timestamp("2026-05-12 09:31:00")],
        "quality_flags": ['["amount_nonzero_with_zero_volume"]'],
        "source": ["tdx_security_bars"], "source_payload_json": ['{}'],
        "source_payload_sha256": ["abc"],
    }), sidecar_dir / "part-000.parquet")
    frame = get_oneMin_quality("20260512", ts_code="000001.SZ", data_root=tmp_path)
    assert len(frame) == 1
    assert frame.iloc[0]["source"] == "tdx_security_bars"


def test_sidecar_correction_and_derived_warning_merge_for_same_bar(tmp_path):
    _write_partition(tmp_path, "20260512")
    sidecar_dir = tmp_path / "_quality" / "trade_date=20260512"
    sidecar_dir.mkdir(parents=True)
    pq.write_table(pa.table({
        "trade_date": ["2026-05-12"], "ts_code": ["000001.SZ"], "time_idx": [0],
        "datetime": [pd.Timestamp("2026-05-12 09:31:00")],
        "quality_flags": ['["pytdx_zero_amount_corrected"]'],
        "source": ["tdx_security_bars"], "source_payload_json": ['{"amount":0}'],
        "source_payload_sha256": ["abc"],
    }), sidecar_dir / "part-000.parquet")
    frame = get_oneMin_quality("20260512", ts_code="000001.SZ", data_root=tmp_path)
    assert len(frame) == 1
    assert set(json.loads(frame.iloc[0]["quality_flags"])) == {
        "pytdx_zero_amount_corrected", "amount_nonzero_with_zero_volume",
    }
    assert frame.iloc[0]["source"] == "historical_stored_values+tdx_security_bars"
    summary = _one_min_quality_summary("20260512", frame, data_root=tmp_path, code_scoped=True)
    assert summary["coverage_status"] == "sidecar_plus_derived_stored_values"
    assert summary["informational_decoder_correction_rows"] == 1
    assert summary["soft_quality_warning_rows"] == 1
    assert summary["quality_bars_in_scope"] == 1


def test_get_ticks_legacy_is_explicit_read_only_and_preserves_raw_codes(tmp_path, monkeypatch):
    import sqlite3
    import coreClient._paths as paths
    from coreClient.data_provider import get_ticks_legacy

    offline = tmp_path / "offline" / "legacy"
    offline.mkdir(parents=True)
    db = offline / "policy_ticks_v1.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE tbl_tick (ts_code TEXT, trade_date TEXT, seqId INTEGER, datetime TEXT, time TEXT, price REAL, vol INTEGER, buyorsell INTEGER, created_at TEXT, PRIMARY KEY(ts_code,trade_date,seqId))")
        conn.execute("INSERT INTO tbl_tick VALUES ('000001.SZ','2026-05-12',7,'2026-05-12 09:31:00','09:31',10.5,100,8,NULL)")
    monkeypatch.setattr(paths, "database_root", lambda: tmp_path)
    frame = get_ticks_legacy("000001.SZ", trade_date="20260512")
    assert frame.iloc[0]["seqId"] == 7
    assert frame.iloc[0]["buyorsell"] == 8
    assert frame.attrs["timestamp_precision"] == "minute"
    assert frame.attrs["seqId_semantics"] == "not_validated_as_exchange_sequence"
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM tbl_tick").fetchone()[0] == 1


def test_get_ticks_legacy_requires_bounded_dates_and_validates_filters(tmp_path, monkeypatch):
    import coreClient._paths as paths
    from coreClient.data_provider import get_ticks_legacy

    legacy = tmp_path / "offline" / "legacy"
    legacy.mkdir(parents=True)
    (legacy / "policy_ticks_v1.db").touch()
    monkeypatch.setattr(paths, "database_root", lambda: tmp_path)
    with pytest.raises(ValueError, match="必须提供 trade_date"):
        get_ticks_legacy("000001.SZ")
    with pytest.raises(ValueError, match="互斥"):
        get_ticks_legacy("000001.SZ", ts_codes=["600000.SH"], trade_date="20260512")
    with pytest.raises(ValueError, match="有效"):
        get_ticks_legacy("000001.SZ", trade_date="20260230")
    with pytest.raises(ValueError, match="不能晚于"):
        get_ticks_legacy("000001.SZ", start_date="20260513", end_date="20260512")


def test_quality_summary_exposes_code_filtered_day_aggregate_exceptions(tmp_path):
    manifest_dir = tmp_path / "_manifest"
    manifest_dir.mkdir()
    audit_dir = tmp_path / "quality_audit" / "20260922"
    audit_dir.mkdir(parents=True)
    acceptance_path = audit_dir / "acceptance.json"
    acceptance_path.write_text(json.dumps({
        "daily_ratio_exceptions": {"amount": [
            {"ts_code": "920415.BJ", "ratio": 1438.7},
            {"ts_code": "920879.BJ", "ratio": 1519.7},
        ]},
        "daily_ohlc_field_exceptions_vs_database": {"open": [
            {"ts_code": "002883.SZ", "minute": 9.22, "daily": 9.23},
        ]},
        "category9_exception_crosschecks": [
            {"ts_code": "920415.BJ", "amount_difference_yuan": 100.0},
            {"ts_code": "920879.BJ", "amount_difference_yuan": 200.0},
        ],
    }))
    evidence_sha = hashlib.sha256(acceptance_path.read_bytes()).hexdigest()
    (manifest_dir / "trade_date=20260922.json").write_text(json.dumps({
        "quality_audit": {
            "status": "accepted_with_known_exceptions_query_follow_up_pending",
            "scope": "full day", "exception_counts": {"open_diff": 1, "daily_amount": 2},
            "relative_to_manifest_dir": {"acceptance_manifest": "../quality_audit/20260922/acceptance.json"},
            "sha256": {"acceptance_manifest": evidence_sha},
        },
    }))
    summary = _one_min_quality_summary("20260922", pd.DataFrame(), data_root=tmp_path, code_scoped=True, ts_code="920415.BJ")
    exceptions = summary["source_audit_exceptions"]
    assert exceptions["status"] == "accepted_with_known_exceptions_query_follow_up_pending"
    assert exceptions["evidence_integrity_status"] == "verified"
    assert exceptions["evidence_sha256"] == evidence_sha
    assert exceptions["details"]["daily_ratio_exceptions"]["amount"] == [{"ts_code": "920415.BJ", "ratio": 1438.7}]
    assert len(exceptions["details"]["category9_exception_crosschecks"]) == 1


def test_unique_quality_rows_skip_groupby_merge(monkeypatch):
    from coreClient.data_provider import _merge_one_min_quality_rows

    count = 51_022
    frame = pd.DataFrame({
        "trade_date": ["2026-09-22"] * count,
        "ts_code": [f"{index:06d}.SZ" for index in range(count)],
        "time_idx": [0] * count,
        "quality_flags": ['["pytdx_zero_amount_corrected"]'] * count,
        "source": ["tdx_security_bars"] * count,
    })
    monkeypatch.setattr(pd.DataFrame, "groupby", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unique rows should bypass groupby")))
    result = _merge_one_min_quality_rows(frame)
    assert len(result) == count
