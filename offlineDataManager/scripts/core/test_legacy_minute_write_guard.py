import pytest

from core.offline_downloader import CNDataDown


def test_stock_legacy_minute_write_fails_before_read_or_network():
    down = object.__new__(CNDataDown)
    with pytest.raises(RuntimeError, match="volume units are not compatible"):
        down.update_minute("000001.SZ", "2026-05-12", client=None)


def test_index_legacy_minute_write_fails_before_read_or_network():
    down = object.__new__(CNDataDown)
    with pytest.raises(RuntimeError, match="index-specific native one-minute source"):
        down.update_minute_index("000001.SH", "2026-05-12", client=None)
