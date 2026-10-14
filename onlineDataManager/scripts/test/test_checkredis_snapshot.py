"""
test_checkredis_snapshot: 连续竞价快照检查 + 拉数据(2026-09-14 v6.3 加全市场快照索引)

AI 调用点:
    from test.test_checkredis_snapshot import (
        check_snapshot,                       # 全局: STREAM + 所有 window + v6.3 timeline/archive
        check_snapshot_for_stock,             # 单股: window 状态
        fetch_snapshot_window,                # 拉某股 ZSET window(单股粒度)
        fetch_snapshot_history,               # 拉 STREAM 最近 N 条
        fetch_snapshot_timeline,              # v6.3 全市场快照索引
        fetch_snapshot_archive_latest,        # v6.3 最新一份全市场快照
        fetch_snapshot_snapshot_at,           # v6.3 N 秒前最近一份
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_snapshot, check_snapshot_for_stock
from core.query_redis import (
    fetch_snapshot_window, fetch_snapshot_history,
    fetch_snapshot_timeline, fetch_snapshot_archive_latest, fetch_snapshot_snapshot_at,
)

__all__ = [
    'check_snapshot', 'check_snapshot_for_stock',
    'fetch_snapshot_window', 'fetch_snapshot_history',
    'fetch_snapshot_timeline', 'fetch_snapshot_archive_latest', 'fetch_snapshot_snapshot_at',
]


if __name__ == "__main__":
    print("=== SNAPSHOT 全局 ===")
    info = check_snapshot()
    print("  STREAM 长度    : " + str(info.get('stream_length', 0)))
    print("  STREAM 首 ID   : " + str(info.get('stream_first_id', '')))
    print("  STREAM 末 ID   : " + str(info.get('stream_last_id', '')))
    print("  window key 数 : " + str(info.get('window_key_count', 0)))
    print("  window 总元素 : " + str(info.get('window_total_size', 0)))
    print("  [v6.3] timeline_size : " + str(info.get('timeline_size', 0)))
    print("  [v6.3] timeline_ttl  : " + str(info.get('timeline_ttl', -2)))
    print("  [v6.3] latest archive: " + str(info.get('latest_archive_key', '')))
    print("  [v6.3] archive size  : " + str(info.get('latest_archive_size', 0)))
    print("  [v6.3] archive ttl   : " + str(info.get('latest_archive_ttl', -2)))
    if info.get('window_key_count', 0) == 0 and info.get('stream_length', 0) > 0:
        print("  ⚠️ STREAM 有数据但 window 已清空(可能收盘后清理)")
    print()
    print("=== SNAPSHOT for 600519.SH ===")
    info = check_snapshot_for_stock("600519.SH")
    print("  exists=" + str(info.get('exists')) + " size=" + str(info.get('size')) + " ttl=" + str(info.get('ttl')))
    print()
    print("=== fetch_snapshot_window ===")
    rows = fetch_snapshot_window("600519.SH")
    print("  600519.SH window: " + str(len(rows)) + " bars")
    print()
    print("=== fetch_snapshot_history ===")
    rows = fetch_snapshot_history(3)
    print("  STREAM 最新 " + str(len(rows)) + " 条")
