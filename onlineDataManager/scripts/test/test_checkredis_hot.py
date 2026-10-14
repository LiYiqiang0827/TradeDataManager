"""
test_checkredis_hot: 热股榜

AI 调用点:
    from test.test_checkredis_hot import (
        check_hot,                        # 检查
        fetch_hot_archive_latest,         # 拉最新全量快照(替代 fetch_hot_rank)
        fetch_hot_stream,                 # 拉 STREAM
        fetch_hot_timeline,               # 拉 timeline
        fetch_hot_snapshot_at,            # 拉 N 秒前快照
    )

【2026-09-14 v6】MyATM 风格 — 删除 fetch_hot_rank,改 4 个新接口
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_hot
from core.query_redis import (
    fetch_hot_archive_latest,
    fetch_hot_stream,
    fetch_hot_timeline,
    fetch_hot_snapshot_at,
)

__all__ = [
    "check_hot",
    "fetch_hot_archive_latest",
    "fetch_hot_stream",
    "fetch_hot_timeline",
    "fetch_hot_snapshot_at",
]

if __name__ == "__main__":
    print("=== HOT (v6 MyATM) ===")
    info = check_hot()
    print(f"  check_hot: {info}")
    snap = fetch_hot_archive_latest()
    print(f"  archive_latest: {len(snap)} 只")
    timeline = fetch_hot_timeline(count=5)
    print(f"  timeline: {len(timeline)} 个 archive entry")
    rows = fetch_hot_stream(3)
    print(f"  stream latest 3: {len(rows)} 条")
    snap_at = fetch_hot_snapshot_at(seconds_before=120)
    print(f"  snapshot_at 120s ago: {len(snap_at)} 只")
