"""
test_checkredis_break: 炸板池

AI 调用点:
    from test.test_checkredis_break import (
        check_break,                      # 检查
        fetch_break_archive_latest,       # 拉最新全量快照(替代 fetch_break_pool)
        fetch_break_stream,               # 拉 STREAM
        fetch_break_timeline,             # 拉 timeline
        fetch_break_snapshot_at,          # 拉 N 秒前快照
    )

【2026-09-14 v6】MyATM 风格 — 删除 fetch_break_pool,改 4 个新接口
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_break
from core.query_redis import (
    fetch_break_archive_latest,
    fetch_break_stream,
    fetch_break_timeline,
    fetch_break_snapshot_at,
)

__all__ = [
    "check_break",
    "fetch_break_archive_latest",
    "fetch_break_stream",
    "fetch_break_timeline",
    "fetch_break_snapshot_at",
]

if __name__ == "__main__":
    print("=== BREAK (v6 MyATM) ===")
    info = check_break()
    print(f"  check_break: {info}")
    snap = fetch_break_archive_latest()
    print(f"  archive_latest: {len(snap)} 只")
    timeline = fetch_break_timeline(count=5)
    print(f"  timeline: {len(timeline)} 个 archive entry")
    rows = fetch_break_stream(3)
    print(f"  stream latest 3: {len(rows)} 条")
    snap_at = fetch_break_snapshot_at(seconds_before=120)
    print(f"  snapshot_at 120s ago: {len(snap_at)} 只")
