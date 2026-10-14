"""
test_checkredis_cursor: STREAM 落盘游标(Redis 视角,get_last_stream_id)

AI 调用点:
    from test.test_checkredis_cursor import (
        check_cursor,                  # 检查单个 stream_key 的 last_id
    )

    用法:
        check_cursor("online:zt:stream")
        check_cursor("online:anomaly:stream")
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_cursor

__all__ = ['check_cursor']


if __name__ == "__main__":
    print("=== CURSOR for online:zt:stream ===")
    print("  " + str(check_cursor("online:zt:stream")))
    print()
    print("=== CURSOR for online:anomaly:stream ===")
    print("  " + str(check_cursor("online:anomaly:stream")))
