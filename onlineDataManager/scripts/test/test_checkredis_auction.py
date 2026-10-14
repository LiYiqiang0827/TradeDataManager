"""
test_checkredis_auction: 集合竞价(2026-09-14 v6.2 加全市场快照索引)

AI 调用点:
    from test.test_checkredis_auction import (
        check_auction,                              # 检查(全局)
        check_auction_for_stock,                    # 检查(单股 window)
        fetch_auction_window,                       # 拉数据(单股 window ZSET)
        fetch_auction_history,                      # 拉数据(STREAM 落盘)
        fetch_auction_timeline,                     # v6.2 全市场快照索引
        fetch_auction_archive_latest,               # v6.2 最新一份全市场快照
        fetch_auction_snapshot_at,                  # v6.2 N 秒前最近一份快照
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_auction
from core.check_redis import check_auction_for_stock
from core.query_redis import fetch_auction_window
from core.query_redis import fetch_auction_history
# 2026-09-14 v6.2 全市场快照索引
from core.query_redis import fetch_auction_timeline
from core.query_redis import fetch_auction_archive_latest
from core.query_redis import fetch_auction_snapshot_at

__all__ = [
    'check_auction',
    'check_auction_for_stock',
    'fetch_auction_window',
    'fetch_auction_history',
    'fetch_auction_timeline',
    'fetch_auction_archive_latest',
    'fetch_auction_snapshot_at',
]

if __name__ == "__main__":
    print("=== AUCTION ===")
    check_auction()
    check_auction_for_stock("600519.SH")
    rows = fetch_auction_window("600519.SH")
    print("  600519.SH window: " + str(len(rows)) + " bars")
    rows = fetch_auction_history(3)
    print("  latest 3: " + str(len(rows)) + " 条")
