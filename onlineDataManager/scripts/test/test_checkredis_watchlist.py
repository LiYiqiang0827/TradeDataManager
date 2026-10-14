"""
test_checkredis_watchlist: watchlist 检查 + 拉数据

AI 调用点:
    from test.test_checkredis_watchlist import (
        check_watchlist,                  # 检查 watchlist 状态(SET + HASH)
        fetch_watchlist,                  # 拉 watchlist ts_code 列表
        fetch_watchlist_sources,          # 拉 sources 映射(ts_code -> "yest" / "prev_lianban" / "yest+prev_lianban")
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_watchlist
from core.query_redis import fetch_watchlist, fetch_watchlist_sources

__all__ = ['check_watchlist', 'fetch_watchlist', 'fetch_watchlist_sources']


if __name__ == "__main__":
    print("=== WATCHLIST 状态 ===")
    info = check_watchlist()
    print("  总数    : " + str(info.get('count', 0)))
    print("  sources : " + str(info.get('sources_count', 0)))
    print("  yest(含交集) : " + str(info.get('yest_count', 0)))
    print("  prev_lb(含) : " + str(info.get('prev_lb_count', 0)))
    flag = "✅" if info.get('consistent') else "⚠️ SET/HASH 不一致"
    print("  一致性  : " + flag)
    print()
    print("=== fetch_watchlist ===")
    codes = fetch_watchlist()
    print("  共 " + str(len(codes)) + " 只股票")
    print()
    print("=== fetch_watchlist_sources ===")
    srcs = fetch_watchlist_sources()
    print("  共 " + str(len(srcs)) + " 个 source 标记")
    if srcs:
        first = next(iter(srcs))
        print("  示例: " + first + " -> " + srcs[first])
