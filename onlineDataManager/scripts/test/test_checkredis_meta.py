"""
test_checkredis_meta: meta HASH

AI 调用点:
    from test.test_checkredis_meta import (
        check_meta,             # 检查(导入名 = 原工具类函数)
        fetch_meta,             # 拉数据(导入名 = 原工具类函数)
        fetch_meta_key,             # 拉数据(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_meta
from core.query_redis import fetch_meta
from core.query_redis import fetch_meta_key

__all__ = ['check_meta', 'fetch_meta', 'fetch_meta_key']

if __name__ == "__main__":
    print("=== META ===")
    check_meta()
    meta = fetch_meta()
    print("  meta 共 " + str(len(meta)) + " 个 field")
    v = fetch_meta_key("watchlist_last_count")
    print("  watchlist_last_count = " + str(v))
