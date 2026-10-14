"""
test_checkredis_allkeys: 所有 online: key 元信息列表

AI 调用点:
    from test.test_checkredis_allkeys import (
        check_all_keys,             # 检查(导入名 = 原工具类函数)
    )
"""

import sys
from pathlib import Path

_TEST_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _TEST_DIR.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


from core.check_redis import check_all_keys

__all__ = ['check_all_keys']

if __name__ == "__main__":
    print("=== ALL KEYS ===")
    info = check_all_keys()
    import json as _json
    print(_json.dumps(info, ensure_ascii=False, indent=2, default=str))
