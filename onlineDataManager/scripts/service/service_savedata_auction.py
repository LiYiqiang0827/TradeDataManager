"""
service/service_savedata_auction.py
=====================================

savedata 一次性服务 — 把 Redis online:auction:stream 落盘到 SQLite。

2026-09-11 重构后:
- 只跑 --once(没有 while True loop)
- scheduler 每天 09:29 + 15:10 各触发一次
- 为什么不在 morning 阶段跑 daemon?因为 auction 数据只有 9:15-9:30 才有,15:00 后不会有新数据
  两次 --once 比一个 6h daemon 更准确(且减少无效循环)

2026-09-14 v6.2:落盘走 online:auction:stream(STREAM),与 v6.1 一致。
  timeline + archive 是查询接口,不影响落盘路径。
"""

from __future__ import annotations

import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SERVICE_DIR.parent))

from service.savedata_loop import SavedataDaemon, main  # noqa: E402


class SavedataAuction(SavedataDaemon):
    KIND = "auction"
    DEFAULT_INTERVAL = 99999.0   # 几乎用不到(因为是 --once 模式)


if __name__ == "__main__":
    # 强制只跑一次(auction 只在 --once 模式有意义)
    sys.argv = [sys.argv[0], "--once"] + [a for a in sys.argv[1:] if a != "--once"]
    sys.exit(main(SavedataAuction))
