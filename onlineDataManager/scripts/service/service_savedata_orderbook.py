"""
service/service_savedata_orderbook.py
======================================

savedata daemon — 把 Redis online:orderbook:* stream 落盘到 SQLite。
"""

from __future__ import annotations

import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SERVICE_DIR.parent))

from service.savedata_loop import SavedataDaemon, main  # noqa: E402


class SavedataOrderbook(SavedataDaemon):
    KIND = "orderbook"
    DEFAULT_INTERVAL = 900.0   # 15 分钟


if __name__ == "__main__":
    sys.exit(main(SavedataOrderbook))
