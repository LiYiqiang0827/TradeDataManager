"""
service/service_savedata_anomaly.py
=====================================

savedata daemon — 异动池,30 分钟一次。
"""

from __future__ import annotations

import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SERVICE_DIR.parent))

from service.savedata_loop import SavedataDaemon, main  # noqa: E402


class SavedataAnomaly(SavedataDaemon):
    KIND = "anomaly"
    DEFAULT_INTERVAL = 1800.0   # 30 分钟


if __name__ == "__main__":
    sys.exit(main(SavedataAnomaly))
