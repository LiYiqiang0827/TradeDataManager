"""
service/service_savedata_watchlist.py
=====================================

watchlist 落盘 daemon(v6.7 2026-09-15:从"覆盖生成 watchlist 重写 SQLite"
                     → "读 STREAM 增量落 SQLite",对齐其他 8 个 kind):

【数据流】
    service_writeredis_watchlist (pre_auction 一次) → Redis online:watchlist:{stream,timeline,archive}
                                                  ↓
    service_savedata_watchlist (每 30 分钟) → 读 STREAM > 游标 → insert_snapshots_batch
                                          → watchlist_<YYYYMMDD>(增量,UNIQUE(ts_code+watchlist_timestamp))

【职责】只落盘,【不写 Redis】(写 Redis 由 service_writeredis_watchlist 负责,只在 pre_auction 一次)

【周期】30 分钟一次(默认)

【v6.7 改造】(2026-09-15):
  - 旧版:子类 override _do_persist,重新调 generate_watchlist + replace_watchlist(覆盖式)
  - 新版:走基类 SavedataDaemon + _do_persist 调 persist_kind(kind="watchlist") → persist_watchlist
    读 STREAM 增量落盘,数据源全部来自 writeredis 已写的 STREAM,不再重新调 generate_watchlist
  - 表 schema 改为 UNIQUE(ts_code, watchlist_timestamp),增量 INSERT
  - 与其他 8 kind 同一模板(replace_watchlist 函数保留但不在 savedata 用,只在 test 里兜底)

启动:
    python3 -m service.service_savedata_watchlist
    python3 -m service.service_savedata_watchlist --interval 1800
    python3 -m service.service_savedata_watchlist --once   # 测试用
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from core.logger import setup_logger                                              # noqa: E402
from core.redis_online import OnlineRedis                                         # noqa: E402
from core.sqlite_client import connect                                            # noqa: E402
from core.persist_client import persist_kind                                       # noqa: E402
from service.savedata_loop import SavedataDaemon                                  # noqa: E402


class SavedataWatchlist(SavedataDaemon):
    KIND = "watchlist"
    DEFAULT_INTERVAL = 1800.0   # 30 分钟

    def __init__(self, log: logging.Logger | None = None):
        super().__init__(log=log)
        self.log = log or setup_logger("service_savedata_watchlist")
        self.round_idx = 0

    def _do_persist(self) -> int:
        """v6.7:走 persist_kind(kind="watchlist") → persist_watchlist 读 STREAM 增量落盘

        与其他 8 kind 同一模板:
            1. 读 online_stream_cursor.watchlist 的 last_id
            2. XREAD online:watchlist:stream > last_id 拿新消息
            3. 解析 STREAM fields → record dict
            4. insert_snapshots_batch(kind="watchlist", ...) 增量落盘
            5. update_cursor 推进

        Returns:
            写入条数(去重后)
        """
        td = self._get_trade_date()
        try:
            n = persist_kind(self.r, kind="watchlist", trade_date=td)
            if n > 0:
                self.round_idx += 1
                self.log.info(
                    f"watchlist 落盘(v6.7 STREAM 增量): trade_date={td} inserted={n} round={self.round_idx}"
                )
                self.r.set_meta("watchlist_last_persist_ts", datetime.now().isoformat(timespec="milliseconds"))
                self.r.set_meta("watchlist_persist_round", self.round_idx)
            return n
        except Exception as e:
            self.log.error(f"watchlist 落盘失败: {e}", exc_info=True)
            self.r.set_meta("watchlist_savedata_last_error", str(e))
            return 0


if __name__ == "__main__":
    from service.savedata_loop import main
    sys.exit(main(SavedataWatchlist))