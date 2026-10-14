"""
service/savedata_loop.py
=========================

savedata daemon 通用循环模板(2026-09-16 v6.15 重构后)。

设计:
- 9 个 savedata daemon(每个 kind 一个,v6.15 加 snapshot_index + 删 savedata_orderbook)都继承这个基类
- scheduler 在 morning_savedata / afternoon_savedata 阶段统一 spawn
- 15:16 阶段切换到 post_savedata 时由 scheduler kill,service 自己不判断时间
- 单职责:每个 service 只落一种 kind 到 SQLite

用法(子类示例):
    from service.savedata_loop import SavedataDaemon

    class SavedataSnapshot(SavedataDaemon):
        KIND = "snapshot"
        DEFAULT_INTERVAL = 900.0   # 15 分钟(v6.15 统一为 15 分钟)

        def main(self) -> int:
            return self.run()  # while True loop
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from core.logger import setup_logger              # noqa: E402
from core.persist_client import persist_kind      # noqa: E402
from core.redis_online import OnlineRedis         # noqa: E402


class SavedataDaemon:
    """savedata daemon 基类(每个 kind 一个)

    子类必须定义:
        KIND            : str            # 比如 "snapshot"
        DEFAULT_INTERVAL: float          # 默认落盘间隔秒

    子类可选覆盖:
        TRADE_DATE      : str | None     # None=今天,或固定 YYYYMMDD
    """

    KIND: str = ""
    DEFAULT_INTERVAL: float = 900.0
    TRADE_DATE: str | None = None

    def __init__(self, log: logging.Logger | None = None):
        if not self.KIND:
            raise ValueError("子类必须定义 KIND")
        self.log = log or setup_logger(f"service_savedata_{self.KIND}")
        self.r = OnlineRedis()

    def _get_trade_date(self) -> str:
        from datetime import datetime
        if self.TRADE_DATE:
            return self.TRADE_DATE
        return datetime.now().strftime("%Y%m%d")

    def _do_persist(self) -> int:
        """落一次盘,返回写入条数

        2026-09-15 v6.8:透传 self._drain 标志给 persist_kind。
        drain=True 时连续读完所有积压(治本模式)。
        """
        td = self._get_trade_date()
        drain = getattr(self, "_drain", False)
        return persist_kind(self.r, kind=self.KIND, trade_date=td, drain=drain)

    def run(self) -> int:
        """while True 循环(daemon 模式)— scheduler kill 才退出"""
        self.log.info(f"savedata[{self.KIND}] daemon 启动")
        # 第一轮立即跑(不等 interval),避免刚启动就空转 N 秒
        try:
            n = self._do_persist()
            self.log.info(f"savedata[{self.KIND}] 初始落盘 n={n}")
        except Exception as e:
            self.log.error(f"savedata[{self.KIND}] 初始落盘失败: {e}", exc_info=True)

        while True:
            time.sleep(self.DEFAULT_INTERVAL)
            try:
                n = self._do_persist()
                self.log.info(f"savedata[{self.KIND}] 落盘 n={n}")
            except Exception as e:
                self.log.error(f"savedata[{self.KIND}] 落盘失败: {e}", exc_info=True)

    def run_once(self) -> int:
        """单次落盘(--once 模式)"""
        try:
            n = self._do_persist()
            self.log.info(f"savedata[{self.KIND}] --once 落盘 n={n}")
            return 0
        except Exception as e:
            self.log.error(f"savedata[{self.KIND}] --once 失败: {e}", exc_info=True)
            return 1


def main(cls: type[SavedataDaemon], extra_args: list = None) -> int:
    """通用 main 入口:解析 --once/--interval/--drain 后实例化子类运行

    2026-09-15 v6.8 加 --drain:
      - drain=True 时,每次 _do_persist 会连续读完所有积压(直到 batch 不满)
      - 治本:即使 savedata 间隔长(默认 15 分钟),STREAM 也不积压
      - 非 drain(默认 False):每轮只落 1 batch(向后兼容)
    """
    import argparse
    parser = argparse.ArgumentParser(description=f"Service: savedata[{cls.KIND}]")
    parser.add_argument("--interval", type=float, default=cls.DEFAULT_INTERVAL,
                        help=f"落盘间隔秒数(默认 {cls.DEFAULT_INTERVAL}s)")
    parser.add_argument("--once", action="store_true", help="只跑一次")
    parser.add_argument("--drain", action="store_true", default=None,
                        help="每次落盘时连续读完所有积压(治本模式,2026-09-15 v6.8;子类可设 DEFAULT_DRAIN=True)")
    args = parser.parse_args(extra_args)

    daemon = cls()
    daemon.DEFAULT_INTERVAL = args.interval
    # ★ v6.8:drain 默认值优先用子类的 DEFAULT_DRAIN,否则默认 False
    daemon._drain = args.drain if args.drain is not None else getattr(cls, "DEFAULT_DRAIN", False)

    if args.once:
        return daemon.run_once()
    return daemon.run()
