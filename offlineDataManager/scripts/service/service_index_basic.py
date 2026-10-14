"""
service_index_basic.py - 指数基本信息更新

数据源:tushare pro.index_basic
文档:https://tushare.pro/document/2?doc_id=94

依赖:settings.py / offline_downloader.py / offline_db_client.py
"""

import sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from config.settings import LOG_FORMAT, LOG_LEVEL, LOG_DIR, PROJECT_ROOT
from core.offline_downloader import CNDataDown


# ====== 业务常量 ======
CTRL_KEY = "cn_index_basic"  # tbl_basic_ctrl key
SERVICE_NAME = "service_index_basic"


def init_logger(name: str):
    """初始化 loguru logger(简化版,每 service 一份 log 文件)"""
    log_path = LOG_DIR / f"{name}.log"
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(str(log_path), format=LOG_FORMAT, level=LOG_LEVEL, rotation="20 MB", encoding="utf-8")
    logger.add(lambda msg: print(msg, end=""), format=LOG_FORMAT, level=LOG_LEVEL)


def main():
    init_logger(SERVICE_NAME)
    from core.offline_db_client import init_db
    init_db("index")  # 确保 index DB 存在并建表

    down = CNDataDown()
    t0 = time.time()

    # 指数基本信息是静态的,每次都全量覆盖(~30 秒)
    n = down.update_index_basic()
    elapsed = time.time() - t0
    logger.info(f"[{SERVICE_NAME}] 覆盖更新 +{n:,} 行, 耗时 {elapsed:.1f}s")


if __name__ == "__main__":
    main()