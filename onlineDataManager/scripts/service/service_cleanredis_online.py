"""
service/service_cleanredis_online.py
=====================================

每日 Redis 清理服务(2026-09-11 重构后):

- 只清理 online: 前缀的 key(其他 namespace 不动)
- 清空 SQLite 的 online_stream_cursor 表(STREAM ID 在 Redis flush 后失效,从 0 重读)
- 不清空 9 个 kind 的历史表(那是数据,要保留)

scheduler 每天 09:00 + 03:00 各触发一次(--once 模式)
2026-09-14:调试期间,凌晨 3 点清残留(原 16:00,2026-09-14 v3 调试期间改的)

启动方式:
    python3 -m service.service_cleanredis_online --once
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SERVICE_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from core.logger import setup_logger                  # noqa: E402
from core.redis_online import OnlineRedis             # noqa: E402
from core.sqlite_client import connect, current_year_month, db_path_for_month, DATA_DIR  # noqa: E402


def clean_redis_online(r: OnlineRedis, log: logging.Logger) -> int:
    """清理 online: 前缀的所有 Redis key"""
    deleted = 0
    cursor = 0
    pattern = "online:*"
    while True:
        cursor, keys = r.r.scan(cursor=cursor, match=pattern, count=500)
        if keys:
            n = r.r.delete(*keys)
            deleted += n
            log.info(f"cleanredis 删除 {n} 个 key(本批 {len(keys)} 个)")
        if cursor == 0:
            break
    return deleted


def clean_sqlite_cursor(log: logging.Logger) -> int:
    """清空 SQLite 里的 online_stream_cursor 表(让今天从头读)

    2026-09-11 重构:遍历所有月份 db,而不是只清当月(可能历史月份还有残留 cursor)
    """
    total_deleted = 0
    data_dir = DATA_DIR
    if not data_dir.exists():
        log.warning(f"data 目录不存在: {data_dir}")
        return 0
    for db_path in sorted(data_dir.glob("online_data_*.db")):
        try:
            conn = connect(db_path.stem.replace("online_data_", ""))
            try:
                # 检查表是否存在
                cur = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='online_stream_cursor'"
                )
                if not cur.fetchone():
                    continue
                cur = conn.execute("SELECT COUNT(*) FROM online_stream_cursor")
                before = cur.fetchone()[0]
                if before == 0:
                    continue
                conn.execute("DELETE FROM online_stream_cursor")
                conn.commit()
                log.info(f"cleanredis 清空 {db_path.name} 的 online_stream_cursor 表,删了 {before} 条")
                total_deleted += before
            finally:
                conn.close()
        except Exception as e:
            log.warning(f"清 {db_path.name} cursor 表失败: {e}")
    return total_deleted


def main() -> int:
    parser = argparse.ArgumentParser(description="Service: cleanredis_online (每日清理)")
    parser.add_argument("--once", action="store_true",
                        help="只跑一次(默认行为,scheduler 触发)")
    parser.add_argument("--skip-redis", action="store_true", help="跳过 Redis 清理(只清 cursor)")
    parser.add_argument("--skip-sqlite", action="store_true", help="跳过 SQLite 清理(只清 Redis)")
    args = parser.parse_args()

    log = setup_logger("service_cleanredis_online")
    log.info(f"启动 service_cleanredis_online args={vars(args)}")

    try:
        r = OnlineRedis()
    except Exception as e:
        log.error(f"Redis 连接失败: {e}")
        return 1

    try:
        if not args.skip_redis:
            n = clean_redis_online(r, log)
            log.info(f"Redis online:* 清掉了 {n} 个 key")
        if not args.skip_sqlite:
            n = clean_sqlite_cursor(log)
            log.info(f"SQLite online_stream_cursor 清掉了 {n} 条")
        log.info("cleanredis_online 完成")
        return 0
    except Exception as e:
        log.error(f"cleanredis_online 失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
