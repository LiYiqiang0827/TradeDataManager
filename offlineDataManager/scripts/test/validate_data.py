"""
~/TradingAgent/offlineDataManager/scripts/test/validate_data.py
验证 daily / kpl / news 数据完整性(对比 2015-2026 区间)
"""
import sys
import os
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
# 父目录 = ~/TradingAgent/(用来 import coreClient.tushare_client)
sys.path.insert(0, str(PROJECT_ROOT.parent))

import sqlite3
from coreClient.tushare_client import TushareClient
from config.settings import DB_PATH_BASIC, DB_PATH_KPL, DB_PATH_NEWS


def count_daily(client, trade_date: str) -> int:
    """用 OFFSET 分页拿某天 daily 总行数"""
    total = 0
    offset = 0
    while True:
        df = client.daily(trade_date=trade_date, limit=6000, offset=offset)
        if df is None or len(df) == 0:
            break
        total += len(df)
        if len(df) < 6000:
            break
        offset += 6000
    return total


def count_kpl_list(client, trade_date: str) -> int:
    """用 OFFSET 拿 kpl_list 总行数"""
    total = 0
    offset = 0
    while True:
        df = client.pro.kpl_list(trade_date=trade_date, limit=8000, offset=offset)
        if df is None or len(df) == 0:
            break
        total += len(df)
        if len(df) < 8000:
            break
        offset += 8000
    return total


def count_kpl_concept_cons(client, trade_date: str) -> int:
    """用 OFFSET 拿 kpl_concept_cons 总行数"""
    total = 0
    offset = 0
    while True:
        df = client.pro.kpl_concept_cons(trade_date=trade_date, limit=3000, offset=offset)
        if df is None or len(df) == 0:
            break
        total += len(df)
        if len(df) < 3000:
            break
        offset += 3000
    return total


def count_news(client, src: str, trade_date: str) -> int:
    """用 OFFSET 拿某源某天 news 总行数"""
    from datetime import datetime, timedelta
    sd = trade_date
    ed_dt = datetime.strptime(trade_date, "%Y%m%d") + timedelta(days=1)
    ed = ed_dt.strftime("%Y%m%d")

    total = 0
    offset = 0
    while True:
        df = client.pro.news(src=src, start_date=sd, end_date=ed, limit=1500, offset=offset)
        if df is None or len(df) == 0:
            break
        total += len(df)
        if len(df) < 1500:
            break
        offset += 1500
    return total


def validate():
    print("=" * 60)
    print("数据完整性验证(用 OFFSET 分页正确拉取)")
    print("=" * 60)

    client = TushareClient()
    conn_basic = sqlite3.connect(str(DB_PATH_BASIC))
    conn_kpl = sqlite3.connect(str(DB_PATH_KPL))
    conn_news = sqlite3.connect(str(DB_PATH_NEWS))

    # 抽样日期
    SAMPLE_DATES = ['20260908', '20260115', '20250115', '20241015', '20240615']

    # 1. daily
    print("\n=== daily 验证 ===")
    for td in SAMPLE_DATES:
        expected = count_daily(client, td)
        actual = conn_basic.execute(
            "SELECT COUNT(*) FROM tbl_cn_day WHERE trade_date = ?", (td,)
        ).fetchone()[0]
        status = "✓" if actual >= expected else "✗"
        print(f"  {status} {td}: 期望 {expected}, 实际 {actual}")

    # 2. kpl_list
    print("\n=== kpl_list 验证 ===")
    for td in SAMPLE_DATES:
        expected = count_kpl_list(client, td)
        actual = conn_kpl.execute(
            "SELECT COUNT(*) FROM tbl_cn_kpl_list WHERE trade_date = ?", (td,)
        ).fetchone()[0]
        status = "✓" if actual >= expected else "✗"
        print(f"  {status} {td}: 期望 {expected}, 实际 {actual}")

    # 3. kpl_concept_cons
    print("\n=== kpl_concept_cons 验证 ===")
    for td in SAMPLE_DATES:
        expected = count_kpl_concept_cons(client, td)
        actual = conn_kpl.execute(
            "SELECT COUNT(*) FROM tbl_cn_kpl_concept_cons WHERE trade_date = ?", (td,)
        ).fetchone()[0]
        status = "✓" if actual >= expected else "✗"
        print(f"  {status} {td}: 期望 {expected}, 实际 {actual}")

    # 4. news(每个源抽样)
    print("\n=== news 验证(9-8) ===")
    for src in ['sina', 'cls', 'eastmoney', '10jqka', 'wallstreetcn']:
        expected = count_news(client, src, '20260908')
        actual = conn_news.execute(
            "SELECT COUNT(*) FROM tbl_news WHERE src = ? AND datetime LIKE '2026-09-08%'",
            (src,)
        ).fetchone()[0]
        status = "✓" if actual >= expected else "✗"
        print(f"  {status} {src}: 期望 {expected}, 实际 {actual}")


if __name__ == "__main__":
    validate()
