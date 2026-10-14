"""
news 库(db_cn_news.db)读取接口演示

覆盖接口:
  get_news         — 新闻快讯 (默认 limit=5000, 支持分页 offset)
  get_major_news   — 长新闻 (同 get_news 模式)
  get_cctv_news    — CCTV 新闻联播 (datetime 是 YYYYMMDD,不带时间戳)
  get_news_ctrl    — news 库断点表

⚠️ tbl_news / tbl_major_news 的 datetime 是 'YYYY-MM-DD HH:MM:SS' 带时间戳。
   start_date / end_date 自动补 00:00:00 / 23:59:59;
   想精确时分秒用 start_datetime / end_datetime。
⚠️ tbl_cctv_news 的 datetime 是 'YYYYMMDD' 无时间。

运行:cd ~/TradingAgent/offlineDataManager/scripts && python3 test/news_demo.py
"""
import os
import sys
from pathlib import Path


def _resolve_root() -> Path:
    env = os.environ.get("TRADING_DATA_MANAGER_ROOT")
    if env:
        p = Path(env).expanduser().resolve()
        if not p.is_dir():
            raise RuntimeError("TRADING_DATA_MANAGER_ROOT must name an existing directory")
        return p
    return Path(__file__).resolve().parents[4]

ROOT = _resolve_root()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

import pandas as pd
pd.set_option('display.width', 250)
pd.set_option('display.max_columns', 10)
pd.set_option('display.max_colwidth', 50)

from core.offline_db_client import (
    get_news, get_major_news, get_cctv_news, get_news_ctrl,
)


def section(title: str):
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")


# ============================================================================
# 1. get_news:新闻快讯
# ============================================================================
section("1. get_news — 新闻快讯(默认 limit=5000, datetime 带时间戳)")

# 1.1 默认最近 5000 条(瞬时返回)
df = get_news()
print(f"\n1.1 默认 limit=5000:")
print(f"rows={len(df):,}, "
      f"range={df['datetime'].min()} ~ {df['datetime'].max()}")

# 1.2 trade_date 单日(自动 [00:00:00, 23:59:59])
df = get_news(trade_date='2026-09-11', limit=5)
print(f"\n1.2 trade_date=2026-09-11 + limit=5:")
print(df[['datetime', 'src', 'title']].to_string(index=False))

# 1.3 start_date + end_date(自动补 00:00:00 / 23:59:59)
df = get_news(start_date='2026-09-09', end_date='2026-09-11', limit=10)
print(f"\n1.3 3 天区间 + limit=10:rows={len(df)}")

# 1.4 精确 datetime(start_datetime + end_datetime)
df = get_news(start_datetime='2026-09-11 09:00:00',
              end_datetime='2026-09-11 17:00:00', limit=5)
print(f"\n1.4 精确 09:00~17:00 区间 + limit=5:")
print(df[['datetime', 'src', 'title']].to_string(index=False))

# 1.5 分页:第二页
df = get_news(limit=5000, offset=5000)
print(f"\n1.5 limit=5000 offset=5000(下一页):rows={len(df):,}")
print(f"首行 datetime:{df.iloc[0]['datetime']}")

# 1.6 src 过滤(单源)
df = get_news(src='sina', limit=5)
print(f"\n1.6 src='sina' + limit=5:rows={len(df)}, src={df['src'].unique()}")

# 1.7 src 多源
df = get_news(src=['sina', 'yicai'], limit=5)
print(f"\n1.7 src=['sina', 'yicai'] + limit=5:rows={len(df)}")

# 1.8 title 模糊
df = get_news(title='习近平', limit=5)
print(f"\n1.8 title='习近平' + limit=5:rows={len(df):,} (全库匹配数)")

# 1.9 content 模糊
df = get_news(content='新能源', limit=5)
print(f"\n1.9 content='新能源' + limit=5:rows={len(df):,}")

# 1.10 title + content 组合(AND)
df = get_news(title='习近平', content='新时代', limit=5)
print(f"\n1.10 title='习近平' + content='新时代' + limit=5:rows={len(df):,}")

# 1.11 全功能:trade_date + src + content + limit
df = get_news(trade_date='2026-09-11', src=['sina'], content='习近平', limit=5)
print(f"\n1.11 trade_date + src + content + limit:rows={len(df)}")
print(df[['datetime', 'src', 'title']].to_string(index=False))


# ============================================================================
# 2. get_major_news:长新闻(同 get_news 模式)
# ============================================================================
section("2. get_major_news — 长新闻(同 get_news 模式,表是 tbl_major_news)")

# 2.1 默认
df = get_major_news(limit=5)
print(f"\n2.1 默认 limit=5:rows={len(df)}")
print(df[['datetime', 'src', 'title']].to_string(index=False))

# 2.2 trade_date 单日 + content
df = get_major_news(trade_date='2026-09-11', content='经济', limit=5)
print(f"\n2.2 trade_date + content='经济' + limit=5:rows={len(df)}")

# 2.3 datetime 精确
df = get_major_news(start_datetime='2026-09-11 20:00:00',
                    end_datetime='2026-09-11 23:59:59', limit=5)
print(f"\n2.3 20:00~23:59 + limit=5:rows={len(df)}")


# ============================================================================
# 3. get_cctv_news:CCTV 新闻联播
# ============================================================================
section("3. get_cctv_news — CCTV 新闻联播(datetime 是 YYYYMMDD 不带时间戳)")

# 3.1 单日
df = get_cctv_news(trade_date='20240911')
print(f"\n3.1 trade_date=2024-09-11:rows={len(df)}")
print(df[['datetime', 'title']].head().to_string(index=False))

# 3.2 区间
df = get_cctv_news(start_date='20240909', end_date='20240913')
print(f"\n3.2 5 天区间:rows={len(df)}")

# 3.3 content 模糊
df = get_cctv_news(content='习近平', start_date='20240101', end_date='20241231')
print(f"\n3.3 2024 年 content 含'习近平':rows={len(df):,}")

# 3.4 content 模糊 + 单日
df = get_cctv_news(trade_date='20240911', content='习近平')
print(f"\n3.4 单日 + content='习近平':rows={len(df)}")
print(df[['datetime', 'title']].head().to_string(index=False))


# ============================================================================
# 4. get_news_ctrl:news 库断点表
# ============================================================================
section("4. get_news_ctrl — news 库断点表(每个新闻源一行)")

# 4.1 全部
df = get_news_ctrl()
print(f"\n4.1 全部:rows={len(df)} (应 11:9 个 news + cctv_news + major_news)")
print(df.to_string(index=False))

# 4.2 单源
df = get_news_ctrl(src='sina')
print(f"\n4.2 src='sina':rows={len(df)}")

# 4.3 多源
df = get_news_ctrl(src=['sina', 'yicai', 'major_news'])
print(f"\n4.3 src=['sina', 'yicai', 'major_news']:rows={len(df)}")

# 4.4 空 src=[] → 0 行
df = get_news_ctrl(src=[])
print(f"\n4.4 src=[]:rows={len(df)} (应 0)")


print("\n" + "=" * 70)
print("  演示完毕")
print("=" * 70)
