"""
basic 库(db_cn_basic.db)读取接口演示

覆盖接口:
  get_day         — 日 K (支持前复权 qfq)
  get_week        — 周 K (已前复权)
  get_month       — 月 K (已前复权)
  get_tradecal    — 交易日历 (支持 offset 模式 + market 过滤)
  get_basic       — 股票基本信息 (exchange + market 多值过滤)
  get_adj_factor  — 复权因子
  get_ctrl_basic  — basic 库断点表

运行:cd ~/TradingAgent/offlineDataManager/scripts && python3 test/basic_demo.py
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
pd.set_option('display.max_columns', 12)
pd.set_option('display.max_colwidth', 50)

from core.offline_db_client import (
    get_day, get_week, get_month,
    get_tradecal, get_basic, get_adj_factor, get_ctrl_basic,
)


def section(title: str):
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")


# ============================================================================
# 1. get_day:日 K (支持前复权)
# ============================================================================
section("1. get_day — 日 K(前复权 / 不复权 / 多股 / 单日 / 区间)")

# 1.1 单股单日(前复权,默认)
df = get_day(ts_code='002579.SZ', trade_date='20240915')
print(f"\n1.1 单股 + 2024-09-15(周日无数据):rows={len(df)}")

# 1.2 单股单日(交易日 + 前复权)
df = get_day(ts_code='002579.SZ', trade_date='20240913')
print(f"\n1.2 单股 + 2024-09-13(周五):rows={len(df)}, close={df.iloc[0]['close']:.2f}")

# 1.3 区间 2024 全年
df = get_day(ts_code='002579.SZ', start_date='20240101', end_date='20241231')
print(f"\n1.3 单股 + 2024 区间:rows={len(df)}, "
      f"date range={df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")

# 1.4 不复权 vs 前复权对比
df_raw = get_day(ts_code='002579.SZ', start_date='20200501', end_date='20200605', qfq=False)
df_qfq = get_day(ts_code='002579.SZ', start_date='20200501', end_date='20200605', qfq=True)
print(f"\n1.4 2020/5/1-6/5 raw vs qfq:")
print(df_qfq[['trade_date', 'close']].assign(
    close_raw=df_raw['close'].values,
    diff=(df_raw['close'].values - df_qfq['close'].values).round(2)
).to_string(index=False))

# 1.5 多股同一天
df = get_day(ts_codes=['002579.SZ', '000001.SZ', '600000.SH'], trade_date='20240913')
print(f"\n1.5 多股 + 2024-09-13:rows={len(df)}")

# 1.6 自定义列
df = get_day(ts_code='002579.SZ', trade_date='20240913',
             columns=['ts_code', 'trade_date', 'close', 'vol'])
print(f"\n1.6 自定义列:cols={list(df.columns)}")


# ============================================================================
# 2. get_week:周 K (已前复权,trade_date 是该周最后交易日)
# ============================================================================
section("2. get_week — 周 K(trade_date 落在所在 ISO 周)")

# 2.1 单股 + 单周
df = get_week(ts_code='002579.SZ', trade_date='20240915')  # 周日,自动归到 W37
print(f"\n2.1 trade_date=2024-09-15(周日):rows={len(df)}")
print(df.to_string(index=False))

# 2.2 区间
df = get_week(ts_code='002579.SZ', start_date='20240901', end_date='20241031')
print(f"\n2.2 2024/9-10 区间:rows={len(df)}")

# 2.3 原始不复权周 K
df = get_week(ts_code='002579.SZ', start_date='20240901', end_date='20241031', qfq=False)
print(f"\n2.3 原始不复权周K:rows={len(df)}")


# ============================================================================
# 3. get_month:月 K (已前复权,trade_date 是该月最后交易日)
# ============================================================================
section("3. get_month — 月 K(trade_date 落在所在自然月)")

df = get_month(ts_code='002579.SZ', trade_date='20240915')  # 9 月的 K
print(f"\n3.1 trade_date=2024-09-15(9 月):rows={len(df)}")
print(df.to_string(index=False))

df = get_month(ts_code='002579.SZ', start_date='20240101', end_date='20240630')
print(f"\n3.2 2024 H1 区间:rows={len(df)}")

df = get_month(ts_code='002579.SZ', start_date='20240101', end_date='20240630', qfq=False)
print(f"\n3.3 原始不复权月K:rows={len(df)}")


# ============================================================================
# 4. get_tradecal:交易日历 (支持 offset 模式 + market 过滤)
# ============================================================================
section("4. get_tradecal — 交易日历(market=SSE 默认 / offset N 个交易日)")

# 4.1 区间(默认只返回 SSE)
df = get_tradecal(start_date='20240909', end_date='20240913')
print(f"\n4.1 market='SSE'(默认) 2024-09-09~13:rows={len(df)}")
print(df.to_string(index=False))

# 4.2 market=None(全部交易所,9/9 和 9/10 各 2 行)
df = get_tradecal(start_date='20240909', end_date='20240910', market=None)
print(f"\n4.2 market=None 2024-09-09~10:rows={len(df)} (2 天 × 2 交易所)")

# 4.3 offset 模式:从某天起 N 个交易日(非交易日自动跳过)
df = get_tradecal(trade_date='20240915', day_num=5)  # 周日 → 9/18 起 5 天
print(f"\n4.3 offset mode:trade_date=2024-09-15(周日) + day_num=5:")
print(f"rows={len(df)}, dates={sorted(df['cal_date'].unique())}")

# 4.4 offset + market=SZSE
df = get_tradecal(trade_date='20240918', day_num=3, market='SZSE')
print(f"\n4.4 offset + market=SZSE:rows={len(df)}")


# ============================================================================
# 5. get_basic:股票基本信息 (exchange + market 多维过滤)
# ============================================================================
section("5. get_basic — 股票基本信息(exchange + market 独立 AND 过滤)")

# 5.1 全表(默认 None = 不过滤)
df = get_basic()
print(f"\n5.1 默认:rows={len(df):,} (全 5,562 只)")

# 5.2 只看沪市
df = get_basic(exchange='SSE')
print(f"\n5.2 exchange='SSE':rows={len(df):,} (沪市 2,318)")

# 5.3 只看创业板
df = get_basic(market='创业板')
print(f"\n5.3 market='创业板':rows={len(df):,} (深市 1,407)")

# 5.4 多值组合
df = get_basic(exchange=['SSE', 'BSE'], market=['主板', '北交所'])
print(f"\n5.4 exchange=[SSE,BSE] + market=[主板,北交所]:rows={len(df):,}")

# 5.5 沪市主板 + 科创板 = 整个沪市
df = get_basic(exchange='SSE', market=['主板', '科创板'])
print(f"\n5.5 exchange='SSE' + market=[主板,科创板]:rows={len(df):,} = 整个沪市")

# 5.6 暂停的(DB 里 P=0,验证架构支持)
df = get_basic(list_status='P')
print(f"\n5.6 list_status='P':rows={len(df)} (当前市场无停牌)")


# ============================================================================
# 6. get_adj_factor:复权因子 (给前复权计算用)
# ============================================================================
section("6. get_adj_factor — 复权因子")

# 6.1 单股最近 1 周
df = get_adj_factor(ts_code='002579.SZ', start_date='20240901', end_date='20240913')
print(f"\n6.1 单股 9 月:rows={len(df)}")
print(df.head().to_string(index=False))

# 6.2 多股同日
df = get_adj_factor(ts_codes=['002579.SZ', '000001.SZ'], trade_date='20240913')
print(f"\n6.2 多股 2024-09-13:rows={len(df)}")


# ============================================================================
# 7. get_ctrl_basic:basic 库断点表
# ============================================================================
section("7. get_ctrl_basic — basic 库断点表")

# 7.1 全部断点
df = get_ctrl_basic()
print(f"\n7.1 全部:rows={len(df)} (5 个)")
print(df.to_string(index=False))

# 7.2 单个
df = get_ctrl_basic(key='cn_basic')
print(f"\n7.2 key='cn_basic':rows={len(df)}")

# 7.3 多个
df = get_ctrl_basic(key=['cn_basic', 'cn_daily', 'cn_adj_factor'])
print(f"\n7.3 多值:rows={len(df)}")


print("\n" + "=" * 70)
print("  演示完毕")
print("=" * 70)
