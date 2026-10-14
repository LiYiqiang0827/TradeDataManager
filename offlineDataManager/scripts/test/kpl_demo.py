"""
kpl 库(db_cn_kpl.db)读取接口演示 — 开盘啦数据

覆盖接口:
  get_kpl_list              — 涨停榜(tushare,第二天早上更新;支持 themes/lu_descs 模糊 + status 连板关键字)
  get_kpl_concept_cons      — 题材成分 (ts_codes=成分股代码, themes=题材名)
  get_kpl_limit_performance  — 涨停表现详情(kpl API 实时,含封单/振幅/炸板/收盘价等细粒数据)

⚠️ tbl_cn_kpl_concept_cons 里 ts_code 是**题材代码**不是股票代码!
   本接口 ts_codes 参数实际查的是 con_code(成分股代码)。
⚠️ kpl 库各表的断点(原 tbl_kpl_ctrl)已废弃,统一存到 db_cn_basic.db:tbl_basic_ctrl
   查询时用 get_ctrl_basic(key='cn_kpl_list' / 'cn_kpl_concept_cons' / 'cn_kpl_limit_performance')。

运行:cd ~/TradingAgent/offlineDataManager/scripts && python3 test/kpl_demo.py
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
    get_kpl_list, get_kpl_concept_cons, get_kpl_limit_performance, get_ctrl_basic,
)


def section(title: str):
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")


# ============================================================================
# 1. get_kpl_list:涨停榜
# ============================================================================
section("1. get_kpl_list — 涨停榜(status 连板关键字很丰富)")

# 1.1 单日全部涨停榜
df = get_kpl_list(trade_date='20240913')
print(f"\n1.1 trade_date=2024-09-13:rows={len(df)}")
print(df[['ts_code', 'name', 'theme', 'status']].head().to_string(index=False))

# 1.2 themes 模糊(查军工题材的所有涨停股)
df = get_kpl_list(themes='军工')
print(f"\n1.2 themes='军工':rows={len(df):,} (theme 含'军工'的涨停)")

# 1.3 themes 多值(OR)
df = get_kpl_list(themes=['军工', '新能源'])
print(f"\n1.3 themes=['军工', '新能源']:rows={len(df):,}")

# 1.4 status 精确:2 连板
df = get_kpl_list(status='2连板', trade_date='20240913')
print(f"\n1.4 status='2连板' + trade_date=2024-09-13:rows={len(df)}")
print(df[['ts_code', 'name', 'status']].to_string(index=False))

# 1.5 status 关键字:非首板
df = get_kpl_list(status='非首板')
print(f"\n1.5 status='非首板':rows={len(df):,} (排除首板)")

# 1.6 status 关键字:N 板以上
df = get_kpl_list(status='2板以上')
print(f"\n1.6 status='2板以上':rows={len(df):,} (≥2 连板,包括 3天2板)")

# 1.7 status 模糊关键字:'天2板'(所有 *天2板)
df = get_kpl_list(status='天2板')
print(f"\n1.7 status='天2板'(模糊后缀):rows={len(df):,}")

# 1.8 status 模糊关键字:'3天'(所有 3天*板)
df = get_kpl_list(status='3天')
print(f"\n1.8 status='3天'(模糊前缀):rows={len(df):,}")

# 1.9 lu_descs 模糊
df = get_kpl_list(lu_descs='印制电路板')
print(f"\n1.9 lu_descs='印制电路板':rows={len(df):,}")

# 1.10 组合:军工 + 3 板以上
df = get_kpl_list(themes='军工', status='3板以上')
print(f"\n1.10 themes='军工' + status='3板以上':rows={len(df):,}")


# ============================================================================
# 2. get_kpl_concept_cons:题材成分 (题材与个股的关系)
# ============================================================================
section("2. get_kpl_concept_cons — 题材成分(查'个股属于什么题材'或'某题材有哪些个股')")

# 2.1 查某股在哪些题材(上汽集团 600104.SH)
df = get_kpl_concept_cons(ts_codes='600104.SH', trade_date='20250310')
print(f"\n2.1 ts_codes='600104.SH' + trade_date=2025-03-10:")
print(f"rows={len(df)}, 主题数={df['name'].nunique()}")
print(df[['name', 'con_name', 'hot_num', 'desc']].head().to_string(index=False))

# 2.2 查某题材的所有成分股
df = get_kpl_concept_cons(themes='光刻机概念', trade_date='20250310')
print(f"\n2.2 themes='光刻机概念' + trade_date=2025-03-10:")
print(f"rows={len(df)}, 成分股={df['con_name'].nunique()} 只")
print(df[['con_name', 'con_code', 'hot_num', 'desc']].head().to_string(index=False))

# 2.3 组合:个股 + 题材
df = get_kpl_concept_cons(ts_codes='600104.SH', themes='光刻机概念', trade_date='20250310')
print(f"\n2.3 ts_codes='600104.SH' + themes='光刻机概念':rows={len(df)}")

# 2.4 descs 模糊:含 AI 技术的成分股
df = get_kpl_concept_cons(descs='AI技术')
print(f"\n2.4 descs='AI技术':rows={len(df):,}")

# 2.5 descs 多值 + 热度过滤
df = get_kpl_concept_cons(descs=['AI', '航天'], min_hot_num=500)
print(f"\n2.5 descs=['AI', '航天'] + min_hot_num=500:rows={len(df):,}")

# 2.6 多股同日
df = get_kpl_concept_cons(ts_codes=['600104.SH', '600030.SH'], trade_date='20250310')
print(f"\n2.6 多股同日:rows={len(df)}, 唯一 con_code={df['con_code'].nunique()}")


# ============================================================================
# 3. kpl 库断点(从 db_cn_basic.db:tbl_basic_ctrl 查,key 前缀 cn_kpl_)
# ============================================================================
section("3. kpl 库断点(查 tbl_basic_ctrl,key 前缀 cn_kpl_)")

# 3.1 三个 kpl 断点一起查
df = get_ctrl_basic(key=['cn_kpl_list', 'cn_kpl_concept_cons', 'cn_kpl_limit_performance'])
print(f"\n3.1 3 个 kpl 断点:rows={len(df)}")
print(df.to_string(index=False))

# 3.2 单个
df = get_ctrl_basic(key='cn_kpl_list')
print(f"\n3.2 key='cn_kpl_list':rows={len(df)}")

# 3.3 多值
df = get_ctrl_basic(key=['cn_kpl_list', 'cn_kpl_concept_cons'])
print(f"\n3.3 多值:rows={len(df)}")


print("\n" + "=" * 70)
print("  演示完毕")
print("=" * 70)


# ============================================================================
# 4. get_kpl_limit_performance:涨停表现详情(kpl API 实时,补 kpl_list 滞后)
# ============================================================================
section("4. get_kpl_limit_performance — 涨停表现详情(实时,字段更细)")


# 4.1 单日全部涨停
df = get_kpl_limit_performance(trade_date='20260911')
print(f"\n4.1 trade_date='2026-09-11': rows={len(df)} (全部涨停股)")
print("默认排序:trade_date DESC, board_count DESC, lu_time ASC")
print(df[['ts_code', 'name', 'board_count', 'lu_time', 'theme', 'amplitude']].head(8).to_string(index=False))

# 4.2 只看连板股(board_counts)
df = get_kpl_limit_performance(trade_date='20260911', board_counts=[2, 3, 4])
print(f"\n4.2 trade_date + board_counts=[2,3,4]: rows={len(df)} (连板股)")
print(df[['ts_code', 'name', 'board_count', 'lu_time', 'theme', 'board_period']].to_string(index=False))

# 4.3 只看曾炸板的(only_broken=True)
df = get_kpl_limit_performance(trade_date='20260911', only_broken=True)
print(f"\n4.3 only_broken=True: rows={len(df)} (封板过程中曾炸开过)")
print(df[['ts_code', 'name', 'board_count', 'amplitude', 'limit_order']].head(10).to_string(index=False))

# 4.4 题材过滤(themes 精确)
df = get_kpl_limit_performance(trade_date='20260911', themes=['通信'])
print(f"\n4.4 themes=['通信']: rows={len(df)} (通信板块涨停股)")
print(df[['ts_code', 'name', 'board_count', 'lu_time']].to_string(index=False))

# 4.5 振幅过滤(min_amplitude)
df = get_kpl_limit_performance(trade_date='20260911', min_amplitude=15.0)
print(f"\n4.5 min_amplitude=15: rows={len(df)} (日内振幅 ≥ 15% 的剧烈震荡股)")
print(df[['ts_code', 'name', 'amplitude', 'turnover_rate', 'close_price']].to_string(index=False))

# 4.6 区间 + 自定义列
df = get_kpl_limit_performance(
    start_date='20260907', end_date='20260911',
    columns=['trade_date', 'ts_code', 'name', 'board_count', 'amplitude', 'is_break', 'close_price'],
)
print(f"\n4.6 start/end_date='2026-09-07'~'2026-09-11' + 自定义列: rows={len(df)}")
print(df.head(10).to_string(index=False))

# 4.7 多股票同日(精确 IN ts_code)
df = get_kpl_limit_performance(
    trade_date='20260911',
    ts_codes=['002790.SZ', '000993.SZ', '603421.SH'],
)
print(f"\n4.7 ts_codes=[3 只连板股]: rows={len(df)}")
print(df[['ts_code', 'name', 'board_count', 'theme']].to_string(index=False))

# 4.8 与 get_kpl_list 对比(数据源差异演示)
print("\n4.8 get_kpl_limit_performance vs get_kpl_list 数据源对比")
df_lp = get_kpl_limit_performance(trade_date='20260911')
df_list = get_kpl_list(trade_date='20260911')
print(f"  limit_performance (kpl API 实时,16:30+ 就有): {len(df_lp)} 只")
print(f"  kpl_list          (tushare,第二天早上更新)     : {len(df_list)} 只")
print("  字段差异:limit_performance 多出 amplitude/is_break/close_price/board_period/lu_time(HH:MM:SS) 等")
print(f"  共同字段:ts_code/name/trade_date/theme/net_change/limit_order/amount/free_float")


print("\n" + "=" * 70)
print("  演示完毕")
print("=" * 70)
