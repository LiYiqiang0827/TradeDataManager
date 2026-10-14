"""
KPLClient 在线 API 演示 — 开盘啦 HTTP 接口实时调用

覆盖接口 (3 个已验证可用):
  1. limit_ladder()                  — 涨停天梯 (实时)
  2. market_sentiment()              — 涨跌统计 (实时)
  3. limit_up_performance(date, n)   — 涨停表现详情 (历史,自动 IP fallback)

运行:
  cd ~ && python3 TradingAgent/coreClient/test/kpl_live_demo.py
  或
  cd ~/TradingAgent/coreClient/test && python3 kpl_live_demo.py

环境:
  - 需要有效的 KPL_AUTH_TOKEN (从开盘啦 App 登录后从 debug_socket.txt 抓)
  - 写入 ~/TradingAgent/coreClient/kpl_config.py:KPL_AUTH_TOKEN
  - 网络: apphis/applhb 自动 IP fallback (见 kpl-api skill pitfall #14)
"""
import sys
import os
# 把 coreClient 父目录加进 path(这样可以 import coreClient.kpl_client)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
from datetime import datetime
import pandas as pd

# pandas 显示设置
pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_colwidth", 40)
pd.set_option("display.float_format", "{:.2f}".format)

from coreClient.kpl_client import KPLClient


def banner(title: str) -> None:
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def demo_limit_ladder():
    """演示 1: 涨停天梯(实时)"""
    banner("1. 涨停天梯 (实时) — limit_ladder()")
    client = KPLClient()
    result = client.limit_ladder()

    print(f"errcode: {result['errcode']}")
    print(f"raw_info (5 元素数组): {result['raw_info']}")
    print()
    print("结构化数据:")
    for k, v in result["data"].items():
        marker = " ★ 总计" if k == "总计" else ""
        print(f"  {k:>8}: {v:>3} 只{marker}")

    # 数据一致性校验
    total_api = sum(result["raw_info"])
    total_structured = result["data"]["总计"]
    print(f"\n一致性校验: raw_info sum={total_api}, data.总计={total_structured}, "
          f"{'✓' if total_api == total_structured else '✗'}")


def demo_market_sentiment():
    """演示 2: 涨跌统计(实时)"""
    banner("2. 涨跌统计 (实时) — market_sentiment()")
    client = KPLClient()
    result = client.market_sentiment()

    info = result.get("info", {})
    print(f"errcode: {result.get('errcode')}")
    print(f"涨停家数 (SJZT): {info.get('SJZT')}")
    print(f"跌停家数 (SJDT): {info.get('SJDT')}")

    # 与涨停天梯对比
    ladder = client.limit_ladder()
    limit_up_total = ladder["data"]["总计"]
    print(f"\n交叉校验: limit_ladder.总计={limit_up_total} vs SJZT={info.get('SJZT')} "
          f"{'✓ 一致' if str(limit_up_total) == info.get('SJZT') else '? 差异(可能包含一字板/炸板)'}")


def demo_limit_up_performance(date: str = "2026-09-11"):
    """演示 3: 涨停表现详情(历史,自动 IP fallback)"""
    banner(f"3. 涨停表现详情 ({date}) — limit_up_performance()")
    client = KPLClient()

    # 3a. 一板
    print(f"\n--- 一板 (board_type=1) ---")
    df1 = client.limit_up_performance(date, board_type=1)
    print(f"行数: {len(df1)}")
    if len(df1) > 0:
        # 关键字段(支持一板只有 19 字段,没有 board_period)
        view_cols = [c for c in ["ts_code", "name", "lu_time", "theme",
                                  "limit_reason", "turnover_rate", "amplitude",
                                  "board_count", "is_break", "free_float", "board_period",
                                  "limit_order", "close_price", "pct_chg",
                                  "main_in", "main_out"]
                     if c in df1.columns]
        df1_display = df1[view_cols].copy()
        df1_display["lu_time"] = pd.to_datetime(
            df1_display["lu_time"], unit="s"
        ).dt.strftime("%H:%M")
        df1_display["free_float"] = (
            df1_display["free_float"] / 1e8
        ).round(2).astype(str) + "亿"
        # main_in / main_out 转成万
        for c in ["main_in", "main_out", "limit_order"]:
            if c in df1_display.columns:
                df1_display[c] = (df1_display[c] / 1e4).round(0).astype(str) + "万"
        print(df1_display.head(10).to_string(index=False))
        if len(df1_display) > 10:
            print(f"... 还有 {len(df1_display) - 10} 只")

    # 3b. 二板
    print(f"\n--- 二板 (board_type=2) ---")
    df2 = client.limit_up_performance(date, board_type=2)
    print(f"行数: {len(df2)}")
    if len(df2) > 0:
        view_cols = [c for c in ["ts_code", "name", "lu_time", "theme",
                                  "pct_chg", "board_count", "board_period"]
                     if c in df2.columns]
        df2_display = df2[view_cols].copy()
        df2_display["lu_time"] = pd.to_datetime(
            df2_display["lu_time"], unit="s"
        ).dt.strftime("%H:%M")
        print(df2_display.to_string(index=False))

    # 3c. 三板 + 四板 + 更高板
    for bt in [3, 4, 5]:
        print(f"\n--- {['一', '二', '三', '四', '更高'][bt-1]}板 (board_type={bt}) ---")
        try:
            df = client.limit_up_performance(date, board_type=bt)
            print(f"行数: {len(df)}")
            if len(df) > 0:
                view_cols = [c for c in ["ts_code", "name", "theme",
                                          "board_count", "board_period"]
                             if c in df.columns]
                print(df[view_cols].to_string(index=False))
            else:
                print("(无数据)")
        except Exception as e:
            print(f"失败: {type(e).__name__}: {str(e)[:100]}")


def demo_compare_with_ladder(date: str = "2026-09-11"):
    """演示 4: 涨停天梯 vs 涨停表现详情 一致性校验"""
    banner(f"4. 一致性校验 ({date}): limit_ladder vs limit_up_performance 各板汇总")
    client = KPLClient()

    # 涨停天梯(实时)— 实际是当前盘中的数据,跟历史日不一定一致
    print("\n[实时] 涨停天梯:")
    ladder = client.limit_ladder()
    print(f"  一板={ladder['data']['一板']}, 二板={ladder['data']['二板']}, "
          f"三板={ladder['data']['三板']}, 四板={ladder['data']['四板']}, "
          f"更高={ladder['data']['更高板']}")

    # 各板汇总(历史日,可能跟实时不一致)
    print(f"\n[{date}] 涨停表现详情 汇总:")
    board_labels = ["一板", "二板", "三板", "四板", "更高板"]
    for bt in range(1, 6):
        df = client.limit_up_performance(date, board_type=bt)
        print(f"  {board_labels[bt-1]} (board_type={bt}): {len(df)} 只")


def main():
    print("=" * 80)
    print("  KPLClient 实时 API 演示")
    print(f"  运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # 检查 token 是否设置
    # 经验: 历史接口(HisHomeDingPan/DailyLimitPerformance)用 token=0 也能拿数据,
    # 所以占位符 0 也能跑(只会实时接口 HomeDingPan/* 返回空)
    from coreClient.kpl_config import get_kpl_auth
    token, user_id = get_kpl_auth()
    using_default_token = (token == "0" or user_id == "0")
    if using_default_token:
        print("\nℹ️  使用占位符 token='0', 部分实时接口可能返回空数据")
        print("  已知可用(token=0): 涨停表现详情 (HisHomeDingPan.DailyLimitPerformance)")
        print("  受限接口: 涨停天梯/涨跌统计实时数据 (HomeDingPan.*)")
        print("  如需真实数据,在开盘啦 App 登录后从 debug_socket.txt 抓 token:")
        print("  tail -1000 ~/Library/Containers/9650172F-0442-4FD5-B8C4-6D1D4E9DCE54/Data/Documents/Debug/debug_socket.txt \\")
        print("    | grep -oE 'userID\":[1-9][0-9]*|\"token\":\"[a-f0-9]{20,}\"' | tail -4")
        print("  写入 ~/TradingAgent/coreClient/kpl_config.py:KPL_AUTH_TOKEN / KPL_USER_ID")
        print("  (继续运行,只演示可用接口)")

    try:
        demo_limit_ladder()
        demo_market_sentiment()
        demo_limit_up_performance(date="2026-09-11")
        demo_compare_with_ladder(date="2026-09-11")
    except Exception as e:
        print(f"\n❌ 失败: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 80)
    print("  ✓ 演示完成")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())