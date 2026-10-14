"""
scheduler_once.py — 一次性测试调度器

按依赖顺序依次跑完所有离线数据下载任务。

依赖顺序(每个阶段依赖前一阶段):
  阶段 0: 元数据(无依赖)
    - service_tradecal   (交易日历表)
    - service_basic      (个股基本表/股票列表)
    - service_index_basic(指数列表)

  阶段 1: K 线数据(需要 basic + tradecal)
    - service_daily        (日 K)
    - service_adj_factor   (复权因子)
    - service_week         (周 K)
    - service_month        (月 K)

  阶段 2: 衍生数据(需要日 K)
    - service_daily_basic   (每日指标)
    - service_moneyflow     (资金流向)
    - service_stk_limit     (涨跌停价格)
    - service_suspend       (停复牌)
    - service_index_daily   (12 只指数日线行情)

  阶段 3: KPL / 开盘啦(无基础数据依赖)
    - service_kpl_list              (涨停榜,2026-09-15 加炸板)
    - service_kpl_concept_cons      (题材成分)
    - service_kpl_limit_performance (涨停表现,同花顺)

  阶段 4: 高级数据(部分需要日 K)
    - service_top_list       (龙虎榜每日)
    - service_top_inst       (龙虎榜机构)
    - service_block_trade    (大宗交易)
    - service_ggt_daily      (港股通日成交)
    - service_hsgt_top10     (沪深股通十大成交股)
    - service_limit_list     (每日涨跌停列表)
    - service_margin         (融资融券交易汇总)
    - service_margin_detail  (融资融券交易明细)
    - service_cyq_perf       (每日筹码及胜率)

  阶段 5: 新闻
    - service_news         (新闻快讯)
    - service_major_news   (长新闻)
    - service_cctv_news    (联播新闻)

运行方式:
  python -m scheduler.scheduler_once                  # 默认:全部依次跑
  python -m scheduler.scheduler_once --only stage0    # 只跑阶段 0
  python -m scheduler.scheduler_once --only kpl_list  # 只跑 1 个 service
  python -m scheduler.scheduler_once --only kpl_list daily_basic index_daily
                                                    # 跑多个 service(空格分隔)
  python -m scheduler.scheduler_once --only kpl_list,daily_basic,index_daily
                                                    # 跑多个 service(逗号分隔)
  python -m scheduler.scheduler_once --only stage0 stage3  # 跑多个阶段
  python -m scheduler.scheduler_once --skip kpl_limit_performance  # 跳过某些
  python -m scheduler.scheduler_once --continue       # 任一失败不中止,继续跑
  python -m scheduler.scheduler_once --dry-run        # 只打印计划

退出码:
  0 = 全部成功
  1 = 任一 service 失败(默认模式)
  2 = 用户中断
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import datetime
from pathlib import Path

from loguru import logger

from config.settings import LOG_FORMAT, LOG_LEVEL, LOG_DIR, PROJECT_ROOT


# ============================================================
# 任务编排(按依赖顺序)
# ============================================================

# 每项:(service_name, extra_args_or_None)
# 注意:extra_args 是 list,传 None 时等同 []
STAGES = [
    (
        "阶段 0 — 元数据(无依赖)",
        [
            ("service_tradecal", ["--trade-date", "20260915"]),  # 交易日历(用今天/昨天)
            ("service_basic", []),                              # 个股基本表(全量覆盖)
            ("service_index_basic", []),                        # 指数基本信息(全量覆盖)
        ],
    ),
    (
        "阶段 1 — K 线数据(需要 basic + tradecal)",
        [
            ("service_daily", ["--trade-date", "20260915"]),    # 日 K(单日)
            ("service_adj_factor", ["--end-date", "20260915"]), # 复权因子(增量)
            ("service_oneMinute", []),                           # TDX 1分钟(依赖日线股票集和复权因子)
            ("service_week", []),                               # 周 K(从 ctrl + adj_factor 派生)
            ("service_month", []),                              # 月 K
        ],
    ),
    (
        "阶段 2 — 衍生数据(需要日 K)",
        [
            ("service_daily_basic", []),    # 每日指标
            ("service_fifteenMinute", []),  # TDX → 本机全市场15分钟DuckDB
            ("service_moneyflow", []),      # 资金流向
            ("service_stk_limit", []),      # 涨跌停价格
            ("service_suspend", []),        # 停复牌
            ("service_index_daily", []),    # 12 只指数日线
        ],
    ),
    (
        "阶段 3 — KPL / 开盘啦(无基础数据依赖)",
        [
            ("service_kpl_list", []),                  # 涨停榜(含炸板)
            ("service_kpl_concept_cons", []),          # 题材成分
            ("service_kpl_limit_performance", []),     # 涨停表现(同花顺实时数据)
        ],
    ),
    (
        "阶段 4 — 高级数据(部分需要日 K)",
        [
            ("service_top_list", []),       # 龙虎榜每日
            ("service_top_inst", []),       # 龙虎榜机构
            ("service_block_trade", []),    # 大宗交易
            ("service_ggt_daily", []),      # 港股通日成交
            ("service_hsgt_top10", []),     # 沪深股通十大成交股
            ("service_limit_list", []),     # 每日涨跌停列表
            ("service_margin", []),         # 融资融券交易汇总
            ("service_margin_detail", []),  # 融资融券交易明细
            ("service_cyq_perf", []),       # 每日筹码及胜率
        ],
    ),
    (
        "阶段 5 — 新闻",
        [
            ("service_news", []),         # 新闻快讯
            ("service_major_news", []),   # 长新闻
            ("service_cctv_news", []),    # 联播新闻
        ],
    ),
]

# 展平所有 service(用于 --skip / --only 解析)
ALL_SERVICES = [(stage_name, svc, args) for stage_name, items in STAGES for svc, args in items]


# ============================================================
# 核心逻辑
# ============================================================

def init_logger():
    """初始化 logger(写文件 + stdout)"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    log_path = LOG_DIR / "scheduler_once.log"
    logger.add(str(log_path), format=LOG_FORMAT, level=LOG_LEVEL, rotation="20 MB", encoding="utf-8")
    logger.add(lambda msg: print(msg, end=""), format=LOG_FORMAT, level=LOG_LEVEL)


def spawn_service(service_name: str, extra_args: list = None,
                   timeout: int = 1800) -> int:
    """同步启动一个 service 子进程,等完成返回 rc

    与 scheduler_updateData.py 的 spawn_service 区别:
    - 默认 sync=True,阻塞等待
    - 默认 timeout=1800s(30 分钟)
    - 不抛异常,只返回 rc(由调用方决定是否中止)
    """
    cmd = [sys.executable, "-m", f"service.{service_name}"]
    if extra_args:
        cmd.extend(extra_args)

    log_file = PROJECT_ROOT / "logs" / f"{service_name}.log"
    SCRIPTS_DIR = PROJECT_ROOT / "scripts"

    logger.info(f"  -> spawn: {' '.join(cmd)}")
    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT.parent) + ":" + os.environ.get("PYTHONPATH", "")}

    start = time.time()
    result = subprocess.run(
        cmd,
        cwd=str(SCRIPTS_DIR),
        stdout=open(log_file, "a"),
        stderr=subprocess.STDOUT,
        env=env,
        timeout=timeout,
    )
    elapsed = time.time() - start
    rc = result.returncode
    err_type = "业务中止" if rc == 2 else ("异常退出" if rc != 0 else "成功")
    logger.info(f"  <- {service_name} {err_type} rc={rc}, 耗时 {elapsed:.1f}s")
    return rc


def parse_args():
    p = argparse.ArgumentParser(
        description="一次性测试调度器:按依赖顺序依次跑完所有离线数据下载任务",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例:
  python -m scheduler.scheduler_once
  python -m scheduler.scheduler_once --only stage0
  python -m scheduler.scheduler_once --skip cctv_news
  python -m scheduler.scheduler_once --continue
  python -m scheduler.scheduler_once --only stage3 --skip kpl_limit_performance

  # 多选(2026-09-16 新增):
  python -m scheduler.scheduler_once --only kpl_list daily_basic index_daily
  python -m scheduler.scheduler_once --only kpl_list,daily_basic,index_daily   # 也支持逗号分隔
  python -m scheduler.scheduler_once --only stage0 stage3                     # 多阶段
""",
    )
    # nargs='+': 接受多个空格分隔的值(也可多次 --only)
    # 同时支持逗号分隔(parse_only_args 内处理)
    p.add_argument("--only", metavar="STAGE_OR_SVC", nargs="+", default=None,
                   help="指定要跑的阶段或 service。"
                        "例:--only stage0  或  --only kpl_list  或  --only kpl_list daily_basic index_daily"
                        "或  --only kpl_list,daily_basic,index_daily(逗号分隔)")
    p.add_argument("--skip", metavar="SVC", action="append", default=[],
                   help="跳过指定 service(可多次使用,例:--skip kpl_list --skip cctv_news)")
    p.add_argument("--continue", dest="continue_on_fail", action="store_true",
                   help="任一 service 失败时继续跑(默认:失败即中止)")
    p.add_argument("--dry-run", action="store_true",
                   help="只打印计划,不实际执行")
    return p.parse_args()


def parse_only_args(only_list) -> tuple:
    """解析 --only 参数(支持空格分隔 + 逗号分隔)

    Args:
        only_list: --only 值(可能是 None / 单个 / 多个)
                   例:None / ["stage3"] / ["kpl_list", "daily_basic", "index_daily"]
                       / ["kpl_list,daily_basic,index_daily"]

    Returns:
        (stage_filters: list[str], svc_filters: set[str])
        例: (["stage3"], {"kpl_list", "daily_basic"})
    """
    if not only_list:
        return [], set()

    # 1. 展平:把所有 --only 项用空格和逗号 split
    raw_tokens = []
    for item in only_list:
        raw_tokens.extend(item.replace(",", " ").split())

    stage_filters = []
    svc_filters = set()
    for tok in raw_tokens:
        tok = tok.strip()
        if not tok:
            continue
        if tok.startswith("stage"):
            stage_filters.append(tok)
        else:
            svc_filters.add(tok)
    return stage_filters, svc_filters


def build_plan(args):
    """根据 --only / --skip 构建执行计划

    支持:
    - --only 不传 → 跑全部
    - --only stageN → 跑该阶段所有 service
    - --only <svc> → 跑该 service
    - --only <svc1> <svc2> ... → 跑多个 service(空格分隔)
    - --only <svc1>,<svc2> → 跑多个 service(逗号分隔)
    - --only 可多次传(append)

    多个 service 按其在 STAGES 中的原始顺序执行(保持依赖顺序)
    """
    skip_set = set(args.skip)
    stage_filters, svc_filters = parse_only_args(args.only)

    plan = []
    for stage_idx, (stage_name, items) in enumerate(STAGES):
        stage_tag = f"stage{stage_idx}"

        # 决定本阶段是否要跑
        # - stage_filters 指定 → 只跑匹配的阶段
        # - svc_filters 指定 → 只跑匹配的 service
        # - 都没指定 → 跑全部
        if stage_filters:
            if stage_tag not in stage_filters:
                continue
            # 该阶段所有 service 都要(忽略 svc_filters)
            stage_items = items
        elif svc_filters:
            # 只保留 svc_filters 中的 service
            stage_items = [(s, a) for s, a in items if s.replace("service_", "") in svc_filters]
            if not stage_items:
                continue
        else:
            stage_items = items

        # --skip 过滤
        filtered = []
        for svc, args_list in stage_items:
            short = svc.replace("service_", "")
            if short in skip_set:
                continue
            filtered.append((svc, args_list))

        if filtered:
            plan.append((stage_name, filtered))

    return plan


def print_plan(plan):
    """打印执行计划(给人看)"""
    total = sum(len(items) for _, items in plan)
    print(f"\n{'='*70}")
    print(f"  计划:共 {len(plan)} 个阶段,{total} 个 service")
    print(f"{'='*70}")
    for stage_idx, (stage_name, items) in enumerate(plan):
        print(f"\n  [{stage_idx}] {stage_name}")
        for i, (svc, args_list) in enumerate(items):
            args_str = " ".join(args_list) if args_list else ""
            print(f"      {i+1}. {svc} {args_str}")
    print(f"\n{'='*70}\n")


def main():
    args = parse_args()
    init_logger()

    plan = build_plan(args)
    if not plan:
        logger.error("执行计划为空(--only / --skip 过滤后没有 service 可跑)")
        return 1

    print_plan(plan)

    if args.dry_run:
        logger.info("[dry-run] 只打印计划,不实际执行")
        return 0

    # 实际执行
    overall_t0 = time.time()
    total = sum(len(items) for _, items in plan)
    completed = 0
    failed = []

    logger.info("=" * 70)
    logger.info(f"  scheduler_once 启动 — {total} 个 service")
    logger.info(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  continue_on_fail: {args.continue_on_fail}")
    logger.info("=" * 70)

    for stage_idx, (stage_name, items) in enumerate(plan):
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"  [{stage_idx+1}/{len(plan)}] {stage_name}(共 {len(items)} 项)")
        logger.info("=" * 70)

        for svc, args_list in items:
            completed += 1
            logger.info(f"[{completed}/{total}] {svc} {' '.join(args_list)}")

            try:
                rc = spawn_service(svc, args_list)
            except subprocess.TimeoutExpired:
                logger.error(f"[{svc}] 超时(>{30}分钟),中止")
                failed.append((svc, "timeout"))
                if not args.continue_on_fail:
                    return 1
                continue
            except Exception as e:
                logger.error(f"[{svc}] 启动失败: {e}")
                failed.append((svc, "spawn_error"))
                if not args.continue_on_fail:
                    return 1
                continue

            if rc != 0:
                err_type = "业务中止" if rc == 2 else "异常退出"
                failed.append((svc, err_type))
                if not args.continue_on_fail:
                    logger.error(f"[{svc}] 失败 rc={rc}({err_type}),中止")
                    logger.error(f"  查看日志: {PROJECT_ROOT / 'logs' / f'{svc}.log'}")
                    return 1
                else:
                    logger.warning(f"[{svc}] 失败 rc={rc}({err_type}),--continue 模式继续")

    # 总结
    elapsed = time.time() - overall_t0
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"  scheduler_once 完成")
    logger.info(f"  总耗时: {elapsed/60:.1f} 分钟 ({elapsed:.1f}s)")
    logger.info(f"  完成: {completed - len(failed)}/{total}")
    if failed:
        logger.warning(f"  失败 {len(failed)} 个:")
        for svc, err in failed:
            logger.warning(f"    - {svc}: {err}")
    logger.info("=" * 70)

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
