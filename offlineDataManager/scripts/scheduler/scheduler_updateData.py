"""
~/TradingAgent/offlineDataManager/scripts/scheduler/scheduler_updateData.py
cn_data 多进程调度器 — 用 subprocess.run 同步启动各个 service 子进程
(参考 MyATM database_scheduler.py 的多 service 架构)

负责:
  1. 按依赖链顺序调度 service_daily → adj_factor → week → month 等
  2. launchd KeepAlive 模式保证进程挂了自动重启
  3. 每日 09:00 / 20:00 / 22:00 跑完整更新
  4. 每 10 分钟跑 news 增量
  5. service_week / service_month 自带 ctrl 一致性校验
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.offline_db_client import get_conn, get_ctrl, show_status
from service.common import latest_completed_trade_date


# ============================================================
# 时间表(参考 MyATM)
# ============================================================
# 每天 3 次完整数据库更新
STR_TIME_FULL_DB_1 = "18:00"  # 收盘后(15:00 A股收盘),基础行情已稳定
STR_TIME_FULL_DB_2 = "20:00"
STR_TIME_FULL_DB_3 = "22:00"

STR_TIME_MORNING = "09:05"  # 早上补:涨跌停价(昨天收盘) + kpl 涨停榜(昨天收盘)

# kpl 涨停表现详情(实时补 kpl_list 滞后)
STR_TIME_KPL_LP_1 = "16:30"  # 收盘后 30 分钟(15:00 A股收盘)
STR_TIME_KPL_LP_2 = "20:00"  # 晚间再补一次

# 前一自然日新闻在05:00抓取窗口结束后做一次LLM提炼，避免与高频新闻写入竞争。
STR_TIME_NEWS_LLM = "05:10"
NEWS_LLM_MODEL = os.environ.get(
    "THEME_VLLM_MODEL", "/home/sysadmin/data/disk02/jc/models/qwen3.8-27b-fp8"
)

# News 更新窗口:08:00 - 次日 05:00
NEWS_START = dtime(8, 0)
NEWS_END = dtime(5, 0)
NEWS_INTERVAL_MIN = 10  # 每 10 分钟


# ============================================================
# 工具函数
# ============================================================
def get_target_date() -> str:
    """获取最近已收盘的交易日。"""
    with get_conn("basic") as conn:
        return latest_completed_trade_date(conn)


def is_news_window() -> bool:
    """判断当前是否在 news 运行时段"""
    now = datetime.now().time()
    return (now >= NEWS_START) or (now <= NEWS_END)


def spawn_service(service_name: str, extra_args: list = None, sync: bool = False,
                   abort_on_failure: bool = True,
                   extra_env: dict[str, str] | None = None) -> subprocess.Popen:
    """启动一个 service 子进程

    Args:
        service_name: service 文件名(不含 .py),如 'service_news'
        extra_args: 额外参数,如 ['--date', '20260910']
        sync: True=阻塞等待完成,False=后台运行
        abort_on_failure: True=子进程 rc != 0 时抛异常(让调用方决定是否中止后续)

    Returns:
        subprocess.Popen 进程对象(sync=False 时),或 returncode(sync=True 时)

    Raises:
        RuntimeError: 子进程 rc != 0 且 abort_on_failure=True
            (rc=1 = 异常退出,rc=2 = 业务中止/校验失败)
    """
    cmd = [sys.executable, "-m", f"service.{service_name}"]
    if extra_args:
        cmd.extend(extra_args)

    log_file = PROJECT_ROOT / "logs" / f"{service_name}.log"
    # service 文件在 PROJECT_ROOT/scripts/service/ 下,所以 cwd 是 scripts/
    SCRIPTS_DIR = PROJECT_ROOT / "scripts"

    logger.info(f"  -> spawn: {' '.join(cmd)}")

    # PYTHONPATH 必须包含 ~/TradingAgent/,这样子进程能 import coreClient.tushare_client
    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT.parent) + ":" + os.environ.get("PYTHONPATH", "")}
    if extra_env:
        env.update(extra_env)

    if sync:
        # 同步模式:等子进程跑完再返回(用于全量更新)
        try:
            with open(log_file, "a") as output:
                result = subprocess.run(
                    cmd,
                    cwd=str(SCRIPTS_DIR),
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    env=env,
                    timeout=1800,  # 30 分钟超时
                )
        except subprocess.TimeoutExpired as exc:
            # subprocess.run 已终止并等待超时子进程。转为既有业务异常，
            # 防止漏捕异常杀死 daemon、被 launchd 重启后又从头重复更新。
            if abort_on_failure:
                raise RuntimeError(
                    f"[{service_name}] 超时 {exc.timeout}s,args={extra_args}\n"
                    f"  查看日志: {log_file}"
                ) from exc
            logger.error(f"[{service_name}] 超时 {exc.timeout}s,args={extra_args}")
            return 124
        rc = result.returncode
        if rc != 0 and abort_on_failure:
            # 区分异常退出 vs 业务中止
            # rc=2 是 service 自己定义的"业务中止/校验失败"
            # rc=1 是 Python 异常
            err_type = "业务中止" if rc == 2 else "异常退出"
            raise RuntimeError(
                f"[{service_name}] {err_type} rc={rc},args={extra_args}\n"
                f"  查看日志: {log_file}"
            )
        return rc
    else:
        # 异步模式:后台跑,不阻塞(用于 news 高频更新)
        return subprocess.Popen(
            cmd,
            cwd=str(SCRIPTS_DIR),
            stdout=open(log_file, "a"),
            stderr=subprocess.STDOUT,
            env=env,
        )


# ============================================================
# 调度任务
# ============================================================


def task_full_update():
    """完整数据库更新(每天 3 次)— 同步启动各 service

    调度规则:
      - scheduler 只负责**顺序调用**,不掺和业务校验
      - 业务校验在 service 内部(service_week / service_month 自带 ctrl 一致性检查)
      - 任一 service rc != 0 → RuntimeError,中止后续

    依赖链(必须严格顺序):
      daily ─→ adj_factor ─→ week ─→ month
      (week / month 内部会校验 ctrl.cn_daily 和 ctrl.cn_adj_factor 是否一致)

    返回值处理:
      - rc=0:成功
      - rc=2:业务中止(校验失败,数据未就绪)
      - rc!=0 && rc!=2:异常退出
    """
    # 先刷新今天的交易日历，再判断最近已收盘交易日。
    # 如果先读本地日历，日历断点停在前一交易日时会把今天误判为
    # “无新交易日”，导致完整更新在进入 tradecal service 前就被跳过。
    today = datetime.now().strftime("%Y%m%d")
    try:
        spawn_service("service_tradecal", ["--trade-date", today], sync=True)
    except RuntimeError as e:
        logger.error(f"[完整更新] 交易日历刷新失败,中止: {e}")
        return

    target = get_target_date()
    # 周末、节假日及盘前可能只需确认上一个交易日已完成。
    # 避免在没有新交易日时重复执行周月全量聚合与大量历史 API 调用。
    if target < datetime.now().strftime("%Y%m%d"):
        with get_conn("basic") as conn:
            daily_ctrl = get_ctrl(conn, "cn_daily")
            adj_ctrl = get_ctrl(conn, "cn_adj_factor")
            if daily_ctrl and adj_ctrl and daily_ctrl >= target and adj_ctrl >= target:
                try:
                    spawn_service("service_fifteenMinute", [], sync=True)
                except RuntimeError as e:
                    logger.error(f"[完整更新] fifteenMin补跑失败(继续): {e}")
                try:
                    spawn_service("service_oneMinute", [], sync=True)
                except RuntimeError as e:
                    logger.error(f"[完整更新] oneMin补跑失败(继续跳过其他重复更新): {e}")
                logger.info(f"[完整更新] 最近交易日 {target} 已完成日线与复权，跳过非交易时段重复更新")
                return
    logger.info("=" * 60)
    logger.info(f"[完整更新] 目标日期 {target}")
    logger.info("=" * 60)

    t0 = time.time()

    # === 第 1 阶段:基础数据 ===
    try:
        spawn_service("service_basic", [], sync=True)
    except RuntimeError as e:
        logger.error(f"[完整更新] 基础数据失败,中止: {e}")
        return

    # === 第 2 阶段:daily ─→ adj_factor ===
    try:
        spawn_service("service_daily", ["--trade-date", target], sync=True)
        spawn_service("service_adj_factor", ["--end-date", target], sync=True)
    except RuntimeError as e:
        logger.error(f"[完整更新] daily/adj_factor 失败,中止: {e}")
        return

    # 两种分钟数据都写入日复权因子，必须等待 daily/adj_factor 完成。
    try:
        spawn_service("service_fifteenMinute", [], sync=True)
    except RuntimeError as e:
        logger.error(f"[完整更新] fifteenMin增量失败(继续其他更新): {e}")
    try:
        spawn_service("service_oneMinute", [], sync=True)
    except RuntimeError as e:
        logger.error(f"[完整更新] oneMin增量失败(继续其他更新): {e}")

    # === 第 3 阶段:周月 K ===
    # 每种周期内部成对原子写入。派生表失败不应阻断独立的 KPL/新闻/
    # daily_basic 等原始数据更新；保留旧派生表，晚间任务再重试。
    for service in ("service_week", "service_month"):
        try:
            spawn_service(service, [], sync=True)
        except RuntimeError as e:
            logger.error(f"[完整更新] {service} 失败(继续其他更新): {e}")

    # === 第 4 阶段:先完成 KPL 主事实，再增量刷新题材知识库 ===
    for svc, args in [
        ("service_kpl_list", ["--trade-date", target]),
        ("service_kpl_concept_cons", ["--trade-date", target]),
        ("service_kpl_limit_performance", ["--trade-date", target]),  # 兜底补 kpl_list 滞后
    ]:
        try:
            spawn_service(svc, args, sync=True)
        except RuntimeError as e:
            logger.error(f"[完整更新] {svc} 失败(继续下一项): {e}")
    try:
        spawn_service("service_theme_graph", ["--mode", "incremental"], sync=True)
    except RuntimeError as e:
        logger.error(f"[完整更新] 题材知识库增量失败(继续其他更新): {e}")

    # 其他相互独立的数据服务不会阻塞题材图谱刷新。
    for svc, args in [
        ("service_stk_limit", []),  # 涨跌停价,跟 kpl_list 一样需要早上更新
        ("service_suspend", []),  # 停复牌,断点增量(按月批量,2026-09-15 新增)
        ("service_top_list", []),  # 龙虎榜每日活跃(2026-09-15 新增,按日循环 ~1400 次)
        ("service_top_inst", []),  # 龙虎榜机构明细(2026-09-15 新增,按日循环 ~1400 次)
        ("service_block_trade", []),  # 大宗交易(2026-09-15 新增,按月循环 + OFFSET 分页)
        ("service_ggt_daily", []),  # 港股通日成交(2026-09-15 新增,按月循环)
        ("service_hsgt_top10", []),  # 沪深股通十大成交股(2026-09-15 新增,按月循环)
        ("service_limit_list", []),  # 每日涨跌停列表(2026-09-15 新增,按月循环 × 3 个 limit_type)
        ("service_moneyflow", []),  # 个股资金流向(2026-09-15 新增,按月循环 + OFFSET 分页)
        ("service_margin", []),  # 融资融券交易汇总(2026-09-15 新增,挂 task_morning,按日循环)
        ("service_margin_detail", []),  # 融资融券交易明细(2026-09-15 新增,挂 task_morning,按月+OFFSET)
        ("service_cyq_perf", []),  # 每日筹码及胜率(2026-09-15 新增,按月+OFFSET,数据 18-21 点更新)
        ("service_daily_basic", []),  # 每日指标(2026-09-15 新增,按日,数据 15-17 点更新)
        ("service_index_basic", []),  # 指数基本信息(2026-09-15 新增,每次覆盖更新,~30 秒)
        ("service_index_daily", []),  # 12 只指数日线行情(2026-09-15 新增,按 ts_code 循环 + limit/offset)
        ("service_cctv_news", []),
    ]:
        try:
            spawn_service(svc, args, sync=True)
        except RuntimeError as e:
            logger.error(f"[完整更新] {svc} 失败(继续下一项): {e}")

    logger.info(f"[完整更新] 全部完成,总用时 {time.time()-t0:.1f}s")


def task_morning_update():
    """早上快速更新(每天 09:05)— 只补:涨跌停价 + 开盘啦涨停榜 + 联播新闻

    跟 task_full_update 的区别:
    - task_full_update 太重(基础数据 + 日 K + 周月 K + kpl + news + 涨跌停)
    - 早上 09:00 之前用户已开盘前打开应用,需要快速拿到"昨天收盘后的状态":
      * 涨跌停价(用做今天的判定基准)
      * 开盘啦涨停榜(昨天涨停/跌停/炸板等)
      * 联播新闻(昨天政策/重要事件)
    - 09:05 触发,1 分钟内跑完
    """
    logger.info("=" * 60)
    logger.info("[早上更新] 09:05 快速补:stk_limit + kpl_list + cctv_news")
    logger.info("=" * 60)

    t0 = time.time()
    target = get_target_date()

    for svc, args in [
        ("service_stk_limit", []),  # 涨跌停价,跟 kpl_list 一样需要早上更新
        ("service_kpl_list", ["--trade-date", target]),  # 昨天涨停榜
        ("service_margin", []),  # 融资融券交易汇总,2026-09-15 新增
        ("service_margin_detail", []),  # 融资融券交易明细,2026-09-15 新增
        ("service_cctv_news", []),  # 联播新闻,早上看昨天汇总
    ]:
        try:
            spawn_service(svc, args, sync=True)
        except RuntimeError as e:
            logger.error(f"[早上更新] {svc} 失败,中止: {e}")
            return

    # 晚间 KPL 如有修订，内容哈希会触发对应日期替换；无变化时本步骤
    # 只扫描七天回看窗口并直接跳过。
    try:
        spawn_service("service_theme_graph", ["--mode", "incremental"], sync=True)
    except RuntimeError as e:
        logger.error(f"[早上更新] 题材知识库增量失败(继续): {e}")

    logger.info(f"[早上更新] 全部完成,总用时 {time.time()-t0:.1f}s")


def task_news_update():
    """News 更新(每 10 分钟)— 异步启动 news 和 major_news"""
    if not is_news_window():
        logger.debug("[news] 不在运行时段,跳过")
        return

    logger.info("[news] 启动 news + major_news service")
    # 用异步启动,不等返回(高频任务,不希望阻塞下一轮)
    spawn_service("service_news", sync=False)
    spawn_service("service_major_news", sync=False)


def task_news_llm_update():
    """05:10生成前一自然日 Major News 分析及 Obsidian 文档。"""
    target = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    logger.info(f"[news_llm] 启动 {target} Major News 每日分析")
    spawn_service(
        "service_daily_major_news_analysis",
        ["--trade-date", target],
        sync=False,
        extra_env={
            "THEME_VLLM_TRANSPORT": os.environ.get("THEME_VLLM_TRANSPORT", "ssh"),
            "THEME_VLLM_MODEL": NEWS_LLM_MODEL,
        },
    )


def task_kpl_limit_performance():
    """kpl 涨停表现详情(每天 16:30 / 20:00)— 实时补 kpl_list 滞后

    16:30 收盘后,kpl API 已有当天完整涨停数据(等 30 分钟确保涨停最终)
    20:00 再补一次,覆盖部分停牌后盘后涨停/晚更新
    """
    logger.info("[kpl_limit_performance] 启动 service")
    # 增量模式(默认参数),自动从本周周一+ctrl.max_date 较大值到今天
    try:
        spawn_service("service_kpl_limit_performance", [], sync=True)
    except RuntimeError as e:
        logger.error(f"[kpl_limit_performance] 失败(继续): {e}")


# ============================================================
# 调度器主循环
# ============================================================
def run_daemon():
    """多进程调度器主循环"""
    import schedule

    logger.info("=" * 60)
    logger.info("cn_data 多进程调度器启动")
    logger.info(f"  早上更新: {STR_TIME_MORNING} (stk_limit + kpl_list + cctv_news)")
    logger.info(f"  完整更新: {STR_TIME_FULL_DB_1} / {STR_TIME_FULL_DB_2} / {STR_TIME_FULL_DB_3}")
    logger.info(f"  kpl_limit_performance: {STR_TIME_KPL_LP_1} / {STR_TIME_KPL_LP_2}")
    logger.info(f"  News: 每 {NEWS_INTERVAL_MIN} 分钟 (08:00 - 05:00)")
    logger.info(f"  News LLM: 每天 {STR_TIME_NEWS_LLM} 分析前一自然日 Major News")
    logger.info("=" * 60)

    # 早上快速更新:09:05 只补涨跌停价 + 涨停榜
    schedule.every().day.at(STR_TIME_MORNING).do(task_morning_update)

    # 完整数据库更新:每天 3 次(18:00 / 20:00 / 22:00,均在收盘后)
    for t in [STR_TIME_FULL_DB_1, STR_TIME_FULL_DB_2, STR_TIME_FULL_DB_3]:
        schedule.every().day.at(t).do(task_full_update)

    # kpl 涨停表现详情:每天 2 次(16:30 收盘后 + 20:00)
    schedule.every().day.at(STR_TIME_KPL_LP_1).do(task_kpl_limit_performance)
    schedule.every().day.at(STR_TIME_KPL_LP_2).do(task_kpl_limit_performance)

    # News:每 10 分钟
    schedule.every(NEWS_INTERVAL_MIN).minutes.do(task_news_update)
    schedule.every().day.at(STR_TIME_NEWS_LLM).do(task_news_llm_update)

    # 启动时立即跑一次完整更新
    logger.info("[启动] 立即执行一次完整更新...")
    task_full_update()

    logger.info("进入调度循环...")
    while True:
        schedule.run_pending()
        time.sleep(30)


# ============================================================
# CLI 入口
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="cn_data 多进程调度器")
    parser.add_argument("--daemon", action="store_true", help="常驻调度进程")
    parser.add_argument("--once", type=str, help="跑一次指定 service(如 service_news)")
    parser.add_argument("--full", action="store_true", help="立即跑完整更新(所有 service)")
    parser.add_argument("--status", action="store_true", help="查看 DB 状态")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.daemon:
        run_daemon()
        return

    if args.once:
        # 跑单个 service
        rc = spawn_service(args.once, sync=True)
        print(f"service {args.once} rc={rc}")
        return

    if args.full:
        task_full_update()
        return

    # 默认:跑一次完整更新
    task_full_update()


if __name__ == "__main__":
    main()
