"""
~/TradingAgent/offlineDataManager/scripts/service/common.py
service 通用工具:日期范围解析(根据 MyATM 思路)+ watchlist / 指数对构造
"""
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

from loguru import logger


def latest_completed_trade_date(conn, now: datetime | None = None) -> str:
    """Return the latest SSE trading day whose post-close data may be available."""
    now = now or datetime.now()
    cutoff = now.date() if (now.hour, now.minute) >= (16, 30) else (now - timedelta(days=1)).date()
    cutoff_text = cutoff.strftime("%Y%m%d")
    row = conn.execute(
        "SELECT MAX(cal_date) FROM tbl_cn_tradecal "
        "WHERE exchange = 'SSE' AND is_open = 1 AND cal_date <= ?",
        (cutoff_text,),
    ).fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"本地 SSE 交易日历无 {cutoff_text} 及之前的开市日")
    return str(row[0])


# ============================================================================
# 2026-09-17 新加:watchlist-driven service 共用工具
# 3 个 service 复用(service_market_minute / _ticks / _index)
# 设计:
#   - load_watchlist_pairs:从 csv 读 (ts_code, trade_date) 对
#   - build_ts_codes_pairs:从 --ts-codes + --start/--end/--trade-date 笛卡尔积算对
#   - build_index_pairs:指数服务专用,默认 INDEX_CODES_HERE × 日期范围
#   - run_market_loop:循环调 down.update_*(...),聚合统计,统一日志
# ============================================================================

def load_watchlist_pairs(watchlist_csv: str) -> list:
    """从 watchlist csv 读 (ts_code, trade_date) 对列表

    csv 必须有 ts_code 和 trade_date 两列(YYYYMMDD 或 YYYY-MM-DD 都接受,
    内部统一转 YYYY-MM-DD)

    Args:
        watchlist_csv: csv 文件绝对路径或相对 PROJECT_ROOT/scripts 的相对路径

    Returns:
        [(ts_code, trade_date), ...] 列表,dash 格式日期

    Raises:
        FileNotFoundError, ValueError(缺列)
    """
    import pandas as pd
    from core.offline_db_client import _ymd_compact_to_dash

    csv_path = Path(watchlist_csv)
    if not csv_path.is_absolute():
        csv_path = PROJECT_ROOT / "scripts" / watchlist_csv
    if not csv_path.exists():
        # 也允许 policyStudy 下的 watchlist(2026-09-17 新加,跨项目复用)
        alt = PROJECT_ROOT / "policyStudy" / "policy" / "题材涨停研究" / "watchlist" / Path(watchlist_csv).name
        if alt.exists():
            csv_path = alt
        else:
            raise FileNotFoundError(f"watchlist 不存在: {csv_path}")

    df = pd.read_csv(csv_path, dtype={"trade_date": str, "ts_code": str})
    required = {"ts_code", "trade_date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"watchlist 缺少必要列 {missing}: {csv_path}")

    df = df.drop_duplicates(subset=["ts_code", "trade_date"]).reset_index(drop=True)
    pairs = []
    for _, r in df.iterrows():
        td = _ymd_compact_to_dash(str(r["trade_date"]))
        pairs.append((str(r["ts_code"]), td))
    return pairs


def build_ts_codes_pairs(args) -> list:
    """从 --ts-codes + 日期参数 笛卡尔积算 (ts_code, trade_date) 对

    日期三种模式(按优先级):
      1. --trade-date 单日
      2. --start-date / --end-date 日期段(过交易日历过滤周末/节假日)
      3. 无日期参数(空,需业务层校验)

    Args:
        args: argparse 参数,需要含 ts_codes / trade_date / start_date / end_date

    Returns:
        [(ts_code, trade_date), ...] 列表,dash 格式
    """
    from core.offline_db_client import _ymd_dash_to_compact, _ymd_compact_to_dash, get_tradecal

    ts_codes_str = getattr(args, "ts_codes", None)
    if not ts_codes_str:
        return []

    ts_codes = [s.strip() for s in ts_codes_str.split(",") if s.strip()]
    if not ts_codes:
        return []

    # 1. --trade-date 单日
    if getattr(args, "trade_date", None):
        td_dash = _ymd_compact_to_dash(args.trade_date)
        return [(tc, td_dash) for tc in ts_codes]

    # 2. --start-date / --end-date 区间(过交易日历)
    sd = getattr(args, "start_date", None)
    ed = getattr(args, "end_date", None)
    if sd and ed:
        sd_compact = _ymd_dash_to_compact(sd)
        ed_compact = _ymd_dash_to_compact(ed)
        cal_df = get_tradecal(start_date=sd_compact, end_date=ed_compact, market="SSE", is_open=True)
        if cal_df is None or len(cal_df) == 0:
            logger.warning(f"[build_ts_codes_pairs] 区间 {sd} ~ {ed} 交易日历为空")
            return []
        trade_dates = [_ymd_compact_to_dash(d) for d in cal_df["cal_date"].astype(str).tolist()]
        pairs = []
        for tc in ts_codes:
            for td in trade_dates:
                pairs.append((tc, td))
        return pairs

    # 3. 无日期参数
    return []


def build_index_pairs(args) -> list:
    """指数 service 专用:默认 INDEX_CODES_HERE × 日期范围

    日期三种模式(跟 ts-codes 同):
      1. --trade-date 单日 × INDEX_CODES_HERE
      2. --start-date / --end-date 区间 × INDEX_CODES_HERE
      3. 空(返回空,业务层校验)
    """
    from core.offline_db_client import _ymd_dash_to_compact, _ymd_compact_to_dash, get_tradecal
    from core.offline_downloader import INDEX_CODES_HERE

    # 1. --trade-date 单日
    if getattr(args, "trade_date", None):
        td_dash = _ymd_compact_to_dash(args.trade_date)
        return [(tc, td_dash) for tc in INDEX_CODES_HERE]

    # 2. --start-date / --end-date 区间
    sd = getattr(args, "start_date", None)
    ed = getattr(args, "end_date", None)
    if sd and ed:
        sd_compact = _ymd_dash_to_compact(sd)
        ed_compact = _ymd_dash_to_compact(ed)
        cal_df = get_tradecal(start_date=sd_compact, end_date=ed_compact, market="SSE", is_open=True)
        if cal_df is None or len(cal_df) == 0:
            logger.warning(f"[build_index_pairs] 区间 {sd} ~ {ed} 交易日历为空")
            return []
        trade_dates = [_ymd_compact_to_dash(d) for d in cal_df["cal_date"].astype(str).tolist()]
        pairs = []
        for tc in INDEX_CODES_HERE:
            for td in trade_dates:
                pairs.append((tc, td))
        return pairs

    # 3. 空
    return []


def resolve_market_pairs(args) -> tuple:
    """watchlist-driven service 共用入口:解析 args → 待下载 (ts_code, trade_date) 对

    优先级:
      1. --watchlist csv:读 (ts_code, trade_date) 对
      2. --ts-codes + 日期参数:笛卡尔积
      3. 都没有:返回 ([], 'no_pairs')

    Returns:
        (pairs: list, source: str)
        source = 'watchlist' / 'ts_codes' / 'no_pairs'
    """
    watchlist = getattr(args, "watchlist", None)
    if watchlist:
        return load_watchlist_pairs(watchlist), "watchlist"

    ts_codes = getattr(args, "ts_codes", None)
    if ts_codes:
        return build_ts_codes_pairs(args), "ts_codes"

    return [], "no_pairs"


def run_market_loop(
    down,
    pairs: list,
    *,
    update_method: str,
    client,
    rate: float = 0.15,
    force: bool = False,
    batch_size: int = 100,
    logger=None,
) -> dict:
    """循环调 down.update_*(...) 拉取每对 (ts_code, trade_date)

    流程:
      - 每 batch_size 对打印一次进度
      - 异常隔离:某对失败不中断,继续下一对
      - 已下载(inserted == 0)归为 skipped,新写入归为 inserted

    Args:
        down: CNDataDown 实例
        pairs: [(ts_code, trade_date), ...] 列表
        update_method: 'update_minute' / 'update_ticks' / 'update_minute_index'
        client: TdxClient 实例
        rate: 限速秒数
        force: 强制重拉
        batch_size: 每 N 对打印一次进度
        logger: loguru logger 实例(可选)

    Returns:
        {
            "total": 总对数,
            "inserted": 新插入对数,
            "skipped": 跳过对数(已下载或拉取空),
            "failed": 失败对数(异常),
            "elapsed": 总耗时秒,
            "inserted_rows": 总新插入行数,
        }
    """
    import time
    log = logger if logger is not None else globals().get("logger")

    method = getattr(down, update_method, None)
    if method is None:
        raise AttributeError(f"CNDataDown 没有 {update_method} 方法")

    stats = {"total": len(pairs), "inserted": 0, "skipped": 0, "failed": 0, "inserted_rows": 0}
    t0 = time.time()

    for i, (ts_code, trade_date) in enumerate(pairs, 1):
        try:
            inserted = method(
                ts_code, trade_date,
                client=client,
                rate=rate,
                force=force,
            )
            if inserted == 0:
                stats["skipped"] += 1
            else:
                stats["inserted"] += 1
                stats["inserted_rows"] += inserted
        except Exception as e:
            stats["failed"] += 1
            if log:
                log.error(f"  [{i}/{len(pairs)}] {trade_date} {ts_code} 异常: {e}")

        # 进度日志
        if i % batch_size == 0 or i == len(pairs):
            elapsed = time.time() - t0
            speed = i / elapsed if elapsed > 0 else 0
            msg = (f"  进度 [{i}/{len(pairs)}] "
                   f"inserted={stats['inserted']} skipped={stats['skipped']} "
                   f"failed={stats['failed']} speed={speed:.1f} 对/s elapsed={elapsed:.1f}s")
            if log:
                log.info(msg)
            else:
                print(msg)

    stats["elapsed"] = time.time() - t0
    return stats


def add_market_args(parser: argparse.ArgumentParser, default_rate: float = 0.15) -> None:
    """给 service argparse 加上 watchlist-driven 共用参数

    加的 5 个参数:
      --watchlist   csv 路径(可选)
      --ts-codes    逗号分隔的 ts_code 列表(可选)
      --trade-date  单日 YYYYMMDD(可选)
      --start-date  日期段起始 YYYY-MM-DD(可选)
      --end-date    日期段结束 YYYY-MM-DD(可选)
      --rate        限速秒数(默认 0.15)
      --force       强制重拉
    """
    parser.add_argument("--watchlist", type=str, default=None,
                        help="watchlist csv 路径(必须有 ts_code + trade_date 列);指定后覆盖 --ts-codes")
    parser.add_argument("--ts-codes", type=str, default=None,
                        help="逗号分隔的 ts_code 列表,如 '000006.SZ,000636.SZ'")
    parser.add_argument("--trade-date", type=str, default=None,
                        help="单日 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--start-date", type=str, default=None,
                        help="日期段起始 YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None,
                        help="日期段结束 YYYY-MM-DD")
    parser.add_argument("--rate", type=float, default=default_rate,
                        help=f"限速秒数(默认 {default_rate})")
    parser.add_argument("--force", action="store_true", help="强制重拉(忽略 ctrl)")


def resolve_date_range(
    conn,
    ctrl_key: str,
    args,
    default_start: str = "20150101",
    ctrl_store: str = "basic",
) -> tuple:
    """根据 args + ctrl 决定实际拉取日期范围

    优先级:
    1. --trade-date(单日,start_date = end_date = trade_date)
    2. --start-date + --end-date(日期段)
    3. 增量(默认 start_date=2015-01-01, end_date=今天)

    start_date 必须 ≥ tbl_ctrl 里的日期 + 1(避免重拉已有数据)

    Args:
        conn: SQLite 连接(用来读对应数据库的 ctrl 表)
        ctrl_key: tbl_ctrl 里对应的 key
        args: argparse 参数,需要含 trade_date / start_date / end_date
        default_start: 默认 start_date(YYYYMMDD)
        ctrl_store: 断点所在数据库，支持 basic / kpl

    Returns:
        (start_date, end_date, desc) 三元组
    """
    from core.offline_db_client import get_ctrl, get_ctrl_kpl

    today = datetime.now().strftime("%Y%m%d")

    # 1. --trade-date 优先(单日)
    if getattr(args, "trade_date", None):
        td = args.trade_date
        return td, td, f"单日 {td}"

    # 2. --start-date + --end-date(日期段)
    # 兼容老的 --start / --end(week / month 用)
    sd = (getattr(args, "start_date", None) or
          getattr(args, "start", None) or
          default_start)
    ed = (getattr(args, "end_date", None) or
          getattr(args, "end", None) or
          today)

    # 3. start_date 必须 ≥ ctrl(允许 ctrl 当天重拉,主键去重保证幂等)
    if ctrl_store == "basic":
        last_ctrl = get_ctrl(conn, ctrl_key)
    elif ctrl_store == "kpl":
        last_ctrl = get_ctrl_kpl(ctrl_key, conn=conn)
    else:
        raise ValueError(f"未知 ctrl_store: {ctrl_store!r},只支持 'basic' / 'kpl'")
    if last_ctrl:
        # 用户最新规则:start_date < ctrl_day → 用 ctrl_day 替代(而不是 ctrl_day + 1)
        # 考虑 ctrl 可能不全,重跑 ctrl_day 一次更安全
        if sd < last_ctrl:
            logger.warning(f"[{ctrl_key}] start_date {sd} < ctrl {last_ctrl},自动调整为 {last_ctrl}")
            sd = last_ctrl

    return sd, ed, f"日期段 {sd} ~ {ed}"
