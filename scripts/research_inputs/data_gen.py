"""
题材涨停研究 — 历史数据批量生成(v3.1,2026-09-16 重构;v3.2,2026-09-16 砍掉 day;
v3.3,2026-09-17 minute 窗口扩展为「涨停日前 1 后 2 共 4 天」;
v3.4,2026-09-17 ticks 改造为同 minute 模式,默认 forward=0 backward=0 即只下载涨停当日;
v3.5,2026-09-17 新增 minute_index kind,按 watchlist 日期范围拉 5 个大盘指数分钟数据;
       交易日历接口迁至 offline_db_client.get_tscode_calendar;
v3.7,2026-09-17 重构:data_gen 只负责窗口计算 + watchlist 编排,实际下载委托给
       offlineDataManager 的 3 个 service(service_intraday / service_ticks / service_intraday_index))

数据落地结构(单表 + ctrl 表,policyStudy v3):
  policy_minute.db:  tbl_minute       + tbl_minute_ctrl
                      tbl_minute_index + tbl_minute_index_ctrl   (v3.5 新增,大盘指数)
  policy_ticks.db:   tbl_tick         + tbl_tick_ctrl

注(2026-09-16 v3.2 起):
  - day 数据**不存** policy 库,统一走 offlineDataManager.get_day
  - 因此 --only day / pending_day / day_xxx_total 已全部移除
  - data_gen 只负责编排:算窗口 → 写临时 watchlist → 调 service → 抽样验证

注(2026-09-17 v3.3 起):
  - minute 窗口从「单日(涨停日)」改为「涨停日前 1 交易日 + 后 2 交易日」共 4 天
  - 用个股交易日历(从 offline 日线拉取,不复权)判断真实交易日(避开周末/节假日/停牌)
  - 多连板相邻日区间用 hash set 去重,避免重复下载

注(2026-09-17 v3.5 起):
  - 新增 minute_index kind:大盘指数分钟,日期范围 = watchlist 的 min/max trade_date
  - 默认不跑(--only minute_index 才触发)

注(2026-09-17 v3.7 起):
  - data_gen 本身不再持有 TdxClient、不再循环拉数据
  - 计算出每种数据的 (ts_code, trade_date) 对列表后,写入临时 watchlist csv
  - 调对应 service 子进程:
      - service_intraday.py --watchlist <temp.csv>            (个股分钟)
      - service_ticks.py     --watchlist <temp.csv>            (个股分笔)
      - service_intraday_index.py --start-date <min> --end-date <max>  (5 指数)
  - 这样 downloader / db_client 的 SQL 责任保持在 offlineDataManager 域内,
    policyStudy 只是编排 + 窗口计算

用法:
  cd ~/TradingAgent/policyStudy/policy/题材涨停研究
  python3 data_gen.py watchlist_题材涨停研究_20260908_20260911
  python3 data_gen.py watchlist_tczt_20260101_20260831 --only minute
  python3 data_gen.py watchlist_tczt_20260101_20260831 --only ticks
  python3 data_gen.py watchlist_tczt_20260101_20260831 --only minute_index
  python3 data_gen.py /absolute/path/to/watchlist.csv
  python3 data_gen.py --help

参数:
  watchlist_name       watchlist 名字(不带 .csv 后缀,自动找 <watchlist_dir>/ 目录)
                       或绝对路径(直接用)
  --watchlist-dir      watchlist 目录(默认 <策略目录>/watchlist/)
  --only               只跑一种数据,默认都跑(minute + ticks);
                       minute_index 只拉 5 指数,默认不跑
  --force              强制覆盖(忽略 ctrl 表过滤,重新下载),会传给 service
  --rate               限速秒数(默认 0.15),会传给 service
  --dry-run            dry-run:只算窗口,不调 service

流程(v3.7 重构后):
  1. 解析 watchlist 路径(传名字找目录,传路径用绝对路径)
  2. 读 watchlist 算 (ts_code, trade_date) 对 + 日期范围 min/max
  3. ctrl 表 filter_pending(只下载缺失的),--force 跳过过滤
  4. 对每种数据:
     - minute: 窗口扩展为涨停日前 1 后 2 → 写临时 watchlist → 调 service_intraday
     - ticks:  窗口扩展为涨停日   → 写临时 watchlist → 调 service_ticks
     - minute_index: 取 watchlist 日期范围 → 调 service_intraday_index (传 --start-date/--end-date)
  5. 抽样验证(从 db_client 读)
"""
import sys
import argparse
import subprocess
import tempfile
import os
from datetime import datetime
from pathlib import Path
from typing import List, Set, Tuple

# 代码、数据库与研究输入分别由 TDM paths 解析，旧环境变量不能把代码路径指回策略仓。
# (offlineDataManager 需要 sys.path,因为 v3.3+ 起 minute/ticks 窗口要调 offline.get_tscode_calendar)
# 注:只能加 'scripts/'(不带 '/core'),因为 offline_db_client 内部需要
# sys.path 有 'scripts/' 才能 import 'config.settings'(见 offline_db_client.py:20-25)
#
# 注(2026-09-17 重构后):
#   - 原 `policyStudy/scripts/policy_db_client.py` 已删除,policy 接口并入 offlineDataManager
#   - 所有 policy 接口走 `from core.offline_db_client import ...`(facade re-export 模式)
#   - sys.path 不再需要 policyStudy/scripts
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from coreClient._paths import manager_root, offline_data_dir, research_root

ROOT = manager_root(__file__, 2)
RESEARCH_ROOT = research_root()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))

import pandas as pd
# tdx_client 不再直接 import — service 子进程自己负责 TdxClient 连接(2026-09-17 v3.7)
from core.offline_db_client import get_tscode_calendar

from core.offline_db_client import (
    ensure_schema,
    filter_pending,
    count_rows,
    _ymd_compact_to_dash,         # YYYYMMDD → YYYY-MM-DD,指数窗口计算要用
    _ymd_dash_to_compact,         # YYYY-MM-DD → YYYYMMDD,指数窗口计算要用
    # PolicyDBClient 不在这里 import,抽样验证分支内延迟 import
    # 个股交易日历走 offline.get_tscode_calendar(同名同义,但在 offline_db_client)
)
# 注意(2026-09-17 v3.7 重构):
#   - data_gen 不再 import CNDataDown / TdxClient,所有下载动作外包给 service 子进程
#   - service_intraday.py / service_ticks.py / service_intraday_index.py
#     都在 ~/TradingAgent/offlineDataManager/scripts/service/ 下
# 注意(2026-09-17 v3.3 起):
#   - minute 窗口扩展为「涨停日前 1 后 2」共 4 天
#   - 通过 offline.get_tscode_calendar 拿个股交易日历
#   - day 数据彻底离开本脚本
# 注意(2026-09-17 v3.5 起):
#   - 新增 minute_index kind:5 个大盘指数的分钟数据(用于涨停 vs 指数走势对比)
#   - 指数列表见 INDEX_CODES_HERE 常量;日期范围来自 watchlist
#   - 接口契约与 fetch_and_write_minute 完全一致,只是 db_kind='minute_index'

# ============================================================================
# 退市过滤(2026-09-19 新增,响应 v1 复验报告)
# ============================================================================
def _get_delist_info(ts_codes: List[str]) -> dict:
    """查 ts_code -> (delist_date_dash, last_trade_date_dash)

    优先用 tbl_cn_basic.delist_date;缺失时 fallback 到 tbl_cn_day MAX(trade_date)
    返回的 key 是 ts_code;value 是 dict(delist_date, last_trade_date)
    """
    import sqlite3
    db_path = str(offline_data_dir() / "db_cn_basic.db")
    conn = sqlite3.connect(db_path)
    result = {}
    for ts in set(ts_codes):
        row = conn.execute(
            "SELECT delist_date FROM tbl_cn_basic WHERE ts_code=?",
            (ts,),
        ).fetchone()
        delist_date = row[0] if row and row[0] else None
        last_row = conn.execute(
            "SELECT MAX(trade_date) FROM tbl_cn_day WHERE ts_code=?",
            (ts,),
        ).fetchone()
        last_trade = last_row[0] if last_row and last_row[0] else None
        result[ts] = {
            "delist_date": delist_date,
            "last_trade_date": last_trade,
        }
    conn.close()
    return result


def filter_watchlist_by_delist(
    df: pd.DataFrame,
    verbose: bool = True,
) -> tuple:
    """过滤掉 watchlist 中 "涨停日 > 退市日 或 > 最后交易日" 的行

    Args:
        df: watchlist df (含 ts_code, trade_date 列,dash 格式)
        verbose: 是否打印被排除的行统计

    Returns:
        (filtered_df, excluded_df)
        - filtered_df: 过滤后的 df
        - excluded_df: 被剔除的行(含 delist_date, last_trade_date, reason 列)
    """
    if df is None or len(df) == 0:
        return df, pd.DataFrame()

    codes = df["ts_code"].unique().tolist()
    info = _get_delist_info(codes)

    keep_idx = []
    excluded_rows = []

    for idx, row in df.iterrows():
        ts = row["ts_code"]
        td = row["trade_date"]  # dash format YYYY-MM-DD
        meta = info.get(ts, {})

        # 权威信号:delist_date
        if meta.get("delist_date"):
            delist_dash = _ymd_compact_to_dash(meta["delist_date"])
            if td > delist_dash:
                row_dict = row.to_dict()
                row_dict["delist_date"] = meta["delist_date"]
                row_dict["last_trade_date"] = meta.get("last_trade_date", "") or ""
                row_dict["reason"] = "trade_date > delist_date (tbl_cn_basic)"
                excluded_rows.append(row_dict)
                continue

        # 兜底信号:last_trade_date from tbl_cn_day
        #   涨停日 > 最后交易日 + 90 天 → 判定为退市后涨停,样本无效
        #   (单纯晚一天可能是节假日/数据缺口,90 天阈值避免误判)
        from datetime import datetime, timedelta
        last_trade = meta.get("last_trade_date")
        if last_trade:
            last_trade_dash = _ymd_compact_to_dash(last_trade)
            # 涨停日 - 最后交易日 > 90 天
            try:
                td_dt = datetime.strptime(td, "%Y-%m-%d")
                last_dt = datetime.strptime(last_trade_dash, "%Y-%m-%d")
                if (td_dt - last_dt).days > 90:
                    row_dict = row.to_dict()
                    row_dict["delist_date"] = meta.get("delist_date") or ""
                    row_dict["last_trade_date"] = last_trade
                    row_dict["reason"] = (
                        f"trade_date 比 last_trade_date 晚 {(td_dt-last_dt).days} 天 > 90 (tbl_cn_day 兜底)"
                    )
                    excluded_rows.append(row_dict)
                    continue
            except ValueError:
                pass

        keep_idx.append(idx)

    filtered = df.loc[keep_idx].reset_index(drop=True)
    excluded_df = pd.DataFrame(excluded_rows)

    if verbose:
        print(f"  退市过滤: 排除 {len(excluded_df)} 对, 剩 {len(filtered)} 对", flush=True)
        if len(excluded_df) > 0:
            by_ts = excluded_df.groupby("ts_code").size().sort_values(ascending=False)
            print(f"  按 ts_code (Top 10):", flush=True)
            for ts, n in by_ts.head(10).items():
                last_td = excluded_df[excluded_df["ts_code"] == ts]["last_trade_date"].iloc[0]
                print(f"    {ts}  {n} 对  (最后交易日: {last_td})", flush=True)
            if len(by_ts) > 10:
                print(f"    ... 还有 {len(by_ts)-10} 只", flush=True)

    return filtered, excluded_df


# ============================================================================
# 路径
# ============================================================================
SCRIPT_DIR = Path(__file__).parent.resolve()
WATCHLIST_DIR = RESEARCH_ROOT / "policy" / "题材涨停研究" / "watchlist"
# 注:policy db 物理文件位于 ~/TradingAgent/offlineDataManager/data/(2026-09-17 搬到此处),
# schema 由 core.offline_db_client.ensure_schema() 自行管理(运行时建表),本脚本不再直接写 DB 路径

# service 路径(2026-09-17 v3.7:data_gen 调 service 子进程)
# SERVICE_DIR = ~/TradingAgent/offlineDataManager/scripts/service/
# 3 个 service 文件名
SERVICE_DIR = Path(ROOT) / "offlineDataManager" / "scripts" / "service"
SERVICE_INTRADAY = SERVICE_DIR / "service_intraday.py"
SERVICE_TICKS = SERVICE_DIR / "service_ticks.py"
SERVICE_INTRADAY_INDEX = SERVICE_DIR / "service_intraday_index.py"
# 临时 watchlist csv 目录(每次跑写一个 temp csv 给 service)
TEMP_WATCHLIST_DIR = Path("/tmp/policy_watchlists")
TEMP_WATCHLIST_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 临时 watchlist 写入(2026-09-17 v3.7)
# ============================================================================
def write_temp_watchlist(pairs: List[Tuple[str, str]], tag: str) -> str:
    """把 (ts_code, trade_date) 对列表写入临时 csv,返回路径

    Args:
        pairs: [(ts_code, trade_date), ...] 列表,dash 格式日期
        tag: 文件名后缀(例 'minute' / 'ticks'),用于区分

    Returns:
        临时 csv 绝对路径
    """
    import pandas as pd
    df = pd.DataFrame(pairs, columns=["ts_code", "trade_date"])
    df = df.drop_duplicates(subset=["ts_code", "trade_date"]).reset_index(drop=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = TEMP_WATCHLIST_DIR / f"watchlist_{tag}_{ts}.csv"
    df.to_csv(path, index=False)
    return str(path)


def resolve_watchlist_path(watchlist_name_or_path: str, watchlist_dir: Path) -> Path:
    """解析 watchlist 参数

    - 传名字(不带 .csv):找 <watchlist_dir>/<name>.csv,找不到报错
    - 传绝对路径或相对路径:直接用
    - 传相对路径时,也尝试找 <watchlist_dir>/<name> 兜底
    """
    p = Path(watchlist_name_or_path)
    if p.is_absolute() and p.exists():
        return p.resolve()
    if p.exists():  # 相对路径相对 cwd 存在
        return p.resolve()
    # 兜底:在 watchlist_dir 下找
    candidate = watchlist_dir / watchlist_name_or_path
    if candidate.exists():
        return candidate.resolve()
    candidate_with_csv = watchlist_dir / f"{watchlist_name_or_path}.csv"
    if candidate_with_csv.exists():
        return candidate_with_csv.resolve()
    raise FileNotFoundError(
        f"watchlist 找不到: {watchlist_name_or_path}\n"
        f"  试过: {p.resolve()} / {candidate} / {candidate_with_csv}"
    )


# ============================================================================
# 1) 读 watchlist
# ============================================================================
def read_watchlist(watchlist_dir: Path, name: str) -> pd.DataFrame:
    """从 <watchlist_dir>/<name>.csv 读 watchlist"""
    csv_path = watchlist_dir / f"{name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"watchlist 不存在: {csv_path}")

    df = pd.read_csv(csv_path, dtype={"trade_date": str, "ts_code": str})
    required = {"trade_date", "ts_code"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"watchlist 缺少必要列 {missing}: {csv_path}")

    df = df.drop_duplicates(subset=["trade_date", "ts_code"]).reset_index(drop=True)
    return df


# ============================================================================
# 2) 分钟窗口计算(2026-09-17 v3.3)
# ============================================================================
def _norm_ymd(s: str) -> str:
    """统一日期格式到 YYYY-MM-DD
    - 接受 YYYYMMDD(8 位紧凑)或 YYYY-MM-DD(10 位带 dash)输入
    - 已经是 dash 就直接返回
    - 异常(非数字、长度不对)原样返回,让上层决定怎么处理
    """
    if not s or not isinstance(s, str):
        return s
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def compute_minute_window(
    zt_dates: List[str],
    trade_calendar: List[str],
    *,
    forward: int = 1,
    backward: int = 2,
) -> Set[str]:
    """算单个 ts_code 的分钟下载日期集合

    对每个涨停日 T,在 trade_calendar 里找 T 的位置,
    往前 forward 个交易日 + 往后 backward 个交易日,加入 set。
    多个涨停日区间自动去重(多连板场景下窗口会重叠)。

    Args:
        zt_dates:         该 ts_code 的涨停日列表(YYYY-MM-DD,可重复)
        trade_calendar:   该 ts_code 的交易日历(YYYY-MM-DD,升序),来自 get_tscode_calendar
        forward:          涨停日前 N 个交易日(默认 1)
        backward:         涨停日及后 N 个交易日(默认 2,含 T 自己)

    Returns:
        set of YYYY-MM-DD 字符串,准备调 fetch_and_write_minute 的所有日期
    """
    if not zt_dates or not trade_calendar:
        return set()

    # 加速:把 trade_calendar 转成 dict {date_str: index}
    idx_map = {d: i for i, d in enumerate(trade_calendar)}
    out: Set[str] = set()

    for zd in zt_dates:
        if zd not in idx_map:
            # 涨停日不在个股交易日历里 → 个股这天停牌或非交易日
            print(f"      ⚠️  涨停日 {zd} 不在交易日历(可能停牌),跳过")
            continue
        i = idx_map[zd]
        # forward: T 之前 N 个交易日(不含 T)
        # backward: T 之后 N 个交易日(不含 T)
        # T 自己始终包含
        lo = max(0, i - forward)
        hi = min(len(trade_calendar), i + backward + 1)  # +1 因为 backward 是「T 之后 N 天」
        for j in range(lo, hi):
            out.add(trade_calendar[j])

    return out


def build_minute_pending(
    df: pd.DataFrame,
    *,
    forward: int = 1,
    backward: int = 2,
) -> List[Tuple[str, List[str]]]:
    """构建分钟下载 pending 列表(ts_code, trade_date 列表)

    步骤(用户需求,2026-09-17):
      1) watchlist 按 ts_code 分组
      2) 每个 ts_code 的涨停日排序:日期从大到小(近的在前)
      3) 对每只股票,start_date = 最早涨停日,end_date = 最晚涨停日
         (get_tscode_calendar 内部用 predays/fudays 加裕量)
      4) 调 offline.get_tscode_calendar 拿个股交易日历
      5) 对每个涨停日,在交易日历里找位置,前后 (forward+backward) 共 4 天
         加入 set(hash set 自动去重多连板区间重叠)
      6) 返回 [(ts_code, [td1, td2, ...]), ...] 列表

    Returns:
        List of (ts_code, sorted_trade_dates_list) tuples
    """
    pending: List[Tuple[str, List[str]]] = []

    # 1) 按 ts_code 分组
    for ts_code, group in df.groupby("ts_code"):
        # 2) 涨停日排序:日期从大到小(近的在前)
        # 注意:watchlist 里的 trade_date 是 YYYYMMDD 格式,先统一转 YYYY-MM-DD
        # 因为 compute_minute_window 期望 dash 格式做 idx_map
        zt_dates_raw = group["trade_date"].tolist()
        zt_dates = [_norm_ymd(d) for d in zt_dates_raw]
        zt_dates = sorted(set(zt_dates), reverse=True)  # 去重 + 排序

        if not zt_dates:
            continue

        # 3) 算 get_tscode_calendar 锚点:最早涨停日 + 最晚涨停日
        min_dash = min(zt_dates)   # desc 排序里 min = 最早
        max_dash = max(zt_dates)   # max = 最晚

        # 4) 拉个股交易日历(用 predays/fudays 加裕量)
        cal_df = get_tscode_calendar(
            ts_code,
            start_date=min_dash,
            end_date=max_dash,
            predays=400,  # 400 自然日 ≈ 280 交易日,覆盖单只股票涨停跨度
            fudays=400,
        )
        # get_tscode_calendar 返回 YYYYMMDD 紧凑格式,转 dash 给 compute_minute_window
        cal = [_norm_ymd(d) for d in cal_df["trade_date"].tolist()]
        if not cal:
            print(f"      ⚠️  {ts_code} 交易日历为空({min_dash}~{max_dash}),跳过")
            continue

        # 5) 计算分钟窗口 set
        date_set = compute_minute_window(
            zt_dates=zt_dates,
            trade_calendar=cal,
            forward=forward,
            backward=backward,
        )

        if date_set:
            # 6) 排序(升序 = 早的在前),便于 ctrl 过滤检查
            pending.append((ts_code, sorted(date_set)))

    return pending


# ============================================================================
# 3) Ticks 窗口计算(2026-09-17 v3.4)
# ============================================================================
def build_ticks_pending(
    df: pd.DataFrame,
    *,
    forward: int = 0,
    backward: int = 0,
) -> List[Tuple[str, List[str]]]:
    """构建 ticks 下载 pending 列表 — 复用 minute 的 window 逻辑

    用户需求(2026-09-17):
      - ticks 默认 forward=0, backward=0(只下载涨停当日)
      - 同样用个股交易日历 + hash set 去重

    实现:完全复用 compute_minute_window(参数化即可),
    共享 _norm_ymd / get_tscode_calendar / compute_minute_window 全部逻辑。
    与 build_minute_pending 唯一区别是默认窗口 (0, 0)。
    """
    return build_minute_pending(
        df, forward=forward, backward=backward
    )


# ============================================================================
# 4) 指数分钟窗口计算(2026-09-17 v3.5)
# ============================================================================
# 用户指定的 5 个大盘指数 — 全部走 pytdx get_history_minute_time_data(market, code, date)
# 已实测 5 个代码都能拉到 240 行/天
# 注意:跟 tdx_config.INDEX_CODES 区别是这里多了 000016.SH(上证50),
#       而 tdx_config 的 INDEX_CODES 不含 000016,所以这里独立维护
INDEX_CODES_HERE: List[str] = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创50
    "000016.SH",  # 上证50
]


def build_minute_index_pending(
    df: pd.DataFrame,
) -> List[Tuple[str, str]]:
    """构建指数分钟下载 pending 列表(2026-09-17 v3.5)

    用户需求:按 watchlist 里的最小日期和最大日期,拉取最小日期到最大日期
    每天的 5 个大盘指数分钟数据。日期范围过交易日历过滤周末/节假日。

    步骤:
      1) 从 watchlist 算 min/max trade_date(YYYYMMDD → dash)
      2) 用 offline.get_tradecal(start, end) 拿这个区间的所有交易日
         (SSE market,因为 watchlist 多数是沪市;但指数本身不分市场,这里只是过滤掉周末/节假日)
      3) 每个指数 × 每个交易日 = 1 个拉取对
      4) 返回 [(ts_code, trade_date), ...] 列表(不分组,扁平)

    Args:
        df: watchlist DataFrame(必须有 trade_date 列)

    Returns:
        List of (ts_code, trade_date) tuples,dash 格式
    """
    if df.empty or "trade_date" not in df.columns:
        return []

    # 1) watchlist 日期范围
    zt_dates = [_norm_ymd(d) for d in df["trade_date"].tolist()]
    zt_dates = [d for d in zt_dates if d]  # 去空
    if not zt_dates:
        return []
    min_dash = min(zt_dates)
    max_dash = max(zt_dates)

    # 2) 交易日历(SSE,过掉周末/节假日)
    from core.offline_db_client import get_tradecal
    cal_df = get_tradecal(
        start_date=_ymd_dash_to_compact(min_dash),
        end_date=_ymd_dash_to_compact(max_dash),
        market="SSE",
        is_open=True,
    )
    if cal_df is None or len(cal_df) == 0:
        print(f"      ⚠️ 指数区间 {min_dash}~{max_dash} 交易日历为空,跳过")
        return []
    trade_dates = [_ymd_compact_to_dash(d) for d in cal_df["cal_date"].astype(str).tolist()]
    trade_dates = sorted(set(trade_dates))

    # 3) 笛卡尔积:5 指数 × N 天
    pending: List[Tuple[str, str]] = []
    for ts_code in INDEX_CODES_HERE:
        for td in trade_dates:
            pending.append((ts_code, td))

    return pending


# ============================================================================
# 主流程
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="题材涨停研究 — 历史数据批量生成(v3.7)")
    parser.add_argument("watchlist", nargs="?",
                        help="watchlist 名字(不带 .csv 后缀,自动找 watchlist 目录) "
                             "或绝对路径(csv 路径)")
    parser.add_argument("--only", choices=["minute", "ticks", "minute_index"], default=None,
                        help="只跑一种数据,默认都跑(minute + ticks);"
                             "minute_index 只拉 5 个大盘指数,默认不跑")
    parser.add_argument("--watchlist-dir", default=str(WATCHLIST_DIR),
                        help=f"watchlist 目录(默认 {WATCHLIST_DIR})")
    parser.add_argument("--force", action="store_true",
                        help="强制覆盖(忽略 ctrl 表过滤,重新下载),会传给 service")
    parser.add_argument("--rate", type=float, default=0.15,
                        help="每只股票间隔秒数(pytdx ~70 req/s,默认 0.15s),会传给 service")
    parser.add_argument("--dry-run", action="store_true",
                        help="dry-run:只算窗口 + ctrl 过滤,不调 service")
    parser.add_argument("--include-delisted", action="store_true",
                        help="不过滤退市股(默认过滤 watchlist 中 trade_date > 退市日/最后交易日的行)")
    args = parser.parse_args()

    if not args.watchlist:
        parser.error("watchlist 是必填(传名字或 csv 绝对路径)")

    # 解析 watchlist 路径(名字 → watchlist_dir 下;路径 → 直接用)
    try:
        watchlist_path = resolve_watchlist_path(args.watchlist, Path(args.watchlist_dir))
    except FileNotFoundError as e:
        parser.error(str(e))

    print(f"=====[ data_gen v3.7 ]=====")
    print(f"watchlist:    {watchlist_path}")
    print(f"only:         {args.only or 'minute+ticks'}")
    print(f"force:        {args.force}")
    print()

    # 0) 确保表 + ctrl 都建好
    print("[0/5] ensure_schema ...")
    if args.only is None:
        kinds = ["minute", "ticks"]
    elif args.only == "minute_index":
        kinds = ["minute_index"]
    else:
        kinds = [args.only]
    for kind in kinds:
        ensure_schema(db_kind=kind, verbose=False)

    # 1) 读 watchlist(直接传绝对路径,绕过 read_watchlist 的目录查找)
    df = pd.read_csv(watchlist_path, dtype={"trade_date": str, "ts_code": str})
    required = {"ts_code", "trade_date"}
    missing = required - set(df.columns)
    if missing:
        parser.error(f"watchlist 缺少必要列 {missing}: {watchlist_path}")
    df = df.drop_duplicates(subset=["ts_code", "trade_date"]).reset_index(drop=True)
    # 统一日期格式到 YYYY-MM-DD
    df["trade_date"] = df["trade_date"].astype(str).map(_ymd_compact_to_dash)
    print(f"[1/5] watchlist (raw): {len(df)} 个 (ts_code, trade_date) 对(去重后)")

    # 1.5) 退市过滤:默认剔除 trade_date > delist_date 或 > 最后交易日 的行
    if not args.include_delisted:
        print(f"[1.5] 退市过滤 (--include-delisted={'YES' if args.include_delisted else 'NO'})...")
        df, excluded_df = filter_watchlist_by_delist(df, verbose=True)
        # 写 excluded_pairs.csv 给 02 复验用
        if len(excluded_df) > 0:
            import time as _time
            excluded_path = Path(args.watchlist_dir) / f"excluded_by_delist_{_time.strftime('%Y%m%d_%H%M%S')}.csv"
            excluded_df.to_csv(excluded_path, index=False, encoding="utf-8-sig")
            print(f"[1.5] 被排除的行已写到: {excluded_path}")
    else:
        print(f"[1.5] 退市过滤: SKIPPED (--include-delisted)")
    pairs = list(zip(df["ts_code"].tolist(), df["trade_date"].tolist()))
    print(f"[1.5] watchlist (退市过滤后): {len(pairs)} 个 (ts_code, trade_date) 对")

    # 2) ctrl 过滤 + 窗口扩展(只算 pending 不拉数据)
    # ---- minute(涨停日前 1 后 2 共 4 天,合并多连板)----
    if args.only in (None, "minute"):
        print(f"[2/5] 计算 minute 窗口(涨停日前 1 后 2 共 4 天,按 ts_code 合并多连板)...")
        minute_with_window = build_minute_pending(df, forward=1, backward=2)
        all_pairs_minute: List[Tuple[str, str]] = []
        for ts_code, tds in minute_with_window:
            for td in tds:
                all_pairs_minute.append((ts_code, td))
        if args.force:
            pending_minute = all_pairs_minute
            print(f"[2/5]   --force minute:跳过 ctrl,全量 {len(pending_minute)} 对")
        else:
            pending_minute = filter_pending("minute", all_pairs_minute)
            skipped = len(all_pairs_minute) - len(pending_minute)
            print(f"[2/5]   minute 窗口摊平后: {len(all_pairs_minute)} 对 → ctrl 跳过 {skipped} 对 → 待下载 {len(pending_minute)} 对")
    else:
        pending_minute = []

    # ---- ticks(涨停日前 0 后 0 共 1 天,只下载涨停日)----
    if args.only in (None, "ticks"):
        print(f"[2/5] 计算 ticks 窗口(涨停日前 0 后 0 共 1 天,只下载涨停日)...")
        ticks_with_window = build_ticks_pending(df, forward=0, backward=0)
        all_pairs_ticks: List[Tuple[str, str]] = []
        for ts_code, tds in ticks_with_window:
            for td in tds:
                all_pairs_ticks.append((ts_code, td))
        if args.force:
            pending_ticks = all_pairs_ticks
            print(f"[2/5]   --force ticks:跳过 ctrl,全量 {len(pending_ticks)} 对")
        else:
            pending_ticks = filter_pending("ticks", all_pairs_ticks)
            skipped = len(all_pairs_ticks) - len(pending_ticks)
            print(f"[2/5]   ticks 窗口摊平后: {len(all_pairs_ticks)} 对 → ctrl 跳过 {skipped} 对 → 待下载 {len(pending_ticks)} 对")
    else:
        pending_ticks = []

    # ---- minute_index(5 个大盘指数 × watchlist 日期范围)----
    pending_minute_index: List[Tuple[str, str]] = []
    watchlist_min_dash = None
    watchlist_max_dash = None
    if args.only in (None, "minute_index"):
        all_pairs_minute_index = build_minute_index_pending(df)
        if args.force:
            pending_minute_index = all_pairs_minute_index
            print(f"[2/5]   --force minute_index:跳过 ctrl,全量 {len(pending_minute_index)} 对 (5 指数 × 区间内交易日)")
        else:
            pending_minute_index = filter_pending("minute_index", all_pairs_minute_index)
            skipped = len(all_pairs_minute_index) - len(pending_minute_index)
            print(f"[2/5]   minute_index 窗口: {len(all_pairs_minute_index)} 对 → ctrl 跳过 {skipped} 对 → 待下载 {len(pending_minute_index)} 对")
        # 用全量(不是 pending)的日期范围喂 service(让 service 自己 ctrl 跳过)
        if all_pairs_minute_index:
            watchlist_min_dash = min(td for _, td in all_pairs_minute_index)
            watchlist_max_dash = max(td for _, td in all_pairs_minute_index)
            # 但分钟索引的范围是 watchlist 原始日期范围(没被 service 过滤掉的影响)
        # 用原始 watchlist 的 min/max 更直接:
        watchlist_min_dash = min(df["trade_date"])
        watchlist_max_dash = max(df["trade_date"])

    n_minute_stocks = len({ts for ts, _ in pending_minute}) if pending_minute else 0
    print(
        f"[2/5] 待下载: minute={len(pending_minute)} 对 ({n_minute_stocks} 股票) | "
        f"ticks={len(pending_ticks)} | minute_index={len(pending_minute_index)}"
    )

    # 3) dry-run 早退
    if args.dry_run:
        print(f"\n[3/5] DRY-RUN:不调 service,只算窗口")
        from collections import defaultdict
        if pending_minute:
            by_ts = defaultdict(list)
            for ts, td in pending_minute:
                by_ts[ts].append(td)
            print(f"  DRY-RUN minute: {len(by_ts)} 股票,每只示例(前 10):")
            for ts in sorted(by_ts.keys())[:10]:
                dates = sorted(by_ts[ts])
                print(f"    {ts}: {len(dates)} 天, {dates[0]} ~ {dates[-1]}")
        if pending_ticks:
            print(f"  DRY-RUN ticks: {len(pending_ticks)} 对")
        if pending_minute_index:
            print(f"  DRY-RUN minute_index: {len(pending_minute_index)} 对 (5 指数 × 区间)")
        return

    # 4) 调 service 子进程(2026-09-17 v3.7)
    t0 = datetime.now()
    service_results = []

    # ---- 4a) service_intraday(个股分钟)----
    if pending_minute:
        temp_csv = write_temp_watchlist(pending_minute, tag="minute")
        print(f"\n[4a/5] 调 service_intraday (临时 csv: {temp_csv}, {len(pending_minute)} 对)")
        cmd = [
            sys.executable, str(SERVICE_INTRADAY),
            "--watchlist", temp_csv,
            "--rate", str(args.rate),
        ]
        if args.force:
            cmd.append("--force")
        print(f"  $ {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=str(SERVICE_DIR.parent))
        service_results.append(("intraday", result.returncode))

    # ---- 4b) service_ticks(个股分笔)----
    if pending_ticks:
        temp_csv = write_temp_watchlist(pending_ticks, tag="ticks")
        print(f"\n[4b/5] 调 service_ticks (临时 csv: {temp_csv}, {len(pending_ticks)} 对)")
        cmd = [
            sys.executable, str(SERVICE_TICKS),
            "--watchlist", temp_csv,
            "--rate", str(args.rate),
        ]
        if args.force:
            cmd.append("--force")
        print(f"  $ {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=str(SERVICE_DIR.parent))
        service_results.append(("ticks", result.returncode))

    # ---- 4c) service_intraday_index(5 指数)----
    # 指数 service 总是调(即使 pending_minute_index 空,service 自身 ctrl skip)
    # 这样用户能看到 "intraday_index: ✅" 确认 service 跑通
    if watchlist_min_dash and watchlist_max_dash:
        n_pairs = len(pending_minute_index) if pending_minute_index else 0
        print(f"\n[4c/5] 调 service_intraday_index (日期 {watchlist_min_dash} ~ {watchlist_max_dash},pending={n_pairs} 对)")
        cmd = [
            sys.executable, str(SERVICE_INTRADAY_INDEX),
            "--start-date", watchlist_min_dash,
            "--end-date", watchlist_max_dash,
            "--rate", str(args.rate),
        ]
        if args.force:
            cmd.append("--force")
        print(f"  $ {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=str(SERVICE_DIR.parent))
        service_results.append(("intraday_index", result.returncode))

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n[4/5] service 全部完成,总耗时 {elapsed:.1f}s")
    for name, rc in service_results:
        status = "✅" if rc == 0 else f"❌ (rc={rc})"
        print(f"  {name}: {status}")

    # 5) 抽样验证(从 db_client 读,不调 service)
    print()
    print(f"[5/5] 抽样验证:")

    # ---- 个股抽样 ----
    if args.only in (None, "minute", "ticks"):
        sample_pairs = pairs[:3]
        for ts_code, trade_date in sample_pairs:
            db_date = trade_date  # 已是 dash
            if args.only in (None, "minute"):
                from core.offline_db_client import PolicyDBClient
                client_v = PolicyDBClient()
                df_m = client_v.get_minute(ts_code, trade_date=db_date)
                cnt = len(df_m)
                if cnt > 0:
                    mn = int(df_m["time_idx"].min())
                    mx = int(df_m["time_idx"].max())
                else:
                    mn = mx = None
                print(f"  {ts_code} {db_date} minute: {cnt} 行 (time_idx {mn}..{mx})")
            if args.only in (None, "ticks"):
                from core.offline_db_client import PolicyDBClient
                client_v = PolicyDBClient()
                df_t = client_v.get_ticks(ts_code, trade_date=db_date)
                cnt = len(df_t)
                if cnt > 0:
                    mn = int(df_t["seqId"].min())
                    mx = int(df_t["seqId"].max())
                else:
                    mn = mx = None
                print(f"  {ts_code} {db_date} ticks:  {cnt} 行 (seqId {mn}..{mx})")

    # ---- 指数抽样 ----
    if args.only in (None, "minute_index") and watchlist_min_dash and watchlist_max_dash:
        from core.offline_db_client import PolicyDBClient
        client_v = PolicyDBClient()
        # 取 watchlist 中最早一天 + 5 个指数做抽样
        for ts in ("000001.SH", "399001.SZ", "399006.SZ", "000688.SH", "000016.SH"):
            df_i = client_v.get_minute_index(ts, trade_date=watchlist_min_dash)
            cnt = len(df_i)
            if cnt > 0:
                mn = int(df_i["time_idx"].min())
                mx = int(df_i["time_idx"].max())
            else:
                mn = mx = None
            print(f"  {ts} {watchlist_min_dash} minute_index: {cnt} 行 (time_idx {mn}..{mx})")


if __name__ == "__main__":
    main()
