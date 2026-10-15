"""
~/cn_data/core/offline_downloader.py
A 股数据下载器(CNDataDown)
- 5 类数据:基本面 / 交易日历 / 开盘啦涨停榜 / 开盘啦题材成分 / 新闻
- 全部走 Tushare pro API
- 支持全量 + 增量(断点管理)
"""
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd
from loguru import logger

# 复用项目根目录下的 config / core 包
# PROJECT_ROOT 是 ~/TradingAgent/offlineDataManager/
# 需要把 ~/TradingAgent/ 也加进 sys.path,这样能找到平级的 coreClient/
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
# 父目录 = ~/TradingAgent/(用来 import coreClient.tushare_client)
_PARENT = PROJECT_ROOT.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from config.settings import (
    DEFAULT_LOOKBACK_DAYS,
    DB_PATH_BASIC,
    DB_PATH_KPL,
    DB_PATH_NEWS,
    DB_PATH,
    SCHEMA_SQL_BASIC,
    SCHEMA_SQL_KPL,
    SCHEMA_SQL_NEWS,
)
from coreClient.tushare_client import TushareClient
from core.periodic_kline_rebuild import rebuild_period_tables
from core.offline_db_client import (
    init_db,
    get_conn,
    snap_ts,
    upsert_df,
    replace_table,
    clear_table,         # 2026-09-17 新加:DELETE 单表(offline_downloader show_status 用)
    get_ctrl,
    update_ctrl,
    get_table_min_max,
    get_ctrl_basic,      # 2026-09-17 新加:读 basic 全部 ctrl 断点(show_status 用)
    get_news_ctrl,       # 2026-09-17 新加:读 news 全部 ctrl 断点(show_status 用)
    # 2026-09-17 policy 工具(原 sibling offline_db_client_policy 合并)
    has_data,
    mark_downloaded,
    mark_status,           # 2026-09-19 新增:tdx_empty 时用
    upsert_rows,
    _ymd_compact_to_dash,
    _ymd_dash_to_compact,
    _get_news_ctrl_value,
    _update_news_ctrl_value,
    show_status,
    table_exists,        # 2026-09-19 新增:检测 tbl_tick_v2 是否已建(双写兼容用)
    TICKS_DB,            # 2026-09-19 新增:table_exists 需要 db_path
)

# ==================== 常量 ====================
TRADECAL_EXCHANGES = ["SSE", "SZSE", "BSE"]  # 上交所 / 深交所 / 北交所
TRADECAL_FULL_START = "20180101"  # 全量初始日期

NEWS_SRC_LIST = [
    "sina", "wallstreetcn", "10jqka", "eastmoney",
    "yuncaijing", "fenghuang", "jinrongjie", "cls", "yicai",
]
# Tushare 接口已停的源(2026-09-11 实测 7 天连续 0 行)
# scheduler 默认跳过以节省 API 配额;手动跑 update_news 时显式传 src_list 可恢复
NEWS_SRC_DISABLED = {"yuncaijing", "fenghuang"}
NEWS_LIMIT = 1500  # tushare 单次返回上限

# 5 大盘指数(2026-09-17 加,从 sibling offline_db_client_policy 合并)
# 用户指定:000001.SH / 399001.SZ / 399006.SZ / 000688.SH / 000016.SH
# 全部走 pytdx get_history_minute_time_data(market, code, date),已实测都能拉 240 行/天
# 区别于 tdx_config.INDEX_CODES(4 个,不含 000016.SH 上证50)
INDEX_CODES_HERE: List[str] = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创50
    "000016.SH",  # 上证50
]


def get_active_news_srcs() -> list:
    """返回当前启用的新闻源列表(全集 - disabled)"""
    return [s for s in NEWS_SRC_LIST if s not in NEWS_SRC_DISABLED]


def _fmt_yyyymmdd(d) -> str:
    """把任意日期/datetime/字符串归一到 YYYYMMDD(给 tushare 用)"""
    if d is None or d == "":
        return ""
    if isinstance(d, str):
        s = d.replace("-", "").replace("/", "")[:8]
        return s
    if isinstance(d, (datetime, pd.Timestamp)):
        return d.strftime("%Y%m%d")
    return str(d)[:8].replace("-", "")


def _fmt_dash(d) -> str:
    """把任意日期归一到 YYYY-MM-DD(存 DB/日志用)"""
    if d is None or d == "":
        return ""
    if isinstance(d, str):
        s = d.replace("/", "-")
        if len(s) >= 10:
            return s[:10]
        return s
    if isinstance(d, (datetime, pd.Timestamp)):
        return d.strftime("%Y-%m-%d")
    return str(d)[:10]


def _next_day(yyyymmdd: str) -> str:
    """YYYYMMDD + 1 day → YYYYMMDD(字符串)"""
    if not yyyymmdd or len(yyyymmdd) != 8:
        return yyyymmdd
    dt = datetime.strptime(yyyymmdd, "%Y%m%d") + timedelta(days=1)
    return dt.strftime("%Y%m%d")


class CNDataDown:
    """A 股数据下载器(全量 + 增量)"""

    def __init__(self):
        self.client = TushareClient()  # 单例
        # 三个数据库各开一个连接(2026-09-10 拆分)
        self.conn_basic = init_db("basic", verbose=False)
        self.conn_kpl = init_db("kpl", verbose=False)
        self.conn_news = init_db("news", verbose=False)
        self.conn_index = init_db("index", verbose=False)  # 2026-09-15 新增
        # 兼容旧代码(self.conn → basic)
        self.conn = self.conn_basic
        logger.info("CNDataDown 初始化完成(4 个 DB 连接)")

    # ====================================================
    # 1. 股票基本信息
    # ====================================================
    def update_basic(self, bFull: bool = False) -> int:
        """股票基本信息

        拉取策略:
        - list_status = L + P(上市 + 暂停)
        - 不拉 D(退市):退市股票不应参与 day/week/month 增量聚合,避免污染历史
        - 如果某只股票从 L → P(停牌),主键去重 + list_status 更新会覆盖
        - 如果某只股票从 L → D(退市),会在 DB 里继续保留(D 数据不会被覆盖)

        写入 tbl_cn_basic(主键 ts_code)
        写入 db_cn_basic.db
        """
        table = "tbl_cn_basic"
        t0 = time.time()
        try:
            # Tushare 一次只能传一个 list_status,合并两个状态的结果
            frames = []
            for status in ("L", "P"):
                df = self.client.stock_basic(
                    list_status=status,
                    fields="ts_code,symbol,name,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_ent_type,act_name,area",
                )
                if df is not None and len(df) > 0:
                    frames.append(df)
            if not frames:
                logger.warning("[基本面] L+P 都没数据,跳过")
                return 0
            df = pd.concat(frames, ignore_index=True)
            # 同一只股票可能 L 和 P 都返回(理论上不会),主键去重保险
            df = df.drop_duplicates(subset=["ts_code"], keep="last")

            df["snap_ts"] = snap_ts()
            # 列表里的日期字段保持原样(ts_code 主键)
            if bFull:
                # 强制全量:清表后写(走 db_client 的 clear_table 工具)
                clear_table(self.conn_basic, table)
            inserted = upsert_df(self.conn_basic, df, table, key_cols=["ts_code"])

            # 断点(basic 是无日期维度的全量表,记一次刷新的 snap_ts)
            update_ctrl(self.conn_basic, "cn_basic", snap_ts())
            elapsed = time.time() - t0
            logger.info(f"[完成] {table:<28} +{inserted:>6,} 行  耗时 {elapsed:.1f}s")
            return inserted
        except Exception as e:
            logger.error(f"[基本面] 失败: {e}", exc_info=True)
            return 0

    # ====================================================
    # 2. 交易日历(按交易所分别)
    # ====================================================
    def update_tradecal(
        self,
        start_date: str = None,
        end_date: str = None,
    ) -> int:
        """交易日历
        - 按 SSE/SZSE/BSE 分别拉
        - 增量:从 ctrl.max_date+1 开始;全量:20180101 开始
        - ctrl key 加 exchange 后缀(主键是 cal_date+exchange)
        - 写入 db_cn_basic.db
        """
        table = "tbl_cn_tradecal"
        total_inserted = 0
        t0 = time.time()

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())

        for exch in TRADECAL_EXCHANGES:
            ctrl_key = f"cn_tradecal_{exch}"
            try:
                if start_date:
                    sd = _fmt_yyyymmdd(start_date)
                else:
                    last = get_ctrl(self.conn_basic, ctrl_key)
                    sd = last if last else TRADECAL_FULL_START

                logger.info(f"[交易日历:{exch}] {sd} ~ {end_date}")
                df = self.client.trade_cal(
                    exchange=exch,
                    start_date=sd,
                    end_date=end_date,
                )
                if df is None or len(df) == 0:
                    logger.info(f"[交易日历:{exch}] 无新数据")
                    continue

                df["snap_ts"] = snap_ts()
                df["exchange"] = exch  # 确保 exchange 列存在

                # 主键去重
                inserted = upsert_df(self.conn_basic, df, table, key_cols=["cal_date", "exchange"])
                total_inserted += inserted

                # 更新断点:拉到这一批最大的 cal_date
                if "cal_date" in df.columns:
                    max_date = df["cal_date"].max()
                    # 归一存 YYYYMMDD
                    max_date_yyyymmdd = _fmt_yyyymmdd(max_date)
                    update_ctrl(self.conn_basic, ctrl_key, max_date_yyyymmdd)
                logger.info(f"[交易日历:{exch}] +{inserted} 行")
            except Exception as e:
                logger.error(f"[交易日历:{exch}] 失败: {e}", exc_info=True)
                continue

        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>6,} 行  耗时 {elapsed:.1f}s")
        return total_inserted

    # ====================================================
    # 3. 开盘啦涨停榜(逐日)
    # ====================================================
    def update_kpl_list(
        self,
        start_date: str = None,
        end_date: str = None,
        tag: Optional[str] = None,
    ) -> int:
        """开盘啦涨停榜(2026-09-15 改造,支持 tag 筛选)

        - 逐日循环(pro.kpl_list 每天调一次,可带 tag)
        - tag=None → 默认下载两次:tag='涨停' + tag='炸板'
        - tag='涨停' 或 '炸板' → 只下载该 tag
        - 增量:从 ctrl.max_date+1 开始;全量:20180101 开始
        - 炸板数据起始 2020-10-09(用户指定)
        - ctrl key: cn_kpl_list
        - 写入 db_cn_kpl.db
        """
        table = "tbl_cn_kpl_list"
        ctrl_key = "cn_kpl_list"
        t0 = time.time()
        total_inserted = 0

        # 决定要下载的 tag 列表
        # 2026-09-15 用户策略(用户原话:下载更新的时候需要下两遍):
        #   tag='涨停' → 只下载涨停
        #   tag='炸板' → 只下载炸板
        #   tag=None   → 下两遍:涨停 + 炸板(默认行为,scheduler 用这个)
        # 下载顺序:先炸板(不更新 ctrl),再涨停(更新 ctrl)
        if tag is None:
            tags_to_download = ["炸板", "涨停"]
        elif tag in ("涨停", "炸板"):
            tags_to_download = [tag]
        else:
            logger.error(f"[KPL榜单] 不支持的 tag: {tag}")
            return 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
            from core.offline_db_client import get_ctrl_kpl
            last = get_ctrl_kpl(ctrl_key, conn=self.conn_kpl)
            sd = last if last else TRADECAL_FULL_START

        # 构造日期序列
        try:
            d_start = datetime.strptime(sd, "%Y%m%d")
            d_end = datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            logger.error(f"[KPL榜单] 日期格式错误: sd={sd} end={end_date}")
            return 0

        # 涨停 + 炸板数据起始日期:
        #   - 涨停 tag: tushare pro.kpl_list 2018-01-02 起有数据(实测),从 TRADECAL_FULL_START=20180101 开始跑
        #   - 炸板 tag: 用户要求从 2020-10-09 开始补(2020-10-09 之前未下载)
        ZB_START = "20201009"  # 炸板起始(用户要求 2020-10-09)
        zb_start_dt = datetime.strptime(ZB_START, "%Y%m%d")

        # 涨停起始:从 TRADECAL_FULL_START 跑(2018-01-01)
        # 但 2018-01-01 是周一但可能没数据,2018-01-02 才是真正第一个交易日
        ZT_START = "20180102"  # 涨停起始(实测最早有数据)
        zt_start_dt = datetime.strptime(ZT_START, "%Y%m%d")

        cur = d_start
        days_processed = 0
        # 2026-09-15 用户要求:
        #   - 炸板下载成功 → 只记录到 zb_last_date(不更新 ctrl)
        #   - 涨停下载成功 → 更新 last_success_date(更新 ctrl)
        # 这样保证下次增量跑时:从涨停最后一次成功日期继续(不会跳过炸板缺失日期)
        last_success_date = None
        zb_last_date = None
        KPL_LIST_LIMIT = 8000
        while cur <= d_end:
            td = cur.strftime("%Y%m%d")
            cur_dt = cur
            day_inserted = 0
            # 对每个 tag 都拉一遍
            for cur_tag in tags_to_download:
                # 炸板数据从 2020-10-09 开始补(用户要求),更早日期不拉炸板
                # 涨停数据从 2018-01-02 开始(实测最早)
                if cur_tag == "炸板" and cur_dt < zb_start_dt:
                    continue
                if cur_tag == "涨停" and cur_dt < zt_start_dt:
                    continue

                offset = 0
                page_count = 0
                while True:
                    try:
                        df = self.client.kpl_list(
                            trade_date=td, tag=cur_tag,
                            limit=KPL_LIST_LIMIT, offset=offset,
                        )
                        if df is None or len(df) == 0:
                            break

                        df["snap_ts"] = snap_ts()
                        inserted = upsert_df(
                            self.conn_kpl, df, table,
                            key_cols=["ts_code", "trade_date", "tag"],
                        )
                        if inserted > 0:
                            day_inserted += inserted
                            # 2026-09-15 改造:不同 tag 单独记录
                            if cur_tag == "涨停":
                                # 涨停成功 → 才更新 ctrl
                                last_success_date = td
                            elif cur_tag == "炸板":
                                # 炸板成功 → 只记本地变量,不更新 ctrl
                                zb_last_date = td

                        if len(df) < KPL_LIST_LIMIT:
                            break

                        offset += KPL_LIST_LIMIT
                        page_count += 1
                        if page_count >= 50:
                            break
                    except Exception as e:
                        logger.error(f"[KPL榜单:{td}] tag={cur_tag} offset={offset} 失败: {e}", exc_info=True)
                        break

            total_inserted += day_inserted
            days_processed += 1
            # 每 20 天打一次进度
            if days_processed % 20 == 0:
                logger.info(f"[KPL榜单] 进度 {td} 累计 +{total_inserted} (涨停 ctrl={last_success_date}, 炸板 last={zb_last_date})")
            cur += timedelta(days=1)

        # 断点更新:只更新涨停的 ctrl(用户要求炸板不参与 ctrl)
        # 涨停成功才更新 ctrl,这样下次增量跑会从涨停最后成功日期继续
        # 注意:炸板数据需要单独维护 ctrl,如果需要可以扩展(暂时只跑涨停更新)
        # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
        if last_success_date:
            from core.offline_db_client import update_ctrl_kpl
            update_ctrl_kpl(self.conn_kpl, ctrl_key, last_success_date)
            logger.info(f"[KPL榜单] 涨停 ctrl 已更新 → {last_success_date}")
        if zb_last_date:
            logger.info(f"[KPL榜单] 炸板 已补到 {zb_last_date} (不参与 ctrl)")
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>6,} 行  耗时 {elapsed:.1f}s")
        return total_inserted

    # ====================================================
    # 3.5 涨停表现详情(实时拉取,补 kpl_list 第二天早上的滞后)
    # ====================================================
    def update_kpl_limit_performance(
        self,
        start_date: str = None,
        end_date: str = None,
        trade_date: str = None,
    ) -> int:
        """开盘啦涨停表现详情(DailyLimitPerformance)

        用 coreClient/kpl_client.get_daily_limit_performance(date) 拉某日所有板数涨停股
        - 实时数据(当天 16:00 后即可拿到完整数据,不用等到第二天早上)
        - 用于补 kpl_list 第二天早上才更新的滞后

        日期参数(跟其他 update_* 一致):
          start_date=None  默认 = max(本周周一, ctrl.max_date)
          end_date=None    默认 = 今天
          trade_date=str   优先于 start/end,只拉一天

        写入 db_cn_kpl.db:tbl_cn_kpl_limit_performance
        仅处理本地 SSE 日历明确开市的日期；日历未知时拒绝请求。
        可核验的 lu_time 源日期必须与请求日期相同，防止假期沿用旧快照。
        断点写入 db_cn_kpl.db:tbl_kpl_ctrl (key=cn_kpl_limit_performance)
        """
        table = "tbl_cn_kpl_limit_performance"
        ctrl_key = "cn_kpl_limit_performance"
        t0 = time.time()
        total_inserted = 0

        # 解析日期参数
        if trade_date:
            td_str = _fmt_yyyymmdd(trade_date)
            sd = td_str
            ed = td_str
        else:
            ed = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
            if start_date:
                sd = _fmt_yyyymmdd(start_date)
            else:
                # 默认 = max(本周周一, ctrl.max_date)
                # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
                from core.offline_db_client import get_ctrl_kpl
                last = get_ctrl_kpl(ctrl_key, conn=self.conn_kpl)
                today = datetime.now()
                # 本周周一(weekday(): Monday=0)
                week_start = today - timedelta(days=today.weekday())
                week_start_str = week_start.strftime("%Y%m%d")
                # 取较大值
                if last:
                    sd = max(last, week_start_str)
                else:
                    sd = week_start_str

        d_start = datetime.strptime(sd, "%Y%m%d")
        d_end = datetime.strptime(ed, "%Y%m%d")
        if d_start > d_end:
            raise ValueError(f"[KPL涨停表现] 起始日期晚于结束日期: {sd} > {ed}")

        # 先核对整个请求区间，未知日历不能按普通工作日猜测或先写部分数据。
        calendar = dict(self.conn_basic.execute(
            "SELECT cal_date, is_open FROM tbl_cn_tradecal "
            "WHERE exchange = 'SSE' AND cal_date BETWEEN ? AND ?",
            (sd, ed),
        ).fetchall())
        requested_dates = [
            (d_start + timedelta(days=offset)).strftime("%Y%m%d")
            for offset in range((d_end - d_start).days + 1)
        ]
        unknown = [td for td in requested_dates if calendar.get(td) not in (0, 1)]
        if unknown:
            raise ValueError(f"[KPL涨停表现] SSE 日历未知，拒绝下载: {','.join(unknown)}")
        open_dates = [td for td in requested_dates if calendar[td] == 1]
        if not open_dates:
            logger.info(f"[KPL涨停表现] {sd}~{ed} 均为休市日，不请求、不入库、不推进断点")
            return 0

        # 懒加载 KPLClient (避免启动时强依赖)
        if not hasattr(self, "_kpl_client"):
            from coreClient.kpl_client import KPLClient
            self._kpl_client = KPLClient()

        days_processed = 0
        last_success_date = None

        for td in open_dates:
            date_str = f"{td[:4]}-{td[4:6]}-{td[6:]}"  # YYYY-MM-DD 给 KPLClient
            day_inserted = 0
            try:
                # 2026-09-15 智能策略:
                #   - 日期 == 今天 → 用实时接口 HomeDingPan(盘中可用,有完整数据)
                #   - 日期 <  今天 → 用历史接口 HisHomeDingPan(便宜,完整历史)
                today = datetime.now().strftime("%Y%m%d")
                if td == today:
                    df = self._kpl_client.fetch_realtime_limit_performance()
                else:
                    df = self._kpl_client.get_daily_limit_performance(date_str)

                if df is None or len(df) == 0:
                    # 开市日空返回不推进断点，下次仍可重试。
                    continue

                df = df.copy()
                # 历史客户端把 Unix epoch 格式化为 HH:MM:SS，无法再验证日期；
                # 不根据时钟字符串推测日期，也不把旧版 UTC 时钟误判为日期错配。
                # 实时接口仍保留 epoch，可用北京时间核对真实交易日。
                if "lu_time" in df.columns:
                    numeric_time = pd.to_numeric(df["lu_time"], errors="coerce")
                    epoch_time = numeric_time[numeric_time.abs() >= 100_000_000]
                    for unit, values in (
                        ("s", epoch_time[epoch_time.abs() < 100_000_000_000]),
                        ("ms", epoch_time[epoch_time.abs() >= 100_000_000_000]),
                    ):
                        source_dates = (
                            pd.to_datetime(values, unit=unit, utc=True, errors="coerce")
                            .dt.tz_convert("Asia/Shanghai").dt.strftime("%Y%m%d")
                        )
                        if source_dates.isna().any() or (source_dates != td).any():
                            raise ValueError(
                                f"[KPL涨停表现] lu_time 源日期与请求 {td} 不符，拒绝整日入库"
                            )

                # 准备入库(转 trade_date 为 YYYYMMDD 格式;补 snap_ts)
                df["trade_date"] = td  # 覆盖为 YYYYMMDD
                df["snap_ts"] = snap_ts()
                # 只保留表里有的列
                keep_cols = [
                    "trade_date", "ts_code", "name", "board_type", "board_count",
                    "lu_time", "theme", "limit_reason", "is_break", "amplitude",
                    "turnover_rate", "limit_order", "lu_limit_order", "net_change",
                    "main_in", "main_out", "amount", "free_float", "close_price",
                    "pct_chg", "board_period", "theme_id", "sector_id", "snap_ts",
                ]
                for c in keep_cols:
                    if c not in df.columns:
                        df[c] = None
                df = df[keep_cols]

                inserted = upsert_df(self.conn_kpl, df, table, key_cols=["trade_date", "ts_code"])
                if inserted > 0:
                    day_inserted += inserted
                    last_success_date = td

                total_inserted += day_inserted
                days_processed += 1
                if days_processed % 20 == 0:
                    logger.info(f"[KPL涨停表现] 进度 {td} 累计 +{total_inserted}")
            except Exception as e:
                logger.error(f"[KPL涨停表现:{td}] 失败: {e}", exc_info=True)

        if last_success_date:
            # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
            from core.offline_db_client import update_ctrl_kpl
            update_ctrl_kpl(self.conn_kpl, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>6,} 行  耗时 {elapsed:.1f}s")
        return total_inserted

    # ====================================================
    # 4. 开盘啦题材成分(逐日)
    # ====================================================
    def update_kpl_concept_cons(
        self,
        start_date: str = None,
        end_date: str = None,
    ) -> int:
        """开盘啦题材成分
        - 逐日循环
        - 注意:client.kpl_concept_cons 内部映射到 pro.kpl_concept
        - ctrl key: cn_kpl_concept_cons
        - 写入 db_cn_kpl.db
        """
        table = "tbl_cn_kpl_concept_cons"
        ctrl_key = "cn_kpl_concept_cons"
        t0 = time.time()
        total_inserted = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
            from core.offline_db_client import get_ctrl_kpl
            last = get_ctrl_kpl(ctrl_key, conn=self.conn_kpl)
            sd = last if last else TRADECAL_FULL_START

        try:
            d_start = datetime.strptime(sd, "%Y%m%d")
            d_end = datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            logger.error(f"[KPL题材] 日期格式错误: sd={sd} end={end_date}")
            return 0

        cur = d_start
        days_processed = 0
        last_success_date = None
        KPL_CONCEPT_LIMIT = 3000  # tushare kpl_concept_cons 单次最大 3000
        while cur <= d_end:
            td = cur.strftime("%Y%m%d")
            # 单日 OFFSET + LIMIT 分页(理论上 < 3000,但加保护)
            offset = 0
            page_count = 0
            day_inserted = 0
            while True:
                try:
                    df = self.client.kpl_concept_cons(trade_date=td, limit=KPL_CONCEPT_LIMIT, offset=offset)
                    if df is None or len(df) == 0:
                        break

                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(self.conn_kpl, df, table, key_cols=["ts_code", "con_code", "trade_date"])
                    if inserted > 0:
                        day_inserted += inserted
                        last_success_date = td

                    if len(df) < KPL_CONCEPT_LIMIT:
                        break

                    offset += KPL_CONCEPT_LIMIT
                    page_count += 1
                    if page_count >= 50:
                        logger.warning(f"[KPL题材:{td}] 超过 50 页,停止")
                        break
                except Exception as e:
                    logger.error(f"[KPL题材:{td}] offset={offset} 失败: {e}", exc_info=True)
                    break

            total_inserted += day_inserted
            days_processed += 1
            if days_processed % 20 == 0:
                logger.info(f"[KPL题材] 进度 {td} 累计 +{total_inserted}")
            cur += timedelta(days=1)

        if last_success_date:
            # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
            from core.offline_db_client import update_ctrl_kpl
            update_ctrl_kpl(self.conn_kpl, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>6,} 行  耗时 {elapsed:.1f}s")
        return total_inserted

    # ====================================================
    # 5. 新闻(9 源独立断点)
    # ====================================================
    def update_news(
        self,
        src_list: list = None,
        start_date: str = None,
        end_date: str = None,
    ) -> dict:
        """新闻(9 源并行,逐日循环)

        Tushare news 接口 limit=1500,范围查询只能拿最近 1500 条。
        要拉历史必须**逐日循环**每天调一次,主键 (datetime, src) 去重保证不重复。

        - 每个源独立从 tbl_news_ctrl 读断点(datetime 粒度)
        - 逐日循环:每天调一次 self.client.news(src=..., 单日)
        - 写入 tbl_news(主键 datetime+src+md5)
        - 返回 {src: 新增行数}
        - 写入 db_cn_news.db

        Args:
            src_list: 要拉的新闻源列表,默认 NEWS_SRC_LIST (9 个)
            start_date: 起始日期(YYYY-MM-DD 或 YYYYMMDD),None 用 ctrl
            end_date: 结束日期(YYYY-MM-DD 或 YYYYMMDD),None 用今天
        """
        table = "tbl_news"
        t0 = time.time()
        if src_list is None:
            # 默认用 active 列表(过滤掉 disabled 源)
            src_list = get_active_news_srcs()
            if NEWS_SRC_DISABLED:
                logger.info(f"[新闻] 跳过 disabled 源: {sorted(NEWS_SRC_DISABLED)}")

        # end_date 统一为今天
        if end_date:
            end_dt = _fmt_dash(end_date)
        else:
            end_dt = datetime.now().strftime("%Y-%m-%d")

        result = {}

        for src in src_list:
            try:
                # 每源独立断点(YYYY-MM-DD HH:MM:SS 格式,在 db_cn_news.db)
                last = _get_news_ctrl_value(self.conn_news, src)
                if start_date:
                    sd = _fmt_dash(start_date)
                elif last:
                    # news 断点是 datetime 粒度,从 max_date 当天 00:00:00 重拉当天,
                    # 避免漏掉当天剩余时间的新料
                    sd = last[:10]
                else:
                    # 默认起始日期:2020-01-01(tushare news 数据从 2020+ 密集)
                    # 2018-2019 数据稀疏没必要拉,2020+ 数据完整
                    sd = "2020-01-01"

                # 构造日期序列(逐日循环)
                try:
                    d_start = datetime.strptime(sd, "%Y-%m-%d")
                    d_end = datetime.strptime(end_dt, "%Y-%m-%d")
                except ValueError:
                    logger.error(f"[新闻:{src}] 日期格式错误: sd={sd} end={end_dt}")
                    result[src] = 0
                    continue

                logger.info(f"[新闻:{src}] 逐日 {sd} ~ {end_dt}")

                src_inserted = 0
                cur = d_start
                days_processed = 0
                last_success_dt = None
                while cur <= d_end:
                    td = cur.strftime("%Y-%m-%d")
                    # 每天范围 +1 buffer(tushare 单日有时返 0)
                    ed_td_dt = cur + timedelta(days=1)
                    ed_td = ed_td_dt.strftime("%Y-%m-%d")

                    # 单日用 OFFSET + LIMIT 分页(单日可能 > 1500)
                    offset = 0
                    page_count = 0
                    import hashlib
                    while True:
                        try:
                            df = self.client.news(
                                src=src,
                                start_date=td,
                                end_date=ed_td,
                                limit=NEWS_LIMIT,
                                offset=offset,
                            )
                            if df is None or len(df) == 0:
                                break

                            if "src" not in df.columns:
                                df["src"] = src

                            # 计算 md5 主键(基于 content 字段,模仿 MyATM 做法)
                            # 即使同一时刻同源,如果 content 相同也算同一条
                            df["md5"] = df["content"].apply(
                                lambda x: hashlib.md5(
                                    (str(x) if x is not None else "").encode("utf-8")
                                ).hexdigest()
                            )

                            df["snap_ts"] = snap_ts()
                            # 写入 db_cn_news.db
                            inserted = upsert_df(self.conn_news, df, table, key_cols=["datetime", "src", "md5"])
                            if inserted > 0:
                                src_inserted += inserted
                                if "datetime" in df.columns:
                                    max_dt = str(df["datetime"].max())
                                    if last_success_dt is None or max_dt > last_success_dt:
                                        last_success_dt = max_dt

                            # 返回 < limit 说明当天拉完
                            if len(df) < NEWS_LIMIT:
                                break

                            offset += NEWS_LIMIT
                            page_count += 1
                            # 安全上限:单日最多 100 页(15万条)
                            if page_count >= 100:
                                logger.warning(f"[新闻:{src}] {td} 超过 100 页,停止")
                                break
                        except Exception as e:
                            logger.error(f"[新闻:{src}] {td} offset={offset} 失败: {e}", exc_info=True)
                            break

                    days_processed += 1
                    # 每 100 天打一次进度
                    if days_processed % 100 == 0:
                        logger.info(f"  [新闻:{src}] 进度 {td} 累计 +{src_inserted:,}")
                    cur += timedelta(days=1)

                # 更新该源断点(在 db_cn_news.db)
                if last_success_dt:
                    _update_news_ctrl_value(self.conn_news, src, last_success_dt)

                result[src] = src_inserted
                logger.info(f"[新闻:{src}] +{src_inserted} 行  ({days_processed} 天)")

            except Exception as e:
                logger.error(f"[新闻:{src}] 异常: {e}", exc_info=True)
                result[src] = 0
                continue

        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} 累计 +{sum(v for v in result.values() if isinstance(v, int)):>10,} 行  耗时 {elapsed:.1f}s")
        logger.info(f"  各源新增: {result}")
        return result

    # ====================================================
    # 5b. 长新闻 major_news(每 10 分钟更新)
    # ====================================================
    def update_major_news(self, start_date: str = None) -> dict:
        """长新闻(major_news)增量更新

        Tushare major_news 接口字段: title, pub_time, src, url
        每 10 分钟跑一次,从 tbl_news_ctrl 的 major_news 断点起
        写入 db_cn_news.db,ctrl 也写到 db_cn_news.db 的 tbl_news_ctrl

        Returns:
            {"inserted": int, "max_dt": str}
        """
        table = "tbl_major_news"
        ctrl_key = "major_news"
        t0 = time.time()
        result = {}

        # 起点:从 tbl_news_ctrl 读 major_news 断点(YYYY-MM-DD HH:MM:SS) 或 1 天前
        last = _get_news_ctrl_value(self.conn_news, ctrl_key)
        if last:
            sd = last
        elif start_date:
            sd = _fmt_dash(start_date)
        else:
            # 默认从今天开始(增量)
            from datetime import timedelta as _td
            sd = (datetime.now() - _td(days=1)).strftime("%Y-%m-%d")
            sd = f"{sd} 00:00:00"

        ed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"[长新闻] {sd} ~ {ed}")
        try:
            df = self.client.pro.major_news(start_date=sd, end_date=ed, limit=2000)
            if df is not None and len(df) > 0:
                # 计算 md5(基于 title)
                import hashlib
                df["md5"] = df["title"].apply(
                    lambda x: hashlib.md5(str(x).encode()).hexdigest() if x else "no_title"
                )
                # pub_time → datetime
                df["datetime"] = df["pub_time"].astype(str)
                df["snap_ts"] = snap_ts()
                # 写入 db_cn_news.db
                inserted = upsert_df(self.conn_news, df, table, key_cols=["datetime", "src", "md5"])

                # 更新 ctrl:写到 db_cn_news.db 的 tbl_news_ctrl
                if "pub_time" in df.columns:
                    max_dt = str(df["pub_time"].max())
                    _update_news_ctrl_value(self.conn_news, ctrl_key, max_dt)

                result["inserted"] = inserted
                logger.info(f"[长新闻] +{inserted} 行")
            else:
                result["inserted"] = 0
                logger.info(f"[长新闻] 无新数据")
        except Exception as e:
            logger.error(f"[长新闻] 失败: {e}", exc_info=True)
            result["error"] = str(e)

        result["elapsed"] = time.time() - t0
        return result

    # ====================================================
    # 5c. CCTV 新闻联播(cctv_news,每日 1 次更新)
    # ====================================================
    def update_cctv_news(self, start_date: str = None) -> dict:
        """CCTV 新闻联播(每日 1 次)

        Tushare cctv_news 接口字段: date (YYYYMMDD), title, content
        每天更新一次,从 tbl_news_ctrl 的 cctv_news 断点起
        写入 db_cn_news.db,ctrl 也写到 db_cn_news.db 的 tbl_news_ctrl
        """
        table = "tbl_cctv_news"
        ctrl_key = "cctv_news"
        t0 = time.time()
        result = {}

        # 起点:从 tbl_news_ctrl 读 cctv_news 断点
        last = _get_news_ctrl_value(self.conn_news, ctrl_key)
        if last:
            # last = YYYYMMDD
            try:
                dt = datetime.strptime(last, "%Y%m%d")
                sd = (dt + timedelta(days=1)).strftime("%Y%m%d")
            except ValueError:
                sd = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
        elif start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            # 默认从 7 天前开始(增量)
            sd = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")

        ed = datetime.now().strftime("%Y%m%d")

        logger.info(f"[CCTV] {sd} ~ {ed}")
        cur = datetime.strptime(sd, "%Y%m%d")
        end = datetime.strptime(ed, "%Y%m%d")
        total_inserted = 0
        last_success_date = None
        while cur <= end:
            td = cur.strftime("%Y%m%d")
            try:
                df = self.client.pro.cctv_news(date=td)
                if df is not None and len(df) > 0:
                    df["datetime"] = td  # 用 date 作 datetime
                    # Tushare cctv_news 不返回 md5。SQLite 组合主键允许 NULL，过去
                    # 因而会把同一天相同节目重复写入。这里补稳定主键并统一来源。
                    from core.news_event_store import stable_cctv_md5
                    if "src" not in df.columns:
                        df["src"] = "cctv"
                    else:
                        df["src"] = df["src"].fillna("cctv").replace("", "cctv")
                    df["md5"] = df.apply(
                        lambda row: stable_cctv_md5(td, row.get("title"), row.get("content")),
                        axis=1,
                    )
                    df["snap_ts"] = snap_ts()
                    # 写入 db_cn_news.db
                    inserted = upsert_df(self.conn_news, df, table, key_cols=["datetime", "md5"])
                    if inserted > 0:
                        total_inserted += inserted
                        last_success_date = td
            except Exception as e:
                logger.error(f"[CCTV:{td}] 失败: {e}", exc_info=True)
            cur += timedelta(days=1)

        if last_success_date:
            # ctrl 写到 db_cn_news.db 的 tbl_news_ctrl
            _update_news_ctrl_value(self.conn_news, ctrl_key, last_success_date)

        result["inserted"] = total_inserted
        result["elapsed"] = time.time() - t0
        logger.info(f"[CCTV] +{total_inserted} 行  耗时 {result['elapsed']:.1f}s")
        return result

    # ====================================================
    # 6. 全量每日流程
    # ====================================================
    def update_all_daily(self) -> dict:
        """全量每日流程(按依赖顺序)"""
        logger.info("=" * 60)
        logger.info("🚀 CNDataDown.update_all_daily 启动")
        logger.info("=" * 60)
        result = {}

        # 1. basic(无依赖)
        try:
            result["basic"] = self.update_basic(bFull=False)
        except Exception as e:
            logger.error(f"[update_basic] 异常: {e}", exc_info=True)
            result["basic"] = 0

        # 2. tradecal(无依赖)
        try:
            result["tradecal"] = self.update_tradecal()
        except Exception as e:
            logger.error(f"[update_tradecal] 异常: {e}", exc_info=True)
            result["tradecal"] = 0

        # 3. kpl_list + kpl_concept_cons(依赖 tradecal)
        try:
            result["kpl_list"] = self.update_kpl_list()
        except Exception as e:
            logger.error(f"[update_kpl_list] 异常: {e}", exc_info=True)
            result["kpl_list"] = 0

        try:
            result["kpl_concept_cons"] = self.update_kpl_concept_cons()
        except Exception as e:
            logger.error(f"[update_kpl_concept_cons] 异常: {e}", exc_info=True)
            result["kpl_concept_cons"] = 0

        # 4. news(无依赖)
        try:
            news_res = self.update_news()
            result["news"] = sum(news_res.values())
            result["news_detail"] = news_res
        except Exception as e:
            logger.error(f"[update_news] 异常: {e}", exc_info=True)
            result["news"] = 0

        logger.info("=" * 60)
        logger.info(f"✅ update_all_daily 完成: {result}")
        logger.info("=" * 60)
        return result

    # ====================================================
    # 6. A 股日 K 线(按日期循环拉所有股票)
    # ====================================================
    def update_daily(self, start_date: str = None, end_date: str = None) -> int:
        """A 股日 K 线 - 按日期循环(每天所有股票)

        ⚠️ 重要设计原则:tbl_cn_day 存的是 **不复权原始数据**
           - 写入字段(open/close/low/high/vol/amount)直接来自 pro.daily,**不做任何复权计算**
           - tushare pro.daily 接口**没有复权参数**(官方明确"未复权行情")
           - 前复权只在 update_week / update_month 里临时计算(不写入日表)
           - 好处:原始数据可追溯,任意时刻都能根据 adj_factor 重算前复权

        Tushare 推荐做法: pro.daily(trade_date=YYYYMMDD) 一次返回当天全部股票
        比循环 ts_code 快得多(一次 = ~5500 行 = 23 年历史)

        数据可用范围:Tushare 日线从 1991 年起(平安银行上市)
        增量:从 ctrl.max_date 开始(允许 ctrl 那一天重拉,主键去重)
        全量:start_date='20150101'(11 年 ~1180 万行)

        写入 db_cn_basic.db

        Args:
            start_date: YYYYMMDD,默认 20150101(全量时)
            end_date: YYYYMMDD,默认今天
        """
        table = "tbl_cn_day"
        ctrl_key = "cn_daily"
        t0 = time.time()
        total_inserted = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            # ctrl 表在 db_cn_basic.db(同一张表,直接读)
            # 默认从 ctrl 那一日开始(允许 ctrl 当天重拉,主键去重保证幂等)
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20150101"

        # 构造日期序列
        try:
            d_start = datetime.strptime(sd, "%Y%m%d")
            d_end = datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            logger.error(f"[日线] 日期格式错误: sd={sd} end={end_date}")
            return 0

        cur = d_start
        days_processed = 0
        last_success_date = None
        DAILY_LIMIT = 6000  # tushare daily 单次最大 6000
        while cur <= d_end:
            td = cur.strftime("%Y%m%d")
            # 单日 OFFSET + LIMIT 分页(2024+ 后期每天可能 > 6000)
            offset = 0
            page_count = 0
            day_inserted = 0
            while True:
                try:
                    df = self.client.daily(trade_date=td, limit=DAILY_LIMIT, offset=offset)
                    if df is None or len(df) == 0:
                        break

                    df["snap_ts"] = snap_ts()
                    # 写入 db_cn_basic.db
                    inserted = upsert_df(self.conn_basic, df, table, key_cols=["ts_code", "trade_date"])
                    if inserted > 0:
                        day_inserted += inserted
                        last_success_date = td

                    # 返回 < limit 说明当天拉完
                    if len(df) < DAILY_LIMIT:
                        break

                    offset += DAILY_LIMIT
                    page_count += 1
                    # 安全上限:单日最多 50 页(30万条)
                    if page_count >= 50:
                        logger.warning(f"[日线:{td}] 超过 50 页,停止")
                        break
                except Exception as e:
                    logger.error(f"[日线:{td}] offset={offset} 失败: {e}", exc_info=True)
                    break

            total_inserted += day_inserted
            days_processed += 1
            # 每 100 天打一次进度
            if days_processed % 100 == 0:
                logger.info(f"[日线] 进度 {td} 累计 +{total_inserted:,}")
            cur += timedelta(days=1)

        if last_success_date:
            # ctrl 表在 db_cn_basic.db(同一张表)
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  耗时 {elapsed:.1f}s")
        return total_inserted

    # ====================================================
    # 6a. A 股复权因子(从 pro.adj_factor 拉取)
    # ====================================================
    def update_adj_factor(self, start_date: str = None, end_date: str = None) -> int:
        """复权因子增量更新

        Tushare pro.adj_factor 接口:ts_code, trade_date, adj_factor
        拉取频率:每天(与 daily 同步)
        写入 db_cn_basic.db

        复权因子用于前复权处理:
        - 前复权价 = 原始价 × adj_factor
        - 前复权成交量 = 原始成交量 ÷ adj_factor

        实现:按**日期循环**(不是按股票循环),每次 pro.adj_factor(trade_date=YYYYMMDD)
        一天调一次拿当天所有股票,比逐只股票循环快得多
        """
        table = "tbl_cn_adj_factor"
        ctrl_key = "cn_adj_factor"
        t0 = time.time()
        total_inserted = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            # ctrl 表在 db_cn_basic.db
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20150101"

        try:
            d_start = datetime.strptime(sd, "%Y%m%d")
            d_end = datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            logger.error(f"[复权因子] 日期格式错误: sd={sd} end={end_date}")
            return 0

        # 按日期循环(每天 pro.adj_factor(trade_date=td) 拿当天所有股票)
        ADJ_LIMIT = 6000  # tushare adj_factor 单次最大 6000
        last_success_date = None
        cur = d_start
        days_processed = 0
        while cur <= d_end:
            td = cur.strftime("%Y%m%d")
            offset = 0
            page_count = 0
            day_inserted = 0
            while True:
                try:
                    df = self.client.pro.adj_factor(
                        trade_date=td, limit=ADJ_LIMIT, offset=offset
                    )
                    if df is None or len(df) == 0:
                        break

                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(self.conn_basic, df, table, key_cols=["ts_code", "trade_date"])
                    if inserted > 0:
                        day_inserted += inserted
                        last_success_date = td

                    if len(df) < ADJ_LIMIT:
                        break

                    offset += ADJ_LIMIT
                    page_count += 1
                    if page_count >= 50:
                        logger.warning(f"[复权因子:{td}] 超过 50 页,停止")
                        break
                except Exception as e:
                    logger.error(f"[复权因子:{td}] offset={offset} 失败: {e}", exc_info=True)
                    break

            total_inserted += day_inserted
            days_processed += 1
            if days_processed % 100 == 0:
                logger.info(f"[复权因子] 进度 {td} 累计 +{total_inserted:,}")
            cur += timedelta(days=1)

        if last_success_date:
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>10,} 行  耗时 {elapsed:.1f}s")
        return total_inserted

    # ====================================================
    # 6b. 周 K 线(同时生成前复权和原始不复权表,覆盖更新)
    # ====================================================
    def update_week(self, start_date: str = None, end_date: str = None) -> int:
        """周 K 线 - 同时生成前复权和原始不复权结果(覆盖更新)

        数据流:
        1. 从 db_cn_basic.db 的 tbl_cn_day 读日 K(不复权)
        2. 日 K 直接按 ISO 周聚合,写 tbl_cn_week_origin
        3. 用 tbl_cn_adj_factor 计算前复权后按 ISO 周聚合,写 tbl_cn_week

        完全照搬 MyATM __aggregate_daily_to_freq__ 逻辑:
        - open: 周内第一日 open(头一天)
        - close: 周内最后一日 close(尾一天)
        - high: max(周内 high)
        - low: min(周内 low)
        - vol: sum(周内 vol)
        - amount: sum(周内 amount)
        - trade_date: 周内最后交易日(YYYYMMDD)
        - pre_close: 上一周期 close
        - change: close - pre_close
        - pct_chg: (close - pre_close) / pre_close * 100

        两张表在同一个事务中覆盖更新。

        Args:
            start_date: YYYYMMDD,默认 None(读全表 min)
            end_date: YYYYMMDD,默认 None(读全表 max)
        """
        counts = rebuild_period_tables(
            self.conn_basic,
            freq="weekly",
            aggregate=self._aggregate_daily_to_freq,
            start_date=start_date,
            end_date=end_date,
        )
        return counts["tbl_cn_week"]

    # ====================================================
    # 6c. 月 K 线(同时生成前复权和原始不复权表,覆盖更新)
    # ====================================================
    def update_month(self, start_date: str = None, end_date: str = None) -> int:
        """月 K 线 - 同时生成前复权和原始不复权结果(覆盖更新)

        实现方式同 update_week,只是按自然月聚合。

        写入 tbl_cn_month 和 tbl_cn_month_origin,两张表原子覆盖。
        """
        counts = rebuild_period_tables(
            self.conn_basic,
            freq="monthly",
            aggregate=self._aggregate_daily_to_freq,
            start_date=start_date,
            end_date=end_date,
        )
        return counts["tbl_cn_month"]

    # ====================================================
    # 6d. 每日涨跌停价格(按日循环,全市场)
    # ====================================================
    def update_stk_limit(self, start_date: str = None, end_date: str = None) -> int:
        """每日涨跌停价格(全市场,按日循环)

        数据源:tushare pro.stk_limit
        文档:https://tushare.pro/document/2?doc_id=183
        限制:单次最多 5800 条(A 股全市场 ~5438,单日不超,不用分页)

        ⚠️ 循环策略:按日循环(不按 ts_code 循环)
        理由:实测 start_date+end_date 范围参数不生效,只能 trade_date 单日拉

        Args:
            start_date: 默认 None(走 ctrl.max_date + 1,或 20150101)
            end_date: 默认 None(今天)
        """
        table = "tbl_cn_stk_limit"
        ctrl_key = "cn_stk_limit"
        t0 = time.time()
        total_inserted = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20150101"

        try:
            d_start = datetime.strptime(sd, "%Y%m%d")
            d_end = datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            logger.error(f"[涨跌停] 日期格式错误: sd={sd} end={end_date}")
            return 0

        cur = d_start
        last_success_date = None
        while cur <= d_end:
            td = cur.strftime("%Y%m%d")
            try:
                # 单日全市场,~5438 行,< 5800 限制
                df = self.client.stk_limit(trade_date=td)
                if df is None or len(df) == 0:
                    # 非交易日 / 没数据,不推进断点
                    cur += timedelta(days=1)
                    continue
                df["snap_ts"] = snap_ts()
                inserted = upsert_df(
                    self.conn_basic, df, table,
                    key_cols=["trade_date", "ts_code"],
                )
                if inserted > 0:
                    last_success_date = td
                total_inserted += inserted
            except Exception as e:
                logger.error(f"[涨跌停:{td}] 失败: {e}", exc_info=True)
            cur += timedelta(days=1)

        if last_success_date:
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_suspend(self, start_date: str = None, end_date: str = None) -> int:
        """每日停复牌信息(2026-09-15 新增)

        数据源:tushare pro.suspend_d
        文档:https://tushare.pro/document/2?doc_id=214
        限制:单次最多 5000 条;积分要求 2000+
        返回字段:ts_code, trade_date, suspend_timing(日内时段,可能空), suspend_type(S/R)
        重要特性:停牌期间每天都有一行快照(从停牌第一天一直覆盖到复牌前一天)

        ⚠️ 循环策略:按月循环(实测 start_date+end_date 范围参数有效)
        理由:
          - 按天循环:2852 天 = 2852 次调用,触限流 13 次(实测 ~25 分钟)
          - 按月循环:503 个月 ≈ 503 次调用,**不会触限流**,~5 分钟跑完
          - 单月行数 ~200,远低于 5000 限制,不用分页

        Args:
            start_date: 默认 ctrl.cn_suspend(初次默认 20150101)
            end_date: 默认今天
        """
        table = "tbl_cn_suspend"
        ctrl_key = "cn_suspend"
        t0 = time.time()
        total_inserted = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20150101"

        # 按月循环(实测范围参数生效)
        from datetime import datetime as _dt
        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd
        total_calls = 0
        while cur <= end:
            # 当月最后一天
            if cur.month == 12:
                next_month = cur.replace(year=cur.year + 1, month=1)
            else:
                next_month = cur.replace(month=cur.month + 1)
            cur_end = min(next_month - _dt.resolution, end)
            cur_sd = cur.strftime("%Y%m%d")
            cur_ed = cur_end.strftime("%Y%m%d")

            try:
                df = self.client.suspend_d(start_date=cur_sd, end_date=cur_ed)
                total_calls += 1
                if df is not None and len(df) > 0:
                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_basic, df, table,
                        key_cols=["trade_date", "ts_code"],
                    )
                    total_inserted += inserted
                    last_success_date = cur_ed
            except Exception as e:
                logger.error(f"[suspend:{cur_sd}~{cur_ed}] 失败: {e}", exc_info=True)
                break  # 失败保留 ctrl,下次从 last_success_date 继续

            cur = next_month

        # 只有有数据写入才推进 ctrl(避免空跑时 ctrl 误推到 sd)
        if total_inserted > 0:
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_top_list(self, start_date: str = None, end_date: str = None) -> int:
        """龙虎榜每日活跃(2026-09-15 新增)

        数据源:tushare pro.top_list
        文档:https://tushare.pro/document/2?doc_id=106
        限制:必填 trade_date(单日),不支持范围
        实际起始:2020-12-01(tushare 后期加入)
        返回 15 列,单日 ~65 行

        ⚠️ 按日循环(实测无范围参数):
          - 起始~今天 ~1400 个交易日
          - 200/分钟限频,需要 sleep,约 7-15 分钟
          - top_inst 同步跑要 30+ 分钟(655 行/日)
        """
        return self._update_daily_top("top_list", "tbl_cn_top_list", "cn_top_list",
                                       start_date, end_date,
                                       key_cols=["trade_date", "ts_code"])

    def update_top_inst(self, start_date: str = None, end_date: str = None) -> int:
        """龙虎榜机构买卖明细(2026-09-15 新增)

        数据源:tushare pro.top_inst
        文档:https://tushare.pro/document/2?doc_id=107
        限制:必填 trade_date(单日),不支持范围
        实际起始:2020-12-01
        返回 10 列,单日 ~655 行

        ⚠️ 按日循环(实测无范围参数):
          - 数据量大(655 行/日),写入用 upsert
          - 起始~今天 ~1400 个交易日,30-60 分钟
        """
        return self._update_daily_top("top_inst", "tbl_cn_top_inst", "cn_top_inst",
                                       start_date, end_date,
                                       key_cols=["trade_date", "ts_code", "exalter", "side"])

    def _update_daily_top(self, api_method: str, table: str, ctrl_key: str,
                           start_date: str, end_date: str,
                           key_cols: list) -> int:
        """top_list / top_inst 共用的按日循环拉取实现(2026-09-15 新增)

        全部写在 db_cn_kpl.db(用户要求"都放在 kpl db 下")

        Args:
            api_method: tushare 接口名(如 'top_list' / 'top_inst')
            table: SQLite 表名
            ctrl_key: tbl_basic_ctrl key(虽然放 kpl db,ctrl 仍统一在 basic)
            start_date: 默认 ctrl 或 None
            end_date: 默认今天
            key_cols: upsert 主键列
        """
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        t0 = time.time()
        total_inserted = 0
        total_calls = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
            from core.offline_db_client import get_ctrl_kpl
            last = get_ctrl_kpl(ctrl_key, conn=self.conn_kpl)
            sd = last if last else "20201201"  # 实际起始日期

        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd

        while cur <= end:
            cur_sd = cur.strftime("%Y%m%d")
            try:
                # 跳过周六周日(数据库里 trade_date 是交易日,周末不该有数据)
                if cur.weekday() >= 5:
                    cur += _td(days=1)
                    continue
                method = getattr(self.client, api_method)
                df = method(trade_date=cur_sd)
                total_calls += 1
                if df is not None and len(df) > 0:
                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_kpl, df, table,  # ← 用 conn_kpl(用户要求)
                        key_cols=key_cols,
                    )
                    total_inserted += inserted
                last_success_date = cur_sd
            except Exception as e:
                logger.error(f"[{api_method}:{cur_sd}] 失败: {e}", exc_info=True)
                # 单日失败不中止,继续下一天
                last_success_date = cur_sd
            cur += _td(days=1)

        # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl,top_list/top_inst 等共用)
        from core.offline_db_client import update_ctrl_kpl
        update_ctrl_kpl(self.conn_kpl, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_block_trade(self, start_date: str = None, end_date: str = None) -> int:
        """大宗交易(2026-09-15 新增,用户要求)

        数据源:tushare pro.block_trade
        文档:https://tushare.pro/document/2?doc_id=355
        限制:单次最多 1000 条;积分要求 2000+
        实际起始:2020-12-29(实测)
        返回 7 列:trade_date, ts_code, price, vol, amount, buyer, seller

        ⚠️ 按月循环(范围参数有效)+ OFFSET 分页(1000 行/页)
        """
        return self._update_paged_top("block_trade", "tbl_cn_block_trade", "cn_block_trade",
                                        start_date, end_date,
                                        key_cols=["trade_date", "ts_code", "buyer", "seller"],
                                        default_start="20201229")  # 实际起始

    def update_ggt_daily(self, start_date: str = None, end_date: str = None) -> int:
        """港股通每日成交(2026-09-15 新增,用户要求)

        数据源:tushare pro.ggt_daily
        文档:doc_id 未确认(网络搜索失败,可能是 199 / 297 / 299,待查)
        限制:单次最多 1000 条;积分要求 2000+
        实际起始:**2015-01-05**(实测,tushare 早期就有)
        返回 5 列:trade_date, buy_amount, buy_volume, sell_amount, sell_volume

        ⚠️ 按月循环(实测范围参数有效),单月 ~30 行,远低于 1000 限制
        """
        return self._update_paged_top("ggt_daily", "tbl_cn_ggt_daily", "cn_ggt_daily",
                                        start_date, end_date,
                                        key_cols=["trade_date"])

    def update_hsgt_top10(self, start_date: str = None, end_date: str = None) -> int:
        """沪深股通十大成交股(2026-09-15 新增,用户要求)

        数据源:tushare pro.hsgt_top10
        文档:https://tushare.pro/document/2?doc_id=356
        限制:单次最多 600/月(实测);积分要求 2000+
        实际起始:2014-11-17(沪港通开通日)
        返回 11 列:trade_date, ts_code, name, close, change, rank, market_type, amount, net_amount, buy, sell

        ⚠️ 按月循环(实测范围参数有效),单月 ~600 行,接近单次上限,可能需要分页
        """
        return self._update_paged_top("hsgt_top10", "tbl_cn_hsgt_top10", "cn_hsgt_top10",
                                        start_date, end_date,
                                        key_cols=["trade_date", "ts_code", "market_type"])

    def _update_paged_top(self, api_method: str, table: str, ctrl_key: str,
                            start_date: str, end_date: str,
                            key_cols: list, page_size: int = 1000,
                            default_start: str = "20150101") -> int:
        """block_trade / ggt_daily / hsgt_top10 共用按月循环 + OFFSET 分页(2026-09-15 新增)

        区别于 _update_daily_top:这个按月 + 支持 OFFSET 分页(因单次最多 1000)
        """
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        t0 = time.time()
        total_inserted = 0
        total_calls = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
            from core.offline_db_client import get_ctrl_kpl
            last = get_ctrl_kpl(ctrl_key, conn=self.conn_kpl)
            sd = last if last else default_start  # 默认起始

        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd

        while cur <= end:
            if cur.month == 12:
                next_month = cur.replace(year=cur.year + 1, month=1)
            else:
                next_month = cur.replace(month=cur.month + 1)
            cur_end = min(next_month - _td(days=1), end)
            cur_sd = cur.strftime("%Y%m%d")
            cur_ed = cur_end.strftime("%Y%m%d")

            try:
                method = getattr(self.client, api_method)
                # 单月 OFFSET 分页
                offset = 0
                while True:
                    df = method(start_date=cur_sd, end_date=cur_ed, limit=page_size, offset=offset)
                    total_calls += 1
                    if df is None or len(df) == 0:
                        break
                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_kpl, df, table,
                        key_cols=key_cols,
                    )
                    total_inserted += inserted
                    # 不满一页说明已经拉完
                    if len(df) < page_size:
                        break
                    offset += page_size
                    # 安全上限(防止无限循环)
                    if offset > 50000:
                        logger.warning(f"[{api_method}:{cur_sd}] 单月超过 50000 行,中止该月")
                        break
                last_success_date = cur_ed
            except Exception as e:
                logger.error(f"[{api_method}:{cur_sd}~{cur_ed}] 失败: {e}", exc_info=True)
                break  # 失败保留 ctrl

            cur = next_month

        # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl,block_trade/ggt/hsgt 共用)
        from core.offline_db_client import update_ctrl_kpl
        update_ctrl_kpl(self.conn_kpl, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_limit_list(self, start_date: str = None, end_date: str = None) -> int:
        """每日涨跌停列表(2026-09-15 新增)

        数据源:tushare pro.limit_list_d
        文档:https://tushare.pro/document/2?doc_id=298
        限制:单次最多 2500 条;积分要求 5000+
        实际起始:**2019-11-28**(用户告知,实测 2019-11-28 有 26 行涨停)
        返回 18 列,含 limit_type: U=涨停 D=跌停 Z=炸板
        单日 < 2500 行,不会超限

        ⚠️ 按月循环,3 个 limit_type(U/D/Z)各跑一次:
          - 起始 2019-11-28,7 年
          - 全量 ~84 个月 × 3 个 type = 252 次调用
          - 实测 ~5-10 分钟跑完

        Args:
            start_date: 默认 ctrl.cn_limit_list 或 20191128(实测起始)
            end_date: 默认今天
        """
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        table = "tbl_cn_limit_list"
        ctrl_key = "cn_limit_list"
        t0 = time.time()
        total_inserted = 0
        total_calls = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
            from core.offline_db_client import get_ctrl_kpl
            last = get_ctrl_kpl(ctrl_key, conn=self.conn_kpl)
            sd = last if last else "20191128"  # 实测起始(用户告知)

        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd

        while cur <= end:
            if cur.month == 12:
                next_month = cur.replace(year=cur.year + 1, month=1)
            else:
                next_month = cur.replace(month=cur.month + 1)
            cur_end = min(next_month - _td(days=1), end)
            cur_sd = cur.strftime("%Y%m%d")
            cur_ed = cur_end.strftime("%Y%m%d")

            try:
                # 3 个 limit_type 各跑一次
                for lt in ("U", "D", "Z"):
                    df = self.client.limit_list_d(
                        start_date=cur_sd, end_date=cur_ed, limit_type=lt,
                    )
                    total_calls += 1
                    if df is not None and len(df) > 0:
                        df["snap_ts"] = snap_ts()
                        inserted = upsert_df(
                            self.conn_kpl, df, table,
                            key_cols=["trade_date", "ts_code", "limit"],
                        )
                        total_inserted += inserted
                last_success_date = cur_ed
            except Exception as e:
                logger.error(f"[limit_list_d:{cur_sd}~{cur_ed}] 失败: {e}", exc_info=True)
                break  # 失败保留 ctrl

            cur = next_month

        # 2026-09-15: 改用本地 ctrl (tbl_kpl_ctrl)
        from core.offline_db_client import update_ctrl_kpl
        update_ctrl_kpl(self.conn_kpl, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_moneyflow(self, start_date: str = None, end_date: str = None) -> int:
        """个股资金流向(2026-09-15 新增,用户要求)

        数据源:tushare pro.moneyflow
        文档:https://tushare.pro/document/2?doc_id=170
        限制:单次最多 6000 条;积分要求 2000+
        实际起始:**2015-01-05**(文档说 2010 起,实测 2015 起)
        返回 20 列:小/中/大/特大单的买卖(数量+金额)+ 净流入

        ⚠️ 按月循环 + OFFSET 分页(单月 > 6000,需分页):
          - 全量 2015-01 ~ 2026-09 = ~141 个月
          - 每月 ~6000/6000 = 1-2 页 OFFSET
          - 全量 1440 次调用,触限流,约 7-10 分钟
        """
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        table = "tbl_cn_moneyflow"
        ctrl_key = "cn_moneyflow"
        t0 = time.time()
        total_inserted = 0
        total_calls = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20150105"  # 实际起始

        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd

        while cur <= end:
            if cur.month == 12:
                next_month = cur.replace(year=cur.year + 1, month=1)
            else:
                next_month = cur.replace(month=cur.month + 1)
            cur_end = min(next_month - _td(days=1), end)
            cur_sd = cur.strftime("%Y%m%d")
            cur_ed = cur_end.strftime("%Y%m%d")

            try:
                # 单月 OFFSET 分页(6000/页)
                offset = 0
                while True:
                    df = self.client.moneyflow(
                        start_date=cur_sd, end_date=cur_ed, limit=6000, offset=offset,
                    )
                    total_calls += 1
                    if df is None or len(df) == 0:
                        break
                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_basic, df, table,
                        key_cols=["trade_date", "ts_code"],
                    )
                    total_inserted += inserted
                    # 不满一页说明已经拉完
                    if len(df) < 6000:
                        break
                    offset += 6000
                    # 安全上限(防止无限循环)
                    if offset > 100000:
                        logger.warning(f"[moneyflow:{cur_sd}] 单月超过 10 万行,中止该月")
                        break
                last_success_date = cur_ed
            except Exception as e:
                logger.error(f"[moneyflow:{cur_sd}~{cur_ed}] 失败: {e}", exc_info=True)
                break

            cur = next_month

        # 只有有数据写入才推进 ctrl(避免空跑时 ctrl 误推到 sd)
        if total_inserted > 0:
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_margin(self, start_date: str = None, end_date: str = None) -> int:
        """融资融券交易汇总(2026-09-15 新增,用户要求,挂 task_morning)

        数据源:tushare pro.margin
        文档:https://tushare.pro/document/2?doc_id=58
        限制:单次最多 3 行(SSE + SZSE + BSE),无 range 参数支持
        实际起始:**2010-04-01**(BSE 2021 开业前只有 SSE/SZSE 2 行)
        返回 9 列:trade_date, exchange_id, rzye, rzmre, rzche, rqye, rqmcl, rzrqye, rqyl

        ⚠️ 按日循环:
          - 数据量小,15 年 × 250 天 = ~3750 天,~1 万行
          - 200/分钟限频,3750 次调用 / 200 = 18 分钟上限,实际 ~15 分钟
          - 但融资融券数据盘后才出,放 task_morning(09:05)抓"昨天"
        """
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        table = "tbl_cn_margin"
        ctrl_key = "cn_margin"
        t0 = time.time()
        total_inserted = 0
        total_calls = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20150105"  # 实际起始(用户确认,2015-01-05 有数据)

        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd

        while cur <= end:
            cur_sd = cur.strftime("%Y%m%d")
            try:
                # 跳过周末(融资融券数据是交易日才有)
                if cur.weekday() >= 5:
                    cur += _td(days=1)
                    continue
                df = self.client.margin(trade_date=cur_sd)
                total_calls += 1
                if df is not None and len(df) > 0:
                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_basic, df, table,
                        key_cols=["trade_date", "exchange_id"],
                    )
                    total_inserted += inserted
                last_success_date = cur_sd
            except Exception as e:
                logger.error(f"[margin:{cur_sd}] 失败: {e}", exc_info=True)
                # 单日失败不中止,继续下一天
                last_success_date = cur_sd
            cur += _td(days=1)

        # 只有有数据写入才推进 ctrl(避免空跑时 ctrl 误推到 sd)
        if total_inserted > 0:
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_margin_detail(self, start_date: str = None, end_date: str = None) -> int:
        """融资融券交易明细(2026-09-15 新增,用户要求,挂 task_morning)

        数据源:tushare pro.margin_detail
        文档:https://tushare.pro/document/2?doc_id=59
        限制:单次最多 6000 条;积分要求 2000+
        实际起始:**2017-12-29**(文档说 2019 起,实测更早)
        返回 10 列:trade_date, ts_code, rzye, rqye, rzmre, rqyl, rzche, rqchl, rqmcl, rzrqye
        主键:(trade_date, ts_code)

        按月循环 + OFFSET 分页:
          - 单月 2000-6000 行(2018 早期约 1000,2024 约 4000)
          - 全量 2017-12 ~ 2026-09 = ~106 个月
          - 单月 OFFSET 0-1 次
          - 全量约 150-200 次调用
        """
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        table = "tbl_cn_margin_detail"
        ctrl_key = "cn_margin_detail"
        t0 = time.time()
        total_inserted = 0
        total_calls = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20141231"  # 实际起始(用户确认,2014-12-31 有数据,实测最早)

        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd

        while cur <= end:
            if cur.month == 12:
                next_month = cur.replace(year=cur.year + 1, month=1)
            else:
                next_month = cur.replace(month=cur.month + 1)
            cur_end = min(next_month - _td(days=1), end)
            cur_sd = cur.strftime("%Y%m%d")
            cur_ed = cur_end.strftime("%Y%m%d")

            try:
                # 单月 OFFSET 分页(6000/页)
                offset = 0
                while True:
                    df = self.client.margin_detail(
                        start_date=cur_sd, end_date=cur_ed, limit=6000, offset=offset,
                    )
                    total_calls += 1
                    if df is None or len(df) == 0:
                        break
                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_basic, df, table,
                        key_cols=["trade_date", "ts_code"],
                    )
                    total_inserted += inserted
                    # 不满一页说明已经拉完
                    if len(df) < 6000:
                        break
                    offset += 6000
                    # 安全上限(防止无限循环)
                    if offset > 100000:
                        logger.warning(f"[margin_detail:{cur_sd}] 单月超过 10 万行,中止该月")
                        break
                last_success_date = cur_ed
            except Exception as e:
                logger.error(f"[margin_detail:{cur_sd}~{cur_ed}] 失败: {e}", exc_info=True)
                break

            cur = next_month

        # 只有有数据写入才推进 ctrl(避免空跑时 ctrl 误推到 sd)
        if total_inserted > 0:
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_cyq_perf(self, start_date: str = None, end_date: str = None) -> int:
        """每日筹码及胜率(2026-09-15 新增,用户要求)

        数据源:tushare pro.cyq_perf
        文档:https://tushare.pro/document/2?doc_id=293
        限制:单次最多 6000 条;积分要求 5000+
        实际起始:**2020-01-02**(用户要求从 2020-01-01 开始,实测最早有数据;文档说 2018 起)
        返回 11 列:ts_code, trade_date, his_low, his_high,
                    cost_5pct, cost_15pct, cost_50pct, cost_85pct, cost_95pct,
                    weight_avg, winner_rate
        主键:(trade_date, ts_code)

        按日循环(逐日调用 trade_date,单日 5578 行 < 6000 限制):
          - 接口要求 ts_code 或 trade_date 至少传 1 个
          - 单日全市场 ~5578 行
          - 全量 2020-01 ~ 2026-09 = ~1660 天
          - 全量约 1660 次调用
        """
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        table = "tbl_cn_cyq_perf"
        ctrl_key = "cn_cyq_perf"
        t0 = time.time()
        total_inserted = 0
        total_calls = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20200102"  # 实际起始(用户要求 2020-01-01)

        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd

        while cur <= end:
            cur_sd = cur.strftime("%Y%m%d")
            try:
                # 单日调用 trade_date(单日 ~5578 行 < 6000 限制)
                df = self.client.cyq_perf(trade_date=cur_sd)
                total_calls += 1
                if df is not None and len(df) > 0:
                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_basic, df, table,
                        key_cols=["trade_date", "ts_code"],
                    )
                    total_inserted += inserted
                last_success_date = cur_sd
            except Exception as e:
                logger.error(f"[cyq_perf:{cur_sd}] 失败: {e}", exc_info=True)
                break

            cur += _td(days=1)

        # 只有有数据写入才推进 ctrl(避免空跑时 ctrl 误推到 sd)
        if total_inserted > 0:
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_daily_basic(self, start_date: str = None, end_date: str = None) -> int:
        """每日指标(2026-09-15 新增,用户要求)

        数据源:tushare pro.daily_basic
        文档:https://tushare.pro/document/2?doc_id=32
        限制:单次最多 6000 条;积分要求 2000+
        实际起始:**2015-01-05**(用户要求 2015-01-01 起,实测最早)
        返回 18 列:ts_code, trade_date, close, turnover_rate, turnover_rate_f,
                    volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
                    total_share, float_share, free_share, total_mv, circ_mv
        主键:(trade_date, ts_code)

        按日循环:
          - 单日全市场 (~5341 行 status='L') < 6000 限制,无需 OFFSET
          - 必须传 limit=6000 + status='L'(用户提示,文档说必填,实际不传也行但稳妥起见默认传)
          - 全量 2015-01-05 ~ 2026-09-15 = ~2850 天
          - 全量约 2850 次调用,触发限流 sleep 22s 约 ~70 分钟,总 ~2-3 小时
        """
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        table = "tbl_cn_daily_basic"
        ctrl_key = "cn_daily_basic"
        t0 = time.time()
        total_inserted = 0
        total_calls = 0

        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())
        if start_date:
            sd = _fmt_yyyymmdd(start_date)
        else:
            last = get_ctrl(self.conn_basic, ctrl_key)
            sd = last if last else "20150105"  # 实际起始(用户要求 2015-01-01)

        cur = _dt.strptime(sd, "%Y%m%d")
        end = _dt.strptime(end_date, "%Y%m%d")
        last_success_date = sd

        while cur <= end:
            cur_sd = cur.strftime("%Y%m%d")
            try:
                # 单日调用 trade_date,显式传 limit=6000 + status='L'(用户提示必填)
                df = self.client.daily_basic(
                    trade_date=cur_sd,
                    limit=6000,
                    status="L",  # L=上市,P=退市,D=退市
                )
                total_calls += 1
                if df is not None and len(df) > 0:
                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_basic, df, table,
                        key_cols=["trade_date", "ts_code"],
                    )
                    total_inserted += inserted
                last_success_date = cur_sd
            except Exception as e:
                logger.error(f"[daily_basic:{cur_sd}] 失败: {e}", exc_info=True)
                break

            cur += _td(days=1)

        # 只有有数据写入才推进 ctrl(避免空跑时 ctrl 误推到 sd)
        if total_inserted > 0:
            update_ctrl(self.conn_basic, ctrl_key, last_success_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def update_index_basic(self) -> int:
        """指数基本信息(2026-09-15 新增,用户要求)

        数据源:tushare pro.index_basic
        文档:https://tushare.pro/document/2?doc_id=94
        数据量:~950 行(SW / CSI / MSCI / SSE / SZSE 全市场指数)
        返回 8 列:ts_code, name, market, publisher, category, base_date, base_point, list_date
        主键:ts_code

        ⚠️ 用户要求每次覆盖更新(replace_table 全量)
          - 不需要日期参数(指数列表是静态的)
          - 不需要增量断点
          - 每次跑全量 ~950 行,~30 秒
        """
        table = "tbl_cn_index_basic"
        ctrl_key = "cn_index_basic"
        t0 = time.time()
        total_inserted = 0

        try:
            # 1 次调用获取全部指数
            df = self.client.index_basic()
            if df is None or len(df) == 0:
                logger.warning("[index_basic] 接口返回空,跳过")
                return 0

            # 添加 snap_ts
            df["snap_ts"] = snap_ts()

            # 全量替换(replace_table)
            replace_table(self.conn_index, df, table)
            total_inserted = len(df)

            # 更新 ctrl(标记覆盖完成,不重要日期,2026-09-15 改为 index DB)
            from core.offline_db_client import update_ctrl_index
            update_ctrl_index(self.conn_index, ctrl_key, "all")

            logger.info(f"[覆盖更新] {table:<28} {total_inserted:>6,} 行  耗时 {time.time()-t0:.1f}s")
        except Exception as e:
            logger.error(f"[index_basic] 失败: {e}", exc_info=True)
            return 0

        return total_inserted

    # ====================================================
    # 12 只指定指数的日线行情(2026-09-15 新增,用户要求)
    # ====================================================
    # 用户指定的 12 只指数(固定列表,跟 db 同步)
    INDEX_DAILY_CODES = [
        "000001.SH",   # 上证综指
        "000016.SH",   # 上证50
        "000905.SH",   # 中证500
        "000852.SH",   # 中证1000
        "000300.SH",   # 沪深300
        "399001.SZ",   # 深证成指
        "399106.SZ",   # 深证综指
        "399006.SZ",   # 创业板指
        "399102.SZ",   # 创业板综指
        "000680.SH",   # 上证科创板50
        "000688.SH",   # 科创50
        "899050.BJ",   # 北证50
    ]

    def update_index_daily(self, start_date: str = None, end_date: str = None) -> int:
        """12 只指数日线行情(2026-09-15 新增,用户要求)

        数据源:tushare pro.index_daily
        文档:https://tushare.pro/document/2?doc_id=95
        限制:单次默认 8000 行,需 limit + offset 分页
        实际起始:**1993-01-01**(用户要求);部分指数更晚成立(如 000300.SH 2005 年)
        返回 11 列:ts_code, trade_date, close, open, high, low, pre_close,
                    change, pct_chg, vol, amount
        主键:(trade_date, ts_code)

        增量逻辑(2026-09-15 改造):
        - 更新前:读 `tbl_index_ctrl.cn_index_daily` 拿上次最大日期
        - 从 `max_date + 1` 开始拉(避免重拉)
        - 更新后:写 `tbl_index_ctrl.cn_index_daily` 推到今天

        按 ts_code 循环 + 每只 ts_code 内 limit + offset 分页:
          - 12 只指数
          - 单只 ~8200 行(1993-2026)
          - 单只 2 页 limit=6000
          - 12 只共 ~10 万行,~24 次调用,触发限流 ~3 分钟
        """
        from core.offline_db_client import get_ctrl_index, update_ctrl_index

        table = "tbl_cn_index_daily"
        ctrl_key = "cn_index_daily"
        t0 = time.time()
        total_inserted = 0
        total_calls = 0
        end_date = _fmt_yyyymmdd(end_date) if end_date else _fmt_yyyymmdd(datetime.now())

        PAGE_SIZE = 6000  # 留 2000 余量,避免触上限 8000
        MAX_OFFSET = 50000  # 安全上限

        # 增量起点计算:
        # - 用户显式传 start_date → 严格用 user_sd(可能拉历史数据)
        # - 用户没传(None) → 用 ctrl.max_date+1 做增量
        last = get_ctrl_index(ctrl_key, conn=self.conn_index)
        # 注意:start_date 默认值是 "19930101",所以必须用 is None 判断
        if start_date is not None:
            user_sd = _fmt_yyyymmdd(start_date)
            sd = user_sd
            logger.info(f"[index_daily] 用户指定 start_date={sd}")
        else:
            user_sd = "19930101"
            if last:
                # 从 ctrl.max_date 后一天开始
                try:
                    from datetime import datetime as _dt
                    last_dt = _dt.strptime(last, "%Y%m%d")
                    next_dt = last_dt + timedelta(days=1)
                    sd = max(user_sd, next_dt.strftime("%Y%m%d"))
                except ValueError:
                    sd = user_sd
                logger.info(f"[index_daily] 增量模式:从 ctrl.max_date={last} 后一天={sd} 开始")
            else:
                sd = user_sd
                logger.info(f"[index_daily] 全量模式:从 {sd} 开始")

        # 纯增量模式下(start_date is None),如果起点 > end_date,说明数据已经是最新的,直接空跑
        if sd > end_date and start_date is None:
            logger.info(f"[index_daily] 数据已是最新(sd={sd} > end_date={end_date}),无新增,跳过")
            update_ctrl_index(self.conn_index, ctrl_key, last if last else end_date)
            return 0

        last_success_date = sd

        for ts_code in self.INDEX_DAILY_CODES:
            ts_inserted = 0
            offset = 0
            page_count = 0
            try:
                while True:
                    df = self.client.index_daily(
                        ts_code=ts_code,
                        start_date=sd,
                        end_date=end_date,
                        limit=PAGE_SIZE,
                        offset=offset,
                    )
                    total_calls += 1
                    if df is None or len(df) == 0:
                        break

                    df["snap_ts"] = snap_ts()
                    inserted = upsert_df(
                        self.conn_index, df, table,
                        key_cols=["trade_date", "ts_code"],
                    )
                    ts_inserted += inserted
                    total_inserted += inserted

                    logger.info(f"[index_daily:{ts_code}] offset={offset} +{inserted} 行(累计 {len(df)})")

                    # 不满一页说明已经拉完
                    if len(df) < PAGE_SIZE:
                        break

                    offset += PAGE_SIZE
                    page_count += 1
                    if offset > MAX_OFFSET:
                        logger.warning(f"[index_daily:{ts_code}] offset 超过 {MAX_OFFSET},中止")
                        break

                if ts_inserted > 0:
                    last_success_date = df["trade_date"].max()
                    logger.info(f"[index_daily:{ts_code}] 完成 +{ts_inserted:,} 行,{page_count + 1} 页")
                else:
                    logger.warning(f"[index_daily:{ts_code}] 无数据(指数 {ts_code} 可能还未成立)")
            except Exception as e:
                logger.error(f"[index_daily:{ts_code}] 失败: {e}", exc_info=True)
                # 单只失败不阻塞其他指数,继续
                continue

        # 只有有数据写入才推进 ctrl(避免空跑时 ctrl 误推)
        if total_inserted > 0:
            update_ctrl_index(self.conn_index, ctrl_key, last_success_date)
        elif last:
            # 空跑:保持 ctrl 不变(不要推到 sd)
            logger.info(f"[index_daily] 本次无新数据,ctrl 保持 {last}")
        else:
            # 全量首次 + 无数据:写 end_date 让下次跳过
            update_ctrl_index(self.conn_index, ctrl_key, end_date)
        elapsed = time.time() - t0
        logger.info(f"[完成] {table:<28} +{total_inserted:>12,} 行  调用 {total_calls} 次  耗时 {elapsed:.1f}s")
        return total_inserted

    def _aggregate_daily_to_freq(self, df_day, freq: str = "weekly"):
        """把日线数据按 weekly / monthly 聚合(照搬 MyATM)

        聚合规则:
        - open: 期内第一日 open(头一天)
        - close: 期内最后一日 close(尾一天)
        - high: max(期内 high)
        - low: min(期内 low)
        - vol: sum(期内 vol)
        - amount: sum(期内 amount)
        - trade_date: 期内最后交易日
        - pre_close: 上一周期 close(组间 shift)
        - change: close - pre_close
        - pct_chg: (close - pre_close) / pre_close * 100

        Args:
            df_day: 日线 DataFrame(必须含 ts_code, trade_date, open, high, low, close, vol, amount)
            freq: 'weekly' (ISO 周) / 'monthly' (自然月)
        """
        # first/last 必须建立在明确的交易日顺序上。
        df = df_day.sort_values(["ts_code", "trade_date"]).reset_index(drop=True).copy()

        # 标记分组键
        if freq == "monthly":
            df["_group"] = df["trade_date"].astype(str).str[:6]  # '202001'
        elif freq == "weekly":
            iso = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d").dt.isocalendar()
            df["_iso_year"] = iso["year"].astype(int)
            df["_iso_week"] = iso["week"].astype(int)
            df["_group"] = df["_iso_year"].astype(str) + "-W" + df["_iso_week"].astype(str).str.zfill(2)
        else:
            raise ValueError(f"freq must be 'weekly' or 'monthly', got {freq}")

        # 列名映射(tushare daily 用 vol/amount,我们的是)
        col_map = {"ts_code": "ts_code"}
        if "vol" in df.columns:
            col_map["vol"] = "vol"
        elif "volume" in df.columns:
            col_map["volume"] = "vol"
        if "amount" not in df.columns:
            # tushare 的 amount 是成交额(千元)
            if "turnover" in df.columns:
                col_map["turnover"] = "amount"
        df = df.rename(columns={v: k for k, v in col_map.items() if v != k})

        agg_dict = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "vol": "sum",
            "amount": "sum",
            "trade_date": "last",
        }
        agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}
        df_agg = df.groupby(["ts_code", "_group"]).agg(agg_dict).reset_index()

        # 按 ts_code + trade_date 排序
        df_agg = df_agg.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        # pre_close = 上一周期 close(同 ts_code)
        df_agg["pre_close"] = df_agg.groupby("ts_code")["close"].shift(1)
        df_agg["change"] = df_agg["close"] - df_agg["pre_close"]
        df_agg["pct_chg"] = (df_agg["change"] / df_agg["pre_close"] * 100).round(4)

        # 加 snap_ts
        df_agg["snap_ts"] = snap_ts()

        # 删 _group
        df_agg = df_agg.drop(columns=["_group"])

        return df_agg

    # ====================================================
    # 7. policy 业务 — tdx 数据下载(2026-09-17 从 sibling offline_db_client_policy 搬过来)
    # ====================================================
    # 与第 1-6 节(tushare 数据)的区别:
    #   - 数据源不同:这里走 tdx_client(通达信),不是 tushare
    #   - 调用方:caller 必须传 TdxClient 实例进来(类似 self.conn_basic / self.conn_news 的连接复用模式)
    #   - 落库:全部经 db_client 的 has_data / upsert_rows / mark_downloaded,
    #          本类不直接写 SQL(原则:下载器不碰 SQL,跟第 1-6 节一致)
    #   - 索引列表:本类顶部常量 INDEX_CODES_HERE(5 指数,policy 研究用)
    def update_minute(
        self,
        ts_code: str,
        trade_date: str,
        *,
        client,  # TdxClient 实例(由 caller 创建并复用)
        rate: float = 0.15,
        force: bool = False,
    ) -> int:
        """单只单日个股分钟:has_data → 拉 tdx → upsert_rows → mark → 限速

        流程:
          1. force=False 时调 has_data('minute', ts_code, trade_date) 查 ctrl;
             已下载则 return 0
          2. 调 client.get_history_minute(ts_code, int(trade_date)) 拉数据
             (失败/空 → mark_status('tdx_empty') + return 0,2026-09-19 改)
          3. trade_date 统一 dash
          4. 转 DataFrame → upsert_rows('minute', df)
          5. mark_downloaded('minute', [(ts_code, trade_date)]) 兜底 mark
        """
        raise RuntimeError(
            "legacy policy_minute.db/tbl_minute writes are disabled: historical "
            "volume units are not compatible with the current native TDX bars. "
            "Keep legacy rows read-only and use coreClient.data_provider.get_oneMin "
            "for canonical stock OHLCV."
        )
        # 1) 幂等检查
        if not force and has_data("minute", ts_code, trade_date):
            return 0

        # 2) 拉数据(client 必传)
        date_int = int(_ymd_dash_to_compact(trade_date))
        try:
            rows = client.get_history_minute(ts_code, date_int)
        except Exception as e:
            print(f"      ⚠️ {ts_code} {trade_date} minute 拉取失败: {e}")
            mark_status("minute", "tdx_empty", [(ts_code, trade_date)])  # 2026-09-19:标 tdx_empty,防止下次重跑空拉
            return 0
        if not rows:
            print(f"      ⚠️ {ts_code} {trade_date} minute 拉取为空")
            mark_status("minute", "tdx_empty", [(ts_code, trade_date)])  # 2026-09-19:同上
            return 0

        # 3) trade_date 统一 dash
        for r in rows:
            if "trade_date" in r:
                td = str(r["trade_date"])
                if len(td) == 8 and td.isdigit():
                    r["trade_date"] = _ymd_compact_to_dash(td)

        # 4) 转 DF → 写入
        minute_cols = ["ts_code", "trade_date", "datetime", "time_idx", "price", "vol"]
        df = pd.DataFrame(rows)[minute_cols]
        inserted = upsert_rows("minute", df)

        # 5) 兜底 mark
        mark_downloaded("minute", [(ts_code, trade_date)])

        # 6) 限速
        if rate > 0:
            time.sleep(rate)

        return inserted if inserted and inserted > 0 else 0

    def update_minute_index(
        self,
        ts_code: str,
        trade_date: str,
        *,
        client,
        rate: float = 0.15,
        force: bool = False,
    ) -> int:
        """大盘指数单日分钟:has_data → 拉 tdx → upsert_rows → mark → 限速

        ts_code 必须是指数代码(如 000001.SH / 399001.SZ / ...),
        pytdx get_history_minute_time_data 对指数也直接支持(已实测 5 指数都 240 行)。
        落库到 minute_index kind(同 db 文件、不同表 + 不同 ctrl)。
        """
        raise RuntimeError(
            "legacy policy_minute.db/tbl_minute_index writes are disabled: no "
            "verified index-specific native one-minute source contract is configured. "
            "Keep the legacy index series read-only until a separate index source is validated."
        )
        if not force and has_data("minute_index", ts_code, trade_date):
            return 0

        date_int = int(_ymd_dash_to_compact(trade_date))
        try:
            rows = client.get_history_minute(ts_code, date_int)
        except Exception as e:
            print(f"      ⚠️ {ts_code} {trade_date} minute_index 拉取失败: {e}")
            return 0
        if not rows:
            print(f"      ⚠️ {ts_code} {trade_date} minute_index 拉取为空")
            return 0

        for r in rows:
            if "trade_date" in r:
                td = str(r["trade_date"])
                if len(td) == 8 and td.isdigit():
                    r["trade_date"] = _ymd_compact_to_dash(td)

        minute_cols = ["ts_code", "trade_date", "datetime", "time_idx", "price", "vol"]
        df = pd.DataFrame(rows)[minute_cols]
        inserted = upsert_rows("minute_index", df)

        mark_downloaded("minute_index", [(ts_code, trade_date)])

        if rate > 0:
            time.sleep(rate)

        return inserted if inserted and inserted > 0 else 0

    def update_ticks(
        self,
        ts_code: str,
        trade_date: str,
        *,
        client,
        rate: float = 0.15,
        force: bool = False,
    ) -> int:
        """单只单日个股分笔成交:has_data → 拉 tdx → upsert_rows → mark → 限速

        2026-09-19 双写兼容改造(详见 docs/落库方案_v2.md):
        - 检测 tbl_tick_v2 是否存在 → 走新逻辑,使用新接口 fetch_history_ticks_with_meta
          + 落库 gate is_safe_to_persist + 新字段 seqId_in_minute
        - 否则 → 走旧逻辑(原有路径,不动)
        - 默认不破坏生产 db
        """
        v2_exists = table_exists(TICKS_DB, "tbl_tick_v2")
        if not force and has_data(db_kind="ticks_v2" if v2_exists else "ticks", ts_code=ts_code, trade_date=trade_date):
            return 0

        date_int = int(_ymd_dash_to_compact(trade_date))
        if v2_exists:
            # 新逻辑:fetch_history_ticks_with_meta + 落库 gate
            from coreClient.tdx_ticks_meta import (
                fetch_history_ticks_with_meta, is_safe_to_persist, IncompleteDataError,
            )
            try:
                rows, meta = fetch_history_ticks_with_meta(client, ts_code, date_int)
            except Exception as e:
                print(f"      ⚠️ {ts_code} {trade_date} ticks 拉取失败: {e}")
                return 0
            if not is_safe_to_persist(meta):
                # 抛错:不写库、不 mark、整批拒绝(避免重抓时覆盖好的旧数据)
                raise IncompleteDataError(meta)
            if not rows:
                print(f"      ⚠️ {ts_code} {trade_date} ticks 拉取为空")
                return 0
            # 补充 4 个审计字段(详见 docs/落库方案_v2.md 3.3 / 3.4)
            now_iso = datetime.now().isoformat(timespec="seconds")
            fetched_host = client.ip
            fetch_complete = 1 if meta.pagination_complete else 0
            for r in rows:
                r["fetch_seq"] = 1  # 第一次写入默认 1;后续 INSERT OR REPLACE 时覆盖
                r["fetched_at"] = now_iso
                r["fetched_host"] = fetched_host
                r["fetch_complete"] = fetch_complete
            tick_cols = [
                "ts_code", "trade_date", "time", "seqId_in_minute",
                "datetime", "price", "vol", "buyorsell",
                "fetch_seq", "fetched_at", "fetched_host", "fetch_complete",
            ]
            df = pd.DataFrame(rows)[tick_cols]
            inserted = upsert_rows("ticks_v2", df)
            mark_downloaded("ticks_v2", [(ts_code, trade_date)])
            if rate > 0:
                time.sleep(rate)
            return inserted if inserted and inserted > 0 else 0

        # 旧逻辑(原代码路径,完全不变)
        try:
            rows = client.get_history_ticks(ts_code, date_int)
        except Exception as e:
            print(f"      ⚠️ {ts_code} {trade_date} ticks 拉取失败: {e}")
            return 0
        if not rows:
            print(f"      ⚠️ {ts_code} {trade_date} ticks 拉取为空")
            return 0

        for r in rows:
            if "trade_date" in r:
                td = str(r["trade_date"])
                if len(td) == 8 and td.isdigit():
                    r["trade_date"] = _ymd_compact_to_dash(td)

        tick_cols = ["ts_code", "trade_date", "datetime", "time", "seqId", "price", "vol", "buyorsell"]
        df = pd.DataFrame(rows)[tick_cols]
        inserted = upsert_rows("ticks", df)

        mark_downloaded("ticks", [(ts_code, trade_date)])

        if rate > 0:
            time.sleep(rate)

        return inserted if inserted and inserted > 0 else 0

    # ====================================================
    # 8. 状态查询
    # ====================================================
    def show_status(self):
        """打印所有表的状态(行数 + 日期范围)"""
        show_status(DB_PATH)
        # 顺便打 ctrl 断点(走 db_client 工具,不再直接 SQL)
        try:
            print("-" * 60)
            print("Ctrl 断点(tbl_basic_ctrl,db_cn_basic.db):")
            df_basic_ctrl = get_ctrl_basic()  # 走 db_client,全表按 key 升序
            for _, r in df_basic_ctrl.iterrows():
                print(f"  {r['key']:<32} max_date={r['max_date']}")
            print("-" * 60)
            print("新闻源断点(tbl_news_ctrl,db_cn_news.db):")
            df_news_ctrl = get_news_ctrl()  # 走 db_client,全表按 src 升序
            for _, r in df_news_ctrl.iterrows():
                print(f"  {r['src']:<20} max_date={r['max_date']}")
            print()
        except Exception as e:
            logger.error(f"[show_status] 断点查询失败: {e}", exc_info=True)
