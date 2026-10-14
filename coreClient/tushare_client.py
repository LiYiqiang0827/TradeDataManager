"""
~/TradingAgent/coreClient/tushare_client.py
TushareClient 单例封装(复用一个 pro_api)

路径:~/TradingAgent/coreClient/tushare_client.py
配置:tushare_config.py(同目录下)
"""
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger
from typing import Optional

# 让 from coreClient.tushare_config import ... 工作
# (当 PYTHONPATH 包含 ~/TradingAgent/ 时,Python 会自动找到 coreClient 这个目录)
_HERE = Path(__file__).resolve().parent  # ~/TradingAgent/coreClient
_PARENT = _HERE.parent                    # ~/TradingAgent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from coreClient.tushare_config import (
    TUSHARE_TOKEN,
    TUSHARE_RATE_LIMIT_PER_MIN,
    TUSHARE_RETRY_MAX,
    TUSHARE_RETRY_SLEEP,
)


# ==================== Tushare API 限流控制 ====================
class RateLimiter:
    """简单的滑动窗口限流器(200/min)"""
    def __init__(self, max_per_min: int = 200):
        self.max_per_min = max_per_min
        self.calls = []  # 时间戳列表

    def acquire(self):
        """获取一次调用权限(超限自动 sleep)"""
        now = time.time()
        # 移除 60s 之前的记录
        self.calls = [t for t in self.calls if now - t < 60]
        if len(self.calls) >= self.max_per_min:
            sleep_for = 60 - (now - self.calls[0]) + 0.1
            logger.warning(f"触发限流,sleep {sleep_for:.1f}s")
            time.sleep(sleep_for)
            self.calls = []
        self.calls.append(time.time())


# ==================== 重试装饰器 ====================
def with_retry(func):
    """网络错误重试装饰器"""
    def wrapper(*args, **kwargs):
        last_err = None
        for attempt in range(TUSHARE_RETRY_MAX):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_err = e
                logger.warning(f"{func.__name__} 第 {attempt+1}/{TUSHARE_RETRY_MAX} 次失败: {e}")
                time.sleep(TUSHARE_RETRY_SLEEP * (attempt + 1))
        raise last_err
    return wrapper


# ==================== Tushare 客户端单例 ====================
class TushareClient:
    """Tushare pro API 客户端(单例)"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        os.environ["TUSHARE_TOKEN"] = TUSHARE_TOKEN
        import tushare as ts
        self.pro = ts.pro_api()
        self.rate_limiter = RateLimiter(TUSHARE_RATE_LIMIT_PER_MIN)
        logger.info("TushareClient 初始化完成")

    @with_retry
    def call(self, api_func_name: str, **params):
        """通用调用入口(带限流 + 重试)

        用法:
            client.call("stock_basic", list_status="L", exchange="SSE")
            → tushare.pro.stock_basic(list_status=..., exchange=...)
        """
        self.rate_limiter.acquire()
        api_func = getattr(self.pro, api_func_name)
        df = api_func(**params)
        return df

    # ---- 便捷封装(常用接口)----

    def stock_basic(self, **params):
        """股票基本信息"""
        return self.call("stock_basic", **params)

    def trade_cal(self, **params):
        """交易日历"""
        return self.call("trade_cal", **params)

    def stk_limit(self, **params):
        """每日涨跌停价格

        接口:tushare pro.stk_limit
        文档:https://tushare.pro/document/2?doc_id=183
        限制:单次最多 5800 条(可循环,总量不限)
        返回字段:trade_date, ts_code, up_limit, down_limit

        用法:
            # 单日全市场
            df = client.stk_limit(trade_date='20190625')
            # 单只一段时间
            df = client.stk_limit(ts_code='002149.SZ', start_date='20190115', end_date='20190615')
            # 单只全历史(默认)
            df = client.stk_limit(ts_code='000001.SZ')
        """
        return self.call("stk_limit", **params)

    def suspend_d(self, **params):
        """每日停复牌信息

        接口:tushare pro.suspend_d
        文档:https://tushare.pro/document/2?doc_id=214
        限制:单次最多 5000 条;积分要求 2000+
        返回字段:ts_code, trade_date, suspend_timing(日内时段,可能空), suspend_type(S/R)

        用法:
            # 单日全市场停牌
            df = client.suspend_d(trade_date='20200401')
            # 单只股票历史
            df = client.suspend_d(ts_code='002155.SZ', suspend_type='S')
            # 范围
            df = client.suspend_d(start_date='20240901', end_date='20240910')
        """
        return self.call("suspend_d", **params)

    def top_list(self, **params):
        """龙虎榜每日活跃

        接口:tushare pro.top_list
        文档:https://tushare.pro/document/2?doc_id=106
        限制:必填 trade_date(单日);积分要求 2000+
        实际起始:2020-12-01(tushare 后期加入)
        返回字段(15):trade_date, ts_code, name, close, pct_change, turnover_rate,
                     amount, l_sell, l_buy, l_amount, net_amount, net_rate,
                     amount_rate, float_values, reason

        用法:
            # 单日龙虎榜
            df = client.top_list(trade_date='20240913')
        """
        return self.call("top_list", **params)

    def top_inst(self, **params):
        """龙虎榜机构买卖明细

        接口:tushare pro.top_inst
        文档:https://tushare.pro/document/2?doc_id=107
        限制:必填 trade_date(单日);积分要求 2000+
        实际起始:2020-12-01
        返回字段(10):trade_date, ts_code, exalter, buy, buy_rate, sell,
                     sell_rate, net_buy, side, reason

        用法:
            # 单日机构明细
            df = client.top_inst(trade_date='20240913')
        """
        return self.call("top_inst", **params)

    def block_trade(self, **params):
        """大宗交易

        接口:tushare pro.block_trade
        文档:https://tushare.pro/document/2?doc_id=355
        限制:单次最多 1000 条;积分要求 2000+
        实际起始:2020-12-29(实测)
        返回字段(7):trade_date, ts_code, price, vol, amount, buyer, seller

        用法:
            # 单日大宗
            df = client.block_trade(trade_date='20240930')
            # 范围
            df = client.block_trade(start_date='20240901', end_date='20240930')
        """
        return self.call("block_trade", **params)

    def ggt_daily(self, **params):
        """港股通每日成交

        接口:tushare pro.ggt_daily
        文档:https://tushare.pro/document/2?doc_id=298
        限制:单次最多 1000 条;积分要求 2000+
        实际起始:2017-01-03(港股通 2016-12-05 开通)
        返回字段(5):trade_date, buy_amount, buy_volume, sell_amount, sell_volume

        用法:
            # 单日
            df = client.ggt_daily(trade_date='20240930')
            # 范围
            df = client.ggt_daily(start_date='20240901', end_date='20240930')
        """
        return self.call("ggt_daily", **params)

    def hsgt_top10(self, **params):
        """沪深股通十大成交股

        接口:tushare pro.hsgt_top10
        文档:https://tushare.pro/document/2?doc_id=356
        限制:积分要求 2000+
        实际起始:2014-11-17(沪港通开通日)
        返回字段(11):trade_date, ts_code, name, close, change, rank,
                      market_type, amount, net_amount, buy, sell

        用法:
            # 单日
            df = client.hsgt_top10(trade_date='20240930')
            # 范围
            df = client.hsgt_top10(start_date='20240901', end_date='20240930')
        """
        return self.call("hsgt_top10", **params)

    def moneyflow(self, **params):
        """个股资金流向

        接口:tushare pro.moneyflow
        文档:https://tushare.pro/document/2?doc_id=170
        限制:单次最多 6000 条;积分要求 2000+
        实际起始:2015-01-05(文档说 2010 起,实测 2015 起)
        返回 20 列:小/中/大/特大单的买卖(数量+金额)+ 净流入

        用法:
            # 单日全市场
            df = client.moneyflow(trade_date='20240913')
            # 单只历史
            df = client.moneyflow(ts_code='000001.SZ', start_date='20240101', end_date='20240930')
            # 范围(注意:单月 > 6000,需要 OFFSET 分页)
            df = client.moneyflow(start_date='20150101', end_date='20150131', limit=6000, offset=0)
        """
        return self.call("moneyflow", **params)

    def margin(self, **params):
        """融资融券交易汇总

        接口:tushare pro.margin
        文档:https://tushare.pro/document/2?doc_id=58
        限制:单次最多 3 行(3 个交易所);无 range 参数支持,需按日循环
        实际起始:2010-04-01(BSE 2021 开业后才有 3 行,之前只有 SSE/SZSE 2 行)
        返回 9 列:trade_date, exchange_id, rzye(融资余额), rzmre(融资买入), rzche(融资偿还),
                    rqye(融券余额), rqmcl(融券卖出), rzrqye(融资融券余额), rqyl(融券余量)

        用法:
            # 单日 3 交易所
            df = client.margin(trade_date='20240913')
            # 指定交易所
            df = client.margin(trade_date='20240913', exchange_id='SSE')
        """
        return self.call("margin", **params)

    def margin_detail(self, **params):
        """融资融券交易明细

        接口:tushare pro.margin_detail
        文档:https://tushare.pro/document/2?doc_id=59
        限制:单次最多 6000 行,单月通常 2000-6000 行,需 OFFSET 分页
        实际起始:2017-12-29
        返回 10 列:trade_date, ts_code, rzye(融资余额), rqye(融券余额),
                    rzmre(融资买入), rqyl(融券余量), rzche(融资偿还),
                    rqchl(融券偿还), rqmcl(融券卖出), rzrqye(融资融券余额)

        用法:
            # 单日
            df = client.margin_detail(trade_date='20240913')  # 3923 行
            # 单只历史
            df = client.margin_detail(ts_code='000001.SZ', start_date='20240101', end_date='20240131')
            # 范围(需 OFFSET 分页)
            df = client.margin_detail(start_date='20240901', end_date='20240930')  # 可能超 6000
        """
        return self.call("margin_detail", **params)

    def cyq_perf(self, **params):
        """每日筹码及胜率

        接口:tushare pro.cyq_perf
        文档:https://tushare.pro/document/2?doc_id=293
        限制:单次最多 6000 条;积分要求 5000+
        实际起始:**2020-01-02**(用户要求 2020-01-01 起,实测 2018-01-02 也行)
        返回 11 列:ts_code, trade_date, his_low, his_high,
                    cost_5pct, cost_15pct, cost_50pct, cost_85pct, cost_95pct,
                    weight_avg(加权成本), winner_rate(胜率%)

        用法:
            # 单日全市场(5578 行)
            df = client.cyq_perf(trade_date='20240913')
            # 单只月度
            df = client.cyq_perf(ts_code='000001.SZ', start_date='20240101', end_date='20240131')
            # 单只单日
            df = client.cyq_perf(ts_code='000001.SZ', trade_date='20240913')
        """
        return self.call("cyq_perf", **params)

    def daily_basic(self, **params):
        """每日指标(重要基本面指标)

        接口:tushare pro.daily_basic
        文档:https://tushare.pro/document/2?doc_id=32
        限制:单次最多 6000 条;积分要求 2000+
        实际起始:**2015-01-05**(用户要求 2015-01-01,实测最早)
        返回 18 列:ts_code, trade_date, close, turnover_rate, turnover_rate_f,
                    volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
                    total_share, float_share, free_share, total_mv, circ_mv

        注意:用户提示 limit 和 status 是必填(虽然实测不传也 OK,文档说需要,稳妥起见默认传)
        status: L=上市, P=退市, D=退市

        用法:
            # 单日全市场活跃股(~5341 行)
            df = client.daily_basic(trade_date='20240913')
            # 单只月度
            df = client.daily_basic(ts_code='000001.SZ', start_date='20240101', end_date='20240131')
            # 单只单日
            df = client.daily_basic(ts_code='000001.SZ', trade_date='20240913')
            # 退市股
            df = client.daily_basic(trade_date='20240913', status='P')
        """
        return self.call("daily_basic", **params)

    def index_basic(self, **params):
        """指数基本信息(全市场指数清单)

        接口:tushare pro.index_basic
        文档:https://tushare.pro/document/2?doc_id=94
        返回 8 列:ts_code, name, market, publisher, category, base_date, base_point, list_date
        数据量:~950 行(SW / MSCI / CSI / SSE / SZSE 等)

        用法:
            # 全部市场
            df = client.index_basic()
            # 按市场过滤
            df = client.index_basic(market='SW')    # 申万指数
            df = client.index_basic(market='CSI')   # 中证指数
            df = client.index_basic(market='MSCI')  # MSCI 指数
            df = client.index_basic(market='SSE')   # 上交所指数
            df = client.index_basic(market='SZSE')  # 深交所指数
        """
        return self.call("index_basic", **params)

    def index_daily(self, **params):
        """指数日线行情

        接口:tushare pro.index_daily
        文档:https://tushare.pro/document/2?doc_id=95
        限制:单次默认 8000 行,需 limit + offset 分页
        实际起始:**1993-01-04**(部分指数,如 000300.SH 2005 年才成立)

        返回 11 列:ts_code, trade_date, close, open, high, low,
                    pre_close, change, pct_chg, vol, amount

        用法:
            # 单只指数
            df = client.index_daily(ts_code='000001.SH')
            # 单只指数历史范围
            df = client.index_daily(ts_code='000001.SH', start_date='20240101', end_date='20240913')
            # limit + offset 分页
            df1 = client.index_daily(ts_code='000001.SH', start_date='19930101', end_date='20260915', limit=6000)
            df2 = client.index_daily(ts_code='000001.SH', start_date='19930101', end_date='20260915', limit=6000, offset=6000)
        """
        return self.call("index_daily", **params)

    def limit_list_d(self, **params):
        """每日涨跌停列表

        接口:tushare pro.limit_list_d
        文档:https://tushare.pro/document/2?doc_id=298
        限制:单次最多 2500 条;积分要求 5000+(200次/分钟,1万次/天)
        实际起始:2020 年起(文档说法);实测更早
        返回 18 列:trade_date, ts_code, industry, name, close, pct_chg, amount,
                    limit_amount, float_mv, total_mv, turnover_ratio, fd_amount,
                    first_time, last_time, open_times, up_stat, limit_times, limit

        limit_type:
          - U: 涨停
          - D: 跌停
          - Z: 炸板

        用法:
            # 单日涨停
            df = client.limit_list_d(trade_date='20240913', limit_type='U')
            # 单月涨停范围
            df = client.limit_list_d(start_date='20240901', end_date='20240930', limit_type='U')
            # 不传 limit_type:返回当日所有 U/D/Z
        """
        return self.call("limit_list_d", **params)

    def daily(self, **params):
        """日 K(单次只能 1 只)— 未复权原始数据

        ⚠️ tushare pro.daily 接口**不支持复权参数**(官方明确"未复权行情")
        要拿前复权必须用 pro_bar(只能单 ts_code,不适用全市场)
        所以我们的设计:tbl_cn_day 存不复权原始数据,
        前复权在 update_week/update_month 里临时用 adj_factor 算
        """
        return self.call("daily", **params)

    def daily_qfq_range(self, start_date: str, end_date: str, qfq: bool = True) -> pd.DataFrame:
        """拉取日期范围内所有日 K 数据(自动按日期循环 + OFFSET 分页)

        Tushare daily 范围查询默认只返最新 6000 行,要做全历史必须按日期循环。
        本方法自动遍历每天 + 分页,返回完整日线数据。

        Args:
            start_date: YYYYMMDD
            end_date: YYYYMMDD
            qfq: 是否前复权(默认 True)
        """
        all_df = []
        cur = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        DAILY_LIMIT = 6000
        while cur <= end:
            td = cur.strftime("%Y%m%d")
            offset = 0
            while True:
                params = {"trade_date": td, "limit": DAILY_LIMIT, "offset": offset}
                if qfq:
                    params["adj"] = "qfq"
                df = self.call("daily", **params)
                if df is None or len(df) == 0:
                    break
                all_df.append(df)
                if len(df) < DAILY_LIMIT:
                    break
                offset += DAILY_LIMIT
            cur += timedelta(days=1)

        if not all_df:
            return pd.DataFrame()
        return pd.concat(all_df, ignore_index=True)

    def weekly(self, **params):
        """周 K"""
        return self.call("weekly", **params)

    def monthly(self, **params):
        """月 K"""
        return self.call("monthly", **params)

    def adj_factor(self, **params):
        """复权因子"""
        return self.call("adj_factor", **params)

    def kpl_list(self, **params):
        """开盘啦涨停榜榜单"""
        return self.call("kpl_list", **params)

    def kpl_concept_cons(self, **params):
        """开盘啦题材成分(ts_code, name, con_name, con_code, trade_date, desc, hot_num)"""
        return self.call("kpl_concept_cons", **params)

    def kpl_concept(self, **params):
        """开盘啦题材排行(trade_date, ts_code, name, z_t_num, up_num)"""
        return self.call("kpl_concept", **params)

    def news(self, **params):
        """新闻(9 源之一)"""
        return self.call("news", **params)

    def major_news(self, **params):
        """头条新闻(重要新闻,跟普通 news 区分)

        tushare 接口:major_news
        """
        return self.call("major_news", **params)


# ==================== 测试 ====================
if __name__ == "__main__":
    client = TushareClient()
    df = client.stock_basic(list_status="L", exchange="SSE", fields="ts_code,name,industry")
    print(f"SH basic: {len(df)} 只")
    print(df.head())
