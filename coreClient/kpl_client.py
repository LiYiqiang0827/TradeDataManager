"""
~/TradingAgent/coreClient/kpl_client.py
开盘啦(kpl)API 客户端单例封装(限流 + 重试)

路径:~/TradingAgent/coreClient/kpl_client.py
配置:kpl_config.py(同目录下,auth_token + hosts + 限流 + 重试)

最常用的 6 个方法:
  1. market_sentiment()           涨跌统计
  2. limit_up_performance(date, board_type)  涨停表现详情 (1-5板)
  3. daily_limit_index()          涨停板数
  4. lhb_stock_list(date)         龙虎榜股票
  5. stock_trend(stock_id, day)   个股分时
  6. zt_gene(stock_id)            涨停基因

底层通用调用 .call(c, a, host=..., **params) 可访问任何 endpoint(参考 trading:kpl-api skill)。
"""
import sys
import time
import json
import requests
from pathlib import Path

import pandas as pd
from loguru import logger
from datetime import datetime
from typing import Optional, Union

# 让 from coreClient.kpl_config import ... 工作
_HERE = Path(__file__).resolve().parent
_PARENT = _HERE.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from coreClient.kpl_config import (
    get_kpl_auth,
    KPL_BASE_URL,
    KPL_HOST_APPHWHQ,
    KPL_HOST_APPHIS,
    KPL_HOST_APPLHB,
    KPL_HOST_APPPHQ,
    KPL_RATE_LIMIT_PER_MIN,
    KPL_RETRY_MAX,
    KPL_RETRY_SLEEP,
)


# ==================== 限流控制 ====================
class RateLimiter:
    """简单的滑动窗口限流器"""
    def __init__(self, max_per_min: int = 30):
        self.max_per_min = max_per_min
        self.calls = []

    def acquire(self):
        """获取一次调用权限(超限自动 sleep)"""
        now = time.time()
        self.calls = [t for t in self.calls if now - t < 60]
        if len(self.calls) >= self.max_per_min:
            sleep_for = 60 - (now - self.calls[0]) + 0.1
            logger.warning(f"[KPL] 触发限流,sleep {sleep_for:.1f}s")
            time.sleep(sleep_for)
            self.calls = []
        self.calls.append(time.time())


# ==================== 重试装饰器 ====================
# 只对网络错误重试(JSONDecodeError 是数据问题,重试没意义)
RETRYABLE_EXCEPTIONS = (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.HTTPError)


def with_retry(func):
    """网络错误重试装饰器(不重试 JSONDecodeError 等数据问题)"""
    def wrapper(*args, **kwargs):
        last_err = None
        for attempt in range(KPL_RETRY_MAX):
            try:
                return func(*args, **kwargs)
            except RETRYABLE_EXCEPTIONS as e:
                last_err = e
                logger.warning(f"[KPL] {func.__name__} 第 {attempt+1}/{KPL_RETRY_MAX} 次网络失败: {e}")
                time.sleep(KPL_RETRY_SLEEP * (attempt + 1))
            except Exception:
                # 非网络错误(比如 JSONDecodeError)不重试,直接抛
                raise
        raise last_err
    return wrapper


# ==================== 股票代码补全交易所后缀 ====================
# 数据源: ~/TradingAgent/offlineDataManager/data/db_cn_basic.db:tbl_cn_basic
# 验证日期: 2026-09-13(5562 只 L 状态股票全量扫描)
#
# 规则映射表(代码前 3 位 → 交易所后缀):
EXCHANGE_SUFFIX_RULES = {
    # 深主板(SZSE 主板)
    "000": "SZ", "002": "SZ", "003": "SZ", "001": "SZ",
    # 创业板(SZSE 创业板)
    "300": "SZ", "301": "SZ", "302": "SZ",
    # 上主板(SSE 主板)
    "600": "SH", "601": "SH", "603": "SH", "605": "SH",
    # 科创板(SSE 科创板)
    "688": "SH", "689": "SH",
    # 北交所(BSE)— 2026 年北交所统一使用 920 前缀
    "920": "BJ",
}


def add_exchange_suffix(ts_code: str) -> str:
    """给 6 位股票代码补全交易所后缀(无后缀 → .SH/.SZ/.BJ)

    Args:
        ts_code: 6 位代码("000978")或已有后缀("000978.SZ")

    Returns:
        完整代码("000978.SZ")

    Raises:
        ValueError: 无法识别前缀时(可能是 ETF/退市股票)

    规则(2026-09-13 验证):
        000/001/002/003 → SZ(深主板)
        300/301/302 → SZ(创业板)
        600/601/603/605 → SH(上主板)
        688/689 → SH(科创板)
        920 → BJ(北交所)

    示例:
        >>> add_exchange_suffix("000978")
        '000978.SZ'
        >>> add_exchange_suffix("688783.SH")
        '688783.SH'
        >>> add_exchange_suffix("920001")
        '920001.BJ'
    """
    if "." in ts_code:
        return ts_code  # 已经有后缀,直接返回

    prefix = ts_code[:3]
    suffix = EXCHANGE_SUFFIX_RULES.get(prefix)
    if suffix is None:
        raise ValueError(
            f"无法识别股票代码 '{ts_code}' 的交易所前缀 '{prefix}',"
            f"已支持前缀: {sorted(EXCHANGE_SUFFIX_RULES.keys())}"
        )
    return f"{ts_code}.{suffix}"


def add_exchange_suffix_series(series) -> "pd.Series":
    """批量给 pd.Series 补全后缀(向量化)

    Args:
        series: pd.Series of 股票代码(纯数字或带后缀)

    Returns:
        pd.Series, 每个值都补全后缀
    """
    return series.astype(str).map(add_exchange_suffix)


# ==================== KPL 客户端单例 ====================
class KPLClient:
    """开盘啦(kpl)HTTP API 客户端(单例)

    认证:auth_token 自动从 kpl_config 或 ~/.kpl-api/.auth.json 加载
    限流:KPL_RATE_LIMIT_PER_MIN 次/分钟
    重试:KPL_RETRY_MAX 次(指数退避)

    用法:
        client = KPLClient()                # 单例
        df = client.market_sentiment()       # 涨跌统计
        df = client.limit_up_performance('2026-09-11', board_type=1)  # 一板
        df = client.lhb_stock_list('2026-09-11')
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        self.token, self.user_id = get_kpl_auth()
        self.rate_limiter = RateLimiter(KPL_RATE_LIMIT_PER_MIN)
        logger.info(f"KPLClient 初始化完成(token: {self.token[:10]}..., user_id: {self.user_id[:6]}...)")

    # ==================== IP 直连 fallback ====================
    # 国内常见 aphis/applhb 等子域名被 DNS 屏蔽或被代理干扰
    # 解决:用预解析的 IP 直连 + Host 头 + 关掉 SSL 验证
    _HOST_IP_FALLBACK = {
        # host_short: known IP (2026-09-12 通过 dig +8.8.8.8 验证)
        "aphis.longhuvip.com": "124.71.63.67",
        "applhb.longhuvip.com": "113.45.200.207",     # 多 IP,选第一个
        "apphwhq.longhuvip.com": "139.159.142.175",
        "apphq.longhuvip.com": "139.159.142.175",
    }

    def _call_via_ip(self, host: str, payload: dict, headers: dict) -> dict:
        """IP 直连 fallback(国内 DNS 屏蔽场景)"""
        import urllib3
        urllib3.disable_warnings()

        ip = self._HOST_IP_FALLBACK.get(host)
        if not ip:
            raise ConnectionError(f"没有 {host} 的 IP fallback,无法继续")

        # 构造 IP 直连 URL,加 Host 头模拟域名
        url_ip = KPL_BASE_URL.format(host=ip)
        headers_with_host = dict(headers)
        headers_with_host["Host"] = host

        # 关代理,直连 IP
        session = requests.Session()
        session.trust_env = False  # 不读 HTTP_PROXY/HTTPS_PROXY 环境变量
        r = session.post(url_ip, data=payload, headers=headers_with_host, timeout=30, verify=False)
        r.raise_for_status()
        return r.json()

    @with_retry
    def call(self, c: str, a: str, host: str = KPL_HOST_APPHWHQ, **params) -> Union[dict, list]:
        """通用调用入口(带限流 + 重试)

        Args:
            c:      controller(模块名)
            a:      action(操作)
            host:   子域名(默认实时盯盘)
            **params: 业务参数(Token + UserID 自动加 + 公共参数自动加)

        Returns:
            dict / list(API 返回的 JSON;关键字段是 'info' 不是 'list')

        用法:
            client.call("HomeDingPan", "MarketStockZDNum")
            client.call("HisHomeDingPan", "DailyLimitPerformance", Day="2026-09-11", PidType=1)

        备注:
            aphis/applhb 等子域名在中国大陆常被 DNS 屏蔽或被代理干扰,会自动回退到 IP 直连 + Host 头(见 pitfall #13)
        """
        self.rate_limiter.acquire()
        url = KPL_BASE_URL.format(host=host)
        # 必须的 headers(否则返回空 list,模拟 App 的 Android UA)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 7.1.2; SM-G988N Build/NRD90M)",
        }
        # 公共参数(参考 kpl-api skill 抓包模板)
        payload = {
            "Token": self.token,
            "UserID": self.user_id,
            "apiv": "w44",
            "PhoneOS": "0",
            "PhoneOSNew": "1",
            "VerSion": "5.23.0.4",
            "Version": "5.23.0.4",
            "DeviceID": "7905c37c-ccc6-3420-afbc-fbc91cd509b2",
            "Index": "0",
            "c": c,           # controller,服务器靠这个路由(关键!)
            "a": a,           # action,具体操作(关键!)
            **params,         # 业务参数可覆盖公共参数(罕见用法)
        }
        try:
            r = requests.post(url, data=payload, headers=headers, timeout=30)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            # DNS/SSL 失败(国内常见 aphis 被屏蔽)→ 回退到 IP 直连
            logger.warning(f"[KPL] {host} 直连失败({type(e).__name__}: {str(e)[:80]}), 尝试 IP 直连 fallback")
            return self._call_via_ip(host, payload, headers)

    # ====================================================================
    # 2. 涨停天梯(各板数统计)
    # ====================================================================
    def limit_ladder(self) -> dict:
        """涨停天梯 — 一板/二板/三板/四板/更高板的家数统计

        endpoint: HomeDingPan/DailyLimitIndex  (apphwhq, 实时)

        Returns:
            {
                "errcode": "0",
                "data": {
                    "一板": 33,
                    "二板": 4,
                    "三板": 2,
                    "四板": 1,
                    "更高板": 0,
                    "总计": 40,
                },
                "raw_info": [33, 4, 2, 1, 0],
            }
        """
        result = self.call("HomeDingPan", "DailyLimitIndex")
        info_list = result.get("info", [])

        # API 返回 [一板, 二板, 三板, 四板, 更高板] 的数量
        labels = ["一板", "二板", "三板", "四板", "更高板"]
        data = {label: info_list[i] for i, label in enumerate(labels) if i < len(info_list)}
        data["总计"] = sum(info_list) if info_list else 0

        return {
            "errcode": result.get("errcode"),
            "data": data,
            "raw_info": info_list,
        }

    # ====================================================================
    # 1. 涨跌统计(市场情绪快照)
    # ====================================================================
    def market_sentiment(self) -> dict:
        """涨跌统计 — 涨停/跌停家数、涨家数/跌家数

        endpoint: HomeDingPan/MarketStockZDNum  (apphwhq)
        无参数
        """
        return self.call("HomeDingPan", "MarketStockZDNum")

    # ====================================================================
    # 2. 涨停表现详情(按板数过滤)
    # ====================================================================
    # 涨停表现详情 — DailyLimitPerformance 返回嵌套数组结构
    # 实测(2026-09-12, 9/9 桂林旅游 000978 用离线库 db_cn_kpl.db 对照验证):
    # 每只股票 23 元素数组,精确字段含义如下:
    #
    #   [0]  ts_code        股票代码 ("000978")
    #   [1]  name           股票简称 ("桂林旅游")
    #   [2]  tag            标记 (0;9/11 一板所有 33 只都是 0,可能是保留字段/废弃)
    #   [3]  remark         备注 ("")
    #   [4]  lu_time        涨停时间(unix timestamp;离线库 "13:06:24")
    #   [5]  theme          主题材简称 ("旅游";与 [12] limit_reason 区分)
    #   [6]  limit_order    涨停封单金额(元;与离线库 limit_order 字段一致)
    #   [7]  lu_limit_order 涨停板封单金额(元;与离线库 lu_limit_order 字段一致)
    #   [8]  net_change     净额(元;与离线库 net_change 字段一致)
    #   [9]  main_in        主力流入(元;验证: 9/9 桂林=350355562,推断与净额相关)
    #   [10] main_out       主力流出(元;验证: 9/9 桂林=-378709942,合计=net_change)
    #   [11] amount         成交额(元;与离线库 amount 字段一致)
    #   [12] limit_reason   涨停原因全文("旅游、AI应用";多个题材逗号分隔)
    #   [13] free_float     流通市值(元;与离线库 free_float 字段一致)
    #   [14] turnover_rate  换手率(%;与离线库 turnover_rate 字段一致)
    #   [15] board_count    板数(1-5;与离线库 status="N连板" 对应)
    #   [16] is_break       是否炸板(0=封死 1=曾炸开;9/11 一板 33 只中 18 只=1)
    #   [17] amplitude      振幅(%;验证: 9/11 风华高科 9.63=(55.99-51.09)/50.9*100=9.63%)
    #   [18] board_period   "X天Y板" 字符串(如 "5天3板";一板时为空字符串 "")
    #   [19] theme_id       主题材 ID(6位字符串;"801330"=旅游);与 [20] 不同维度
    #   [20] sector_id      板块/分类 ID(数值;与 [19] theme_id 不同分类维度)
    #                         例:9/11 通信 801660 → sector_id=11 (TMT/通信设备)
    #                            9/11 AI应用 803023 → sector_id=5 (科技/AI)
    #                            9/11 电阻电容 801083 → sector_id=3 (电子元件)
    #   [21] close_price    收盘价(元;验证: 9/11 风华高科 55.99=tushare close 55.99)
    #   [22] pct_chg        涨幅(%;验证: 9/11 风华高科 10=tushare pct_chg 10.0)
    LIMIT_PERFORMANCE_FIELDS = [
        "ts_code",         # [0]
        "name",            # [1]
        "tag",             # [2] 标记 (恒为 0)
        "remark",          # [3] 备注
        "lu_time",         # [4] 涨停时间(unix timestamp)
        "theme",           # [5] 主题材简称
        "limit_order",     # [6] 涨停封单金额(元)
        "lu_limit_order",  # [7] 涨停板封单金额(元)
        "net_change",      # [8] 净额(元)
        "main_in",         # [9] 主力流入(元)
        "main_out",        # [10] 主力流出(元)
        "amount",          # [11] 成交额(元)
        "limit_reason",    # [12] 涨停原因全文(多题材)
        "free_float",      # [13] 流通市值(元)
        "turnover_rate",   # [14] 换手率(%)
        "board_count",     # [15] 板数(1-5)
        "is_break",        # [16] 是否炸板(0/1)
        "amplitude",       # [17] 振幅(%)
        "board_period",    # [18] "X天Y板",一板为空
        "theme_id",        # [19] 主题材 ID (6位字符串)
        "sector_id",       # [20] 板块/分类 ID (数值)
        "close_price",     # [21] 收盘价(元)
        "pct_chg",         # [22] 涨幅(%)
    ]

    @classmethod
    def _stock_row(cls, stock_array: list, date: str, board_type: int) -> dict:
        """单个股票数组 → dict (统一用 23 元素映射)

        一板和二板的元素数都是 23,只是 [18] board_period 一板时空字符串,二板时 "X天Y板"
        """
        n = len(stock_array) if isinstance(stock_array, list) else 0
        if n < 2:
            return {"ts_code": str(stock_array), "name": "", "trade_date": date,
                    "board_type": board_type}

        row = {"trade_date": date, "board_type": board_type}
        for i, field in enumerate(cls.LIMIT_PERFORMANCE_FIELDS):
            if i < n:
                row[field] = stock_array[i]
            else:
                row[field] = None
        return row

    @classmethod
    def _parse_limit_performance(cls, raw_info: list, date: str, board_type: int) -> pd.DataFrame:
        """解析 DailyLimitPerformance 返回的嵌套数组 → DataFrame

        处理多种 edge case:
          - 正常: [[s1, s2, ...]]  → DataFrame
          - 空数据: [[], "YYYY-MM-DD"]  → 空 DataFrame
          - 单股票: [[s1]] → 1 行 DataFrame
        """
        rows = []
        for group in raw_info:
            if not isinstance(group, list) or len(group) == 0:
                continue  # 跳过空数组或日期字符串
            # group 是 [[s1, s2, ...]] (单股票嵌套) 还是 [s1, s2, ...]?
            if isinstance(group[0], list):
                # 嵌套 [[s1, s2, ...]]
                for stock in group:
                    if isinstance(stock, list) and len(stock) >= 2:
                        rows.append(cls._stock_row(stock, date, board_type))
            else:
                # 单股票 [s1, s2, ...] (罕见)
                if len(group) >= 2:
                    rows.append(cls._stock_row(group, date, board_type))
        return pd.DataFrame(rows)

    def limit_up_performance(
        self,
        date: str,
        board_type: int = 1,
        page_size: int = 2000,
        st: int = 2000,
    ) -> pd.DataFrame:
        """涨停表现详情 — 某日某板数的具体股票列表

        endpoint: HisHomeDingPan/DailyLimitPerformance  (apphis)

        Args:
            date:        YYYY-MM-DD(例 '2026-09-11')
            board_type:  1=一板 / 2=二板 / 3=三板 / 4=四板 / 5=更高板
            page_size:   单页大小(默认 2000)
            st:          st=2000 是稳定参数,不要改

        Returns:
            pd.DataFrame(列: ts_code/name/trade_date/board_type/lu_time/theme/...)

        自动 fallback: DNS 屏蔽时改用 IP 直连(见 pitfall #13)
        """
        result = self.call(
            "HisHomeDingPan", "DailyLimitPerformance",
            host=KPL_HOST_APPHIS,
            Day=date,
            PidType=board_type,
            Type=1,  # Type=1 是排序
            st=st,
            Order=0,
        )
        raw_info = result.get("info", []) if isinstance(result, dict) else result
        return self._parse_limit_performance(raw_info, date, board_type)

    def get_daily_limit_performance(
        self,
        date: str,
        sort_by: str = "board_then_time",
        add_suffix: bool = True,
    ) -> pd.DataFrame:
        """涨停表现详情 — 拉取某日所有板数的涨停股票,合并 + 排序 + 补全 ts_code

        endpoint: HisHomeDingPan/DailyLimitPerformance  (apphis,自动 IP fallback)

        这是 KPLClient 的**主要高层方法**,输入一个日期,输出结构化的全部涨停数据:

        1. 自动遍历 5 个板数(PidType=1..5),合并结果
        2. lu_time unix timestamp → 北京时间 "HH:MM:SS"
        3. ts_code 自动补全交易所后缀(.SH/.SZ/.BJ)
        4. 按 连板高→低 + 涨停时间早→晚 排序(默认)

        Args:
            date:       YYYY-MM-DD(例 '2026-09-11')
            sort_by:    排序方式:
                          'board_then_time' (默认): 连板高→低, 时间早→晚
                          'time':              仅按涨停时间早→晚
                          'board':             仅按连板高→低
                          None:                不排序(按 API 返回顺序)
            add_suffix: 是否给 ts_code 补全交易所后缀(默认 True)

        Returns:
            pd.DataFrame 25 列:
                ts_code(已补全后缀), name, trade_date, board_count, lu_time,
                theme, limit_reason, is_break, amplitude, turnover_rate,
                limit_order, close_price, pct_chg, board_period, free_float,
                main_in, main_out, theme_id, sector_id, board_type, tag, remark, amount, ...

            空 DataFrame(无涨停数据时返回)

        Raises:
            requests.exceptions.RequestException: 网络错误(超过 IP fallback 重试次数)

        示例:
            >>> df = client.get_daily_limit_performance('2026-09-11')
            >>> df[['ts_code','name','board_count','lu_time','theme']].head(5)
                    ts_code      name  board_count   lu_time  theme
                002790.SZ      瑞尔特           4  09:25:00  地产链
                000993.SZ     闽东电力           3  09:25:00  绿色电力
                603421.SH     鼎信通讯           3  10:20:33  电气设备
                002912.SZ     中新赛克           2  09:25:00  AI应用
                600876.SH     凯盛新能           2  09:31:30    光伏

            >>> # 不排序,保留原序
            >>> df = client.get_daily_limit_performance('2026-09-11', sort_by=None)

            >>> # 不要后缀
            >>> df = client.get_daily_limit_performance('2026-09-11', add_suffix=False)
        """
        all_dfs = []
        for bt in [1, 2, 3, 4, 5]:
            try:
                df = self.limit_up_performance(date, board_type=bt)
                if len(df) > 0:
                    all_dfs.append(df)
            except Exception as e:
                # 某板数接口失败不阻塞其他板数(记日志)
                logger.warning(f"[KPL] board_type={bt} 拉取失败: {e}")

        if not all_dfs:
            return pd.DataFrame()

        df = pd.concat(all_dfs, ignore_index=True)

        # lu_time 是 Unix epoch（UTC 基准）；必须显式转到北京时间。
        # 直接 pd.to_datetime(...).strftime 会按无时区 UTC 格式化，
        # 将 09:31 错写成 01:31，破坏涨停先后顺序和一字板判断。
        df["lu_time"] = (
            pd.to_datetime(df["lu_time"], unit="s", utc=True)
            .dt.tz_convert("Asia/Shanghai")
            .dt.strftime("%H:%M:%S")
        )

        # ts_code 补全交易所后缀(默认开)
        if add_suffix:
            df["ts_code"] = add_exchange_suffix_series(df["ts_code"])

        # 排序
        if sort_by == "board_then_time":
            df = df.sort_values(
                ["board_count", "lu_time"],
                ascending=[False, True],
            ).reset_index(drop=True)
        elif sort_by == "time":
            df = df.sort_values(["lu_time"], ascending=[True]).reset_index(drop=True)
        elif sort_by == "board":
            df = df.sort_values(
                ["board_count"], ascending=[False]
            ).reset_index(drop=True)
        # else: 保持 API 原序

        return df

    # ====================================================================
    # 2.5 涨停表现详情 — 实时盯盘 host(盘中可用,2026-09-14 新增)
    # ====================================================================
    def fetch_realtime_limit_performance(
        self,
        sort_by: str = "board_then_time",
        add_suffix: bool = True,
    ) -> pd.DataFrame:
        """涨停表现详情 — 实时盯盘 host(apphwhq)版本

        与 get_daily_limit_performance(date) 的区别:
          - 后者用 HisHomeDingPan(apphis) 历史接口,必须传 Day=YYYY-MM-DD,盘中拉 today 返 0 行
          - 本方法用 HomeDingPan(apphwhq) 实时盯盘 host,**盘中可用**(2026-09-14 验证 9/14 11:18 拉 today 返 39 只一板)

        endpoint: HomeDingPan/DailyLimitPerformance  (apphwhq)

        用途:onlineDataManager 的 service_writeredis_limitperformance 每 30s 拉一次当前涨停详情,
        打 data_timestamp(拉取瞬间 unix int),XADD 进 STREAM → 落盘成盘中涨停快照轨迹。
        同只股多次拉的快照都会落盘(以 ts_code + limit_performance_timestamp 为 UNIQUE key),
        等于把"涨停列表的演化"完整保留。

        Args:
            sort_by: 排序方式(同 get_daily_limit_performance):
                       'board_then_time'(默认):连板高→低,时间早→晚
                       'time':                 仅按涨停时间
                       'board':                仅按连板
                       None:                   不排序
            add_suffix: 是否给 ts_code 补全交易所后缀(默认 True)

        Returns:
            pd.DataFrame 25 列(同 get_daily_limit_performance):
                ts_code(已补全后缀), name, trade_date, board_count, lu_time(unix int),
                theme, limit_reason, is_break, amplitude, turnover_rate,
                limit_order, close_price, pct_chg, board_period, free_float,
                main_in, main_out, theme_id, sector_id, board_type, tag, remark, amount

            盘中可能为空 DataFrame(集合竞价前/非交易日/接口异常时)
        """
        today = datetime.now().strftime("%Y-%m-%d")
        all_dfs = []
        for bt in [1, 2, 3, 4, 5]:
            try:
                result = self.call(
                    "HomeDingPan", "DailyLimitPerformance",
                    PidType=bt, Type=1, st=2000, Order=0,
                    # host 默认就是 KPL_HOST_APPHWHQ,显式传一下避免混淆
                )
                df = self._parse_limit_performance(
                    result.get("info", []) if isinstance(result, dict) else result,
                    today, bt,
                )
                if len(df) > 0:
                    all_dfs.append(df)
            except Exception as e:
                logger.warning(f"[KPL] realtime board_type={bt} 拉取失败: {e}")

        if not all_dfs:
            return pd.DataFrame()

        df = pd.concat(all_dfs, ignore_index=True)

        # lu_time 保留原始 unix timestamp(int64)— 用户要求"原封不动存源数据"
        # (offlineDataManager 历史版 get_daily_limit_performance 是历史接口,
        #  客户端把 unix int 处理成 HH:MM:SS 字符串落库;但我们这是盘中实时,保留原值更灵活)
        # df["lu_time"] 仍为 int64

        if add_suffix:
            df["ts_code"] = add_exchange_suffix_series(df["ts_code"])

        if sort_by == "board_then_time":
            df = df.sort_values(
                ["board_count", "lu_time"],
                ascending=[False, True],
            ).reset_index(drop=True)
        elif sort_by == "time":
            df = df.sort_values(["lu_time"], ascending=[True]).reset_index(drop=True)
        elif sort_by == "board":
            df = df.sort_values(
                ["board_count"], ascending=[False]
            ).reset_index(drop=True)

        return df

    # ====================================================================
    # 3. 涨停板数(实际涨停/曾涨停/炸板)
    # ====================================================================
    def daily_limit_index(self, date: Optional[str] = None) -> dict:
        """涨停板数 — 当日实际涨停/曾涨停/炸板统计

        endpoint: HomeDingPan/DailyLimitIndex  (apphwhq,实时)
                 HisHomeDingPan/DailyLimitIndex (apphis,历史)
        Args:
            date: None = 实时(默认);YYYY-MM-DD = 历史某日
        """
        if date is None:
            return self.call("HomeDingPan", "DailyLimitIndex")
        return self.call(
            "HisHomeDingPan", "DailyLimitIndex",
            host=KPL_HOST_APPHIS,
            Day=date,
        )

    # ====================================================================
    # 4. 龙虎榜股票
    # ====================================================================
    def lhb_stock_list(self, date: str) -> pd.DataFrame:
        """龙虎榜股票 — 某日上榜股票 + 净额/涨幅/买入席位等

        endpoint: LongHuBang/GetStockList  (applhb)
        Args:
            date: YYYY-MM-DD(例 '2026-09-11')
        Returns:
            pd.DataFrame(列: ts_code/name/buy_amount/sell_amount/net_amount/...)
        """
        result = self.call(
            "LongHuBang", "GetStockList",
            host=KPL_HOST_APPLHB,
            Time=date,
        )
        return pd.DataFrame(result if isinstance(result, list) else [result])

    # ====================================================================
    # 5. 个股分时
    # ====================================================================
    def stock_trend(self, stock_id: str, day: str) -> pd.DataFrame:
        """个股分时 — 某日某股的分时数据(分钟级)

        endpoint: StockL2Data/GetStockTrend  (apphwhq)
        Args:
            stock_id: 6 位股票代码(无后缀,例 '600104')
            day:       YYYY-MM-DD(例 '2026-09-11')
        Returns:
            pd.DataFrame(列: time/price/volume/amount/...)
        """
        result = self.call(
            "StockL2Data", "GetStockTrend",
            host=KPL_HOST_APPHWHQ,
            StockID=stock_id,
            Day=day,
        )
        return pd.DataFrame(result if isinstance(result, list) else [result])

    # ====================================================================
    # 6. 涨停基因(为什么涨停)
    # ====================================================================
    def zt_gene(self, stock_id: str) -> dict:
        """涨停基因 — 某股为什么涨停(题材/概念归因)

        endpoint: StockL2Data/GetZhangTingGene  (apphwhq)
        Args:
            stock_id: 6 位股票代码(无后缀,例 '600104')
        Returns:
            dict(题材/概念/涨停原因)
        """
        return self.call(
            "StockL2Data", "GetZhangTingGene",
            host=KPL_HOST_APPHWHQ,
            StockID=stock_id,
        )


# ==================== 测试 ====================
if __name__ == "__main__":
    client = KPLClient()

    print("\n=== 1. 涨跌统计 ===")
    sentiment = client.market_sentiment()
    print(f"返回类型: {type(sentiment).__name__}")
    if isinstance(sentiment, dict):
        for k, v in list(sentiment.items())[:10]:
            print(f"  {k}: {v}")

    print("\n=== 2. 涨停表现详情 (2026-09-11 一板) ===")
    try:
        df = client.limit_up_performance("2026-09-11", board_type=1)
        print(f"rows: {len(df)}")
        if len(df) > 0:
            print(df.head(3).to_string(index=False))
    except Exception as e:
        print(f"失败(可能 token 无效): {e}")

    print("\n=== 3. 龙虎榜 (2026-09-11) ===")
    try:
        df = client.lhb_stock_list("2026-09-11")
        print(f"rows: {len(df)}")
    except Exception as e:
        print(f"失败: {e}")
