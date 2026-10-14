"""
~/TradingAgent/coreClient/tdx_ticks_meta.py
TDX ticks 分页元信息扩展(2026-09-18 新增)

为什么需要这个:
- 旧 get_history_ticks() 只返 list,丢失了分页完整性信号
- 验证发现 wrapper 内部 seen 去重可能吞掉真实成交(2026-09-18)
- 上层无法区分"数据全 vs 截断 vs 单页失败"

与 tdx_client.py 的关系:
- 不修改 TdxClient 任何方法
- 提供独立函数 fetch_history_ticks_with_meta(client, ts_code, date, max_pages)
- 调用方:from coreClient.tdx_ticks_meta import fetch_history_ticks_with_meta, TicksFetchMeta

不映射/不删除 buyorsell=5/8、零量、重复 — 原样透传,标注完整性。
落库侧由调用方根据 pagination_complete 决定是否写入。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from coreClient.tdx_config import TDX_TICKS_PAGE_SIZE, TDX_TICKS_RETRY_BACKOFF
from coreClient.tdx_client import (
    _date_int_to_str,
    _enrich_ticks,
    _ticks_time_to_datetime,
    code_of,
    market_of,
)


# ============================================================
# 终止原因枚举(4 个值,与 pytdx 服务端响应结合判定)
# ============================================================
# "last_page_short"  末页 len < page_size,正常结束 — 数据完整
# "empty"           第一页就是空 — 非交易日 / 数据缺失(可能=完整=空集)
# "max_pages_hit"    达到 max_pages 上限 — 可能截断,不完整
# "page_error"       某页异常,返回的是已收到的部分 — 不完整


@dataclass
class TicksFetchMeta:
    """一次 fetch_history_ticks_with_meta 调用的分页元信息

    字段说明:
        host                  实际连接的主机 IP
        port                  端口
        page_size             单页请求大小(默认 TDX_TICKS_PAGE_SIZE)
        pages_requested       本次发起的页数
        pages_succeeded       返回成功的页数
        pages_failed          异常页数(每页都重试了 TDX_TICKS_RETRY_BACKOFF 次)
        raw_rows              三页裸 row 总和(未经任何去重)
        returned_rows         实际返回的 ticks 行数(去重后)
        rows_removed_by_seen  raw_rows - returned_rows(>0 表示发生了去重)
        termination           "last_page_short" | "empty" | "max_pages_hit" | "page_error"
        pagination_complete   True = 数据完整;False = 触顶/出错,不要落库
        buyorsell_values      出现的 buyorsell 取值集合
        first_tick            首条 tick 摘要 {time, price, vol, buyorsell} 或 None
        last_tick             末条 tick 摘要 同上 或 None
    """
    host: str
    port: int
    page_size: int
    pages_requested: int
    pages_succeeded: int
    pages_failed: int
    raw_rows: int
    returned_rows: int
    rows_removed_by_seen: int
    termination: str
    pagination_complete: bool
    buyorsell_values: set[int] = field(default_factory=set)
    first_tick: dict | None = None
    last_tick: dict | None = None

    def to_dict(self) -> dict:
        """JSON-friendly dict(set 转 list)"""
        return {
            "host": self.host,
            "port": self.port,
            "page_size": self.page_size,
            "pages_requested": self.pages_requested,
            "pages_succeeded": self.pages_succeeded,
            "pages_failed": self.pages_failed,
            "raw_rows": self.raw_rows,
            "returned_rows": self.returned_rows,
            "rows_removed_by_seen": self.rows_removed_by_seen,
            "termination": self.termination,
            "pagination_complete": self.pagination_complete,
            "buyorsell_values": sorted(self.buyorsell_values),
            "first_tick": self.first_tick,
            "last_tick": self.last_tick,
        }


def fetch_history_ticks_with_meta(
    client: Any,
    ts_code: str,
    date: int,
    max_pages: int = 50,
    page_size: int = TDX_TICKS_PAGE_SIZE,
) -> tuple[list[dict], TicksFetchMeta]:
    """拉 1 只 1 天的全部分笔成交,同时返回分页元信息

    关键差异 vs 旧 get_history_ticks:
    - 不抛异常(单页失败也尽量返回已拉到的部分)
    - 返回的 ticks 是逆序分页 + 末尾保险排序后的时间正序 list
    - meta 字段记录分页完整性和去重数量

    Args:
        client:    已连接成功的 client 实例(实际类型 TdxClient;mock 也接受 duck-typed)
        ts_code:   6 位代码 + 市场后缀,如 "000006.SZ"
        date:      YYYYMMDD 整数
        max_pages: 分页上限(默认 50 = 10 万笔)
        page_size: 单页请求大小(默认 TDX_TICKS_PAGE_SIZE;测试可注入小值)

    Returns:
        (ticks, meta) 元组
        - ticks: list of dict,字段含 ts_code/trade_date/datetime/time/seqId/price/vol/buyorsell
        - meta: TicksFetchMeta 实例,反映分页过程

    注意:
        - 调用方负责根据 meta.pagination_complete 决定是否落库
        - 此函数**不映射、不删除 buyorsell=5/8、零量、重复条目** — 原样透传
        - 与旧 get_history_ticks 输出差异:仅当旧接口遇到 page_error 时会丢失已拉部分;
          本函数总是返回已收到的数据 + 标注 pagination_complete=False
    """
    market = market_of(ts_code)
    code = code_of(ts_code)

    pages: list[list[dict]] = []
    seen: set[tuple] = set()
    raw_rows = 0
    pages_succeeded = 0
    pages_failed = 0
    termination = "last_page_short"  # 默认
    first_failure_page = None
    page_idx = -1  # 让 linter 满意;真实值由循环更新

    for page_idx in range(max_pages):
        start = page_idx * page_size

        # 单页拉取 + retry(直接复用 TDX_TICKS_RETRY_BACKOFF 退避列表)
        chunk: list = []
        page_error = None
        for attempt, backoff in enumerate([0] + list(TDX_TICKS_RETRY_BACKOFF)):
            try:
                chunk = client.api.get_history_transaction_data(
                    market, code, start=start, count=page_size, date=date,
                )
                page_error = None
                break
            except Exception as e:
                page_error = (type(e).__name__, str(e)[:200])
                if attempt < len(TDX_TICKS_RETRY_BACKOFF):
                    import time as _time
                    _time.sleep(backoff)

        if page_error is not None:
            pages_failed += 1
            if first_failure_page is None:
                first_failure_page = start
            termination = "page_error"
            # 不抛,直接退出循环,保留已拉到的部分
            break

        pages_succeeded += 1
        raw_rows += len(chunk)

        if not chunk:
            # 0 笔
            if page_idx == 0:
                termination = "empty"
            break

        # seen 去重(与旧实现一致:key = (time, price, vol))
        page_unique: list[dict] = []
        for r in chunk:
            if not isinstance(r, dict):
                r = dict(r)
            key = (r.get("time", ""), r.get("price", 0), r.get("vol", 0), r.get("buyorsell", 0))
            if key in seen:
                continue
            seen.add(key)
            page_unique.append(r)
        pages.append(page_unique)

        # 最后一页判断:返回数 < page_size
        if len(chunk) < page_size:
            termination = "last_page_short"
            break

        # 达到 max_pages 上限
        if page_idx == max_pages - 1:
            termination = "max_pages_hit"

    # 逆序拼接 + 保险排序(复用 tdx_client 的内部 helper)
    from coreClient.tdx_client import _reverse_concat_and_sort
    sorted_ticks = _reverse_concat_and_sort(pages, seen)

    # enrich(补 ts_code/trade_date/datetime/seqId)
    enriched = _enrich_ticks(sorted_ticks, ts_code, date)

    # buyorsell 取值集合
    buyorsell_values = set()
    for r in enriched:
        v = r.get("buyorsell")
        if v is not None:
            try:
                buyorsell_values.add(int(v))
            except (TypeError, ValueError):
                pass

    # 首末条摘要
    def _summary(r: dict | None) -> dict | None:
        if r is None:
            return None
        return {
            "time": r.get("time"),
            "price": r.get("price"),
            "vol": r.get("vol"),
            "buyorsell": r.get("buyorsell"),
        }

    first_tick = _summary(enriched[0]) if enriched else None
    last_tick = _summary(enriched[-1]) if enriched else None

    pagination_complete = termination in ("last_page_short", "empty")

    meta = TicksFetchMeta(
        host=client.ip,
        port=client.port,
        page_size=page_size,
        pages_requested=page_idx + 1 if (pages_succeeded + pages_failed) > 0 else 0,
        pages_succeeded=pages_succeeded,
        pages_failed=pages_failed,
        raw_rows=raw_rows,
        returned_rows=len(enriched),
        rows_removed_by_seen=max(raw_rows - len(enriched), 0),
        termination=termination,
        pagination_complete=pagination_complete,
        buyorsell_values=buyorsell_values,
        first_tick=first_tick,
        last_tick=last_tick,
    )
    return enriched, meta


# ============================================================
# 落库 gate(2026-09-19 决策,详见 docs/落库方案_v2.md)
# ============================================================

def is_safe_to_persist(meta: TicksFetchMeta) -> bool:
    """落库 gate:只有 pagination_complete=True 的 fetch 结果才允许写入 tbl_tick_v2

    调用方在写 DB 前必须显式调本函数,False 时必须抛 IncompleteDataError 拒绝落库。
    这把"数据完整性"从传输层(wrapper)传导到持久化层(落库),防止残缺数据入库。

    注意:
        - "empty" 也算 complete(非交易日 / 数据缺失本身就是完整状态)
        - "max_pages_hit" 和 "page_error" 是 incomplete,不能落库
    """
    return meta.pagination_complete


class IncompleteDataError(RuntimeError):
    """落库 gate 拒绝异常:fetch 到的数据不完整,不允许写入

    Raises:
        IncompleteDataError(meta):当 is_safe_to_persist(meta) 为 False 时
    """

    def __init__(self, meta: TicksFetchMeta):
        self.meta = meta
        super().__init__(
            f"refusing to persist incomplete ticks: termination={meta.termination}, "
            f"raw_rows={meta.raw_rows}, returned_rows={meta.returned_rows}, "
            f"pages_succeeded={meta.pages_succeeded}, pages_failed={meta.pages_failed}"
        )