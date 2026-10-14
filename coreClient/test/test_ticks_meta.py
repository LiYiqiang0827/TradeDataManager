"""
~/TradingAgent/coreClient/test/test_ticks_meta.py
tdx_ticks_meta 单元测试(2026-09-18 新增)

覆盖:
1. TicksFetchMeta dataclass 字段齐全 + to_dict() 正确
2. 新接口与旧接口对同一请求返回 ticks 完全一致(在 mock pytdx 下)
3. max_pages=1 时必须正确判定 termination="max_pages_hit" + pagination_complete=False
4. 单页空时 termination="empty" + pagination_complete=True
5. 单页异常 termination="page_error" + pagination_complete=False + 不抛

不依赖网络,用 mock api 注入测试数据。
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, main

# 让 from coreClient.xxx import ... 工作
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from coreClient import tdx_ticks_meta
from coreClient.tdx_ticks_meta import (
    TicksFetchMeta,
    fetch_history_ticks_with_meta,
)
from typing import Any


# ============================================================
# Mock 工厂
# ============================================================

def _make_row(hour: int, minute: int, price: float, vol: int, buyorsell: int) -> dict:
    """构造一条像 pytdx 返回的 raw tick"""
    return {
        "time": f"{hour:02d}:{minute:02d}",
        "price": price,
        "vol": vol,
        "buyorsell": buyorsell,
    }


def _make_client_with_pages(pages: list[list[dict]], fail_on_page: int | None = None) -> SimpleNamespace:
    """构造一个 mock client + mock api

    Args:
        pages: 模拟 pytdx 服务端按 start 顺序返回的页
                每页列表长度 = 实际返回条数
                如果想测 max_pages_hit: 必须保证每页 == TDX_TICKS_PAGE_SIZE (2000)
                如果想测 last_page_short: 让某页 < TDX_TICKS_PAGE_SIZE
        fail_on_page: 第几页(按 caller 视角的 page_idx)强制抛异常
    """
    api = SimpleNamespace()
    call_count = {"n": 0}

    def get_history_transaction_data(market, code, start, count, date):
        call_count["n"] += 1
        # caller 视角:每页 count 条,所以 page_idx = start // count
        page_idx = start // count
        if fail_on_page is not None and page_idx == fail_on_page:
            raise ConnectionError(f"mock page error at start={start}")
        if page_idx >= len(pages):
            return []
        return pages[page_idx]

    api.get_history_transaction_data = get_history_transaction_data
    client = SimpleNamespace(
        api=api,
        ip="127.0.0.1",
        port=7709,
        _call_count=call_count,
    )
    return client


# ============================================================
# TicksFetchMeta dataclass
# ============================================================

class TestTicksFetchMetaDataclass(TestCase):
    def test_field_count(self):
        m = TicksFetchMeta(
            host="x", port=1, page_size=2, pages_requested=3,
            pages_succeeded=4, pages_failed=5, raw_rows=6, returned_rows=7,
            rows_removed_by_seen=8, termination="last_page_short",
            pagination_complete=True,
        )
        d = m.to_dict()
        # 14 个字段
        self.assertEqual(len(d), 14)
        self.assertEqual(d["host"], "x")
        self.assertEqual(d["buyorsell_values"], [])
        self.assertEqual(d["termination"], "last_page_short")
        self.assertTrue(d["pagination_complete"])

    def test_buyorsell_set_to_sorted_list(self):
        m = TicksFetchMeta(
            host="x", port=1, page_size=2, pages_requested=3,
            pages_succeeded=4, pages_failed=5, raw_rows=6, returned_rows=7,
            rows_removed_by_seen=8, termination="last_page_short",
            pagination_complete=True,
            buyorsell_values={8, 1, 5, 0},
        )
        d = m.to_dict()
        self.assertEqual(d["buyorsell_values"], [0, 1, 5, 8])


# ============================================================
# 新接口 vs 旧接口对账
# ============================================================

class TestNewVsOldInterfaceEquality(TestCase):
    """新接口 fetch_history_ticks_with_meta 与旧接口 get_history_ticks
    对**同一请求**必须返回完全相同的 ticks list(dict 等值)"""

    def setUp(self):
        # 3 页,每页 3 条,首末页短(故意让 page1/page2 的 (time,price,vol) 都不重复)
        self.page1 = [
            _make_row(9, 30, 10.0, 100, 1),
            _make_row(9, 31, 10.1, 200, 0),
            _make_row(9, 32, 10.2, 300, 1),
        ]
        self.page2 = [
            _make_row(9, 33, 10.3, 150, 0),
            _make_row(9, 34, 10.4, 300, 1),
            _make_row(9, 35, 10.5, 250, 0),
        ]
        self.page3 = [
            _make_row(14, 0, 10.6, 200, 1),
        ]
        # 跨页去重 fixture: page2_dup 第一条与 page1 第一条 (time,price,vol) 完全一致
        # 注意 buyorsell 不同(1 vs 0),这正是会被 seen 吞掉的真实场景
        self.page2_dup = [
            _make_row(9, 30, 10.0, 100, 0),  # 同 page1 第一条 (time,price,vol),buyorsell 不同
            _make_row(9, 33, 10.3, 150, 0),
            _make_row(9, 34, 10.4, 300, 1),
        ]

    def _run_old(self, pages, ts_code, date, page_size):
        from coreClient.tdx_client import _reverse_concat_and_sort, _enrich_ticks
        client = _make_client_with_pages(pages)
        # 直接调底层逻辑走原 wrapper 路径(模拟旧 wrapper 的 page_size 行为)
        seen = set()
        out_pages = []
        from coreClient.tdx_config import TDX_TICKS_RETRY_BACKOFF
        import time as _time
        for page_idx in range(50):
            start = page_idx * page_size
            chunk = []
            for attempt, backoff in enumerate([0] + list(TDX_TICKS_RETRY_BACKOFF)):
                try:
                    chunk = client.api.get_history_transaction_data(0, "000001", start, page_size, date)
                    break
                except Exception:
                    if attempt < len(TDX_TICKS_RETRY_BACKOFF):
                        _time.sleep(backoff)
            if not chunk:
                break
            page_unique = []
            for r in chunk:
                if not isinstance(r, dict):
                    r = dict(r)
                key = (r.get("time", ""), r.get("price", 0), r.get("vol", 0), r.get("buyorsell", 0))
                if key in seen:
                    continue
                seen.add(key)
                page_unique.append(r)
            out_pages.append(page_unique)
            if len(chunk) < page_size:
                break
        return _enrich_ticks(_reverse_concat_and_sort(out_pages, seen), ts_code, date)

    def test_new_equals_old_no_dup(self):
        # 每页 3 条,page_size=3
        pages = [self.page1, self.page2, self.page3]
        page_size = 3
        old = self._run_old(pages, "000001.SZ", 20260827, page_size)
        client = _make_client_with_pages(pages)
        new, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=10, page_size=page_size,
        )
        self.assertEqual(len(old), len(new))
        for o, n in zip(old, new):
            self.assertEqual(o, n)
        self.assertTrue(meta.pagination_complete)
        self.assertEqual(meta.termination, "last_page_short")
        self.assertEqual(meta.raw_rows, 7)
        self.assertEqual(meta.returned_rows, 7)
        self.assertEqual(meta.rows_removed_by_seen, 0)

    def test_new_equals_old_with_cross_page_dup(self):
        # page2_dup 第一条与 page1 第一条 (time,price,vol) 完全一致,buyorsell 不同
        # 2026-09-19 修复 seen key 后: 不再吞同 (time,price,vol) 不同 buyorsell 的成交
        # 本测试断言: 旧新接口在新 seen key 下一致(都不吞)
        pages = [self.page1, self.page2_dup, self.page3]
        page_size = 3
        old = self._run_old(pages, "000001.SZ", 20260827, page_size)
        client = _make_client_with_pages(pages)
        new, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=10, page_size=page_size,
        )
        self.assertEqual(len(old), len(new))
        for o, n in zip(old, new):
            self.assertEqual(o, n)
        # 修复后:raw_rows=7, returned_rows=7,rows_removed=0(同 hash 不同 buyorsell 不再被吞)
        self.assertEqual(meta.raw_rows, 7)
        self.assertEqual(meta.returned_rows, 7)
        self.assertEqual(meta.rows_removed_by_seen, 0)


# ============================================================
# termination 判定
# ============================================================

class TestTerminationJudgement(TestCase):
    def test_last_page_short_normal_completion(self):
        pages = [
            [_make_row(9, 30, 10.0, 100, 1), _make_row(9, 31, 10.1, 200, 0)],
            [_make_row(9, 35, 10.2, 150, 1)],
        ]
        client = _make_client_with_pages(pages)
        ticks, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=10, page_size=2,
        )
        self.assertEqual(meta.termination, "last_page_short")
        self.assertTrue(meta.pagination_complete)
        self.assertEqual(meta.pages_succeeded, 2)
        self.assertEqual(meta.pages_failed, 0)

    def test_max_pages_hit_triggers(self):
        # 每页 2 条,page_size=2 → 全 full,触发 max_pages_hit
        pages = [
            [_make_row(9, 30, 10.0, 100, 1), _make_row(9, 31, 10.1, 200, 0)],
            [_make_row(9, 32, 10.2, 150, 1), _make_row(9, 33, 10.3, 250, 0)],
            [_make_row(9, 34, 10.4, 300, 1), _make_row(9, 35, 10.5, 350, 0)],
        ]
        client = _make_client_with_pages(pages)
        ticks, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=1, page_size=2,
        )
        self.assertEqual(meta.termination, "max_pages_hit")
        self.assertFalse(meta.pagination_complete)
        self.assertEqual(meta.pages_succeeded, 1)
        self.assertEqual(meta.pages_failed, 0)

    def test_empty_termination(self):
        pages = [[]]  # 第 0 页就是空
        client = _make_client_with_pages(pages)
        ticks, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=10, page_size=2,
        )
        self.assertEqual(meta.termination, "empty")
        self.assertTrue(meta.pagination_complete)
        self.assertEqual(meta.returned_rows, 0)
        self.assertEqual(len(ticks), 0)

    def test_page_error_does_not_raise(self):
        # 第 2 页异常:第 0/1 页都是 full,page_idx=2 强制抛
        # page_size=2,3 页 mock 数据:page_idx=0,1,2;但 page_idx=2 raise
        # 注:必须先让 page_idx=1 返回非空(否则会走 empty 短路,到不了 page_idx=2)
        pages = [
            [_make_row(9, 30, 10.0, 100, 1), _make_row(9, 31, 10.1, 200, 0)],
            [_make_row(9, 32, 10.2, 150, 1), _make_row(9, 33, 10.3, 250, 0)],
            # page_idx=2 fail,在 mock 里 raise
        ]
        client = _make_client_with_pages(pages, fail_on_page=2)
        ticks, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=10, page_size=2,
        )
        self.assertEqual(meta.termination, "page_error")
        self.assertFalse(meta.pagination_complete)
        self.assertEqual(meta.pages_succeeded, 2)
        self.assertEqual(meta.pages_failed, 1)
        # 第 2 页没拉到,所以 returned_rows = 4(前 2 页去重后)
        self.assertEqual(meta.returned_rows, 4)


# ============================================================
# 边界情况
# ============================================================

class TestBuyorsellCollection(TestCase):
    def test_buyorsell_set_collected(self):
        pages = [
            [_make_row(9, 30, 10.0, 100, 0), _make_row(9, 31, 10.1, 200, 1)],
            [_make_row(9, 32, 10.2, 150, 5), _make_row(9, 33, 10.3, 250, 8)],
        ]
        client = _make_client_with_pages(pages)
        ticks, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=10, page_size=2,
        )
        self.assertEqual(meta.buyorsell_values, {0, 1, 5, 8})


class TestFirstLastTickSummary(TestCase):
    def test_first_last_picked_from_sorted(self):
        # 服务端逆序给:page0 是末段时间,page1 是首段时间
        pages = [
            [_make_row(14, 55, 10.5, 200, 1), _make_row(15, 0, 10.6, 250, 0)],
            [_make_row(9, 30, 10.0, 100, 1), _make_row(9, 31, 10.1, 200, 0)],
        ]
        client = _make_client_with_pages(pages)
        ticks, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=10, page_size=2,
        )
        self.assertIsNotNone(meta.first_tick)
        self.assertIsNotNone(meta.last_tick)
        assert meta.first_tick is not None
        assert meta.last_tick is not None
        self.assertEqual(meta.first_tick["time"], "09:30")
        self.assertEqual(meta.last_tick["time"], "15:00")


# ============================================================
# seqId_in_minute 行为(2026-09-19 新增)
# ============================================================

class TestSeqIdInMinuteBehaviour(TestCase):
    """验证 seqId_in_minute 含义:同分钟内自增,跨分钟归零"""

    def test_seq_in_minute_increments_within_minute(self):
        # 同一分钟内 3 笔成交
        pages = [[
            _make_row(9, 30, 10.0, 100, 1),
            _make_row(9, 30, 10.0, 200, 0),
            _make_row(9, 30, 10.1, 50, 1),
        ]]
        client = _make_client_with_pages(pages)
        ticks, _ = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=5, page_size=3,
        )
        self.assertEqual([t["seqId_in_minute"] for t in ticks], [0, 1, 2])
        # 旧 seqId 字段必须不存在
        self.assertNotIn("seqId", ticks[0])

    def test_seq_in_minute_resets_across_minutes(self):
        # 跨 3 个分钟,每分钟 1 笔
        pages = [[
            _make_row(9, 30, 10.0, 100, 1),
            _make_row(9, 31, 10.1, 200, 0),
            _make_row(9, 32, 10.2, 300, 1),
        ]]
        client = _make_client_with_pages(pages)
        ticks, _ = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=5, page_size=3,
        )
        # 每分钟都从 0 开始
        self.assertEqual([t["seqId_in_minute"] for t in ticks], [0, 0, 0])
        times = [t["time"] for t in ticks]
        self.assertEqual(times, ["09:30", "09:31", "09:32"])

    def test_seq_in_minute_idempotent(self):
        # 同一组输入跑两次,seqId_in_minute 必须完全一致
        pages = [[
            _make_row(10, 0, 10.0, 100, 1),
            _make_row(10, 0, 10.1, 200, 0),
            _make_row(10, 1, 10.2, 300, 1),
        ]]
        client1 = _make_client_with_pages(pages)
        ticks1, _ = fetch_history_ticks_with_meta(
            client1, "000001.SZ", 20260827, max_pages=5, page_size=3,
        )
        client2 = _make_client_with_pages(pages)
        ticks2, _ = fetch_history_ticks_with_meta(
            client2, "000001.SZ", 20260827, max_pages=5, page_size=3,
        )
        self.assertEqual(
            [t["seqId_in_minute"] for t in ticks1],
            [t["seqId_in_minute"] for t in ticks2],
        )


# ============================================================
# 落库 gate(2026-09-19 新增)
# ============================================================

class TestPersistGate(TestCase):
    """is_safe_to_persist + IncompleteDataError 行为"""

    def test_safe_when_last_page_short(self):
        from coreClient.tdx_ticks_meta import is_safe_to_persist
        pages = [[_make_row(9, 30, 10.0, 100, 1), _make_row(9, 31, 10.1, 200, 0)]]
        client = _make_client_with_pages(pages)
        _, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=5, page_size=2,
        )
        self.assertTrue(is_safe_to_persist(meta))

    def test_safe_when_empty(self):
        from coreClient.tdx_ticks_meta import is_safe_to_persist
        pages = [[]]
        client = _make_client_with_pages(pages)
        _, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=5, page_size=2,
        )
        # "empty" 也算 complete(非交易日 = 完整空集)
        self.assertTrue(is_safe_to_persist(meta))

    def test_unsafe_when_max_pages_hit(self):
        from coreClient.tdx_ticks_meta import is_safe_to_persist
        pages = [
            [_make_row(9, 30, 10.0, 100, 1), _make_row(9, 31, 10.1, 200, 0)],
        ]
        client = _make_client_with_pages(pages)
        _, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=1, page_size=2,
        )
        self.assertFalse(is_safe_to_persist(meta))

    def test_unsafe_when_page_error(self):
        from coreClient.tdx_ticks_meta import is_safe_to_persist
        pages = [
            [_make_row(9, 30, 10.0, 100, 1), _make_row(9, 31, 10.1, 200, 0)],
            [_make_row(9, 32, 10.2, 150, 1), _make_row(9, 33, 10.3, 250, 0)],
        ]
        client = _make_client_with_pages(pages, fail_on_page=2)
        _, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=5, page_size=2,
        )
        self.assertFalse(is_safe_to_persist(meta))

    def test_incomplete_data_error_carries_meta(self):
        from coreClient.tdx_ticks_meta import IncompleteDataError
        pages = [[_make_row(9, 30, 10.0, 100, 1), _make_row(9, 31, 10.1, 200, 0)]]
        client = _make_client_with_pages(pages)
        _, meta = fetch_history_ticks_with_meta(
            client, "000001.SZ", 20260827, max_pages=1, page_size=2,
        )
        err = IncompleteDataError(meta)
        self.assertIs(err.meta, meta)
        self.assertIn("max_pages_hit", str(err))


if __name__ == "__main__":
    main(verbosity=2)