import logging
import unittest
from datetime import datetime
from unittest.mock import patch

from service import service_writeredis_minute as service


class FakeRedis:
    def __init__(self, watchlist):
        self.watchlist = watchlist
        self.meta = {}
        self.writes = []

    def get_watchlist(self):
        if isinstance(self.watchlist, Exception):
            raise self.watchlist
        return self.watchlist

    # Matches OnlineRedis.get_meta(field): no default argument is accepted.
    def get_meta(self, key):
        return self.meta.get(key)

    def set_meta(self, key, value):
        self.meta[key] = value

    def put_minute(self, *, ts_code, data):
        self.writes.append((ts_code, data))


class FakeTdx:
    ip = "test-tdx"
    port = 7709

    def __init__(self, raw=None, live=None, errors=None):
        self.raw = raw or {}
        self.live = live or {}
        self.errors = errors or {}
        self.raw_calls = []
        self.live_calls = []

    def download_one_minute(self, ts_code, *, max_pages):
        self.raw_calls.append((ts_code, max_pages))
        error = self.errors.get(("raw", ts_code))
        if error:
            raise error
        return self.raw.get(ts_code, [])

    def get_minute_kline(self, ts_code):
        self.live_calls.append(ts_code)
        error = self.errors.get(("live", ts_code))
        if error:
            raise error
        return self.live.get(ts_code, [])


def one_bar(ts_code):
    return [{"ts_code": ts_code, "datetime": "2026-10-15 09:31:00", "time_idx": 0}]


class MinuteServiceHealthTests(unittest.TestCase):
    def setUp(self):
        self.log = logging.getLogger("test_minute_service_health")
        self.log.addHandler(logging.NullHandler())

    def test_preflight_executes_raw_and_live_getters_for_two_reference_codes(self):
        client = FakeTdx(
            raw={"000001.SZ": one_bar("000001.SZ"), "600000.SH": one_bar("600000.SH")},
            live={"000001.SZ": one_bar("000001.SZ")},
        )
        with patch.object(service, "TDX_IP_POOL", [("primary", 7709)]), patch.object(
            service, "TDX_HISTORY_BAR_IP_POOL", [("fallback", 7709)]
        ):
            result = service.probe_minute_source(client)
        self.assertEqual(result["status"], "preflight_ok")
        self.assertEqual(client.raw_calls, [("000001.SZ", 1), ("600000.SH", 1)])
        self.assertEqual(client.live_calls, ["000001.SZ", "600000.SH"])
        self.assertEqual(result["native_history_counts"], {"000001.SZ": 1, "600000.SH": 1})
        self.assertEqual(result["primary_pool_size"], 1)
        self.assertEqual(result["fallback_pool_size"], 1)

    def test_empty_raw_source_is_degraded_even_when_pools_are_nonempty(self):
        client = FakeTdx()
        with patch.object(service, "TDX_IP_POOL", [("primary", 7709)]), patch.object(
            service, "TDX_HISTORY_BAR_IP_POOL", [("fallback", 7709)]
        ):
            result = service.probe_minute_source(client)
        self.assertEqual(result["status"], "degraded_native_raw_unavailable")
        self.assertIn("no bars", result["error"])

    def test_empty_fallback_pool_is_degraded_even_if_mock_raw_works(self):
        client = FakeTdx(raw={"000001.SZ": one_bar("000001.SZ")}, live={"000001.SZ": one_bar("000001.SZ")})
        with patch.object(service, "TDX_IP_POOL", [("primary", 7709)]), patch.object(
            service, "TDX_HISTORY_BAR_IP_POOL", []
        ):
            result = service.probe_minute_source(client)
        self.assertEqual(result["status"], "degraded_config_fallback_pool_empty")

    def test_empty_primary_pool_is_not_counted_as_native_capability(self):
        client = FakeTdx(raw={"000001.SZ": one_bar("000001.SZ")}, live={"000001.SZ": one_bar("000001.SZ")})
        with patch.object(service, "TDX_IP_POOL", []), patch.object(
            service, "TDX_HISTORY_BAR_IP_POOL", [("fallback", 7709)]
        ):
            result = service.probe_minute_source(client)
        self.assertEqual(result["status"], "degraded_config_primary_pool_empty")

    def test_startup_refuses_empty_primary_pool_and_records_failure(self):
        redis = FakeRedis([])
        with patch.object(service, "TDX_IP_POOL", []), patch.object(
            service, "TdxClient", side_effect=AssertionError("must not construct")
        ):
            with self.assertRaisesRegex(RuntimeError, "TDX_IP_POOL is empty"):
                service._new_tdx_client(redis)
        self.assertEqual(redis.meta["minute_preflight_status"], "degraded_config_primary_pool_empty")
        self.assertIn("TDX_IP_POOL is empty", redis.meta["minute_current_error"])

    def test_empty_source_streak_and_round_number_advance_across_rounds(self):
        redis = FakeRedis(["000001.SZ"])
        client = FakeTdx()
        first = service.process_minute_round(redis, client, log=self.log)
        second = service.process_minute_round(redis, client, log=self.log)
        self.assertEqual(first["zero_write_streak"], 1)
        self.assertEqual(second["zero_write_streak"], 2)
        self.assertEqual(redis.meta["minute_zero_write_streak"], 2)
        self.assertEqual(redis.meta["minute_last_round"], 2)

    def test_opening_wait_is_not_degraded_or_counted_as_zero_write_streak(self):
        client = FakeTdx(
            raw={"000001.SZ": one_bar("000001.SZ")},
            live={},
        )
        redis = FakeRedis(["000001.SZ", "600000.SH"])
        cases = (
            ("09:29", "waiting_first_completed_bar"),
            ("09:31", "waiting_first_completed_bar"),
            ("09:32", "degraded_current_getter_empty"),
        )
        for hhmm, expected in cases:
            with self.subTest(time=hhmm), patch.object(service, "TDX_IP_POOL", [("p", 7709)]), patch.object(
                service, "TDX_HISTORY_BAR_IP_POOL", [("f", 7709)]
            ):
                now = datetime.fromisoformat(f"2026-10-15T{hhmm}:00+08:00")
                preflight = service.probe_minute_source(client, now=now)
                self.assertEqual(preflight["status"], expected)
                redis.meta["minute_preflight_native_history_counts"] = preflight["native_history_counts"]
                redis.meta["minute_zero_write_streak"] = 0
                result = service.process_minute_round(redis, client, log=self.log, now=now)
                if hhmm < "09:32":
                    self.assertEqual(result["status"], "waiting_first_completed_bar")
                    self.assertEqual(result["zero_write_streak"], 0)
                    self.assertEqual(
                        redis.meta["minute_current_error"],
                        "native history is available; no completed current-session bar is expected yet",
                    )
                else:
                    self.assertEqual(result["status"], "degraded_empty_source_unknown")
                    self.assertEqual(result["zero_write_streak"], 1)

    def test_no_watchlist_is_distinct_and_clears_stale_error_without_success(self):
        redis = FakeRedis([])
        redis.meta.update({"minute_last_error": "old error", "minute_last_success_at": "old success"})
        client = FakeTdx()
        result = service.process_minute_round(redis, client, log=self.log)
        self.assertEqual(result["status"], "no_watchlist")
        self.assertEqual(redis.meta["minute_health_status"], "no_watchlist")
        self.assertEqual(redis.meta["minute_last_error"], "")
        self.assertEqual(redis.meta["minute_last_success_at"], "old success")
        self.assertEqual(redis.meta["minute_last_round"], 1)
        self.assertEqual(client.live_calls, [])

    def test_all_empty_round_degrades_and_does_not_refresh_success_timestamp(self):
        redis = FakeRedis(["000001.SZ", "600000.SH"])
        redis.meta.update({
            "minute_zero_write_streak": 2,
            "minute_last_success_at": "old success",
            "minute_last_error": "stale error",
        })
        result = service.process_minute_round(redis, FakeTdx(), log=self.log)
        self.assertEqual(result["status"], "degraded_empty_source_unknown")
        self.assertEqual(result["zero_write_streak"], 3)
        self.assertEqual(redis.meta["minute_last_error"], "all watchlist getters returned no rows; source/timing cause unknown")
        self.assertEqual(redis.meta["minute_current_error"], redis.meta["minute_last_error"])
        self.assertTrue(redis.meta["minute_last_attempt_at"])
        self.assertEqual(redis.meta["minute_last_success_at"], "old success")
        self.assertEqual(redis.meta["minute_last_empty"], 2)

    def test_success_round_clears_error_resets_streak_and_advances_success(self):
        redis = FakeRedis(["000001.SZ"])
        redis.meta.update({"minute_zero_write_streak": 4, "minute_last_error": "previous outage"})
        result = service.process_minute_round(
            redis, FakeTdx(live={"000001.SZ": one_bar("000001.SZ")}), log=self.log
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(redis.meta["minute_zero_write_streak"], 0)
        self.assertEqual(redis.meta["minute_last_error"], "")
        self.assertTrue(redis.meta["minute_last_success_at"])
        self.assertEqual(len(redis.writes), 1)

    def test_all_symbol_errors_are_visible_and_count_as_zero_write_streak(self):
        redis = FakeRedis(["000001.SZ", "600000.SH"])
        client = FakeTdx(errors={
            ("live", "000001.SZ"): TimeoutError("host timeout"),
            ("live", "600000.SH"): TimeoutError("host timeout"),
        })
        result = service.process_minute_round(redis, client, log=self.log)
        self.assertEqual(result["status"], "degraded_source_errors")
        self.assertEqual(result["zero_write_streak"], 1)
        self.assertEqual(redis.meta["minute_last_failed"], 2)
        self.assertIn("TimeoutError", redis.meta["minute_current_error"])
        self.assertNotIn("minute_last_success_at", redis.meta)


if __name__ == "__main__":
    unittest.main()
