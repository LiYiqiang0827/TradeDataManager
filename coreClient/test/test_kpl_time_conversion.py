from unittest.mock import patch

from coreClient.kpl_client import KPLClient


def test_daily_limit_performance_uses_shanghai_clock():
    client = object.__new__(KPLClient)
    rows = [[
        "000001", "示例", 0, "", 1789954296, "测试题材",
        0, 0, 0, 0, 0, 0, "", 0, 0, 1, 0, 0, "", "", 0, 0, 0,
    ]]

    with patch.object(client, "limit_up_performance") as mocked:
        mocked.side_effect = [
            client._parse_limit_performance(rows, "2026-09-21", 1),
            client._parse_limit_performance([], "2026-09-21", 2),
            client._parse_limit_performance([], "2026-09-21", 3),
            client._parse_limit_performance([], "2026-09-21", 4),
            client._parse_limit_performance([], "2026-09-21", 5),
        ]
        result = client.get_daily_limit_performance("2026-09-21")

    assert result.iloc[0]["lu_time"] == "09:31:36"
