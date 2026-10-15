from __future__ import annotations

import logging

import pytest

from service.savedata_loop import SavedataDaemon


class FakeSavedata(SavedataDaemon):
    KIND = "fixture"

    def __init__(self, result: int):
        self.result = result
        self.log = logging.getLogger("test.savedata_once_rc")

    def _do_persist(self) -> int:
        return self.result


@pytest.mark.parametrize(("persist_result", "expected_rc"), [(0, 0), (1, 0), (-1, 1)])
def test_run_once_only_treats_negative_persistence_sentinel_as_failure(persist_result, expected_rc):
    assert FakeSavedata(persist_result).run_once() == expected_rc
