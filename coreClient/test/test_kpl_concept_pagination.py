import pandas as pd

from coreClient import data_provider


class FakeTushare:
    def __init__(self):
        self.offsets = []

    def kpl_concept_cons(self, **params):
        offset = params["offset"]
        self.offsets.append(offset)
        if offset == 0:
            return pd.DataFrame({
                "ts_code": ["000001.KP"] * 3000,
                "con_code": [f"{index:06d}.SZ" for index in range(3000)],
            })
        if offset == 3000:
            return pd.DataFrame({
                "ts_code": ["000002.KP"],
                "con_code": ["600000.SH"],
            })
        return pd.DataFrame()


def test_kpl_concept_online_fetches_all_pages(monkeypatch):
    client = FakeTushare()
    monkeypatch.setattr(data_provider, "_get_tushare", lambda: client)

    result = data_provider.get_kpl_concept_cons(
        trade_date="20260923",
        source="online",
    )

    assert len(result) == 3001
    assert client.offsets == [0, 3000]
