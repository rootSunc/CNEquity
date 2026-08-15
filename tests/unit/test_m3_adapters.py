from datetime import date

from cn_market_lake.adapters.eastmoney.capital import fetch_fund_flow


class FakeClient:
    def get(self, url, **kwargs):
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "data": {
                        "total": 1,
                        "diff": [
                            {
                                "f12": "600519",
                                "f13": 1,
                                "f62": 100.0,
                                "f66": 10.0,
                                "f72": 20.0,
                                "f78": 30.0,
                                "f84": 40.0,
                            }
                        ],
                    }
                }

        return Resp()

    def close(self):
        return None


def test_fetch_fund_flow_parses_clist():
    df = fetch_fund_flow(date(2024, 6, 28), client=FakeClient())
    assert df.height == 1
    assert df["symbol"][0] == "600519.SH"
    assert df["main_net_inflow"][0] == 100.0
