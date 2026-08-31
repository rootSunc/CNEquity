from datetime import date

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError
from cnequity.adapters.eastmoney.share_unlock import fetch_share_unlock_schedule
from cnequity.adapters.macro.indicators import fetch_macro_indicators
from cnequity.config import Config
from cnequity.derive.market_breadth import compute_market_breadth
from cnequity.domain.schemas import validate_dataframe
from cnequity.orchestrator.registry import get_step
from cnequity.query import load


class FakeDatacenterClient:
    def __init__(self, batches: dict[str, list[dict]]):
        self.batches = batches
        self.closed = False

    def get(self, url, **kwargs):
        class Resp:
            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                return None

            def json(self):
                return {"success": True, "result": {"data": self._data}}

        for key, rows in self.batches.items():
            if key in url:
                return Resp(rows)
        return Resp([])

    def close(self):
        self.closed = True


class FakeCninfoClient:
    def __init__(self, announcements: list[dict]):
        self.announcements = announcements

    def post(self, url, data=None, **kwargs):
        class Resp:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        return Resp(
            {
                "announcements": self.announcements,
                "hasMore": False,
            }
        )

    def close(self):
        return None


def test_v11_steps_registered():
    for name in (
        "macro_indicators",
        "market_breadth",
        "share_unlock_schedule",
        "regulatory_events",
    ):
        assert get_step(name).fn is not None


def _no_social_financing(monkeypatch):
    """Keep tests hermetic — 社融 is a live MOFCOM call."""
    from cnequity.adapters.macro import indicators as macro_indicators

    monkeypatch.setattr(macro_indicators, "_social_financing_rows", lambda _td, config=None: [])


def test_macro_indicators_parses_treasury_and_shibor(monkeypatch):
    _no_social_financing(monkeypatch)
    client = FakeDatacenterClient(
        {
            "RPTA_WEB_TREASURYYIELD": [{"SOLAR_DATE": "2024-06-28", "EMM00166466": 2.25}],
            "RPT_IMP_INTRESTRATEN": [{"REPORT_DATE": "2024-06-28", "IR_RATE": 1.85}],
            "RPTA_WEB_RATE": [{"TRADE_DATE": "2024-06-28", "LPR1Y": 3.45}],
        }
    )
    df = fetch_macro_indicators(date(2024, 6, 28), client=client)  # type: ignore[arg-type]
    ids = set(df["indicator_id"].to_list())
    assert {"cnbond_yield_10y", "shibor_3m"}.issubset(ids)
    out = validate_dataframe(
        df.with_columns(
            source=pl.lit("eastmoney"),
            data_version=pl.lit("v1"),
            fetched_at=pl.lit("2024-06-28T00:00:00+00:00"),
        ),
        "macro_indicators",
    )
    assert out["obs_date"][0] == date(2024, 6, 28)


def test_macro_indicators_skips_nonfinite_source_values(monkeypatch):
    _no_social_financing(monkeypatch)
    client = FakeDatacenterClient(
        {
            "RPTA_WEB_TREASURYYIELD": [
                {"SOLAR_DATE": "2024-06-28", "EMM00166466": "nan"},
                {"SOLAR_DATE": "2024-06-28", "EMM00166466": 2.25},
                {"SOLAR_DATE": None, "EMM00166466": 2.5},
                {"SOLAR_DATE": "2024-06-27", "EMM00166466": 2.75},
            ],
            "RPT_IMP_INTRESTRATEN": [{"REPORT_DATE": "2024-06-28", "IR_RATE": "inf"}],
            "RPTA_WEB_RATE": [{"TRADE_DATE": "2024-06-28", "LPR1Y": "-inf"}],
        }
    )
    df = fetch_macro_indicators(date(2024, 6, 28), client=client)  # type: ignore[arg-type]
    assert df.filter(pl.col("indicator_id") == "cnbond_yield_10y").height == 1
    assert df.filter(pl.col("indicator_id") == "shibor_3m").is_empty()
    assert df.filter(pl.col("indicator_id") == "lpr_1y").is_empty()


def test_macro_indicators_rejects_empty_fetch(monkeypatch, tmp_path):
    from cnequity.steps import macro_risk
    from cnequity.storage.state import StateStore

    cfg = Config(data_root=tmp_path / "data")
    cfg.staging_root.mkdir(parents=True)
    StateStore(cfg.meta_root).set_date("macro_indicators", date(2024, 6, 27))
    monkeypatch.setattr(
        macro_risk,
        "fetch_macro_indicators",
        lambda *_args, **_kwargs: pl.DataFrame(),
    )
    with pytest.raises(RuntimeError, match="macro_indicators: no rows returned"):
        macro_risk.step_macro_indicators(cfg, date(2024, 6, 28), "run-empty", {})


def test_macro_indicators_warns_when_one_daily_series_is_missing(monkeypatch, tmp_path):
    from cnequity.steps import macro_risk

    cfg = Config(data_root=tmp_path / "data")
    cfg.staging_root.mkdir(parents=True)
    monkeypatch.setattr(
        macro_risk,
        "fetch_macro_indicators",
        lambda *_args, **_kwargs: pl.DataFrame(
            {
                "indicator_id": ["cnbond_yield_10y", "pmi_manufacturing"],
                "obs_date": [date(2024, 6, 28), date(2024, 5, 31)],
                "value": [2.25, 49.5],
                "frequency": ["daily", "monthly"],
                "source": ["eastmoney", "eastmoney"],
            }
        ),
    )

    result = macro_risk.step_macro_indicators(cfg, date(2024, 6, 28), "run-gap", {})

    assert result["status"] == "warning"
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "daily_series_gap"
    assert finding["missing_dates"]["2024-06-28"] == ["shibor_3m"]
    assert len(finding["missing_dates"]) == 5


def test_macro_indicators_honours_eastmoney_source_switch(tmp_path):
    from cnequity.steps import macro_risk

    cfg = Config(data_root=tmp_path / "data", sources={"eastmoney": False})
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        macro_risk.step_macro_indicators(cfg, date(2024, 6, 28), "run-disabled", {})


def test_macro_indicators_fails_loud_on_daily_source_error(monkeypatch):
    _no_social_financing(monkeypatch)

    def _boom(*_args, **_kwargs):
        raise EastMoneyDatacenterError("treasury schema changed")

    monkeypatch.setattr("cnequity.adapters.macro.indicators.fetch_datacenter", _boom)
    with pytest.raises(EastMoneyDatacenterError, match="treasury schema changed"):
        fetch_macro_indicators(date(2024, 6, 28), client=FakeDatacenterClient({}))  # type: ignore[arg-type]


def test_macro_indicators_closes_owned_client_on_source_error(monkeypatch):
    _no_social_financing(monkeypatch)
    created: list[FakeDatacenterClient] = []

    def _factory(**_kwargs):
        client = FakeDatacenterClient({})
        created.append(client)
        return client

    def _boom(*_args, **_kwargs):
        raise EastMoneyDatacenterError("source unavailable")

    monkeypatch.setattr("cnequity.adapters.macro.indicators.EastMoneyClient", _factory)
    monkeypatch.setattr("cnequity.adapters.macro.indicators.fetch_datacenter", _boom)
    with pytest.raises(EastMoneyDatacenterError, match="source unavailable"):
        fetch_macro_indicators(date(2024, 6, 28))
    assert created[0].closed is True


_EM_MONTHLY_BATCHES = {
    "RPTA_WEB_TREASURYYIELD": [{"SOLAR_DATE": "2024-06-28", "EMM00166466": 2.25}],
    # EastMoney publishes monthly observations dated at month start.
    "RPT_ECONOMY_PMI": [
        {"REPORT_DATE": "2024-05-01 00:00:00", "TIME": "2024年05月份", "MAKE_INDEX": 49.5},
        {"REPORT_DATE": "2024-07-01 00:00:00", "TIME": "2024年07月份", "MAKE_INDEX": 49.0},
    ],
    "RPT_ECONOMY_CURRENCY_SUPPLY": [
        {
            "REPORT_DATE": "2024-05-01 00:00:00",
            "TIME": "2024年05月份",
            "BASIC_CURRENCY": 3010000.0,
            "BASIC_CURRENCY_SAME": 7.0,
        }
    ],
}


def test_macro_monthly_series_come_from_eastmoney_directly(monkeypatch):
    """PMI and M2 are read from the EastMoney reports, not an AkShare wrapper.

    Both wrappers requested this same datacenter endpoint, so going direct keeps
    the publisher and drops the parsing layer (issue #3).
    """
    _no_social_financing(monkeypatch)
    df = fetch_macro_indicators(
        date(2024, 6, 28),
        client=FakeDatacenterClient(_EM_MONTHLY_BATCHES),  # type: ignore[arg-type]
    )
    by_id = dict(zip(df["indicator_id"].to_list(), df["source"].to_list(), strict=True))
    assert by_id["pmi_manufacturing"] == "eastmoney"
    assert by_id["m2_yoy"] == "eastmoney"


def test_macro_m2_reads_the_yoy_column_not_a_positional_fallback():
    """`m2_yoy` must be M2 年同比, i.e. BASIC_CURRENCY_SAME.

    The AkShare path matched columns by substring with a
    `next(..., columns[-1])` default. Its hint "M2-同比增长" never matched the
    real label "货币和准货币(M2)-同比增长" (the bracket breaks the substring), so
    it silently fell through to the *last* column — 流通中的现金(M0)-环比增长 —
    and the lake stored M0 month-over-month growth under the name `m2_yoy`.
    Reading a named field cannot fail that way.
    """
    from cnequity.adapters.macro.indicators import _EM_MONTHLY_SERIES, _eastmoney_monthly

    assert _EM_MONTHLY_SERIES["m2_yoy"]["value_column"] == "BASIC_CURRENCY_SAME"
    rows = _eastmoney_monthly(
        FakeDatacenterClient(_EM_MONTHLY_BATCHES),  # type: ignore[arg-type]
        date(2024, 6, 28),
    )
    m2 = [r for r in rows if r["indicator_id"] == "m2_yoy"]
    assert [r["value"] for r in m2] == [7.0]


def test_macro_monthly_obs_dates_land_on_month_end_and_respect_trade_date():
    """Month-start REPORT_DATE is converted; future months are not published early."""
    from cnequity.adapters.macro.indicators import _eastmoney_monthly

    rows = _eastmoney_monthly(
        FakeDatacenterClient(_EM_MONTHLY_BATCHES),  # type: ignore[arg-type]
        date(2024, 6, 28),
    )
    pmi = [r for r in rows if r["indicator_id"] == "pmi_manufacturing"]
    # 2024-05 kept at month end; 2024-07 is past trade_date and dropped.
    assert [r["obs_date"] for r in pmi] == [date(2024, 5, 31)]


def test_social_financing_comes_from_pboc(monkeypatch):
    from cnequity.adapters.macro import indicators as macro_indicators

    monkeypatch.setattr(
        macro_indicators,
        "_social_financing_rows",
        lambda td, config=None: [
            {
                "indicator_id": "social_financing",
                "obs_date": date(2024, 5, 31),
                "value": 2000.0,
                "frequency": "monthly",
                "source": "pboc",
            }
        ],
    )
    df = fetch_macro_indicators(
        date(2024, 6, 28),
        client=FakeDatacenterClient(_EM_MONTHLY_BATCHES),  # type: ignore[arg-type]
    )
    row = df.filter(pl.col("indicator_id") == "social_financing")
    assert row.height == 1
    assert row["source"][0] == "pboc"


def test_canonical_social_financing_fetch_is_strict(monkeypatch):
    from cnequity.adapters.macro import indicators as macro_indicators
    from cnequity.adapters.pboc import social_financing

    seen: dict[str, bool] = {}

    def _fail(*, config=None, start_year=2015, strict=False):
        seen["strict"] = strict
        raise RuntimeError("partial PBOC series")

    monkeypatch.setattr(social_financing, "fetch_social_financing", _fail)
    with pytest.raises(RuntimeError, match="partial PBOC series"):
        macro_indicators._social_financing_rows(date(2024, 6, 28))
    assert seen == {"strict": True}


@pytest.mark.parametrize("enabled", [False])
def test_social_financing_honours_sources_pboc(tmp_path, enabled):
    from cnequity.adapters.macro.indicators import _social_financing_rows

    cfg = Config(data_root=tmp_path / "data")
    cfg.sources = {"pboc": enabled}
    assert _social_financing_rows(date(2024, 6, 28), config=cfg) == []


def test_share_unlock_schedule_parses():
    client = FakeDatacenterClient(
        {
            "RPT_LIFT_STAGE": [
                {
                    "SECURITY_CODE": "600519",
                    "FREE_DATE": "2024-08-01",
                    "ABLE_FREE_SHARES": 1_000_000,
                    "FREE_RATIO": 0.5,
                    "FREE_SHARES_TYPE": "首发原股东",
                }
            ]
        }
    )
    df = fetch_share_unlock_schedule(date(2024, 6, 28), client=client)  # type: ignore[arg-type]
    assert df.height == 1
    assert df["symbol"][0] == "600519.SH"
    assert df["unlock_date"][0] == date(2024, 8, 1)


def test_share_unlock_missing_numeric_fields_remain_null():
    client = FakeDatacenterClient(
        {
            "RPT_LIFT_STAGE": [
                {
                    "SECURITY_CODE": "600519",
                    "FREE_DATE": "2024-08-01",
                    "ABLE_FREE_SHARES": "",
                    "CURRENT_FREE_SHARES": None,
                    "FREE_RATIO": "bad",
                    "FREE_SHARES_TYPE": "首发原股东",
                }
            ]
        }
    )
    df = fetch_share_unlock_schedule(date(2024, 6, 28), client=client)  # type: ignore[arg-type]
    assert df["unlock_shares"][0] is None
    assert df["unlock_ratio"][0] is None


def test_regulatory_events_filters_titles():
    from cnequity.derive.regulatory_events import regulatory_events_from_announcements

    events = regulatory_events_from_announcements(
        pl.DataFrame(
            {
                "announcement_id": ["123", "456"],
                "symbol": ["600519.SH", "600519.SH"],
                "title": ["关于收到行政处罚决定书的公告", "2024年半年度报告摘要"],
                "announce_date": [date(2024, 6, 28), date(2024, 6, 28)],
            }
        )
    )
    assert events.height == 1
    assert events["event_type"][0] == "penalty"
    assert events["event_id"][0] == "reg-123"


@pytest.fixture
def breadth_lake(tmp_path):
    root = tmp_path / "data"
    curated = root / "curated"

    cal = curated / "trading_calendar" / "trade_date=2024-06-27"
    cal.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27)],
            "is_trading": [True],
            "source": ["seed"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-27T00:00:00+00:00"],
        }
    ).write_parquet(cal / "part-0.parquet")

    for d, closes in (
        (date(2024, 6, 27), {"A.SH": 10.0, "B.SH": 20.0, "C.SH": 30.0}),
        (date(2024, 6, 28), {"A.SH": 11.0, "B.SH": 18.0, "C.SH": 30.0}),
    ):
        part = curated / "daily_bars" / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": list(closes.keys()),
                "trade_date": [d] * 3,
                "open": list(closes.values()),
                "high": list(closes.values()),
                "low": list(closes.values()),
                "close": list(closes.values()),
                "volume": [100, 100, 100],
                "amount": [1000.0, 1000.0, 1000.0],
                "source": ["tdx"] * 3,
                "data_version": ["v1"] * 3,
                "fetched_at": ["2024-06-28T00:00:00+00:00"] * 3,
            }
        ).write_parquet(part / "part-0.parquet")

    duplicate_part = curated / "daily_bars" / "trade_date=2024-06-28"
    pl.DataFrame(
        {
            "symbol": ["A.SH"],
            "trade_date": [date(2024, 6, 28)],
            "open": [11.0],
            "high": [11.0],
            "low": [11.0],
            "close": [11.0],
            "volume": [100],
            "amount": [1000.0],
            "source": ["tdx"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:01+00:00"],
        }
    ).write_parquet(duplicate_part / "part-duplicate.parquet")

    return Config(data_root=root)


def test_market_breadth_computed_from_daily_bars(breadth_lake):
    df = compute_market_breadth(breadth_lake, date(2024, 6, 28))
    assert df.height == 7
    metrics = dict(zip(df["metric_id"].to_list(), df["value"].to_list(), strict=True))
    assert metrics["advance_count"] == 1.0
    assert metrics["decline_count"] == 1.0
    assert metrics["flat_count"] == 1.0
    assert metrics["total_count"] == 3.0


def test_market_breadth_excludes_no_trade_placeholders(tmp_path):
    root = tmp_path / "data"
    curated = root / "curated"
    calendar = curated / "trading_calendar" / "trade_date=2024-06-27"
    calendar.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27)],
            "is_trading": [True],
            "source": ["seed"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-27T00:00:00+00:00"],
        }
    ).write_parquet(calendar / "part-0.parquet")

    for trade_date, closes in (
        (date(2024, 6, 27), [10.0, 20.0]),
        (date(2024, 6, 28), [11.0, 20.0]),
    ):
        part = curated / "daily_bars" / f"trade_date={trade_date.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": ["600001.SH", "600002.SH"],
                "trade_date": [trade_date] * 2,
                "open": closes,
                "high": closes,
                "low": closes,
                "close": closes,
                # The second row is a suspended/no-trade placeholder on the
                # requested day and must not become a breadth flat count.
                "volume": [100, 100 if trade_date == date(2024, 6, 27) else 0],
                "amount": [1000.0, 1000.0 if trade_date == date(2024, 6, 27) else 0.0],
                "source": ["tdx"] * 2,
                "data_version": ["v1"] * 2,
                "fetched_at": [f"{trade_date}T00:00:00+00:00"] * 2,
            }
        ).write_parquet(part / "part-0.parquet")

    df = compute_market_breadth(Config(data_root=root), date(2024, 6, 28))
    metrics = dict(zip(df["metric_id"].to_list(), df["value"].to_list(), strict=True))
    assert metrics["advance_count"] == 1.0
    assert metrics["flat_count"] == 0.0
    assert metrics["total_count"] == 1.0


def test_market_breadth_uses_board_and_st_specific_limit_thresholds(tmp_path):
    root = tmp_path / "data"
    curated = root / "curated"
    calendar = curated / "trading_calendar" / "trade_date=2024-06-27"
    calendar.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27)],
            "is_trading": [True],
            "source": ["seed"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-27T00:00:00+00:00"],
        }
    ).write_parquet(calendar / "part-0.parquet")

    symbols = ["600001.SH", "300001.SZ", "688001.SH", "920001.BJ"]
    for trade_date, closes in (
        (date(2024, 6, 27), [10.0] * 4),
        (date(2024, 6, 28), [10.5, 12.0, 12.0, 13.0]),
    ):
        part = curated / "daily_bars" / f"trade_date={trade_date.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [trade_date] * len(symbols),
                "open": closes,
                "high": closes,
                "low": closes,
                "close": closes,
                "volume": [100] * len(symbols),
                "amount": [1000.0] * len(symbols),
                "source": ["tdx"] * len(symbols),
                "data_version": ["v1"] * len(symbols),
                "fetched_at": [f"{trade_date}T00:00:00+00:00"] * len(symbols),
            }
        ).write_parquet(part / "part-0.parquet")

    status = curated / "trading_status" / "trade_date=2024-06"
    status.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": symbols,
            "trade_date": [date(2024, 6, 28)] * len(symbols),
            "status": ["st", "normal", "normal", "normal"],
            "source": ["eastmoney"] * len(symbols),
            "data_version": ["v1"] * len(symbols),
            "fetched_at": ["2024-06-28T00:00:00+00:00"] * len(symbols),
        }
    ).write_parquet(status / "part-0.parquet")

    df = compute_market_breadth(Config(data_root=root), date(2024, 6, 28))
    metrics = dict(zip(df["metric_id"].to_list(), df["value"].to_list(), strict=True))
    assert metrics["limit_up_count"] == 4.0


def test_load_macro_indicators_by_date_range(tmp_path):
    root = tmp_path / "data"
    part = root / "curated" / "macro_indicators" / "obs_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "indicator_id": ["shibor_3m"],
            "obs_date": [date(2024, 6, 28)],
            "value": [1.85],
            "frequency": ["daily"],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(part / "part-0.parquet")
    cfg = Config(data_root=root)
    df = load("macro_indicators", start="2024-06-28", end="2024-06-28", config=cfg)
    assert df.height == 1
    assert df["indicator_id"][0] == "shibor_3m"


def test_parse_series_obs_date_handles_month_formats():
    from cnequity.adapters.macro.indicators import _parse_series_obs_date

    # Monthly-only helper: every accepted form maps to the month's last day.
    # EastMoney reports monthly observations at month *start*, and curated has
    # always keyed them at month end, so the conversion has to happen here.
    assert _parse_series_obs_date("2026-07-01 00:00:00") == date(2026, 7, 31)
    assert _parse_series_obs_date(date(2026, 7, 1)) == date(2026, 7, 31)
    assert _parse_series_obs_date("2024-06-28") == date(2024, 6, 30)
    assert _parse_series_obs_date("2024-06") == date(2024, 6, 30)
    assert _parse_series_obs_date("2024年6月份") == date(2024, 6, 30)
    assert _parse_series_obs_date("2024年12月") == date(2024, 12, 31)
    assert _parse_series_obs_date("garbage") is None
    assert _parse_series_obs_date(None) is None


def test_lake_health_snapshot(tmp_path):
    import polars as pl

    from cnequity.config import Config
    from cnequity.quality.audit import lake_health

    # The health snapshot is an offline unit test. Without a curated
    # instruments file, the delisted-universe report falls back to the source
    # adapter; opt into the deterministic TDX fixture instead of letting CI
    # probe the public network.
    cfg = Config(data_root=tmp_path / "data", tdx_allow_mock=True)
    # one populated dataset up to date, calendar seed present via bundled seed
    part = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
            "amount": [1.0],
            "source": ["tdx_protocol"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(part / "part-0.parquet")

    health = lake_health(cfg, date(2024, 6, 28))
    assert "findings_by_severity" in health
    assert "historical_universe_validity" in health
    assert "daily_bars" not in health["empty_datasets"]
    # most datasets have no data in this minimal lake
    assert "fund_flow" in health["empty_datasets"]
    assert "economic_calendar" in health["expected_empty_datasets"]
    assert "economic_calendar" not in health["empty_datasets"]
    assert (cfg.meta_root / "quality" / "health-latest.json").exists()
    assert (cfg.meta_root / "quality" / "historical-validity-latest.json").exists()
