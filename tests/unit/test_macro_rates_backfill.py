"""macro_indicators backfill: the daily rates are read as a window, not a day."""

from __future__ import annotations

from datetime import date
from urllib.parse import parse_qs, urlsplit

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError
from cnequity.adapters.macro import indicators
from cnequity.adapters.macro.indicators import DAILY_SERIES_FIRST_OBS, fetch_daily_rates_range
from cnequity.config import Config
from cnequity.steps import macro_risk
from cnequity.steps.common import BACKFILL_START


class RecordingDatacenterClient:
    """Answers each report with its rows and records every request."""

    def __init__(self, batches: dict[str, list[dict]]):
        self.batches = batches
        self.requests: list[dict[str, str]] = []
        self.closed = False

    def get(self, url, **kwargs):
        query = {key: values[0] for key, values in parse_qs(urlsplit(url).query).items()}
        self.requests.append(query)
        rows = self.batches.get(query["reportName"], [])

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"success": True, "result": {"data": rows}}

        return Resp()

    def close(self):
        self.closed = True


def _rate_rows(indicator: str, days: list[date], value: float = 1.5) -> list[dict]:
    return [
        {
            "indicator_id": indicator,
            "obs_date": day,
            "value": value,
            "frequency": "daily",
            "source": "eastmoney",
        }
        for day in days
    ]


def _staged(cfg: Config) -> pl.DataFrame:
    files = list((cfg.staging_root / "macro_indicators").rglob("*.parquet"))
    return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")


@pytest.fixture(autouse=True)
def _no_live_usdcnh(monkeypatch):
    """The rate tests answer the datacenter themselves. Sina stays off the network."""
    monkeypatch.setattr(indicators, "fetch_usdcnh_closes", lambda *args, **kwargs: [])


@pytest.fixture
def cfg(tmp_path):
    c = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    c.staging_root.mkdir(parents=True)
    return c


def _backfill(cfg: Config, start: date | None, end: date | None) -> Config:
    cfg._backfill = True
    cfg._backfill_start = start
    cfg._backfill_end = end
    return cfg


def test_range_fetch_asks_each_report_for_the_whole_window():
    client = RecordingDatacenterClient(
        {
            "RPTA_WEB_TREASURYYIELD": [
                {"SOLAR_DATE": "2016-01-04 00:00:00", "EMM00166466": 2.82},
                {"SOLAR_DATE": "2016-01-05 00:00:00", "EMM00166466": 2.84},
            ],
            "RPT_IMP_INTRESTRATEN": [{"REPORT_DATE": "2016-01-04 00:00:00", "IR_RATE": 3.09}],
        }
    )

    df = fetch_daily_rates_range(date(2016, 1, 1), date(2016, 12, 31), client=client)  # type: ignore[arg-type]

    filters = {req["reportName"]: req["filter"] for req in client.requests}
    assert filters == {
        "RPTA_WEB_TREASURYYIELD": "(SOLAR_DATE>='2016-01-01')(SOLAR_DATE<='2016-12-31')",
        "RPT_IMP_INTRESTRATEN": (
            '(MARKET_CODE="001")(CURRENCY_CODE="CNY")(INDICATOR_ID="203")'
            "(REPORT_DATE>='2016-01-01')(REPORT_DATE<='2016-12-31')"
        ),
    }
    assert {req["sortTypes"] for req in client.requests} == {"1"}
    # LPR is not read as a range: its report changes benchmark in 2019.
    assert "RPTA_WEB_RATE" not in filters
    assert df.to_dicts() == [
        {
            "indicator_id": "cnbond_yield_10y",
            "obs_date": date(2016, 1, 4),
            "value": 2.82,
            "frequency": "daily",
            "source": "eastmoney",
        },
        {
            "indicator_id": "cnbond_yield_10y",
            "obs_date": date(2016, 1, 5),
            "value": 2.84,
            "frequency": "daily",
            "source": "eastmoney",
        },
        {
            "indicator_id": "shibor_3m",
            "obs_date": date(2016, 1, 4),
            "value": 3.09,
            "frequency": "daily",
            "source": "eastmoney",
        },
    ]
    assert client.closed is False


def test_range_fetch_drops_days_the_report_carries_without_a_value():
    """Treasury rows exist for US-only sessions with the CN 10Y column null."""
    client = RecordingDatacenterClient(
        {
            "RPTA_WEB_TREASURYYIELD": [
                {"SOLAR_DATE": "2016-01-01 00:00:00", "EMM00166466": None},
                {"SOLAR_DATE": "2016-01-04 00:00:00", "EMM00166466": "nan"},
                {"SOLAR_DATE": None, "EMM00166466": 2.9},
                {"SOLAR_DATE": "2016-01-05 00:00:00", "EMM00166466": 2.84},
            ],
        }
    )

    df = fetch_daily_rates_range(date(2016, 1, 1), date(2016, 1, 5), client=client)  # type: ignore[arg-type]

    assert df.select("indicator_id", "obs_date").rows() == [("cnbond_yield_10y", date(2016, 1, 5))]


def test_range_fetch_refuses_rows_outside_the_window():
    client = RecordingDatacenterClient(
        {"RPT_IMP_INTRESTRATEN": [{"REPORT_DATE": "2015-12-31 00:00:00", "IR_RATE": 3.09}]}
    )

    with pytest.raises(EastMoneyDatacenterError, match="range filter was not applied"):
        fetch_daily_rates_range(date(2016, 1, 1), date(2016, 1, 5), client=client)  # type: ignore[arg-type]


def test_range_fetch_closes_the_client_it_opened(monkeypatch):
    created: list[RecordingDatacenterClient] = []

    def _factory(**_kwargs):
        created.append(RecordingDatacenterClient({}))
        return created[-1]

    monkeypatch.setattr(indicators, "EastMoneyClient", _factory)

    df = fetch_daily_rates_range(date(2016, 1, 1), date(2016, 1, 5))

    assert df.is_empty()
    assert df.schema["obs_date"] == pl.Date
    assert created[0].closed is True


def test_first_observations_cover_exactly_the_required_daily_series():
    assert set(DAILY_SERIES_FIRST_OBS) == macro_risk._REQUIRED_DAILY_MACRO_INDICATORS


def test_backfill_writes_the_window_history_and_the_run_day_rows(cfg, monkeypatch):
    """The old backfill fetched the run day once, so no history ever landed."""
    run_day = date(2026, 9, 25)
    sessions = [date(2016, 1, 4), date(2016, 1, 5), date(2016, 1, 6)]
    calls: list[tuple] = []

    def _range(start, end, *, client=None, config=None):
        calls.append((start, end, config))
        return pl.DataFrame(
            [
                *_rate_rows("shibor_3m", sessions),
                *_rate_rows("cnbond_yield_10y", sessions),
                # An interbank make-up Saturday: published, but not a session.
                *_rate_rows("shibor_3m", [date(2016, 1, 9)]),
            ]
        )

    monkeypatch.setattr(macro_risk, "fetch_daily_rates_range", _range)
    monkeypatch.setattr(macro_risk, "list_trading_dates", lambda _cfg, _s, _e: sessions)
    monkeypatch.setattr(
        macro_risk,
        "fetch_macro_indicators",
        lambda day, config=None, **kwargs: pl.DataFrame(
            [
                *_rate_rows("shibor_3m", [day], value=1.43),
                {
                    "indicator_id": "pmi_manufacturing",
                    "obs_date": date(2026, 8, 31),
                    "value": 49.4,
                    "frequency": "monthly",
                    "source": "eastmoney",
                },
            ]
        ),
    )
    _backfill(cfg, date(2016, 1, 1), date(2016, 1, 6))

    result = macro_risk.step_macro_indicators(cfg, run_day, "run-bf", {})

    assert calls == [(date(2016, 1, 1), date(2016, 1, 6), cfg)]
    assert result["rows_written"] == 8
    assert result.get("status") != "warning"
    staged = _staged(cfg)
    assert sorted(staged.select("indicator_id", "obs_date").rows()) == sorted(
        [
            *(("shibor_3m", day) for day in sessions),
            *(("cnbond_yield_10y", day) for day in sessions),
            ("shibor_3m", run_day),
            ("pmi_manufacturing", date(2026, 8, 31)),
        ]
    )
    assert set(staged["source"].to_list()) == {"eastmoney"}


def test_backfill_defaults_to_the_lake_start_and_stops_at_the_run_day(cfg, monkeypatch):
    calls: list[tuple] = []
    run_day = date(2026, 9, 25)

    def _range(start, end, *, client=None, config=None):
        calls.append((start, end))
        return pl.DataFrame(_rate_rows("shibor_3m", [run_day]))

    monkeypatch.setattr(macro_risk, "fetch_daily_rates_range", _range)
    monkeypatch.setattr(macro_risk, "list_trading_dates", lambda _cfg, _s, _e: [run_day])
    monkeypatch.setattr(
        macro_risk, "fetch_macro_indicators", lambda day, config=None, **kwargs: pl.DataFrame()
    )
    _backfill(cfg, None, date(2026, 12, 31))

    macro_risk.step_macro_indicators(cfg, run_day, "run-bf", {})

    assert calls == [(BACKFILL_START, run_day)]


def test_backfill_reports_sessions_a_series_misses_but_not_before_it_began(cfg, monkeypatch):
    """Shibor starts 2006-10-08; a treasury hole after that is a real gap."""
    sessions = [date(2006, 9, 29), date(2006, 10, 9), date(2006, 10, 10)]
    monkeypatch.setattr(
        macro_risk,
        "fetch_daily_rates_range",
        lambda start, end, *, client=None, config=None: pl.DataFrame(
            [
                *_rate_rows("cnbond_yield_10y", [date(2006, 9, 29), date(2006, 10, 9)]),
                *_rate_rows("shibor_3m", [date(2006, 10, 9), date(2006, 10, 10)]),
            ]
        ),
    )
    monkeypatch.setattr(macro_risk, "list_trading_dates", lambda _cfg, _s, _e: sessions)
    monkeypatch.setattr(
        macro_risk, "fetch_macro_indicators", lambda day, config=None, **kwargs: pl.DataFrame()
    )
    _backfill(cfg, date(2006, 9, 1), date(2006, 10, 10))

    result = macro_risk.step_macro_indicators(cfg, date(2026, 9, 26), "run-bf", {})

    assert result["status"] == "warning"
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "daily_series_gap"
    # 2006-09-29 predates Shibor; the Saturday run day is not a session.
    assert finding["missing_dates"] == {"2006-10-10": ["cnbond_yield_10y"]}


def test_backfill_with_a_window_after_the_run_day_fetches_no_history(cfg, monkeypatch):
    def _range(*_args, **_kwargs):
        raise AssertionError("an empty window must not reach the source")

    monkeypatch.setattr(macro_risk, "fetch_daily_rates_range", _range)
    monkeypatch.setattr(
        macro_risk,
        "fetch_macro_indicators",
        lambda day, config=None, **kwargs: pl.DataFrame(_rate_rows("shibor_3m", [day])),
    )
    _backfill(cfg, date(2026, 10, 1), None)

    result = macro_risk.step_macro_indicators(cfg, date(2026, 9, 25), "run-bf", {})

    assert result["rows_written"] == 1


def test_daily_run_reads_only_a_short_recent_window(cfg, monkeypatch):
    from cnequity.storage.state import StateStore

    calls: list[tuple[date, date]] = []

    def _range(start, end, *, client=None, config=None):
        calls.append((start, end))
        return pl.DataFrame(
            [*_rate_rows("shibor_3m", [end]), *_rate_rows("cnbond_yield_10y", [end])]
        )

    day = date(2024, 6, 28)
    StateStore(cfg.meta_root).set_date("macro_indicators", date(2024, 6, 27))
    monkeypatch.setattr(macro_risk, "fetch_daily_rates_range", _range)
    monkeypatch.setattr(
        macro_risk, "list_trading_dates", lambda _cfg, _s, _e: [date(2024, 6, 27), day]
    )
    monkeypatch.setattr(
        macro_risk,
        "fetch_macro_indicators",
        lambda d, config=None, **kwargs: pl.DataFrame(
            [*_rate_rows("shibor_3m", [d]), *_rate_rows("cnbond_yield_10y", [d])]
        ),
    )

    result = macro_risk.step_macro_indicators(cfg, day, "run-daily", {})

    # The lookback window, never the lake's whole history.
    assert calls == [(date(2024, 6, 27), day)]
    assert result.get("status") == "warning"  # 06-27 was asked for and not answered
    gap = result["context_updates"]["audit_findings"][0]
    assert gap["missing_dates"] == {"2024-06-27": ["cnbond_yield_10y", "shibor_3m"]}


def test_range_fetch_keeps_the_offshore_yuan_close_and_ignores_a_bad_quote(monkeypatch):
    calls: list[tuple[date, date]] = []

    def _closes(start, end, **kwargs):
        calls.append((start, end))
        return [
            {
                "indicator_id": "usdcnh",
                "obs_date": date(2026, 10, 29),
                "value": 6.6558,
                "frequency": "daily",
                "source": "sina",
            },
            {
                "indicator_id": "usdcnh",
                "obs_date": date(2026, 10, 30),
                "value": 6.6528,
                "frequency": "daily",
                "source": "sina",
            },
        ]

    monkeypatch.setattr(indicators, "fetch_usdcnh_closes", _closes)
    client = RecordingDatacenterClient(
        {"RPTA_WEB_TREASURYYIELD": [{"SOLAR_DATE": "2026-10-30 00:00:00", "EMM00166466": 1.62}]}
    )

    df = fetch_daily_rates_range(date(2026, 10, 29), date(2026, 10, 30), client=client)  # type: ignore[arg-type]

    assert calls == [(date(2026, 10, 29), date(2026, 10, 30))]
    yuan = df.filter(pl.col("indicator_id") == "usdcnh").sort("obs_date")
    assert yuan.select("obs_date", "value", "source").rows() == [
        (date(2026, 10, 29), 6.6558, "sina"),
        (date(2026, 10, 30), 6.6528, "sina"),
    ]
    assert df.filter(pl.col("indicator_id") == "cnbond_yield_10y").height == 1

    def _broken(start, end, **kwargs):
        raise RuntimeError("sina refused")

    monkeypatch.setattr(indicators, "fetch_usdcnh_closes", _broken)
    kept = fetch_daily_rates_range(date(2026, 10, 30), date(2026, 10, 30), client=client)  # type: ignore[arg-type]
    assert kept["indicator_id"].to_list() == ["cnbond_yield_10y"]
