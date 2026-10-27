from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config, ScheduleGroup, WaveConfig, validate_config
from cnequity.domain.schemas import validate_dataframe


def test_fund_flow_schema_normalization():
    raw = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "main_net_inflow": [1_000_000.0],
            "super_large_net_inflow": [500_000.0],
            "large_net_inflow": [300_000.0],
            "medium_net_inflow": [100_000.0],
            "small_net_inflow": [100_000.0],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    )
    out = validate_dataframe(raw, "fund_flow")
    assert out.height == 1


@pytest.fixture
def cfg(tmp_path):
    # These step tests use normalized vendor fakes; exact-wire archive
    # contracts are covered by the adapter/archive tests below.  Keep the
    # wrapper tests explicit so a captureless fake cannot accidentally look
    # like a production archive path.
    return Config(data_root=tmp_path / "data", raw_archive_enabled=False)


def test_step_fund_flow_writes_staging(cfg, monkeypatch):
    from cnequity.steps import capital as cap
    from cnequity.storage.state import StateStore

    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
            }
        )

    monkeypatch.setattr(cap, "fetch_fund_flow", fake_fetch)
    cfg.staging_root.mkdir(parents=True)
    result = cap.step_fund_flow(cfg, date(2024, 6, 28), "run-1", {})
    assert result["rows_written"] == 1
    staged = list(cfg.staging_root.glob("fund_flow/**/*.parquet"))
    assert len(staged) == 1


@pytest.mark.parametrize(
    ("dataset", "step_name", "fetch_name"),
    [
        ("fund_flow", "step_fund_flow", "fetch_fund_flow"),
        ("margin_trading", "step_margin_trading", "fetch_margin_trading"),
    ],
)
def test_capital_steps_reject_empty_canonical_feeds(
    cfg, monkeypatch, dataset, step_name, fetch_name
):
    from cnequity.steps import capital as cap
    from cnequity.storage.state import StateStore

    StateStore(cfg.meta_root).set_date(dataset, date(2024, 6, 27))
    # These assert the empty-feed guard, not the vendor. margin_trading now
    # defaults to the exchange path, so pin the source the fetch is patched on.
    cfg.margin_trading_source = "eastmoney"
    monkeypatch.setattr(cap, fetch_name, lambda *_args, **_kwargs: pl.DataFrame())
    with pytest.raises(RuntimeError, match=f"{dataset}: no rows returned"):
        getattr(cap, step_name)(cfg, date(2024, 6, 28), "run-empty", {})


def test_margin_trading_rejects_partial_daily_feed(cfg, monkeypatch):
    from cnequity.steps import capital as cap
    from cnequity.storage.state import StateStore

    StateStore(cfg.meta_root).set_date("margin_trading", date(2024, 6, 27))
    cfg.margin_trading_source = "eastmoney"
    monkeypatch.setattr(
        cap,
        "fetch_margin_trading",
        lambda trade_date, **kwargs: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "margin_balance": [1.0],
                "margin_buy": [0.0],
                "short_balance": [0.0],
                "short_sell_volume": [0.0],
            }
        ),
    )
    with pytest.raises(RuntimeError, match="margin_trading: incomplete daily snapshot"):
        cap.step_margin_trading(cfg, date(2024, 6, 28), "run-partial", {})


def test_northbound_holdings_rejects_empty_daily_feed(cfg, monkeypatch):
    from cnequity.steps import capital as cap

    monkeypatch.setattr(
        cap,
        "fetch_northbound_holdings",
        lambda *_args, **_kwargs: pl.DataFrame(),
    )
    with pytest.raises(RuntimeError, match="northbound_holdings: no rows returned"):
        cap.step_northbound_holdings(cfg, date(2024, 6, 28), "run-empty", {})


def test_northbound_holdings_backfill_surfaces_missing_quarters(cfg, monkeypatch):
    from cnequity.steps import capital as cap

    cfg._backfill = True
    cfg._backfill_start = date(2020, 1, 1)
    cfg._backfill_end = date(2020, 6, 30)
    monkeypatch.setattr(cap, "fetch_northbound_holdings", lambda *_args, **_kwargs: pl.DataFrame())

    result = cap.step_northbound_holdings(cfg, date(2026, 6, 30), "run-backfill", {})

    assert result["status"] == "warning"
    assert result["missing_periods"] == 2
    assert result["context_updates"]["audit_findings"][0]["check"] == ("backfill_missing_quarters")


def test_northbound_holdings_rejects_partial_period(cfg, monkeypatch):
    from cnequity.steps import capital as cap

    monkeypatch.setattr(
        cap,
        "fetch_northbound_holdings",
        lambda *_args, **_kwargs: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2026, 6, 30)],
                "channel": ["SH"],
                "holding_shares": [1.0],
                "holding_mv": [2.0],
                "holding_ratio": [0.1],
            }
        ),
    )

    with pytest.raises(RuntimeError, match="northbound_holdings: incomplete quarterly snapshot"):
        cap.step_northbound_holdings(cfg, date(2026, 8, 16), "run-partial", {})


def test_northbound_holdings_rejects_missing_exchange_channel(cfg, monkeypatch):
    from cnequity.steps import capital as cap

    monkeypatch.setattr(
        cap,
        "fetch_northbound_holdings",
        lambda *_args, **_kwargs: pl.DataFrame(
            {
                "symbol": [f"600{i:03d}.SH" for i in range(120)],
                "trade_date": [date(2026, 6, 30)] * 120,
                "channel": ["SH"] * 120,
                "holding_shares": [1.0] * 120,
                "holding_mv": [2.0] * 120,
                "holding_ratio": [0.1] * 120,
            }
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="northbound_holdings: incomplete quarterly snapshot.*missing SZ",
    ):
        cap.step_northbound_holdings(cfg, date(2026, 8, 16), "run-missing-channel", {})


def test_northbound_flows_skips_retired_window_without_source_request(cfg, monkeypatch):
    from cnequity.steps import capital as cap

    cfg._backfill = True
    cfg._backfill_start = date(2025, 1, 1)
    cfg._backfill_end = date(2026, 1, 1)

    def unexpected_fetch(*args, **kwargs):
        raise AssertionError("retired northbound window should not hit EastMoney")

    monkeypatch.setattr(cap, "fetch_northbound_flows_range", unexpected_fetch)

    result = cap.step_northbound_flows(cfg, date(2026, 8, 16), "run-retired", {})

    assert result["rows_written"] == 0
    assert "outside northbound flow publication range" in result["note"]


def test_northbound_flows_clips_backfill_to_published_range(cfg, monkeypatch):
    from cnequity.steps import capital as cap

    cfg._backfill = True
    cfg._backfill_start = date(2010, 1, 1)
    cfg._backfill_end = date(2026, 1, 1)
    seen = {}

    def fake_fetch(start, end, **kwargs):
        seen["window"] = (start, end)
        return pl.DataFrame()

    monkeypatch.setattr(cap, "fetch_northbound_flows_range", fake_fetch)

    result = cap.step_northbound_flows(cfg, date(2026, 8, 16), "run-clipped", {})

    assert result == {"rows_read": 0, "rows_written": 0}
    assert seen["window"] == (date(2014, 11, 17), date(2024, 8, 16))


def test_northbound_flows_rejects_partial_published_range(cfg, monkeypatch):
    from cnequity.steps import capital as cap

    requested = [date(2024, 6, 27), date(2024, 6, 28)]
    monkeypatch.setattr(cap, "incremental_trade_dates", lambda *args: requested)
    monkeypatch.setattr(cap, "list_trading_dates", lambda *args: requested)
    monkeypatch.setattr(
        cap,
        "fetch_northbound_flows_range",
        lambda *args, **kwargs: pl.DataFrame(
            {
                "trade_date": [date(2024, 6, 28)],
                "channel": ["SH"],
                "net_buy": [1.0],
                "buy_amount": [2.0],
                "sell_amount": [1.0],
            }
        ),
    )

    with pytest.raises(RuntimeError, match="northbound_flows: incomplete published range"):
        cap.step_northbound_flows(cfg, date(2024, 6, 28), "run-partial", {})


def test_northbound_flows_tolerates_small_gap_from_untracked_hk_holidays(cfg, monkeypatch):
    """A handful of missing day/channel rows must not fail the whole fetch.

    There is no Hong Kong / Stock Connect holiday calendar in this codebase,
    so a mainland trading day that Stock Connect skips because HKEX (not
    SSE/SZSE) is closed always looks "missing" against the mainland-only
    expected set. That must stay a warning, not a hard failure, as long as
    the gap is small relative to the window.
    """
    from cnequity.steps import capital as cap

    requested = [date(2024, 6, 3) + timedelta(days=i) for i in range(20)]
    monkeypatch.setattr(cap, "incremental_trade_dates", lambda *args: requested)
    monkeypatch.setattr(cap, "list_trading_dates", lambda *args: requested)
    # 40 expected (day, channel) rows (20 days x SH+SZ); observed omits just
    # one HK-holiday-style day entirely, an ~5% gap, well under tolerance.
    hk_holiday = requested[7]
    rows = [(day, channel) for day in requested for channel in ("SH", "SZ") if day != hk_holiday]
    monkeypatch.setattr(
        cap,
        "fetch_northbound_flows_range",
        lambda *args, **kwargs: pl.DataFrame(
            {
                "trade_date": [day for day, _ in rows],
                "channel": [channel for _, channel in rows],
                "net_buy": [1.0] * len(rows),
                "buy_amount": [2.0] * len(rows),
                "sell_amount": [1.0] * len(rows),
            }
        ),
    )

    result = cap.step_northbound_flows(cfg, requested[-1], "run-tolerant", {})
    assert result["rows_written"] == len(rows)


def test_trading_status_rejects_empty_feed(cfg, monkeypatch):
    from cnequity.steps import reference

    monkeypatch.setattr(reference, "load_symbols", lambda _cfg: ["600519.SH"])
    monkeypatch.setattr(
        reference,
        "fetch_trading_status",
        lambda *_args, **_kwargs: pl.DataFrame(),
    )
    with pytest.raises(RuntimeError, match="trading_status: no rows returned"):
        reference.step_trading_status(cfg, date(2024, 6, 28), "run-empty", {})


def test_trading_status_rejects_partial_feed(cfg, monkeypatch):
    from cnequity.steps import reference

    monkeypatch.setattr(reference, "load_symbols", lambda _cfg: ["600519.SH", "000001.SZ"])
    monkeypatch.setattr(
        reference,
        "fetch_trading_status",
        lambda *_args, **_kwargs: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 28)],
                "is_trading": [True],
                "status": ["normal"],
            }
        ),
    )
    with pytest.raises(RuntimeError, match="incomplete daily snapshot"):
        reference.step_trading_status(cfg, date(2024, 6, 28), "run-partial", {})


def _write_status_history(cfg, rows: list[tuple[str, date, str, str]]) -> None:
    """Curated trading_status rows as (symbol, trade_date, status, source)."""
    for month in sorted({day.strftime("%Y-%m") for _, day, _, _ in rows}):
        part = cfg.curated_root / "trading_status" / f"trade_date={month}"
        part.mkdir(parents=True, exist_ok=True)
        selected = [row for row in rows if row[1].strftime("%Y-%m") == month]
        pl.DataFrame(
            {
                "symbol": [symbol for symbol, *_ in selected],
                "trade_date": [day for _, day, _, _ in selected],
                "is_trading": [status == "normal" for *_, status, _ in selected],
                "status": [status for *_, status, _ in selected],
                "risk_warning": [False] * len(selected),
                "source": [source for *_, source in selected],
                "data_version": ["v1"] * len(selected),
                "fetched_at": [datetime(2026, 1, 1, tzinfo=timezone.utc)] * len(selected),
            }
        ).write_parquet(part / "part-merged.parquet")


def _staged_status(cfg) -> pl.DataFrame:
    staged = list((cfg.staging_root / "trading_status").rglob("*.parquet"))
    return pl.concat([pl.read_parquet(path) for path in staged], how="diagonal_relaxed").sort(
        "symbol"
    )


def _eastmoney_down(reference, monkeypatch, symbols: list[str]) -> None:
    monkeypatch.setattr(reference, "load_symbols", lambda _cfg: symbols)
    monkeypatch.setattr(
        reference,
        "fetch_trading_status",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("EastMoney down")),
    )


def test_trading_status_outage_writes_no_row_for_symbols_without_evidence(cfg, monkeypatch):
    """An unknown symbol is missing, not halted — and the step says so."""
    from cnequity.domain.schemas import with_provenance
    from cnequity.steps import reference

    status_part = cfg.curated_root / "trading_status" / "trade_date=2024-06"
    status_part.mkdir(parents=True)
    with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 27)],
                "is_trading": [True],
                "status": ["normal"],
            }
        ),
        source="eastmoney",
        data_version="v1",
    ).write_parquet(status_part / "part-merged.parquet")
    _eastmoney_down(reference, monkeypatch, ["600519.SH", "000001.SZ"])

    result = reference.step_trading_status(cfg, date(2024, 6, 28), "run-cached", {})

    finding = result["context_updates"]["audit_findings"][-1]
    assert finding["check"] == "trading_status_cached_fallback"
    assert finding["coverage"] == [
        {
            "trade_date": "2024-06-28",
            "exchange_boards": 0,
            "eastmoney": 0,
            "carried_forward": 1,
            "without_row": 1,
            "undated_without_row": 0,
        }
    ]
    assert result["status"] == "warning"
    assert result["coverage_status"] == "partial"
    out = _staged_status(cfg)
    assert out["symbol"].to_list() == ["600519.SH"]
    assert out["status"].to_list() == ["normal"]
    assert out["source"].unique().to_list() == ["eastmoney_cached"]


def test_trading_status_cache_survives_a_long_holiday(cfg, monkeypatch):
    """09-30 → 10-08 is eight calendar days but one session apart."""
    from cnequity.steps import reference

    _write_status_history(
        cfg,
        [
            ("600519.SH", date(2026, 9, 30), "normal", "eastmoney"),
            ("000711.SZ", date(2026, 9, 30), "suspended", "eastmoney"),
        ],
    )
    _eastmoney_down(reference, monkeypatch, ["600519.SH", "000711.SZ"])

    result = reference.step_trading_status(cfg, date(2026, 10, 8), "run-holiday", {})

    assert "coverage_status" not in result
    out = _staged_status(cfg)
    assert out["symbol"].to_list() == ["000711.SZ", "600519.SH"]
    assert out["status"].to_list() == ["suspended", "normal"]
    assert out["trade_date"].unique().to_list() == [date(2026, 10, 8)]


def test_trading_status_cache_never_chains_on_its_own_rows(cfg, monkeypatch):
    """A carried row is not evidence: only the first-hand reading is carried."""
    from cnequity.steps import reference

    _write_status_history(
        cfg,
        [
            ("600519.SH", date(2026, 9, 30), "normal", "eastmoney"),
            ("000001.SZ", date(2026, 9, 30), "normal", "eastmoney"),
            # The partial 10-08 the old cache produced: one name carried,
            # the other missing, and a third known only from the cache.
            ("600519.SH", date(2026, 10, 8), "normal", "eastmoney_cached"),
            ("300750.SZ", date(2026, 10, 8), "suspended", "eastmoney_cached"),
        ],
    )
    _eastmoney_down(reference, monkeypatch, ["600519.SH", "000001.SZ", "300750.SZ"])

    result = reference.step_trading_status(cfg, date(2026, 10, 9), "run-chain", {})

    out = _staged_status(cfg)
    assert out["symbol"].to_list() == ["000001.SZ", "600519.SH"]
    assert out["status"].to_list() == ["normal", "normal"]
    assert result["coverage_status"] == "partial"


def test_trading_status_cache_expires_after_five_sessions(cfg, monkeypatch):
    from cnequity.steps import reference

    _write_status_history(cfg, [("600519.SH", date(2026, 9, 22), "normal", "eastmoney")])
    _eastmoney_down(reference, monkeypatch, ["600519.SH"])

    with pytest.raises(RuntimeError, match="EastMoney down"):
        reference.step_trading_status(cfg, date(2026, 10, 8), "run-expired", {})


def test_trading_status_cached_fallback_reads_only_recent_window(cfg, monkeypatch):
    from cnequity.steps import reference

    requested: dict[str, object] = {}

    def fake_load(dataset, **kwargs):
        requested.update(kwargs)
        assert dataset == "trading_status"
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 27)],
                "is_trading": [True],
                "status": ["normal"],
            }
        )

    monkeypatch.setattr("cnequity.query.reader.load", fake_load)
    monkeypatch.setattr(reference, "load_symbols", lambda _cfg: ["600519.SH"])
    monkeypatch.setattr(
        reference,
        "fetch_trading_status",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("EastMoney down")),
    )

    reference.step_trading_status(cfg, date(2024, 6, 28), "run-bounded-cache", {})

    assert requested["start"] == date(2024, 6, 21)
    assert requested["end"] == date(2024, 6, 27)


def test_trading_status_rejects_too_old_cached_snapshot(cfg, monkeypatch):
    from cnequity.domain.schemas import with_provenance
    from cnequity.steps import reference

    status_part = cfg.curated_root / "trading_status" / "trade_date=2024-06"
    status_part.mkdir(parents=True)
    with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 20)],
                "is_trading": [True],
                "status": ["normal"],
            }
        ),
        source="eastmoney",
        data_version="v1",
    ).write_parquet(status_part / "part-merged.parquet")
    monkeypatch.setattr(reference, "load_symbols", lambda _cfg: ["600519.SH"])
    monkeypatch.setattr(reference, "fetch_trading_status", lambda *_a, **_k: pl.DataFrame())

    with pytest.raises(RuntimeError, match="trading_status: no rows returned"):
        reference.step_trading_status(cfg, date(2024, 6, 28), "run-stale-cache", {})


def test_trading_status_preserves_incremental_findings(cfg, monkeypatch):
    from cnequity.steps import reference

    row = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "is_trading": [True],
            "status": ["N"],
        }
    )
    finding = {"dataset": "trading_status", "check": "coverage_gap"}
    monkeypatch.setattr(reference, "load_symbols", lambda _cfg: ["600519.SH"])
    monkeypatch.setattr(
        reference,
        "fetch_incremental_daily",
        lambda *args, **kwargs: (row, [finding]),
    )

    result = reference.step_trading_status(cfg, date(2024, 6, 28), "run-1", {})

    assert result["context_updates"]["audit_findings"] == [finding]


def test_valuation_snapshot_filters_to_bar_universe(cfg, monkeypatch):
    """The EastMoney clist returns delisted names with no bar; the daily snapshot
    must drop them so valuation stays in lock-step with daily_bars (audit:
    valuation_bars_orphan_symbol)."""
    from cnequity.steps import fundamentals as fund
    from cnequity.storage.state import StateStore

    # Bar universe: only 600519.SH has ever traded.
    bars_part = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    bars_part.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 28)]}).write_parquet(
        bars_part / "part-merged.parquet"
    )

    StateStore(cfg.meta_root).set_date("valuation_metrics", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                # 600519.SH trades; 000003.SZ is a delisted orphan the clist still returns.
                "symbol": ["600519.SH", "000003.SZ"],
                "trade_date": [trade_date, trade_date],
                "pe_ttm": [30.0, 1.0],
                "pb": [9.0, 0.1],
                "ps_ttm": [12.0, 0.5],
                "total_mv": [2.0e12, 1.0e8],
                "float_mv": [2.0e12, 1.0e8],
            }
        )

    monkeypatch.setattr(fund, "fetch_valuation_metrics", fake_fetch)
    cfg.staging_root.mkdir(parents=True)
    result = fund.step_valuation_metrics(cfg, date(2024, 6, 28), "run-1", {})

    assert result["rows_written"] == 1
    staged = pl.read_parquet(list(cfg.staging_root.glob("valuation_metrics/**/*.parquet")))
    assert staged["symbol"].to_list() == ["600519.SH"]


def test_validate_config_accepts_capital_group(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        # Explicit: the Config default is 8, which validate_config rejects on
        # macOS. This test is about the group shape, not the worker count.
        workers=1,
        daily_waves=[WaveConfig(name="core", parallel=True, steps=["instruments"])],
        schedule_groups={
            "capital": ScheduleGroup(at="16:30", steps=["fund_flow", "margin_trading"]),
        },
    )
    assert validate_config(cfg) == []


def test_northbound_reads_the_hsgt_report_not_an_index_fund_flow_kline():
    """Regression: northbound must not be sourced from a fund-flow kline.

    It used to read ``push2his /stock/fflow/kline/get?secid=1.000001`` and map
    f52/f53 onto SH/SZ. Those fields are 上证指数's 主力净流入 and 小单净流入 —
    two legs of a zero-sum decomposition, not two geographic channels — so the
    column carried plausible-looking numbers that were never northbound at all.
    """
    from cnequity.adapters.eastmoney import capital

    assert capital._NORTH_FLOW_REPORT == "RPT_MUTUAL_DEAL_HISTORY"
    assert capital._NORTHBOUND_CHANNELS == {"001": "SH", "003": "SZ"}
    assert not hasattr(capital, "_FFLOW_KLINE_URL")
    assert not hasattr(capital, "_KAMT_URL")


def test_backup_snapshot_failure_does_not_abort_the_ca_backfill(tmp_path, monkeypatch):
    """Regression: a best-effort audit artifact took down the primary fetch.

    `snapshot_corporate_actions_backup` writes an EastMoney snapshot for
    cross-source audit. When EastMoney changed its filter grammar it started
    raising, and the raise propagated out of the step — aborting
    `cne backfill corporate_actions` before TDX, the actual canonical source,
    was contacted at all.
    """
    from datetime import date

    import polars as pl

    from cnequity.steps import events

    monkeypatch.setattr(events, "load_symbols", lambda cfg: ["600519.SH"])

    def _boom(*a, **k):
        raise RuntimeError("EastMoney datacenter rejected schema")

    monkeypatch.setattr(events, "snapshot_corporate_actions_backup", _boom)
    fetched = {}

    def _fake_tdx(trade_date, **kwargs):
        fetched["called"] = True
        return pl.DataFrame(
            [
                {
                    "symbol": "600519.SH",
                    "ex_date": date(2024, 6, 28),
                    "action_type": "cash_dividend",
                    "cash_dividend": 1.0,
                    "bonus_ratio": 0.0,
                    "transfer_ratio": 0.0,
                    "allotment_ratio": None,
                    "allotment_price": None,
                }
            ]
        )

    monkeypatch.setattr(events, "fetch_corporate_actions", _fake_tdx)
    cfg = Config(
        data_root=tmp_path / "lake",
        failover_backfill_snapshots=True,
        raw_archive_enabled=False,
    )
    cfg._backfill = True
    out = events.step_corporate_actions(cfg, date(2024, 6, 28), "run-1", {})

    assert fetched.get("called") is True, "TDX must still be contacted"
    assert out["rows_written"] == 1


def test_ca_backfill_skips_optional_backup_snapshot_by_default(tmp_path, monkeypatch):
    """A stalled audit artifact must not sit on the canonical fetch path."""
    from datetime import date

    import polars as pl

    from cnequity.steps import events

    monkeypatch.setattr(events, "load_symbols", lambda cfg: ["600519.SH"])

    def _unexpected(*a, **k):
        raise AssertionError("optional backup snapshot should not be called")

    monkeypatch.setattr(events, "snapshot_corporate_actions_backup", _unexpected)
    monkeypatch.setattr(events, "fetch_corporate_actions", lambda *a, **k: pl.DataFrame())
    cfg = Config(data_root=tmp_path / "lake", raw_archive_enabled=False)
    cfg._backfill = True
    out = events.step_corporate_actions(cfg, date(2024, 6, 28), "run-1", {})

    assert out["rows_written"] == 0


def test_a_carried_row_is_labelled_by_the_reading_it_repeats(cfg, monkeypatch):
    """A day carried from the exchange's reading is not filed under EastMoney."""
    from cnequity.domain.schemas import with_provenance
    from cnequity.steps import reference

    status_part = cfg.curated_root / "trading_status" / "trade_date=2024-06"
    status_part.mkdir(parents=True)
    with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 27)],
                "is_trading": [True],
                "status": ["normal"],
            }
        ),
        source="exchange",
        data_version="v1",
    ).write_parquet(status_part / "part-merged.parquet")
    _eastmoney_down(reference, monkeypatch, ["600519.SH"])

    reference.step_trading_status(cfg, date(2024, 6, 28), "run-carried", {})

    assert _staged_status(cfg)["source"].to_list() == ["exchange_cached"]
