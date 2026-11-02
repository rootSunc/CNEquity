from datetime import date, datetime, timedelta, timezone

import polars as pl

from cnequity.config import Config
from cnequity.derive import adj_factors as derive
from cnequity.derive.factor_arbitration import (
    arbitrate_factor_sources,
    record_source_overrides,
    source_overrides,
)

FETCHED = datetime(2026, 9, 29, tzinfo=timezone.utc)
DAYS = [date(2024, 1, 1) + timedelta(days=i) for i in range(30)]


def _steps(level_changes: dict[int, float]) -> list[float]:
    level, out = 1.0, []
    for index in range(len(DAYS)):
        level *= level_changes.get(index, 1.0)
        out.append(level)
    return out


# The true corporate-action steps; raw closes gap by exactly these.
TRUTH = {"600001.SH": {5: 1.05, 20: 1.03}, "600002.SH": {12: 1.04}, "600003.SH": {}}


def _write_bars(cfg: Config, symbol: str, steps: dict[int, float], *, skip=()) -> None:
    for index, (day, level) in enumerate(zip(DAYS, _steps(steps), strict=True)):
        if index in skip:
            continue
        part = cfg.curated_root / "daily_bars" / f"trade_date={day}"
        part.mkdir(parents=True, exist_ok=True)
        close = 100.0 / level
        pl.DataFrame(
            {
                "symbol": [symbol],
                "trade_date": [day],
                "open": [close],
                "high": [close],
                "low": [close],
                "close": [close],
                "pre_close": [close],
                "volume": [100.0],
                "amount": [close * 100],
            }
        ).write_parquet(part / f"{symbol}.parquet")


def _lake(tmp_path, *, suspended=(), **series) -> Config:
    cfg = Config(data_root=tmp_path)
    for symbol, steps in TRUTH.items():
        _write_bars(cfg, symbol, steps, skip=suspended if symbol == "600001.SH" else ())
    sina = {
        "600001.SH": _steps({5: 1.05}),  # step at 5 agreed; misses the action at 20
        "600002.SH": _steps({12: 1.04}),  # step at 12 with no recorded action
        "600003.SH": _steps({}),  # still on its action at 8
        **{symbol.replace("_", "."): steps for symbol, steps in series.items()},
    }
    rows = [
        {
            "symbol": s,
            "trade_date": d,
            "adjust_type": "hfq",
            "factor": f,
            "source": "sina",
            "data_version": "v1",
            "fetched_at": FETCHED,
        }
        for s, series in sina.items()
        for d, f in zip(DAYS, series, strict=True)
    ]
    frame = pl.DataFrame(rows)
    for day in DAYS:
        part = cfg.derived_root / "adj_factors" / f"trade_date={day}"
        part.mkdir(parents=True)
        frame.filter(pl.col("trade_date") == day).write_parquet(part / "part-0.parquet")
    actions = [("600001.SH", 5), ("600001.SH", 20), ("600003.SH", 8)]
    part = cfg.curated_root / "corporate_actions" / "ex_date=2024"
    part.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "symbol": s,
                "ex_date": DAYS[i],
                "action_type": "cash_dividend",
                "cash_dividend": 0.5,
                "bonus_ratio": 0.0,
                "transfer_ratio": 0.0,
                "allotment_ratio": None,
                "allotment_price": None,
                "split_factor": 1.0,
                "source": "tdx_protocol",
                "data_version": "v1",
                "fetched_at": FETCHED,
            }
            for s, i in actions
        ],
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    ).write_parquet(part / "part-0.parquet")
    return cfg


def _fake_baostock(symbols, start, end, *, config=None):
    truth = {
        "600001.SH": _steps({5: 1.05, 20: 1.03}),
        "600002.SH": _steps({12: 1.04}),
        "600003.SH": _steps({}),
    }
    frame = pl.DataFrame(
        [
            {"symbol": s, "trade_date": d, "factor": f}
            for s in symbols
            for d, f in zip(DAYS, truth[s], strict=True)
        ]
    )
    return frame, []


def test_baostock_settles_each_contradiction_and_names_the_switches(tmp_path):
    cfg = _lake(tmp_path)
    report = arbitrate_factor_sources(cfg, fetch=_fake_baostock)
    assert report["verdicts"] == {
        "sina_missed_step": 1,
        "action_missing": 1,
        "action_unconfirmed": 1,
    }
    # Only the security whose Sina series is shown wrong switches; baostock
    # also saw its agreed step at day 5.
    assert report["switch"] == ["600001.SH"]

    assert record_source_overrides(cfg, report) == ["600001.SH"]
    assert source_overrides(cfg)["600001.SH"]["source"] == "baostock"


def test_an_overridden_symbol_is_served_from_baostock_with_its_own_cache(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    record_source_overrides(
        cfg, {"switch": ["600001.SH"], "switch_detail": {}, "evidence": "e.json"}
    )
    series = pl.DataFrame({"trade_date": DAYS, "factor": _steps({5: 1.05, 20: 1.03})})
    calls = []

    def backup(config, symbol, adjust_type, sym_bars, *, overridden=False):
        calls.append(symbol)
        return series

    monkeypatch.setattr(derive, "_resolve_factors_via_backup", backup)
    bars = pl.DataFrame({"trade_date": DAYS})
    factors, vendor = derive._resolve_factors(
        cfg, "600001.SH", "hfq", bars, force=True, client=None
    )
    assert vendor == "baostock" and factors.equals(series)
    # The next plain run reads baostock's cache, not Sina's.
    factors, vendor = derive._resolve_factors(
        cfg, "600001.SH", "hfq", bars, force=False, client=None
    )
    assert vendor == "baostock" and calls == ["600001.SH"]


def test_an_interrupted_sweep_resumes_from_unanswered_symbols(tmp_path, monkeypatch):
    from cnequity.derive import factor_arbitration

    monkeypatch.setattr(factor_arbitration, "_BATCH", 1)
    cfg = _lake(tmp_path)
    asked: list[str] = []

    def dies_on_third(symbols, start, end, *, config=None):
        if len(asked) == 2:
            raise KeyboardInterrupt
        asked.extend(symbols)
        return _fake_baostock(symbols, start, end, config=config)

    try:
        arbitrate_factor_sources(cfg, fetch=dies_on_third)
    except KeyboardInterrupt:
        pass
    assert asked == ["600001.SH", "600002.SH"]

    resumed: list[str] = []

    def records(symbols, start, end, *, config=None):
        resumed.extend(symbols)
        return _fake_baostock(symbols, start, end, config=config)

    report = arbitrate_factor_sources(cfg, fetch=records)
    assert resumed == ["600003.SH"]
    assert report["switch"] == ["600001.SH"]
    # --apply after a preview reuses the preview's evidence.
    resumed.clear()
    assert arbitrate_factor_sources(cfg, fetch=records)["switch"] == ["600001.SH"]
    assert resumed == []


def test_failed_symbols_are_asked_again(tmp_path):
    cfg = _lake(tmp_path)

    def loses_600003(symbols, start, end, *, config=None):
        frame, _ = _fake_baostock(symbols, start, end, config=config)
        return frame.filter(pl.col("symbol") != "600003.SH"), ["600003.SH"]

    assert arbitrate_factor_sources(cfg, fetch=loses_600003)["baostock_unserved"] == 1
    asked: list[str] = []

    def records(symbols, start, end, *, config=None):
        asked.extend(symbols)
        return _fake_baostock(symbols, start, end, config=config)

    report = arbitrate_factor_sources(cfg, fetch=records)
    assert asked == ["600003.SH"] and report["baostock_unserved"] == 0


def _events(level_changes: dict[int, float], *, listed: int = 0) -> list[tuple[date, float]]:
    """Baostock's factor table: the listing row, then one row per ex-date."""
    level, out = 1.0, [(DAYS[listed], 1.0)]
    for index in sorted(level_changes):
        level *= level_changes[index]
        out.append((DAYS[index], level))
    return out


def test_ex_date_rows_settle_contradictions_like_a_daily_series(tmp_path):
    cfg = _lake(tmp_path)
    truth = {
        "600001.SH": _events({5: 1.05, 20: 1.03}),
        "600002.SH": _events({12: 1.04}),
        "600003.SH": _events({}),
    }

    def events(symbols, start, end, *, config=None):
        assert start == date(1990, 1, 1)
        rows = [{"symbol": s, "trade_date": d, "factor": f} for s in symbols for d, f in truth[s]]
        return pl.DataFrame(rows), []

    report = arbitrate_factor_sources(cfg, fetch=events)
    assert report["verdicts"] == {
        "sina_missed_step": 1,
        "action_missing": 1,
        "action_unconfirmed": 1,
    }
    assert report["switch"] == ["600001.SH"]


def test_sessions_before_the_first_baostock_row_get_no_verdict(tmp_path):
    cfg = _lake(tmp_path)

    def events(symbols, start, end, *, config=None):
        # 600003's first row lands after its action at day 8: no opinion there.
        truth = {
            "600001.SH": _events({5: 1.05, 20: 1.03}),
            "600002.SH": _events({12: 1.04}),
            "600003.SH": _events({}, listed=10),
        }
        rows = [{"symbol": s, "trade_date": d, "factor": f} for s in symbols for d, f in truth[s]]
        return pl.DataFrame(rows), []

    report = arbitrate_factor_sources(cfg, fetch=events)
    assert "action_unconfirmed" not in report["verdicts"]
    assert report["judged"] == 2


def test_a_small_dividend_both_vendors_see_is_a_missing_action(tmp_path):
    # A 0.1% step is below Sina's materiality bar but not below Baostock's
    # six-decimal factor: both vendors saw the dividend the lake never recorded.
    cfg = _lake(tmp_path, **{"600004_SH": _steps({15: 1.001})})

    def events(symbols, start, end, *, config=None):
        truth = {
            "600001.SH": _events({5: 1.05, 20: 1.03}),
            "600002.SH": _events({12: 1.04}),
            "600003.SH": _events({}),
            "600004.SH": _events({15: 1.001}),
        }
        rows = [{"symbol": s, "trade_date": d, "factor": f} for s in symbols for d, f in truth[s]]
        return pl.DataFrame(rows), []

    report = arbitrate_factor_sources(cfg, fetch=events)
    assert report["verdicts"]["action_missing"] == 2
    assert "sina_spurious_step" not in report["verdicts"]
    assert report["switch"] == ["600001.SH"]


def test_a_step_across_a_long_suspension_does_not_prove_sina_wrong(tmp_path):
    # 600001 is suspended from day 10 to 19: its day-20 gap spans 11 days.
    cfg = _lake(tmp_path, suspended=range(10, 20))
    report = arbitrate_factor_sources(cfg, fetch=_fake_baostock)
    assert report["verdicts"]["sina_missed_step"] == 1
    assert report["switch"] == [] and report["sina_wrong_unproven"] == 1


def test_share_reform_dates_never_prove_sina_wrong(tmp_path, monkeypatch):
    from cnequity.derive import factor_arbitration

    monkeypatch.setattr(factor_arbitration, "_SHARE_REFORM", (DAYS[0], DAYS[-1]))
    report = arbitrate_factor_sources(_lake(tmp_path), fetch=_fake_baostock)
    assert report["switch"] == []


def test_a_price_gap_that_backs_sina_blocks_the_switch(tmp_path):
    # The price follows Sina, not Baostock: 600001 never paid the day-20 action.
    cfg = _lake(tmp_path)
    _write_bars(cfg, "600001.SH", {5: 1.05})
    report = arbitrate_factor_sources(cfg, fetch=_fake_baostock)
    assert report["switch"] == []


def test_vendors_a_hair_apart_do_not_prove_sina_wrong(tmp_path):
    # Sina steps a day early and 0.1% high; the price follows Baostock, but the
    # two vendors describe the same event.
    cfg = _lake(tmp_path, **{"600001_SH": _steps({5: 1.05, 19: 1.031})})
    report = arbitrate_factor_sources(cfg, fetch=_fake_baostock)
    assert report["verdicts"]["sina_missed_step"] == 1
    assert report["switch"] == []


def test_an_action_inside_a_halt_pairs_with_the_step_on_resumption(tmp_path):
    from cnequity.quality.cross_checks import factor_action_contradictions

    cfg = _lake(tmp_path)
    # 600002 trades no session from day 8 to 11; its action lands on day 9 and
    # the factor steps on day 12, the first session back.
    for index in range(8, 12):
        day = DAYS[index]
        part = cfg.derived_root / "adj_factors" / f"trade_date={day}"
        frame = pl.read_parquet(part / "part-0.parquet")
        frame.filter(pl.col("symbol") != "600002.SH").write_parquet(part / "part-0.parquet")
    part = cfg.curated_root / "corporate_actions" / "ex_date=2024"
    actions = pl.read_parquet(part / "part-0.parquet")
    halted = actions.head(1).with_columns(
        pl.lit("600002.SH").alias("symbol"), pl.lit(DAYS[9]).alias("ex_date")
    )
    pl.concat([actions, halted]).write_parquet(part / "part-0.parquet")

    contradictions, _ = factor_action_contradictions(cfg)
    assert "600002.SH" not in contradictions.get_column("symbol").to_list()


def test_actual_market_closes_alone_cannot_prove_a_whole_source_switch(tmp_path):
    cfg = _lake(tmp_path)
    for path in (cfg.curated_root / "daily_bars").rglob("*.parquet"):
        pl.read_parquet(path).drop("pre_close").write_parquet(path)
    report = arbitrate_factor_sources(cfg, fetch=_fake_baostock)
    assert report["switch"] == []


def test_peer_must_cover_the_whole_stored_series(tmp_path):
    cfg = _lake(tmp_path)

    def late(symbols, start, end, *, config=None):
        frame, failed = _fake_baostock(symbols, start, end, config=config)
        return frame.filter(pl.col("trade_date") >= DAYS[2]), failed

    assert arbitrate_factor_sources(cfg, fetch=late)["switch"] == []


def test_existing_material_event_magnitude_must_agree(tmp_path):
    cfg = _lake(tmp_path)

    def wrong(symbols, start, end, *, config=None):
        frame, failed = _fake_baostock(symbols, start, end, config=config)
        frame = frame.with_columns(
            pl.when((pl.col("symbol") == "600001.SH") & (pl.col("trade_date") >= DAYS[5]))
            .then(pl.col("factor") * 1.15 / 1.05)
            .otherwise(pl.col("factor"))
            .alias("factor")
        )
        return frame, failed

    assert arbitrate_factor_sources(cfg, fetch=wrong)["switch"] == []
