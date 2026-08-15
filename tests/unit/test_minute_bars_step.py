from datetime import date, datetime, timedelta

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import get_dataset
from cn_market_lake.domain.schemas import with_provenance
from cn_market_lake.steps import intraday
from cn_market_lake.steps.intraday import (
    MinuteBarsScopeError,
    capture_intraday_bars,
    resolve_scope,
)
from cn_market_lake.storage import StagingWriter


@pytest.fixture
def cfg(tmp_path):
    config = Config(data_root=tmp_path / "lake")
    for sub in ("staging", "curated", "meta"):
        (config.data_root / sub).mkdir(parents=True, exist_ok=True)
    return config


def _write_constituents(config: Config, index_symbol: str, symbols: list[str], as_of: date):
    root = config.curated_root / "index_constituents" / f"as_of_date={as_of:%Y-%m}"
    root.mkdir(parents=True, exist_ok=True)
    df = with_provenance(
        pl.DataFrame(
            {
                "index_symbol": [index_symbol] * len(symbols),
                "symbol": symbols,
                "as_of_date": [as_of] * len(symbols),
                "weight": [1.0] * len(symbols),
            }
        ),
        source="test",
        data_version="v1",
    )
    df.write_parquet(root / f"part-{index_symbol}-{as_of:%Y%m%d}.parquet")


def test_scope_index_uses_latest_as_of(cfg):
    _write_constituents(cfg, "000300.SH", ["600519.SH", "000001.SZ"], date(2026, 6, 30))
    _write_constituents(cfg, "000300.SH", ["600519.SH", "300750.SZ"], date(2026, 7, 31))
    # A rebalance replaces the roster; carrying both as_of dates forward would
    # silently capture names the index no longer holds.
    assert resolve_scope(cfg) == ["300750.SZ", "600519.SH"]


def test_scope_index_ignores_other_indices(cfg):
    _write_constituents(cfg, "000300.SH", ["600519.SH"], date(2026, 7, 31))
    _write_constituents(cfg, "000905.SH", ["603005.SH"], date(2026, 7, 31))
    assert resolve_scope(cfg) == ["600519.SH"]


def test_scope_index_without_constituents_names_the_fix(cfg):
    with pytest.raises(MinuteBarsScopeError, match="index_constituents"):
        resolve_scope(cfg)


def test_scope_watchlist(cfg):
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH", " 000001.SZ "]
    assert resolve_scope(cfg) == ["600519.SH", "000001.SZ"]


def test_scope_watchlist_requires_symbols(cfg):
    cfg.minute_bars_scope = "watchlist"
    with pytest.raises(MinuteBarsScopeError, match="symbols is empty"):
        resolve_scope(cfg)


def test_scope_unknown_value(cfg):
    cfg.minute_bars_scope = "sp500"
    with pytest.raises(MinuteBarsScopeError, match="unknown"):
        resolve_scope(cfg)


def test_scope_all_drops_beijing(cfg, monkeypatch):
    # TDX serves no BJ intraday route, so including them would make every run
    # report hundreds of failures that can never succeed.
    monkeypatch.setattr(
        intraday, "load_symbols", lambda _c: ["600519.SH", "920819.BJ", "000001.SZ"]
    )
    cfg.minute_bars_scope = "all"
    assert resolve_scope(cfg) == ["600519.SH", "000001.SZ"]


def test_step_is_a_no_op_when_disabled(cfg):
    result = capture_intraday_bars(
        cfg, date(2026, 7, 31), "run-1", dataset="minute_bars", frequency="1m"
    )
    assert result["rows_written"] == 0
    assert "disabled" in result["note"]


def test_step_skips_a_frequency_the_config_did_not_ask_for(cfg):
    # Both intraday steps are registered and both are on the intraday group, so
    # the one whose frequency is not configured has to no-op rather than fetch.
    cfg.minute_bars_enabled = True
    cfg.minute_bars_frequencies = ["1m"]
    result = capture_intraday_bars(
        cfg, date(2026, 7, 31), "run-1", dataset="minute_bars_5m", frequency="5m"
    )
    assert result["rows_written"] == 0
    assert "not in [minute_bars].frequencies" in result["note"]


def test_5m_step_runs_when_configured(cfg, monkeypatch):
    cfg.minute_bars_enabled = True
    cfg.minute_bars_frequencies = ["5m"]
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH"]
    monkeypatch.setattr(intraday, "fetch_minute_bars", _fake_fetch())

    result = capture_intraday_bars(
        cfg, date(2026, 7, 31), "run-1", dataset="minute_bars_5m", frequency="5m"
    )

    assert result["rows_written"] == 2
    staged = pl.read_parquet(
        StagingWriter(cfg.staging_root).list_run_files("minute_bars_5m", "run-1")
    )
    # Rows land in the 5m dataset, stamped 5m — not mixed into minute_bars.
    assert staged["frequency"].unique().to_list() == ["5m"]
    assert not StagingWriter(cfg.staging_root).list_run_files("minute_bars", "run-1")


def test_each_intraday_dataset_has_its_own_registered_step():
    import cn_market_lake.steps  # noqa: F401 — register steps
    from cn_market_lake.domain.datasets import intraday_datasets
    from cn_market_lake.orchestrator.registry import STEP_REGISTRY

    for frequency, dataset in intraday_datasets().items():
        assert dataset in STEP_REGISTRY, f"{frequency} has no step"
        assert STEP_REGISTRY[dataset].group == "intraday"


def _fake_fetch(rows_per_symbol: int = 2, failed: list[str] | None = None):
    def fetch(symbols, start, end, *, frequency, **kwargs):
        rows = []
        for sym in symbols:
            for i in range(rows_per_symbol):
                stamp = datetime(end.year, end.month, end.day, 9, 31 + i)
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": end,
                        "bar_time": stamp,
                        "frequency": frequency,
                        "open": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "close": 10.0,
                        "volume": 100,
                        "amount": 1000.0,
                    }
                )
        return pl.DataFrame(rows), list(failed or [])

    return fetch


def test_step_stages_rows_for_the_configured_scope(cfg, monkeypatch):
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH", "000001.SZ"]
    monkeypatch.setattr(intraday, "fetch_minute_bars", _fake_fetch())

    result = capture_intraday_bars(
        cfg, date(2026, 7, 31), "run-1", dataset="minute_bars", frequency="1m"
    )

    assert result["rows_written"] == 4
    assert result["symbols"] == 2
    files = StagingWriter(cfg.staging_root).list_run_files("minute_bars", "run-1")
    staged = pl.read_parquet(files)
    assert staged.height == 4
    # validate_dataframe runs on write, so the staged frame is already the
    # curated contract — provenance included.
    assert {"source", "data_version", "fetched_at"} <= set(staged.columns)
    assert staged["frequency"].unique().to_list() == ["1m"]


def test_step_reports_failed_symbols_as_a_finding(cfg, monkeypatch):
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH", "000001.SZ"]
    monkeypatch.setattr(intraday, "fetch_minute_bars", _fake_fetch(failed=["000001.SZ"]))

    result = capture_intraday_bars(
        cfg, date(2026, 7, 31), "run-1", dataset="minute_bars", frequency="1m"
    )

    findings = result["context_updates"]["audit_findings"]
    assert findings[0]["check"] == "minute_bars_symbol_fetch"
    assert "000001.SZ" in findings[0]["message"]


def test_step_fails_when_nothing_at_all_came_back(cfg, monkeypatch):
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH"]
    monkeypatch.setattr(
        intraday, "fetch_minute_bars", lambda *a, **k: (pl.DataFrame({"symbol": []}), [])
    )
    with pytest.raises(RuntimeError, match="no rows for any"):
        capture_intraday_bars(
            cfg, date(2026, 7, 31), "run-1", dataset="minute_bars", frequency="1m"
        )


def test_backfill_window_is_clamped_to_the_source_horizon(cfg, monkeypatch):
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH"]
    cfg._backfill = True
    cfg._backfill_start = date(2016, 1, 1)
    cfg._backfill_end = date(2026, 7, 31)

    seen: dict = {}

    def fetch(symbols, start, end, **kwargs):
        seen["start"] = start
        return _fake_fetch()(symbols, start, end, **kwargs)

    monkeypatch.setattr(intraday, "fetch_minute_bars", fetch)
    capture_intraday_bars(cfg, date(2026, 7, 31), "run-1", dataset="minute_bars", frequency="1m")

    # 95 trading days back, not 2016: the source has nothing older, so sweeping
    # a decade would spend hours confirming it.
    horizon = get_dataset("minute_bars").earliest_available(date(2026, 7, 31))
    assert seen["start"] == horizon
    assert date(2026, 7, 31) - seen["start"] < timedelta(days=200)


def test_step_reports_symbols_that_returned_no_rows(cfg, monkeypatch):
    """A suspended name legitimately has no bars — but the count must be visible.

    Without it, "asked for 5,400, wrote 5,100" is indistinguishable from a
    silent fetch hole. Measured on a real 60-symbol sweep: 4 symbols returned
    nothing, all four suspended that day (volume=0 in daily_bars).
    """
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH", "000001.SZ", "600485.SH"]

    def fetch(symbols, start, end, *, frequency, **kwargs):
        live = [s for s in symbols if s != "600485.SH"]
        return _fake_fetch()(live, start, end, frequency=frequency, **kwargs)

    monkeypatch.setattr(intraday, "fetch_minute_bars", fetch)
    result = capture_intraday_bars(
        cfg, date(2026, 7, 31), "run-1", dataset="minute_bars", frequency="1m"
    )

    assert result["symbols"] == 3
    assert result["symbols_with_rows"] == 2
    assert result["failed_symbols"] == 0
    # Silence is not an error: a suspended name must not fail the step.
    assert "context_updates" not in result


def test_scope_index_symbol_not_present_in_constituents_data(cfg):
    # index_constituents has data, just not for the requested index — distinct
    # from the "dataset is empty" case, which points at a different fix.
    _write_constituents(cfg, "000300.SH", ["600519.SH"], date(2026, 7, 31))
    cfg.minute_bars_scope = "index:000905.SH"
    with pytest.raises(MinuteBarsScopeError, match="holds no rows for '000905.SH'"):
        resolve_scope(cfg)


def test_empty_window_is_reported_not_fetched(cfg, monkeypatch):
    # A misconfigured window (start after end) must not reach the fetcher at
    # all — there is nothing honest it could return for it.
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH"]
    cfg._backfill = True
    cfg._backfill_start = date(2026, 8, 5)
    cfg._backfill_end = date(2026, 8, 1)

    called = []
    monkeypatch.setattr(intraday, "fetch_minute_bars", lambda *a, **k: called.append(1))
    result = capture_intraday_bars(
        cfg, date(2026, 8, 1), "run-1", dataset="minute_bars", frequency="1m"
    )
    assert result["rows_written"] == 0
    assert "empty window" in result["note"]
    assert called == []


def test_registered_step_delegates_with_its_own_dataset_and_frequency(cfg, monkeypatch):
    """The generated per-dataset step closure, not the helper it wraps.

    Guards the late-binding default-argument pattern in
    ``_register_intraday_steps``: without ``_dataset``/``_frequency`` bound as
    defaults, every generated step would silently call through with whichever
    dataset the loop last saw.
    """
    import cn_market_lake.steps  # noqa: F401 — register steps
    from cn_market_lake.orchestrator.registry import STEP_REGISTRY

    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH"]
    cfg.minute_bars_frequencies = ["5m"]
    monkeypatch.setattr(intraday, "fetch_minute_bars", _fake_fetch())

    step_fn = STEP_REGISTRY["minute_bars_5m"].fn
    result = step_fn(cfg, date(2026, 7, 31), "run-1", {})

    assert result["rows_written"] > 0
    staged = pl.read_parquet(
        StagingWriter(cfg.staging_root).list_run_files("minute_bars_5m", "run-1")
    )
    assert staged["frequency"].unique().to_list() == ["5m"]


def test_approx_trading_days_uses_the_real_calendar_when_available(cfg):
    from cn_market_lake.steps.intraday import _approx_trading_days

    start, end = date(2026, 7, 27), date(2026, 7, 31)
    rows = [
        {
            "trade_date": d,
            "is_trading": d.weekday() < 5,
            "source": "seed",
            "data_version": "v1",
            "fetched_at": "2026-07-31T00:00:00+00:00",
        }
        for d in (start + timedelta(days=i) for i in range((end - start).days + 1))
    ]
    df = with_provenance(
        pl.DataFrame(rows).drop("source", "data_version", "fetched_at"),
        source="seed",
        data_version="v1",
    )
    out = cfg.curated_root / "trading_calendar" / "trade_date=2026"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "part-0.parquet")

    # 2026-07-27..31 is Mon-Fri: 5 real trading days. The 5/7 fallback would
    # give 4 for this same window, so a wrong (fallback) answer here is
    # distinguishable from the real-calendar one being exercised.
    assert _approx_trading_days(cfg, start, end) == 5


def test_a_failing_batch_does_not_abort_the_whole_sweep(cfg, monkeypatch):
    """A whole batch raising (e.g. a connect timeout) must cost that batch,
    not the rest of the run.

    Observed on a real full-market seed: a TCP handshake timed out ~44
    minutes and ~600 reconnects into a run, and — before this fix — took the
    entire step down with it, discarding every batch already fetched.
    """
    monkeypatch.setattr(intraday, "_BATCH_SYMBOLS", 2)
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH", "000001.SZ", "600485.SH", "600001.SH"]

    def fetch(symbols, start, end, *, frequency, **kwargs):
        if "600485.SH" in symbols:
            raise TimeoutError("connect timed out")
        return _fake_fetch()(symbols, start, end, frequency=frequency, **kwargs)

    monkeypatch.setattr(intraday, "fetch_minute_bars", fetch)
    result = capture_intraday_bars(
        cfg, date(2026, 7, 31), "run-1", dataset="minute_bars", frequency="1m"
    )

    # The other batch's rows survive; the failing batch's symbols are
    # recorded as failed rather than silently dropped or fatal to the run.
    assert result["symbols_with_rows"] == 2
    assert result["failed_symbols"] == 2
    assert result["rows_written"] > 0
    findings = result["context_updates"]["audit_findings"]
    assert "600485.SH" in findings[0]["message"]


def test_all_batches_failing_still_raises(cfg, monkeypatch):
    # If every batch fails outright, the existing "nothing at all came back"
    # guard must still fire — this fix tolerates one bad batch, not a
    # completely unreachable source.
    monkeypatch.setattr(intraday, "_BATCH_SYMBOLS", 1)
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH", "000001.SZ"]
    monkeypatch.setattr(
        intraday,
        "fetch_minute_bars",
        lambda *a, **k: (_ for _ in ()).throw(TimeoutError("connect timed out")),
    )
    with pytest.raises(RuntimeError, match="no rows for any"):
        capture_intraday_bars(
            cfg, date(2026, 7, 31), "run-1", dataset="minute_bars", frequency="1m"
        )


def test_max_pages_covers_the_walk_back_from_today_not_just_the_slice_width(cfg, monkeypatch):
    """A chunked backfill slice deep in the past needs a deep page walk.

    The wire always pages back from the live tip (offset 0 = today), not from
    the slice's own `end`. Bounding max_pages by the slice's width (trading
    days *within* [start, end]) starves a slice sitting near the historical
    edge: every page it is allowed lands after `end`, gets discarded by the
    date filter, and the symbol comes back with zero rows -- silently, with
    no error, indistinguishable from "nothing there" until traced back to a
    raw probe. Reproduced live against TDX before this fix: an 8-trading-day
    slice starting ~140 days back got max_pages=4 (from the slice's own
    width) and returned 0 rows; the correct depth (~98 trading days back to
    trade_date) needs max_pages=31 and returns the real data.
    """
    cfg.minute_bars_enabled = True
    cfg.minute_bars_scope = "watchlist"
    cfg.minute_bars_symbols = ["600519.SH"]
    cfg._backfill = True
    # A 10-day-wide slice, but its END is ~140 days before trade_date -- the
    # shape of the oldest chunk in a `_backfill_chunked` sweep.
    cfg._backfill_start = date(2026, 3, 11)
    cfg._backfill_end = date(2026, 3, 20)
    trade_date = date(2026, 8, 1)

    seen: dict = {}

    def fetch(symbols, start, end, *, max_pages, **kwargs):
        seen["max_pages"] = max_pages
        return _fake_fetch()(symbols, start, end, **kwargs)

    monkeypatch.setattr(intraday, "fetch_minute_bars", fetch)
    capture_intraday_bars(cfg, trade_date, "run-1", dataset="minute_bars", frequency="1m")

    # ~98 trading days (2026-03-11 -> 2026-08-01) at 240 bars/day needs ~30
    # pages of 800; the slice's own width (8 trading days) would give 4 --
    # enough to reach nowhere near the real window.
    assert seen["max_pages"] >= 25
