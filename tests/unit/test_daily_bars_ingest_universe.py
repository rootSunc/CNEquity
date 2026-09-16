"""Ingest scope and the order of certification vs the interior-session gate.

Two defects met here. `instruments` lists every code TDX returns, a quarter of
which are ETF/LOF quote codes no research profile selects and no configured
vendor reliably serves; fetching them burned the EastMoney and Sina circuit
breakers on symbols nobody needs and left their unfillable keys in the coverage
gate. And the gate ran *before* the certification that is the only writer of
daily-bar negative evidence, so one unfillable key kept that cache permanently
empty — which is what made every dead code be re-fetched from every vendor on
every run.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.config import Config, load_config, validate_config
from cnequity.domain.schemas import with_provenance
from cnequity.domain.symbols import filter_ingest_universe, in_ingest_universe
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.bars import _finish_daily_bars, step_daily_bars
from cnequity.steps.common import load_negative_evidence
from cnequity.storage import StagingWriter
from cnequity.storage.layout import init_data_layout

D1 = date(2026, 7, 20)
D2 = date(2026, 7, 21)
D3 = date(2026, 7, 22)


def _cfg(tmp_path, **kwargs) -> Config:
    cfg = Config(data_root=tmp_path / "data", workers=1, batch_size=10, **kwargs)
    init_data_layout(cfg)
    return cfg


def _bars(symbols: list[str], day: date) -> pl.DataFrame:
    n = len(symbols)
    return with_provenance(
        pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [day] * n,
                "open": [10.0] * n,
                "high": [11.0] * n,
                "low": [9.0] * n,
                "close": [10.5] * n,
                "volume": [100] * n,
                "amount": [1000.0] * n,
            }
        ),
        source="tdx_protocol",
        data_version="v1",
    )


def _stage(cfg: Config, run_id: str, batch: str, symbols: list[str], day: date) -> None:
    StagingWriter(cfg.staging_root).write_batch("daily_bars", run_id, batch, _bars(symbols, day))


# --------------------------------------------------------------------------
# P0-A — ingest scope
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("symbol", "all_a", "sh_sz"),
    [
        ("600519.SH", True, True),  # main board
        ("000001.SZ", True, True),
        ("300750.SZ", True, True),  # ChiNext
        ("688002.SH", True, True),  # STAR
        ("689009.SH", True, True),  # CDR — listed, still fetched
        ("920184.BJ", True, False),  # Beijing
        ("158030.SZ", False, False),  # LOF
        ("159089.SZ", False, False),  # ETF
        ("512740.SH", False, False),  # ETF
        ("110045.SH", False, False),  # convertible bond
    ],
)
def test_ingest_universe_membership(symbol, all_a, sh_sz):
    code, exchange = symbol.split(".")
    assert in_ingest_universe(code, exchange, "all_a") is all_a
    assert in_ingest_universe(code, exchange, "all_a_sh_sz") is sh_sz
    assert in_ingest_universe(code, exchange, "all_instruments") is True


def test_filter_preserves_order_and_drops_unparseable_symbols():
    given = ["600519.SH", "158030.SZ", "000001.SZ", "not-a-symbol"]
    assert filter_ingest_universe(given, "all_a") == ["600519.SH", "000001.SZ"]
    # `all_instruments` is the documented escape hatch and stays verbatim.
    assert filter_ingest_universe(given, "all_instruments") == given


def test_ingest_universe_defaults_to_all_a_and_is_validated(tmp_path):
    cfg = _cfg(tmp_path)
    assert cfg.ingest_universe == "all_a"
    # Only the universe verdict matters here; a bare Config has no waves.
    assert not [error for error in validate_config(cfg) if "[universe].ingest" in error]

    path = tmp_path / "bad.toml"
    path.write_text(
        f'[data]\nroot = "{(tmp_path / "lake").as_posix()}"\n[universe]\ningest = "everything"\n'
    )
    errors = validate_config(load_config(path))
    assert any("[universe].ingest" in error for error in errors)


def _instrument_lake(tmp_path, **kwargs) -> Config:
    cfg = _cfg(tmp_path, **kwargs)
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "920184.BJ", "158030.SZ", "512740.SH"],
            "name": ["贵州茅台", "北交所某股", "某 LOF", "某 ETF"],
            "asset_type": ["stock", "stock", "etf", "etf"],
            "list_date": [date(2001, 8, 27)] * 4,
            "delist_date": [None] * 4,
        }
    ).write_parquet(instruments / "part-merged.parquet")
    return cfg


def _capture_fetch_scope(monkeypatch) -> list[str]:
    requested: list[str] = []

    def fake_fetch(config, symbols, start, end, run_id, dataset, **kwargs):
        requested.extend(symbols)
        return {"rows_read": 0, "rows_written": 0, "failed_symbols": []}

    # Scope tests exercise routing without consulting a live exchange snapshot.
    monkeypatch.setattr(
        "cnequity.steps.bars._fetch_tip_via_exchange",
        lambda *a, **k: {
            "rows_read": 0,
            "rows_written": 0,
            "covered": set(),
            "source_outcomes": {},
        },
    )
    monkeypatch.setattr("cnequity.steps.bars.fetch_daily_bars_parallel", fake_fetch)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *a, **k: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 0,
            "failed_symbol_names": [],
            "empty_symbol_names": [],
        },
    )
    return requested


def test_daily_fetch_scope_excludes_fund_codes_but_keeps_beijing(tmp_path, monkeypatch):
    cfg = _instrument_lake(tmp_path)
    requested = _capture_fetch_scope(monkeypatch)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")

    with pytest.raises(RuntimeError):
        # The empty fetch leaves both A-share names unresolved; the scope this
        # test asserts on is decided before that.
        step_daily_bars(cfg, D3, run_id, {})

    assert "600519.SH" in requested
    assert "920184.BJ" not in requested  # routed to the Sina fallback, not TDX
    assert not [symbol for symbol in requested if symbol in {"158030.SZ", "512740.SH"}]


def test_all_instruments_restores_the_previous_fetch_scope(tmp_path, monkeypatch):
    cfg = _instrument_lake(tmp_path, ingest_universe="all_instruments")
    requested = _capture_fetch_scope(monkeypatch)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")

    with pytest.raises(RuntimeError):
        step_daily_bars(cfg, D3, run_id, {})

    assert {"158030.SZ", "512740.SH"} <= set(requested)


def test_explicit_backfill_scope_is_never_narrowed(tmp_path, monkeypatch):
    cfg = _instrument_lake(tmp_path)
    requested = _capture_fetch_scope(monkeypatch)
    cfg._backfill = True
    cfg._backfill_start = D1
    cfg._backfill_end = D3
    cfg._backfill_symbols = ["158030.SZ"]
    run_id = Manifest(cfg.manifest_path).start_run("backfill")

    with pytest.raises(RuntimeError):
        step_daily_bars(cfg, D3, run_id, {})

    # An operator who names a symbol gets that symbol.
    assert requested == ["158030.SZ"]


# --------------------------------------------------------------------------
# P0-B — certify before gating
# --------------------------------------------------------------------------


def _finish(cfg: Config, run_id: str, expected: list[str], no_data: list[str]) -> dict:
    return _finish_daily_bars(
        cfg,
        D3,
        run_id,
        start=D1,
        end=D3,
        expected_tdx_symbols=expected,
        expected_fallback_symbols=[],
        tdx_result={"rows_read": 0, "rows_written": 0, "failed_symbols": []},
        sina_result=None,
        expected_no_data_symbols=no_data,
    )


def test_certification_records_negative_evidence_even_when_the_gate_fires(tmp_path):
    """An unfillable key must not cost the whole run its no-data evidence.

    The gate used to raise first, so the certification below it never ran — and
    since that certification is the only writer of daily-bar negative evidence,
    one bad key kept the cache empty and every dead code was re-fetched from
    every vendor on every run.
    """
    cfg = _cfg(tmp_path)
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600000.SH", "600002.SH"],
            "name": ["贵州茅台", "浦发银行", "早已退市"],
            "asset_type": ["stock", "stock", "stock"],
            "list_date": [date(2001, 8, 27)] * 3,
            # Delisted a decade before the window: proof, not a fetch failure.
            "delist_date": [None, None, date(2015, 12, 31)],
        }
    ).write_parquet(instruments / "part-merged.parquet")

    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    for day in (D1, D2, D3):
        _stage(cfg, run_id, f"clean-{day:%m%d}", ["600519.SH"], day)
    # Interior hole on D2 only, so this symbol still has a row on `end` and the
    # symbol-level certification does not claim it.
    _stage(cfg, run_id, "gap-1", ["600000.SH"], D1)
    _stage(cfg, run_id, "gap-3", ["600000.SH"], D3)

    with pytest.raises(RuntimeError, match="interior"):
        _finish(cfg, run_id, ["600519.SH", "600000.SH", "600002.SH"], [])

    evidence = load_negative_evidence(cfg, "daily_bars")
    assert {record["symbol"] for record in evidence} == {"600002.SH"}
    assert {record["reason"] for record in evidence} == {"verified_no_data"}


def test_arbitrated_empty_symbol_is_not_reported_as_an_interior_gap(tmp_path):
    """A symbol two sources agree has no rows is certified, not gated."""
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    for day in (D1, D2, D3):
        _stage(cfg, run_id, f"clean-{day:%m%d}", ["600519.SH"], day)
    # Traded, then went quiet before `end`: partial evidence, so the interior
    # gate sees its missing D3 key.
    _stage(cfg, run_id, "partial-1", ["600000.SH"], D1)
    _stage(cfg, run_id, "partial-2", ["600000.SH"], D2)

    result = _finish(cfg, run_id, ["600519.SH", "600000.SH"], ["600000.SH"])

    assert result["rows_written"] == 0
    checks = {f["check"] for f in result.get("context_updates", {}).get("audit_findings", [])}
    assert "daily_bars_interior_gap" not in checks
    assert "daily_bars_expected_no_data" in checks


def test_uncertified_interior_gap_still_refuses_to_checkpoint(tmp_path):
    """The gate is reordered, not weakened."""
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    _stage(cfg, run_id, "gap-1", ["600000.SH"], D1)
    _stage(cfg, run_id, "gap-3", ["600000.SH"], D3)

    with pytest.raises(RuntimeError, match="interior symbol×session"):
        _finish(cfg, run_id, ["600000.SH"], [])


# --------------------------------------------------------------------------
# P1-E — a raising step keeps its diagnostics
# --------------------------------------------------------------------------


def test_interior_gap_finding_is_written_before_the_step_raises(tmp_path):
    """The message alone cannot say which keys were missing.

    A raising step never reaches `run_audit`, so the finding — which carries
    the missing symbols and sample keys — used to die with the exception and
    leave the operator grepping logs.
    """
    import json

    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    _stage(cfg, run_id, "gap-1", ["600000.SH"], D1)
    _stage(cfg, run_id, "gap-3", ["600000.SH"], D3)

    with pytest.raises(RuntimeError, match="interior"):
        _finish(cfg, run_id, ["600000.SH"], [])

    payload = json.loads((cfg.meta_root / "quality" / "findings" / f"{run_id}.json").read_text())
    gap = next(f for f in payload["findings"] if f["check"] == "daily_bars_interior_gap")
    assert gap["missing_symbols"] == ["600000.SH"]
    assert gap["sample_keys"] == [{"symbol": "600000.SH", "trade_date": D2.isoformat()}]
    assert payload["run_id"] == run_id


def test_persisting_findings_never_replaces_the_real_failure(tmp_path, monkeypatch):
    """A broken meta directory must not turn a gap into an I/O error."""
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    _stage(cfg, run_id, "gap-1", ["600000.SH"], D1)
    _stage(cfg, run_id, "gap-3", ["600000.SH"], D3)

    # Fail the write itself, not the wrapper, so the helper's own guard runs.
    def _boom(*args, **kwargs):
        raise OSError("read-only meta")

    monkeypatch.setattr("cnequity.quality.audit.write_json_atomic", _boom)

    with pytest.raises(RuntimeError, match="interior"):
        _finish(cfg, run_id, ["600000.SH"], [])


# --------------------------------------------------------------------------
# P1-D — the fallback budget serves the research universe first
# --------------------------------------------------------------------------


def test_fallback_queue_puts_research_symbols_ahead_of_quote_codes():
    """A circuit breaker must not strand A shares behind ETF quote codes.

    The per-symbol vendors abandon everything still queued once their circuit
    opens, so an alphabetical queue spent the whole budget on 15xxxx/16xxxx
    before reaching 6xxxxx.
    """
    from cnequity.steps.bars import _research_first

    queue = _research_first(
        {"158030.SZ", "600519.SH", "512740.SH", "000001.SZ", "920184.BJ", "junk"}
    )

    assert queue[:3] == ["000001.SZ", "600519.SH", "920184.BJ"]
    assert set(queue[3:]) == {"158030.SZ", "512740.SH", "junk"}
    # Deterministic, not shuffled: sorted within each class.
    assert queue[3:] == sorted(queue[3:])
    assert _research_first(queue) == queue


# --------------------------------------------------------------------------
# Beijing: exchange snapshot for the tip, per-symbol Sina only behind it
# --------------------------------------------------------------------------


def test_bj_history_window_is_shorter_than_the_tdx_window(tmp_path):
    """Sina serves one request per symbol per session.

    TDX has no Beijing route, so all ~580 BJ symbols come through that
    endpoint; the dataset's 5-session lookback there is ~2,900 requests a run,
    which is what earns HTTP 456 and a vendor-wide cooldown that then strands
    unrelated symbols.
    """
    from cnequity.steps.bars import _bj_history_start

    cfg = _cfg(tmp_path)
    assert cfg.bj_history_lookback_days == 1
    # D1..D3 are consecutive weekday sessions.
    assert _bj_history_start(cfg, D1, D3) == D3

    cfg_deep = _cfg(tmp_path / "deep", bj_history_lookback_days=3)
    assert _bj_history_start(cfg_deep, D1, D3) == D1


def test_bj_tip_comes_from_one_exchange_snapshot_not_one_request_per_symbol(tmp_path, monkeypatch):
    from cnequity.steps.bars import _fetch_bj_tip_via_bse

    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    calls: list[date] = []

    def fake_quotes(trade_date, *, symbols=None, config=None, client=None):
        calls.append(trade_date)
        return pl.DataFrame(
            {
                "symbol": ["920184.BJ", "920204.BJ"],
                "trade_date": [trade_date] * 2,
                "open": [10.0] * 2,
                "high": [11.0] * 2,
                "low": [9.0] * 2,
                "close": [10.5] * 2,
                "volume": [100] * 2,
                "amount": [1000.0] * 2,
            }
        )

    monkeypatch.setattr("cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", fake_quotes)

    result = _fetch_bj_tip_via_bse(cfg, ["920184.BJ", "920204.BJ", "920223.BJ"], D3, run_id)

    # One board sweep for the whole scope, not one call per symbol.
    assert calls == [D3]
    assert result["covered"] == {"920184.BJ", "920204.BJ"}
    staged = pl.read_parquet(
        cfg.staging_root / "daily_bars" / f"run_id={run_id}" / "part-bse-tip-0000.parquet"
    )
    assert staged["source"].unique().to_list() == ["bse"]
    assert staged.height == 2


def test_a_failing_exchange_snapshot_leaves_sina_as_the_backstop(tmp_path, monkeypatch):
    """BSE is the cheaper route, not a new single point of failure."""
    from cnequity.steps.bars import _fetch_bj_tip_via_bse

    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")

    def boom(*args, **kwargs):
        raise RuntimeError("BSE WAF")

    monkeypatch.setattr("cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", boom)

    result = _fetch_bj_tip_via_bse(cfg, ["920184.BJ"], D3, run_id)

    assert result["covered"] == set()
    assert result["rows_written"] == 0
    assert result["source_outcomes"]["bse"]["status"] == "failed"


def test_the_coverage_gate_judges_each_leg_on_its_own_window(tmp_path):
    """A shorter Beijing window must not read as an interior gap."""
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    for day in (D1, D2, D3):
        _stage(cfg, run_id, f"tdx-{day:%m%d}", ["600519.SH"], day)
    # The BJ name only ever had the tip fetched for it.
    _stage(cfg, run_id, "bse-tip", ["920184.BJ"], D3)

    result = _finish_daily_bars(
        cfg,
        D3,
        run_id,
        start=D1,
        end=D3,
        expected_tdx_symbols=["600519.SH"],
        expected_fallback_symbols=["920184.BJ"],
        fallback_start=D3,
        tdx_result={"rows_read": 0, "rows_written": 0, "failed_symbols": []},
        sina_result=None,
        expected_no_data_symbols=[],
    )

    checks = {f["check"] for f in result.get("context_updates", {}).get("audit_findings", [])}
    assert "daily_bars_interior_gap" not in checks


def test_gapfill_uses_the_legs_own_window_not_the_tdx_one(tmp_path, monkeypatch):
    """Narrowing the fetch without narrowing the recovery only moves the cost.

    The Beijing leg is asked for one session, and the recovery chain then
    chased five of them one symbol at a time through THS at 1 req/s — 644
    silent seconds of a 772-second step.
    """
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    for day in (D1, D2, D3):
        _stage(cfg, run_id, f"tdx-{day:%m%d}", ["600519.SH"], day)

    windows: list[tuple] = []

    def spy(config, rid, *, symbols, start, end, require_complete=True):
        windows.append((tuple(sorted(symbols)), start, end))
        return {"rows_read": 0, "rows_written": 0, "filled": False, "complete": False}

    monkeypatch.setattr(bars_mod, "_gapfill_multiday_via_kline", spy)

    with pytest.raises(RuntimeError):
        _finish_daily_bars(
            cfg,
            D3,
            run_id,
            start=D1,
            end=D3,
            expected_tdx_symbols=["600519.SH"],
            expected_fallback_symbols=["920184.BJ"],
            fallback_start=D3,
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "failed_symbols": ["920184.BJ"],
            },
            sina_result=None,
            expected_no_data_symbols=[],
        )

    bj = [w for w in windows if w[0] == ("920184.BJ",)]
    assert bj, f"expected a gap-fill for the Beijing leg, saw {windows}"
    assert bj[0][1] == D3, "the Beijing leg owes only the session it was asked for"


def test_gapfill_keeps_the_full_window_when_the_legs_agree(tmp_path, monkeypatch):
    """With no narrowed leg the recovery window is unchanged."""
    from cnequity.steps import bars as bars_mod

    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    _stage(cfg, run_id, "tdx", ["600519.SH"], D3)
    windows: list[tuple] = []

    def spy(config, rid, *, symbols, start, end, require_complete=True):
        windows.append((tuple(sorted(symbols)), start, end))
        return {"rows_read": 0, "rows_written": 0, "filled": False, "complete": False}

    monkeypatch.setattr(bars_mod, "_gapfill_multiday_via_kline", spy)

    with pytest.raises(RuntimeError):
        _finish_daily_bars(
            cfg,
            D3,
            run_id,
            start=D1,
            end=D3,
            expected_tdx_symbols=["600001.SH"],
            expected_fallback_symbols=[],
            fallback_start=None,
            tdx_result={"rows_read": 0, "rows_written": 0, "failed_symbols": ["600001.SH"]},
            sina_result=None,
            expected_no_data_symbols=[],
        )

    assert windows and windows[0][1] == D1


# --------------------------------------------------------------------------
# SH/SZ tip from each exchange's own whole-board publication
# --------------------------------------------------------------------------


def test_exchange_tip_covers_the_board_and_leaves_tdx_the_remainder(tmp_path, monkeypatch):
    """Two requests for the market instead of one per symbol.

    TDX bills per symbol, so the tip could never be made cheaper while it
    arrived bundled into the same sweep that paid for the reconciliation tail.
    Routing it here is what prices the two separately.
    """
    from cnequity.adapters.exchange.daily_quotes import ExchangeQuotesResult
    from cnequity.steps.bars import _fetch_tip_via_exchange

    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    calls: list[date] = []

    def fake(trade_date, *, config=None):
        calls.append(trade_date)
        return ExchangeQuotesResult(
            quotes=_bars(["600519.SH", "000001.SZ"], trade_date).drop(
                "source", "data_version", "fetched_at"
            ),
            covered=frozenset({"SSE", "SZSE"}),
            failures={},
        )

    monkeypatch.setattr("cnequity.adapters.exchange.daily_quotes.fetch_exchange_daily_quotes", fake)

    result = _fetch_tip_via_exchange(cfg, ["600519.SH", "000001.SZ", "600001.SH"], D3, run_id)

    assert calls == [D3], "one board request set, not one per symbol"
    assert result["covered"] == {"600519.SH"}
    # SZSE report turnover has a different trade scope; vendors retain ownership.
    staged = pl.read_parquet(
        cfg.staging_root / "daily_bars" / f"run_id={run_id}" / "part-exchange-tip-0000.parquet"
    )
    assert staged["source"].unique().to_list() == ["exchange"]


def test_one_exchange_failing_leaves_its_symbols_to_tdx(tmp_path, monkeypatch):
    """SZSE resets the connection often enough that this must never be a swap."""
    from cnequity.adapters.exchange.daily_quotes import ExchangeQuotesResult
    from cnequity.steps.bars import _fetch_tip_via_exchange

    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")

    def half(trade_date, *, config=None):
        return ExchangeQuotesResult(
            quotes=_bars(["600519.SH"], trade_date).drop("source", "data_version", "fetched_at"),
            covered=frozenset({"SSE"}),
            failures={"SZSE": "Connection reset by peer"},
        )

    monkeypatch.setattr("cnequity.adapters.exchange.daily_quotes.fetch_exchange_daily_quotes", half)

    result = _fetch_tip_via_exchange(cfg, ["600519.SH", "000001.SZ"], D3, run_id)

    # The SZ symbol is simply not claimed, so the per-symbol path still owns it.
    assert result["covered"] == {"600519.SH"}


def test_a_failing_snapshot_never_fails_the_step(tmp_path, monkeypatch):
    from cnequity.steps.bars import _fetch_tip_via_exchange

    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")

    def boom(*args, **kwargs):
        raise RuntimeError("exchange down")

    monkeypatch.setattr("cnequity.adapters.exchange.daily_quotes.fetch_exchange_daily_quotes", boom)

    result = _fetch_tip_via_exchange(cfg, ["600519.SH"], D3, run_id)

    assert result["covered"] == set()
    assert result["source_outcomes"]["exchange"]["status"] == "failed"


def test_a_halted_symbol_is_not_written_at_a_zero_price():
    """The board still lists a halted name, with OHL reported as 0.0.

    Nine SH rows looked like that on 2026-09-15 (603400.SH at
    open/high/low = 0.0, close = 54.67). Written through, those zeros reach the
    lake as a >99% single-day drawdown.
    """
    from cnequity.adapters.exchange.daily_quotes import _is_untraded_quote

    assert _is_untraded_quote(0.0, 0.0, 0.0, 54.67) is True
    # A limit-locked session is open == high == low == close, but never at zero.
    assert _is_untraded_quote(10.0, 10.0, 10.0, 10.0) is False
    assert _is_untraded_quote(9.9, 10.1, 9.8, 10.0) is False


def test_an_ex_date_fund_code_cannot_re_enter_the_bar_fetch_scope(tmp_path, monkeypatch):
    """`daily_bars` merges `symbols_to_rebackfill` into its scope *after*
    narrowing to the ingest universe, so anything arriving that way is fetched
    regardless.

    `corporate_actions` carries 348 ETF/LOF codes, and a fund's bars are a NAV
    series — a close with zero volume and zero turnover on every session, which
    every liquidity screen and turnover aggregate then reads as market data.
    Those are exactly the rows `migrate_drop_nav_series_bars.py` had to delete,
    so the list is scoped where it is built.
    """
    from cnequity.steps import events

    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "159118.SZ", "920038.BJ"],
            "ex_date": [date(2026, 9, 15)] * 3,
        }
    )
    captured: dict = {}

    def _scope(cfg):
        today = frame.filter(pl.col("ex_date") == date(2026, 9, 15))
        return events.filter_ingest_universe(
            today["symbol"].unique().to_list(), cfg.ingest_universe
        )

    captured["all_a"] = _scope(Config(data_root=tmp_path / "a", ingest_universe="all_a"))
    captured["all"] = _scope(Config(data_root=tmp_path / "b", ingest_universe="all_instruments"))

    assert sorted(captured["all_a"]) == ["600519.SH", "920038.BJ"]
    # An `all_instruments` lake wants its fund quotes and must keep them.
    assert sorted(captured["all"]) == ["159118.SZ", "600519.SH", "920038.BJ"]
