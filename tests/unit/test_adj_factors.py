import json
from datetime import date

import polars as pl
import pytest

from cn_market_lake.adapters.sina.adj_factors import (
    _parse_sina_factor_payload,
    fetch_adj_factor_series,
    to_sina_symbol,
)
from cn_market_lake.config import load_config
from cn_market_lake.config.bootstrap import path_for_toml
from cn_market_lake.derive.adj_factors import (
    _align_factors_to_bars,
    _cache_path,
    compute_adj_factors,
)


def test_to_sina_symbol():
    assert to_sina_symbol("600519.SH") == "sh600519"
    assert to_sina_symbol("000001.SZ") == "sz000001"


def test_parse_sina_qfq_payload():
    payload = {"data": [{"date": "2024-06-28", "qfq_factor": "2.0"}]}
    text = f"var foo = {json.dumps(payload)};"
    rows = _parse_sina_factor_payload(text)
    assert rows[0]["date"] == "2024-06-28"


def test_fetch_adj_factor_series_qfq():
    payload = {
        "data": [
            {"date": "2024-06-27", "qfq_factor": "2.0"},
            {"date": "2024-06-28", "qfq_factor": "2.0"},
        ]
    }
    body = f"var foo = {json.dumps(payload)};"

    class FakeResponse:
        text = body

        def raise_for_status(self):
            return None

    class FakeClient:
        def get(self, url):
            assert "qfq.js" in url
            return FakeResponse()

        def close(self):
            return None

    df = fetch_adj_factor_series("600519.SH", "qfq", client=FakeClient())
    assert df["factor"].to_list() == [0.5, 0.5]


def test_align_factors_to_bars_forward_fill():
    bars = pl.DataFrame(
        {
            "symbol": ["600519.SH"] * 3,
            "trade_date": [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)],
        }
    )
    factors = pl.DataFrame(
        {
            "trade_date": [date(2024, 6, 27)],
            "factor": [0.5],
        }
    )
    aligned = _align_factors_to_bars(
        bars.filter(pl.col("symbol") == "600519.SH").select("trade_date"),
        "600519.SH",
        factors,
        "qfq",
    )
    assert aligned["factor"].to_list() == [1.0, 0.5, 0.5]


def test_align_factors_to_bars_asof_carries_pre_history_level():
    # Sina emits a sparse step function; the last event predates the first bar and does not
    # land on a bar date. The factor on the first bar must carry that level forward, not
    # reset to 1.0 (which previously turned the next in-window event into a huge fake jump).
    bars = pl.DataFrame({"trade_date": [date(2016, 1, 4), date(2016, 7, 25), date(2016, 7, 26)]})
    factors = pl.DataFrame(
        {
            "trade_date": [date(1990, 12, 19), date(2015, 8, 18), date(2016, 7, 25)],
            "factor": [1.0, 5478.66, 5526.12],
        }
    )
    aligned = _align_factors_to_bars(bars, "600651.SH", factors, "hfq").sort("trade_date")
    assert aligned["factor"].to_list() == [5478.66, 5526.12, 5526.12]


def test_align_factors_to_bars_leading_bar_before_any_event_defaults_one():
    bars = pl.DataFrame({"trade_date": [date(2024, 6, 26), date(2024, 6, 27)]})
    factors = pl.DataFrame({"trade_date": [date(2024, 6, 27)], "factor": [0.5]})
    aligned = _align_factors_to_bars(bars, "600519.SH", factors, "hfq").sort("trade_date")
    assert aligned["factor"].to_list() == [1.0, 0.5]


def test_factor_continuity_findings_flags_break():
    from cn_market_lake.derive.adj_factors import _factor_continuity_findings

    out = pl.DataFrame(
        {
            "symbol": ["600651.SH"] * 3,
            "adjust_type": ["hfq"] * 3,
            "trade_date": [date(2016, 7, 22), date(2016, 7, 25), date(2016, 7, 26)],
            "factor": [1.0, 5526.12, 5526.12],
        }
    )
    findings = _factor_continuity_findings(out)
    assert len(findings) == 1
    assert findings[0]["check"] == "adj_factor_continuity"
    assert findings[0]["severity"] == "error"
    assert findings[0]["symbol"] == "600651.SH"
    assert findings[0]["trade_date"] == "2016-07-25"


def test_factor_continuity_findings_allows_normal_steps():
    from cn_market_lake.derive.adj_factors import _factor_continuity_findings

    # A real 10-for-1 split (10x) and small dividend steps are within bounds.
    out = pl.DataFrame(
        {
            "symbol": ["600651.SH"] * 3,
            "adjust_type": ["hfq"] * 3,
            "trade_date": [date(1991, 8, 26), date(1992, 12, 10), date(1993, 3, 22)],
            "factor": [5.0, 50.0, 93.47],
        }
    )
    assert _factor_continuity_findings(out) == []


@pytest.fixture
def adj_config(tmp_path, monkeypatch):
    cfg_path = tmp_path / "test.toml"
    data_root = tmp_path / "data"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(data_root)}"

[orchestrator]
workers = 1

[sources.sina]
enabled = true
min_interval_seconds = 0

[adj_factors]
source = "sina"
adjust_types = ["hfq"]

[[job.daily.waves]]
name = "finalize"
parallel = false
steps = ["derive_adj_factors"]
"""
    )
    cfg = load_config(cfg_path)
    bars_dir = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    bars_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [100],
            "amount": [100.0],
        }
    ).write_parquet(bars_dir / "part-0.parquet")

    def fake_fetch(symbol, adjust_type, client=None):
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.5]})

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )
    return cfg


def test_compute_adj_factors_writes_derived(adj_config):
    result = compute_adj_factors(adj_config)
    assert result.rows == 1
    assert result.failed == []
    out = adj_config.derived_root / "adj_factors" / "trade_date=2024-06-28" / "part-0.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df["factor"][0] == 0.5
    assert df["adjust_type"][0] == "hfq"
    assert df["source"][0] == "sina"


def _write_bar(cfg, symbol: str, trade_date: date) -> None:
    bars_dir = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    bars_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [symbol],
            "trade_date": [trade_date],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [100],
            "amount": [100.0],
        }
    ).write_parquet(bars_dir / f"{symbol.replace('.', '_')}.parquet")


def _write_factor_cache(cfg, symbol: str, trade_date: date, factor: float = 0.5) -> None:
    path = _cache_path(cfg, symbol, "hfq")
    pl.DataFrame({"trade_date": [trade_date], "factor": [factor]}).write_parquet(path)


def _write_adj_partition(cfg, symbol: str, trade_date: date, factor: float = 0.5) -> None:
    part = cfg.derived_root / "adj_factors" / f"trade_date={trade_date.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [symbol],
            "trade_date": [trade_date],
            "adjust_type": ["hfq"],
            "factor": [factor],
        }
    ).write_parquet(part / f"{symbol.replace('.', '_')}.parquet")


def test_compute_adj_factors_skips_cdr(adj_config, monkeypatch):
    _write_bar(adj_config, "689009.SH", date(2024, 6, 28))
    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.5]})

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)
    assert calls == ["600519.SH"]
    assert result.failed == []
    assert result.findings == []
    out = adj_config.derived_root / "adj_factors" / "trade_date=2024-06-28" / "part-0.parquet"
    df = pl.read_parquet(out)
    assert set(df["symbol"].to_list()) == {"600519.SH"}


def test_compute_adj_factors_reuses_cache_on_non_event_day(adj_config, monkeypatch):
    _write_factor_cache(adj_config, "600519.SH", date(2024, 6, 28))
    _write_bar(adj_config, "600519.SH", date(2024, 6, 29))
    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        return pl.DataFrame({"trade_date": [date(2024, 6, 29)], "factor": [0.8]})

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)
    assert calls == []
    assert result.rows == 2
    out = adj_config.derived_root / "adj_factors" / "trade_date=2024-06-29" / "part-0.parquet"
    df = pl.read_parquet(out)
    assert df["factor"][0] == 0.5


def test_compute_adj_factors_refreshes_corporate_action_symbol(adj_config, monkeypatch):
    _write_factor_cache(adj_config, "600519.SH", date(2024, 6, 28))
    _write_bar(adj_config, "600519.SH", date(2024, 6, 29))
    ca_dir = adj_config.curated_root / "corporate_actions" / "ex_date=2024-06-29"
    ca_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "ex_date": [date(2024, 6, 29)],
            "action_type": ["dividend"],
        }
    ).write_parquet(ca_dir / "part-0.parquet")
    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        return pl.DataFrame({"trade_date": [date(2024, 6, 29)], "factor": [0.8]})

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    compute_adj_factors(adj_config)
    assert calls == ["600519.SH"]


def test_compute_adj_factors_append_only_skips_existing_partitions(adj_config, monkeypatch):
    """With a derived watermark, only new trade_dates are written (ADR-0004)."""
    _write_factor_cache(adj_config, "600519.SH", date(2024, 6, 28), factor=0.5)
    # Seed derived watermark at 2024-06-28.
    seed = compute_adj_factors(adj_config)
    assert seed.rows == 1
    old_path = adj_config.derived_root / "adj_factors" / "trade_date=2024-06-28" / "part-0.parquet"
    old_bytes = old_path.read_bytes()

    _write_bar(adj_config, "600519.SH", date(2024, 6, 29))
    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        return pl.DataFrame({"trade_date": [date(2024, 6, 29)], "factor": [0.8]})

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)
    assert calls == []  # cache reused; no event
    assert result.rows == 1  # only the new date
    assert old_path.read_bytes() == old_bytes  # prior partition untouched
    new_path = adj_config.derived_root / "adj_factors" / "trade_date=2024-06-29" / "part-0.parquet"
    assert pl.read_parquet(new_path)["factor"][0] == 0.5


def test_compute_adj_factors_event_refresh_merges_into_existing(adj_config, monkeypatch):
    """Ex-date refresh rewrites the affected symbol via partition merge, not full replace."""
    _write_factor_cache(adj_config, "600519.SH", date(2024, 6, 28), factor=0.5)
    _write_bar(adj_config, "000001.SZ", date(2024, 6, 28))
    _write_factor_cache(adj_config, "000001.SZ", date(2024, 6, 28), factor=1.0)
    compute_adj_factors(adj_config)

    _write_bar(adj_config, "600519.SH", date(2024, 6, 29))
    _write_bar(adj_config, "000001.SZ", date(2024, 6, 29))
    ca_dir = adj_config.curated_root / "corporate_actions" / "ex_date=2024-06-29"
    ca_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "ex_date": [date(2024, 6, 29)],
            "action_type": ["dividend"],
        }
    ).write_parquet(ca_dir / "part-0.parquet")

    def fake_fetch(symbol, adjust_type, client=None):
        assert symbol == "600519.SH"
        return pl.DataFrame(
            {
                "trade_date": [date(2024, 6, 28), date(2024, 6, 29)],
                "factor": [0.5, 0.8],
            }
        )

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)
    # New date for both symbols + refreshed history for 600519 on 06-28.
    assert result.rows >= 2
    d28 = pl.read_parquet(
        adj_config.derived_root / "adj_factors" / "trade_date=2024-06-28" / "part-0.parquet"
    )
    # Untouched peer symbol retained via merge.
    assert set(d28["symbol"].to_list()) == {"600519.SH", "000001.SZ"}
    d29 = pl.read_parquet(
        adj_config.derived_root / "adj_factors" / "trade_date=2024-06-29" / "part-0.parquet"
    )
    mouti = d29.filter(pl.col("symbol") == "600519.SH")["factor"][0]
    assert mouti == 0.8


def test_compute_adj_factors_refreshes_new_listing(adj_config, monkeypatch):
    _write_factor_cache(adj_config, "600519.SH", date(2024, 6, 28))
    _write_factor_cache(adj_config, "000001.SZ", date(2024, 6, 28))
    _write_bar(adj_config, "000001.SZ", date(2024, 6, 29))
    inst_dir = adj_config.curated_root / "instruments"
    inst_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "list_date": [date(2024, 6, 29)],
        }
    ).write_parquet(inst_dir / "part-merged.parquet")
    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        return pl.DataFrame({"trade_date": [date(2024, 6, 29)], "factor": [1.0]})

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    compute_adj_factors(adj_config)
    assert calls == ["000001.SZ"]


def test_resolve_factors_raises_without_cache(adj_config, monkeypatch):
    from cn_market_lake.derive.adj_factors import AdjFactorsFetchError, _resolve_factors

    def boom(*_a, **_kw):
        raise RuntimeError("sina down")

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        boom,
    )
    sym_bars = pl.DataFrame({"trade_date": [date(2024, 6, 28)]})
    with pytest.raises(AdjFactorsFetchError, match="No cached adj factors"):
        _resolve_factors(
            adj_config,
            "600519.SH",
            "hfq",
            sym_bars,
            force=True,
            client=object(),
        )


def test_compute_adj_factors_fails_over_threshold(adj_config, monkeypatch):
    from cn_market_lake.derive.adj_factors import FAIL_RATIO_THRESHOLD, AdjFactorsDeriveError
    from cn_market_lake.steps.finalize import step_derive_adj_factors

    def boom(*_a, **_kw):
        raise RuntimeError("sina down")

    monkeypatch.setattr(
        "cn_market_lake.derive.adj_factors.fetch_adj_factor_series",
        boom,
    )
    result = compute_adj_factors(adj_config)
    assert len(result.failed) == 1
    assert result.fail_ratio > FAIL_RATIO_THRESHOLD
    assert result.findings[0]["check"] == "adj_factor_fetch_failed"

    with pytest.raises(AdjFactorsDeriveError, match="adj_factors"):
        step_derive_adj_factors(adj_config, date(2024, 6, 28), "run-adj", {})


# --- self-healing history ----------------------------------------------------
# The derive is append-only from its watermark, so `cml backfill daily_bars`
# lands history *behind* the watermark and never gets a factor. On a real lake
# that left 260 stocks with none at all and ~220k unadjusted rows, which read as
# "Sina does not cover 北交所" until a targeted re-derive filled them from 2016.


def test_uncovered_symbols_finds_history_behind_the_watermark(adj_config):
    from cn_market_lake.derive.adj_factors import _uncovered_symbols

    # Bars from 2016; factors only from 2024 — the backfilled years are naked.
    _write_bar(adj_config, "600519.SH", date(2016, 1, 4))
    _write_bar(adj_config, "600519.SH", date(2024, 6, 28))
    _write_adj_partition(adj_config, "600519.SH", date(2024, 6, 28))

    assert _uncovered_symbols(adj_config) == {"600519.SH"}


def test_a_symbol_covered_from_its_first_bar_is_not_reprocessed(adj_config):
    from cn_market_lake.derive.adj_factors import _uncovered_symbols

    _write_bar(adj_config, "600519.SH", date(2024, 6, 28))
    _write_adj_partition(adj_config, "600519.SH", date(2024, 6, 28))
    assert _uncovered_symbols(adj_config) == set()


def test_todays_bar_alone_does_not_mark_a_symbol_uncovered(adj_config):
    """The trap this check walked into first.

    `fac_last < bar_last` holds on every ordinary run — today's bar lands before
    its factor is derived — so including it would force a full-history realign
    of the whole market, daily. New sessions are what the incremental path is
    for; only the backward direction belongs here.
    """
    from cn_market_lake.derive.adj_factors import _uncovered_symbols

    _write_bar(adj_config, "600519.SH", date(2024, 6, 28))
    _write_bar(adj_config, "600519.SH", date(2024, 6, 29))
    _write_adj_partition(adj_config, "600519.SH", date(2024, 6, 28))
    assert _uncovered_symbols(adj_config) == set()
