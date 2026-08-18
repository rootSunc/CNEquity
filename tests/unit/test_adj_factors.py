import json
from datetime import date

import polars as pl
import pytest

from cnequity.adapters.sina.adj_factors import (
    _parse_sina_factor_payload,
    fetch_adj_factor_series,
    to_sina_symbol,
)
from cnequity.config import Config, load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.derive.adj_factors import (
    _align_factors_to_bars,
    _cache_path,
    _load_daily_bar_dates,
    _write_adj_partitions,
    compute_adj_factors,
)


def test_to_sina_symbol():
    assert to_sina_symbol("600519.SH") == "sh600519"
    assert to_sina_symbol("000001.SZ") == "sz000001"


def test_parse_sina_qfq_payload():
    payload = {
        "data": [None, {"date": "2024-06-28", "qfq_factor": "2.0"}],
    }
    text = f"var foo = {json.dumps(payload)};"
    rows = _parse_sina_factor_payload(text)
    assert len(rows) == 1
    assert rows[0]["date"] == "2024-06-28"


def test_parse_sina_factor_payload_rejects_non_list_data():
    text = 'var foo = {"data": {"date": "2024-06-28"}};'
    with pytest.raises(ValueError, match="data is not a list"):
        _parse_sina_factor_payload(text)


def test_parse_sina_factor_payload_rejects_all_malformed_rows():
    text = 'var foo = {"data": [null, "bad"]};'
    with pytest.raises(ValueError, match="no valid rows"):
        _parse_sina_factor_payload(text)


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


def test_fetch_adj_factor_series_skips_invalid_dates_and_dedupes():
    payload = {
        "data": [
            {"date": "2024-06-27", "qfq_factor": "2.0"},
            {"date": "not-a-date", "qfq_factor": "3.0"},
            {"date": "2024-06-27", "qfq_factor": "4.0"},
        ]
    }
    body = f"var foo = {json.dumps(payload)};"

    class FakeResponse:
        text = body

        def raise_for_status(self):
            return None

    class FakeClient:
        def get(self, url):
            return FakeResponse()

        def close(self):
            return None

    df = fetch_adj_factor_series("600519.SH", "qfq", client=FakeClient())
    assert df.height == 1
    assert df["factor"].to_list() == [0.25]


def test_fetch_adj_factor_series_rejects_all_invalid_dates():
    payload = {"data": [{"date": "not-a-date", "qfq_factor": "2.0"}]}
    body = f"var foo = {json.dumps(payload)};"

    class FakeResponse:
        text = body

        def raise_for_status(self):
            return None

    class FakeClient:
        def get(self, url):
            return FakeResponse()

        def close(self):
            return None

    with pytest.raises(ValueError, match="no valid trade dates"):
        fetch_adj_factor_series("600519.SH", "qfq", client=FakeClient())


@pytest.mark.parametrize("raw_factor", ["0", "-1", "nan"])
def test_fetch_adj_factor_series_rejects_invalid_factor(raw_factor):
    payload = {"data": [{"date": "2024-06-28", "qfq_factor": raw_factor}]}
    body = f"var foo = {json.dumps(payload)};"

    class FakeResponse:
        text = body

        def raise_for_status(self):
            return None

    class FakeClient:
        def get(self, url):
            return FakeResponse()

        def close(self):
            return None

    with pytest.raises(ValueError, match="non-positive or non-finite"):
        fetch_adj_factor_series("600519.SH", "qfq", client=FakeClient())


def test_fetch_adj_factor_series_etf_hfq_uses_hfq_s_directly():
    payload = {
        "data": [
            {"d": "2026-07-06", "f": "1", "s": "3.0"},
            {"d": "1900-01-01", "f": "1", "s": "1.0"},
        ]
    }
    body = f"var foo = {json.dumps(payload)};"
    requested: list[str] = []

    class FakeResponse:
        text = body

        def raise_for_status(self):
            return None

    class FakeClient:
        def get(self, url):
            requested.append(url)
            return FakeResponse()

        def close(self):
            return None

    df = fetch_adj_factor_series("588170.SH", "hfq", client=FakeClient())

    assert len(requested) == 1
    assert "hfq.js" in requested[0]
    assert df.sort("trade_date")["factor"].to_list() == [1.0, 3.0]


def test_fetch_adj_factor_series_etf_qfq_converts_s_divisor():
    payload = {
        "data": [
            {"d": "2026-07-06", "f": "1", "s": "1.0"},
            {"d": "1900-01-01", "f": "1", "s": "3.0"},
        ]
    }
    body = f"var foo = {json.dumps(payload)};"
    requested: list[str] = []

    class FakeResponse:
        text = body

        def raise_for_status(self):
            return None

    class FakeClient:
        def get(self, url):
            requested.append(url)
            return FakeResponse()

        def close(self):
            return None

    df = fetch_adj_factor_series("588170.SH", "qfq", client=FakeClient())

    assert len(requested) == 1
    assert "qfq.js" in requested[0]
    assert df.sort("trade_date")["factor"].to_list() == [1 / 3, 1.0]


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


def test_adj_factor_dates_skip_placeholder_only_symbols(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root_27 = cfg.curated_root / "daily_bars" / "trade_date=2024-06-27"
    root_27.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "volume": [100],
        }
    ).write_parquet(root_27 / "part-0.parquet")
    root_28 = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    root_28.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "volume": [0, 0],
        }
    ).write_parquet(root_28 / "part-0.parquet")

    bars = _load_daily_bar_dates(cfg)

    assert bars["symbol"].unique().to_list() == ["600519.SH"]
    assert bars.height == 2


def test_factor_continuity_findings_flags_break():
    from cnequity.derive.adj_factors import _factor_continuity_findings

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
    from cnequity.derive.adj_factors import _factor_continuity_findings

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
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
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


def test_write_adj_partitions_merges_all_shards_and_cleans_stale_siblings(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    part = cfg.derived_root / "adj_factors" / "trade_date=2024-06-28"
    part.mkdir(parents=True)
    fragments = part / "fragments"
    fragments.mkdir()
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "adjust_type": ["hfq"],
            "factor": [0.4],
        }
    ).write_parquet(part / "part-000.parquet")
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "trade_date": [date(2024, 6, 28)],
            "adjust_type": ["hfq"],
            "factor": [0.9],
        }
    ).write_parquet(fragments / "part-001.parquet")

    _write_adj_partitions(
        cfg,
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 28)],
                "adjust_type": ["hfq"],
                "factor": [0.8],
            }
        ),
        replace=False,
    )

    files = sorted(part.rglob("*.parquet"))
    assert [path.name for path in files] == ["part-0.parquet"]
    written = pl.read_parquet(files[0])
    assert written.height == 2
    assert dict(zip(written["symbol"], written["factor"], strict=True)) == {
        "600519.SH": 0.8,
        "000001.SZ": 0.9,
    }


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
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
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
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)
    assert calls == []
    assert result.rows == 2
    out = adj_config.derived_root / "adj_factors" / "trade_date=2024-06-29" / "part-0.parquet"
    df = pl.read_parquet(out)
    assert df["factor"][0] == 0.5


def test_compute_adj_factors_event_refresh_uses_latest_traded_day(adj_config, monkeypatch):
    placeholder_dir = adj_config.curated_root / "daily_bars" / "trade_date=2024-06-29"
    placeholder_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 29)],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [0],
            "amount": [0.0],
        }
    ).write_parquet(placeholder_dir / "part-0.parquet")
    seen: list[date] = []
    monkeypatch.setattr(
        "cnequity.derive.adj_factors._event_refresh_symbols",
        lambda config, trade_date: seen.append(trade_date) or set(),
    )

    compute_adj_factors(adj_config)

    assert seen == [date(2024, 6, 28)]


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
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
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
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
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
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
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
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    compute_adj_factors(adj_config)
    assert calls == ["000001.SZ"]


def test_resolve_factors_raises_without_cache(adj_config, monkeypatch):
    from cnequity.derive.adj_factors import AdjFactorsFetchError, _resolve_factors

    def boom(*_a, **_kw):
        raise RuntimeError("sina down")

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
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
    from cnequity.derive.adj_factors import FAIL_RATIO_THRESHOLD, AdjFactorsDeriveError
    from cnequity.steps.finalize import step_derive_adj_factors

    def boom(*_a, **_kw):
        raise RuntimeError("sina down")

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        boom,
    )
    result = compute_adj_factors(adj_config)
    assert len(result.failed) == 1
    assert result.fail_ratio > FAIL_RATIO_THRESHOLD
    assert result.findings[0]["check"] == "adj_factor_fetch_failed"

    with pytest.raises(AdjFactorsDeriveError, match="adj_factors"):
        step_derive_adj_factors(adj_config, date(2024, 6, 28), "run-adj", {})


def test_compute_adj_factors_bj_failure_is_best_effort(adj_config, monkeypatch):
    """A missing BJ factor must not fail daily:core, but remains retryable."""
    from cnequity.storage.state import StateStore
    from cnequity.steps.finalize import step_derive_adj_factors

    _write_bar(adj_config, "830799.BJ", date(2024, 6, 28))

    def fake_fetch(symbol, adjust_type, client=None):
        if symbol == "830799.BJ":
            raise RuntimeError("empty data")
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.5]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)

    assert result.failed == []
    assert "830799.BJ:hfq" in result.best_effort_failed
    assert result.fail_ratio == 0
    assert result.findings[0]["check"] == "adj_factor_fetch_failed_best_effort"
    assert result.findings[0]["severity"] == "info"
    assert StateStore(adj_config.meta_root).get_string_set("adj_factors", "retry_symbols") == {
        "830799.BJ"
    }

    out = step_derive_adj_factors(adj_config, date(2024, 6, 28), "run-adj", {})
    assert "failed_tasks" not in out


def test_failed_symbol_is_retried_after_global_watermark_advances(adj_config, monkeypatch):
    """A per-symbol failure must not disappear behind another symbol's partition."""
    from cnequity.storage.state import StateStore

    _write_bar(adj_config, "000001.SZ", date(2024, 6, 28))

    def flaky_fetch(symbol, adjust_type, client=None):
        if symbol == "600519.SH":
            raise RuntimeError("sina temporarily unavailable")
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.8]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        flaky_fetch,
    )
    first = compute_adj_factors(adj_config)
    assert first.failed == ["600519.SH:hfq"]
    assert StateStore(adj_config.meta_root).get_string_set("adj_factors", "retry_symbols") == {
        "600519.SH"
    }

    def recovered_fetch(symbol, adjust_type, client=None):
        assert symbol == "600519.SH"
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.7]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        recovered_fetch,
    )
    second = compute_adj_factors(adj_config)
    assert second.failed == []
    assert second.rows == 1
    assert StateStore(adj_config.meta_root).get_string_set("adj_factors", "retry_symbols") == set()
    written = pl.read_parquet(
        adj_config.derived_root / "adj_factors" / "trade_date=2024-06-28" / "part-0.parquet"
    )
    assert written.filter(pl.col("symbol") == "600519.SH")["factor"].to_list() == [0.7]


# --- self-healing history ----------------------------------------------------
# The derive is append-only from its watermark, so `cne backfill daily_bars`
# lands history *behind* the watermark and never gets a factor. On a real lake
# that left 260 stocks with none at all and ~220k unadjusted rows, which read as
# "Sina does not cover 北交所" until a targeted re-derive filled them from 2016.


def test_uncovered_symbols_finds_history_behind_the_watermark(adj_config):
    from cnequity.derive.adj_factors import _uncovered_symbols

    # Bars from 2016; factors only from 2024 — the backfilled years are naked.
    _write_bar(adj_config, "600519.SH", date(2016, 1, 4))
    _write_bar(adj_config, "600519.SH", date(2024, 6, 28))
    _write_adj_partition(adj_config, "600519.SH", date(2024, 6, 28))

    assert _uncovered_symbols(adj_config) == {"600519.SH"}


def test_uncovered_symbols_keeps_bj_for_best_effort(adj_config):
    """BJ stays in the self-heal set so partial Sina coverage is still tried."""
    from cnequity.derive.adj_factors import _uncovered_symbols

    _write_bar(adj_config, "830799.BJ", date(2024, 6, 28))

    assert "830799.BJ" in _uncovered_symbols(adj_config)


def test_a_symbol_covered_from_its_first_bar_is_not_reprocessed(adj_config):
    from cnequity.derive.adj_factors import _uncovered_symbols

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
    from cnequity.derive.adj_factors import _uncovered_symbols

    _write_bar(adj_config, "600519.SH", date(2024, 6, 28))
    _write_bar(adj_config, "600519.SH", date(2024, 6, 29))
    _write_adj_partition(adj_config, "600519.SH", date(2024, 6, 28))
    assert _uncovered_symbols(adj_config) == set()
