import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.adapters.sina.adj_factors import (
    SinaAdjFactorUnavailableError,
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


def test_parse_sina_factor_payload_classifies_empty_series_as_unavailable():
    text = 'var foo = {"data": []};'
    with pytest.raises(SinaAdjFactorUnavailableError, match="empty data"):
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


def test_compute_adj_factors_includes_etf_bars(adj_config, monkeypatch):
    _write_bar(adj_config, "510300.SH", date(2024, 6, 28))
    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [2.0]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)

    assert result.failed == []
    assert "510300.SH" in calls
    out = pl.read_parquet(
        adj_config.derived_root / "adj_factors" / "trade_date=2024-06-28" / "part-0.parquet"
    )
    assert out.filter(pl.col("symbol") == "510300.SH")["factor"].to_list() == [2.0]


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


def test_compute_adj_factors_fetches_uncached_beijing_symbols(adj_config, monkeypatch):
    """Sina serves BJ factors; an absent local cache is not source coverage."""
    _write_bar(adj_config, "920001.BJ", date(2024, 6, 28))
    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.5]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)

    assert result.failed == []
    assert set(calls) == {"600519.SH", "920001.BJ"}
    assert result.findings == []
    out = pl.read_parquet(
        adj_config.derived_root / "adj_factors" / "trade_date=2024-06-28" / "part-0.parquet"
    )
    assert set(out["symbol"].to_list()) == {"600519.SH", "920001.BJ"}


def test_compute_adj_factors_persists_delisted_source_gap_without_retrying(adj_config, monkeypatch):
    from cnequity.storage.state import StateStore

    _write_bar(adj_config, "830799.BJ", date(2024, 6, 28))
    inst_dir = adj_config.curated_root / "instruments"
    inst_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "830799.BJ"],
            "asset_type": ["stock", "stock"],
            "list_date": [date(2001, 1, 1), date(2016, 1, 1)],
            "delist_date": [None, date(2025, 4, 30)],
        }
    ).write_parquet(inst_dir / "part-0.parquet")
    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        if symbol == "830799.BJ":
            raise SinaAdjFactorUnavailableError("Sina adj factor response has empty data")
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.5]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    first = compute_adj_factors(adj_config)
    assert first.failed == []
    assert [finding["check"] for finding in first.findings] == ["adj_factor_source_unavailable"]
    assert StateStore(adj_config.meta_root).get_string_set("adj_factors", "retry_symbols") == set()
    assert StateStore(adj_config.meta_root).get_string_set(
        "adj_factors", "source_unavailable_symbols"
    ) == {"830799.BJ"}

    calls.clear()
    second = compute_adj_factors(adj_config)
    assert second.failed == []
    assert calls == []


def test_compute_adj_factors_parallel_tracks_success_by_future_symbol(adj_config, monkeypatch):
    from cnequity.storage.state import StateStore

    adj_config.workers = 2
    _write_bar(adj_config, "000001.SZ", date(2024, 6, 28))

    def fake_fetch(symbol, adjust_type, client=None):
        if symbol == "600519.SH":
            raise RuntimeError("sina temporarily unavailable")
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.8]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )
    result = compute_adj_factors(adj_config)

    assert result.failed == ["600519.SH:hfq"]
    assert StateStore(adj_config.meta_root).get_string_set("adj_factors", "retry_symbols") == {
        "600519.SH"
    }


def test_compute_adj_factors_uses_independent_source_capped_workers(adj_config, monkeypatch):
    """The HTTP derive budget is independent from the legacy TDX workers."""
    import threading
    import time

    _write_bar(adj_config, "000001.SZ", date(2024, 6, 28))
    _write_bar(adj_config, "000858.SZ", date(2024, 6, 28))
    adj_config.workers = 1
    adj_config.adj_factor_workers = 3
    adj_config.source_concurrency = {"sina": 2}
    active = 0
    peak = 0
    lock = threading.Lock()
    overlapped = threading.Event()

    def fake_fetch(symbol, adjust_type, client=None):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            # The first two tasks must hold slots at the same time.  An event
            # makes this proof deterministic while allowing the fixture's
            # third symbol to run after one of those slots is released.
            with lock:
                if active >= 2:
                    overlapped.set()
            if not overlapped.wait(timeout=1):
                raise AssertionError("source concurrency cap prevented overlap")
            time.sleep(0.02)
        finally:
            with lock:
                active -= 1
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [0.5]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)

    assert result.failed == []
    assert result.rows == 3
    assert peak == 2


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


def test_compute_adj_factors_fills_partial_watermark_partition(adj_config, monkeypatch):
    """Late bars on the watermark date are not hidden by the date watermark."""
    _write_factor_cache(adj_config, "600519.SH", date(2024, 6, 28), factor=0.5)
    _write_factor_cache(adj_config, "000001.SZ", date(2024, 6, 27), factor=1.0)
    _write_adj_partition(adj_config, "600519.SH", date(2024, 6, 28), factor=0.5)
    _write_adj_partition(adj_config, "000001.SZ", date(2024, 6, 27), factor=1.0)
    _write_bar(adj_config, "000001.SZ", date(2024, 6, 28))

    calls: list[str] = []

    def fake_fetch(symbol, adjust_type, client=None):
        calls.append(symbol)
        return pl.DataFrame({"trade_date": [date(2024, 6, 28)], "factor": [1.0]})

    monkeypatch.setattr(
        "cnequity.derive.adj_factors.fetch_adj_factor_series",
        fake_fetch,
    )

    result = compute_adj_factors(adj_config)

    assert result.rows == 1
    assert calls == []
    out = adj_config.derived_root / "adj_factors" / "trade_date=2024-06-28" / "part-0.parquet"
    df = pl.read_parquet(out)
    assert set(df["symbol"].to_list()) == {"600519.SH", "000001.SZ"}
    assert df.filter(pl.col("symbol") == "600519.SH")["factor"][0] == 0.5


# --- self-healing history ----------------------------------------------------
# The derive is append-only from its watermark, so `cne backfill daily_bars`
# lands history *behind* the watermark and never gets a factor. On a real lake
# that left 260 stocks with none at all and ~220k unadjusted rows, which read as
# an append-only derive did not revisit history until a targeted re-derive filled
# them from 2016.


def test_uncovered_symbols_finds_history_behind_the_watermark(adj_config):
    from cnequity.derive.adj_factors import _uncovered_symbols

    # Bars from 2016; factors only from 2024 — the backfilled years are naked.
    _write_bar(adj_config, "600519.SH", date(2016, 1, 4))
    _write_bar(adj_config, "600519.SH", date(2024, 6, 28))
    _write_adj_partition(adj_config, "600519.SH", date(2024, 6, 28))

    assert _uncovered_symbols(adj_config) == {"600519.SH"}


def test_uncovered_symbols_includes_etfs(adj_config):
    from cnequity.derive.adj_factors import _uncovered_symbols

    _write_bar(adj_config, "510300.SH", date(2024, 6, 28))
    inst_dir = adj_config.curated_root / "instruments"
    inst_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["510300.SH", "600519.SH"],
            "asset_type": ["etf", "stock"],
        }
    ).write_parquet(inst_dir / "part-merged.parquet")

    assert "510300.SH" in _uncovered_symbols(adj_config)


def test_uncovered_symbols_uses_newest_nonpriced_instrument_revision(adj_config):
    from cnequity.derive.adj_factors import _uncovered_symbols

    _write_bar(adj_config, "600519.SH", date(2016, 1, 4))
    _write_bar(adj_config, "600519.SH", date(2024, 6, 28))
    _write_adj_partition(adj_config, "600519.SH", date(2024, 6, 28))
    instruments = adj_config.curated_root / "instruments"
    instruments.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "asset_type": ["stock", "index"],
            "fetched_at": [
                datetime(2024, 6, 28, 7, tzinfo=timezone.utc),
                datetime(2024, 6, 28, 8, tzinfo=timezone.utc),
            ],
        }
    ).write_parquet(instruments / "part-revision.parquet")

    assert _uncovered_symbols(adj_config) == set()


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


def test_uncovered_symbols_detects_a_middle_factor_gap(adj_config):
    from cnequity.derive.adj_factors import _uncovered_symbols

    days = [date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28)]
    for day in days:
        _write_bar(adj_config, "600519.SH", day)
    _write_adj_partition(adj_config, "600519.SH", days[0])
    _write_adj_partition(adj_config, "600519.SH", days[2])

    assert _uncovered_symbols(adj_config) == {"600519.SH"}


@pytest.mark.parametrize("terminal_lag", [False, True])
def test_extra_suspension_factor_cannot_hide_missing_traded_date(adj_config, terminal_lag):
    from cnequity.derive.adj_factors import _uncovered_symbols

    # A suspended date has a carried factor; a later traded date lost its row.
    # Counts and endpoints match, but the actual keys are different.
    for day in [24, 26, 27, 28] + ([29] if terminal_lag else []):
        _write_bar(adj_config, "600519.SH", date(2024, 6, day))
    for day in [24, 25, 26, 28]:
        _write_adj_partition(adj_config, "600519.SH", date(2024, 6, day))

    assert _uncovered_symbols(adj_config) == {"600519.SH"}


# --- Recomputed-factor cross-check ------------------------------------------


def _write_bars(cfg, symbol, days, close):
    for day in days:
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": [symbol],
                "trade_date": [day],
                "open": [close],
                "high": [close],
                "low": [close],
                "close": [close],
                "volume": [100],
                "amount": [close * 100],
            }
        ).write_parquet(part / f"{symbol}.parquet")


def _write_action(cfg, symbol, ex_date, **terms):
    part = cfg.curated_root / "corporate_actions" / f"ex_date={ex_date.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    row = {
        "symbol": [symbol],
        "ex_date": [ex_date],
        "action_type": [terms.pop("action_type", "dividend")],
        "cash_dividend": [terms.pop("cash_dividend", 0.0)],
        "bonus_ratio": [terms.pop("bonus_ratio", 0.0)],
        "transfer_ratio": [terms.pop("transfer_ratio", 0.0)],
        "allotment_ratio": [terms.pop("allotment_ratio", 0.0)],
        "allotment_price": [terms.pop("allotment_price", 0.0)],
    }
    assert not terms, terms
    pl.DataFrame(row).write_parquet(part / f"{symbol}-{row['action_type'][0]}.parquet")


def _factor_frame(symbol, days, factors):
    return pl.DataFrame(
        {
            "symbol": [symbol] * len(days),
            "trade_date": list(days),
            "adjust_type": ["hfq"] * len(days),
            "factor": list(factors),
        }
    )


@pytest.fixture
def crosscheck_config(tmp_path):
    cfg_path = tmp_path / "crosscheck.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[adj_factors]
source = "sina"
adjust_types = ["hfq"]
"""
    )
    return load_config(cfg_path)


_DAYS = (date(2024, 6, 26), date(2024, 6, 27), date(2024, 6, 28))


def test_crosscheck_accepts_a_step_that_matches_the_dividend(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    _write_bars(crosscheck_config, "600519.SH", _DAYS, 10.0)
    _write_action(crosscheck_config, "600519.SH", _DAYS[1], cash_dividend=0.5)
    exact = 10.0 / (10.0 - 0.5)
    out = _factor_frame("600519.SH", _DAYS, [1.0, exact, exact])

    assert _corporate_action_crosscheck_findings(crosscheck_config, out) == []


def test_crosscheck_flags_a_dividend_the_vendor_series_missed(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    _write_bars(crosscheck_config, "600519.SH", _DAYS, 10.0)
    _write_action(crosscheck_config, "600519.SH", _DAYS[1], cash_dividend=0.5)
    out = _factor_frame("600519.SH", _DAYS, [1.0, 1.0, 1.0])

    findings = _corporate_action_crosscheck_findings(crosscheck_config, out)
    assert len(findings) == 1
    assert findings[0]["check"] == "adj_factor_corporate_action_divergence"
    assert findings[0]["trade_date"] == "2024-06-27"
    assert findings[0]["expected_ratio"] == pytest.approx(10.0 / 9.5)
    assert findings[0]["actual_ratio"] == pytest.approx(1.0)
    assert findings[0]["divergence_bps"] == pytest.approx(500.0, abs=1.0)
    # 500 bps clears the 200 bps error threshold.
    assert findings[0]["severity"] == "error"


def test_crosscheck_flags_a_step_on_a_day_with_no_action(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    _write_bars(crosscheck_config, "600519.SH", _DAYS, 10.0)
    # An action feed that covers this lake, just not this symbol's step: the
    # absence of an ex-date on 06-28 is then a fact, not a missing dataset.
    _write_action(crosscheck_config, "000001.SZ", _DAYS[1], cash_dividend=0.1)
    out = _factor_frame("600519.SH", _DAYS, [1.0, 1.0, 1.2])

    findings = _corporate_action_crosscheck_findings(crosscheck_config, out)
    assert len(findings) == 1
    assert findings[0]["trade_date"] == "2024-06-28"
    assert findings[0]["expected_ratio"] == pytest.approx(1.0)
    assert findings[0]["severity"] == "error"
    assert "no ex-date" in findings[0]["message"]


def test_crosscheck_will_not_call_a_step_an_error_without_an_action_feed(crosscheck_config):
    """A lake with no corporate_actions cannot arbitrate a factor step.

    `cne init --profile demo --research` builds exactly that lake — bars and
    Sina factors, no actions — and every real dividend in the window looked
    like a factor break, which aborted the demo it was meant to illustrate.
    """
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    _write_bars(crosscheck_config, "600519.SH", _DAYS, 10.0)
    out = _factor_frame("600519.SH", _DAYS, [1.0, 1.0, 1.2])

    findings = _corporate_action_crosscheck_findings(crosscheck_config, out)
    assert len(findings) == 1
    assert findings[0]["trade_date"] == "2024-06-28"
    assert findings[0]["severity"] == "warning"
    assert "no corporate_actions rows" in findings[0]["message"]


def test_crosscheck_handles_bonus_transfer_and_allotment_together(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    # One ex-date carrying two action rows: the ratios add, and the allotment
    # returns ratio*price of cash to the holder.
    _write_bars(crosscheck_config, "000001.SZ", _DAYS, 20.0)
    _write_action(
        crosscheck_config,
        "000001.SZ",
        _DAYS[1],
        action_type="dividend",
        cash_dividend=0.4,
        bonus_ratio=0.3,
        transfer_ratio=0.2,
    )
    _write_action(
        crosscheck_config,
        "000001.SZ",
        _DAYS[1],
        action_type="allotment",
        allotment_ratio=0.5,
        allotment_price=6.0,
    )
    expected = (1 + 0.3 + 0.2 + 0.5) * 20.0 / (20.0 - 0.4 + 0.5 * 6.0)
    out = _factor_frame("000001.SZ", _DAYS, [1.0, expected, expected])

    assert _corporate_action_crosscheck_findings(crosscheck_config, out) == []


def test_crosscheck_reads_back_the_prior_partition_for_an_append_run(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    # The daily case: one new date, whose predecessor is only in the already
    # written derived partitions. Without the read-back there is no step to
    # measure and a missed ex-date passes silently.
    _write_bars(crosscheck_config, "600519.SH", _DAYS[:2], 10.0)
    _write_action(crosscheck_config, "600519.SH", _DAYS[1], cash_dividend=0.5)
    prior = crosscheck_config.derived_root / "adj_factors" / f"trade_date={_DAYS[0].isoformat()}"
    prior.mkdir(parents=True)
    _factor_frame("600519.SH", _DAYS[:1], [1.0]).write_parquet(prior / "part-0.parquet")

    out = _factor_frame("600519.SH", _DAYS[1:2], [1.0])
    findings = _corporate_action_crosscheck_findings(crosscheck_config, out)
    assert len(findings) == 1
    assert findings[0]["trade_date"] == "2024-06-27"


def test_crosscheck_flags_an_action_row_implying_a_nonpositive_price(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    _write_bars(crosscheck_config, "000001.SZ", _DAYS, 10.0)
    _write_action(crosscheck_config, "000001.SZ", _DAYS[1], cash_dividend=99.0)
    out = _factor_frame("000001.SZ", _DAYS, [1.0, 1.0, 1.0])

    findings = _corporate_action_crosscheck_findings(crosscheck_config, out)
    assert [f["check"] for f in findings] == ["adj_factor_action_implies_nonpositive_price"]
    assert findings[0]["severity"] == "error"


def test_crosscheck_stays_within_tolerance_for_vendor_rounding(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    _write_bars(crosscheck_config, "600519.SH", _DAYS, 10.0)
    _write_action(crosscheck_config, "600519.SH", _DAYS[1], cash_dividend=0.5)
    # 20 bps off the exact ratio: below the 50 bps materiality threshold.
    rounded = (10.0 / 9.5) * 1.002
    out = _factor_frame("600519.SH", _DAYS, [1.0, rounded, rounded])

    assert _corporate_action_crosscheck_findings(crosscheck_config, out) == []


def test_crosscheck_can_be_disabled(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    _write_bars(crosscheck_config, "600519.SH", _DAYS, 10.0)
    _write_action(crosscheck_config, "600519.SH", _DAYS[1], cash_dividend=0.5)
    out = _factor_frame("600519.SH", _DAYS, [1.0, 1.0, 1.0])
    crosscheck_config.adj_factors_crosscheck_enabled = False

    assert _corporate_action_crosscheck_findings(crosscheck_config, out) == []


def test_crosscheck_reports_one_finding_per_symbol_with_a_day_count(crosscheck_config):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    _write_bars(crosscheck_config, "600519.SH", _DAYS, 10.0)
    # Two bad steps, neither backed by an action; only the worst is reported.
    out = _factor_frame("600519.SH", _DAYS, [1.0, 1.1, 1.43])

    findings = _corporate_action_crosscheck_findings(crosscheck_config, out)
    assert len(findings) == 1
    assert findings[0]["divergent_days"] == 2
    assert findings[0]["trade_date"] == "2024-06-28"


def test_compute_adj_factors_surfaces_crosscheck_findings(adj_config, monkeypatch):
    _write_action(adj_config, "600519.SH", date(2024, 6, 28), cash_dividend=0.5)
    monkeypatch.setattr(
        "cnequity.derive.adj_factors._prior_factor_rows",
        lambda config, symbols, before: _factor_frame("600519.SH", [date(2024, 6, 27)], [1.0]),
    )
    _write_bars(adj_config, "600519.SH", [date(2024, 6, 27)], 1.0)

    result = compute_adj_factors(adj_config)
    checks = {f["check"] for f in result.findings}
    assert "adj_factor_corporate_action_divergence" in checks


def _write_actions(cfg, rows: list[dict], ex_date: date):
    ca_dir = cfg.curated_root / "corporate_actions" / f"ex_date={ex_date.isoformat()}"
    ca_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        rows,
        schema={
            "symbol": pl.Utf8,
            "ex_date": pl.Date,
            "action_type": pl.Utf8,
            "cash_dividend": pl.Float64,
            "bonus_ratio": pl.Float64,
            "transfer_ratio": pl.Float64,
            "allotment_ratio": pl.Float64,
            "allotment_price": pl.Float64,
            "source": pl.Utf8,
        },
    ).write_parquet(ca_dir / "part-0.parquet")


def test_action_terms_do_not_add_two_vendors_readings_of_one_event(adj_config):
    """EastMoney files 送 as 转; summing both columns doubled the dilution.

    Thirty stored ex-dates carry an EastMoney ``transfer`` row holding the same
    ratio TDX files as ``bonus``. They are one event seen twice, not two events,
    so a 0.4 dilution must stay 0.4 rather than becoming 0.8 and firing a
    spurious crosscheck finding.
    """
    from cnequity.derive.adj_factors import _action_terms

    ex = date(2024, 6, 28)
    _write_actions(
        adj_config,
        [
            {
                "symbol": "600519.SH",
                "ex_date": ex,
                "action_type": "transfer",
                "cash_dividend": 0.0,
                "bonus_ratio": 0.0,
                "transfer_ratio": 0.4,
                "allotment_ratio": None,
                "allotment_price": None,
                "source": "eastmoney",
            },
            {
                "symbol": "600519.SH",
                "ex_date": ex,
                "action_type": "bonus",
                "cash_dividend": 0.0,
                "bonus_ratio": 0.4,
                "transfer_ratio": 0.0,
                "allotment_ratio": None,
                "allotment_price": None,
                "source": "tdx_protocol",
            },
            {
                "symbol": "600519.SH",
                "ex_date": ex,
                "action_type": "cash_dividend",
                "cash_dividend": 0.155,
                "bonus_ratio": 0.0,
                "transfer_ratio": 0.0,
                "allotment_ratio": None,
                "allotment_price": None,
                "source": "tdx_protocol",
            },
        ],
        ex,
    )

    out = _action_terms(adj_config, ["600519.SH"], ex, ex)

    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["_bonus"] + row["_transfer"] == pytest.approx(0.4)
    assert row["_dividend"] == pytest.approx(0.155)


def test_action_terms_keep_the_cash_a_vendor_without_the_dilution_filed(adj_config):
    """The dilution picks one source; the dividend must not ride along with it.

    TDX files 送0.4 and no dividend row, EastMoney files the 0.155 cash and no
    dilution. Binding the cash to the source that wins the dilution would drop
    the dividend and fire exactly the divergence the single-source pick exists
    to silence.
    """
    from cnequity.derive.adj_factors import _action_terms

    ex = date(2024, 6, 28)
    _write_actions(
        adj_config,
        [
            {
                "symbol": "601398.SH",
                "ex_date": ex,
                "action_type": "bonus",
                "cash_dividend": 0.0,
                "bonus_ratio": 0.4,
                "transfer_ratio": 0.0,
                "allotment_ratio": None,
                "allotment_price": None,
                "source": "tdx_protocol",
            },
            {
                "symbol": "601398.SH",
                "ex_date": ex,
                "action_type": "cash_dividend",
                "cash_dividend": 0.155,
                "bonus_ratio": 0.0,
                "transfer_ratio": 0.0,
                "allotment_ratio": None,
                "allotment_price": None,
                "source": "eastmoney",
            },
        ],
        ex,
    )

    out = _action_terms(adj_config, ["601398.SH"], ex, ex)

    assert out.height == 1
    row = out.to_dicts()[0]
    assert row["_bonus"] == pytest.approx(0.4)
    assert row["_dividend"] == pytest.approx(0.155)


def test_action_terms_still_add_rows_from_one_vendor(adj_config):
    """Within a source the ratios genuinely add: one plan, several action rows."""
    from cnequity.derive.adj_factors import _action_terms

    ex = date(2024, 6, 28)
    _write_actions(
        adj_config,
        [
            {
                "symbol": "000001.SZ",
                "ex_date": ex,
                "action_type": "bonus",
                "cash_dividend": 0.0,
                "bonus_ratio": 0.3,
                "transfer_ratio": 0.0,
                "allotment_ratio": None,
                "allotment_price": None,
                "source": "eastmoney",
            },
            {
                "symbol": "000001.SZ",
                "ex_date": ex,
                "action_type": "transfer",
                "cash_dividend": 0.0,
                "bonus_ratio": 0.0,
                "transfer_ratio": 0.5,
                "allotment_ratio": None,
                "allotment_price": None,
                "source": "eastmoney",
            },
        ],
        ex,
    )

    out = _action_terms(adj_config, ["000001.SZ"], ex, ex)

    row = out.to_dicts()[0]
    assert row["_bonus"] == pytest.approx(0.3)
    assert row["_transfer"] == pytest.approx(0.5)


@pytest.mark.parametrize("ratio", [3.0, 0.1])
def test_unit_split_and_consolidation_factor_steps(crosscheck_config, ratio):
    from cnequity.derive.adj_factors import _corporate_action_crosscheck_findings

    cfg = crosscheck_config
    symbol = "159327.SZ"
    _write_bars(cfg, symbol, _DAYS[:1], 10.0)
    _write_bars(cfg, symbol, _DAYS[1:], 10.0 / ratio)
    part = cfg.curated_root / "corporate_actions" / f"ex_date={_DAYS[1]}"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [symbol],
            "ex_date": [_DAYS[1]],
            "action_type": ["unit_split"],
            "split_factor": [ratio],
            "source": ["issuer"],
        }
    ).write_parquet(part / "split.parquet")
    out = _factor_frame(symbol, _DAYS, [1.0, ratio, ratio])
    assert _corporate_action_crosscheck_findings(cfg, out) == []
    wrong = _factor_frame(symbol, _DAYS, [1.0, 1.0, 1.0])
    findings = _corporate_action_crosscheck_findings(cfg, wrong)
    assert findings and findings[0]["expected_ratio"] == pytest.approx(ratio)
    assert "split=" in findings[0]["message"]


def test_conflicting_unit_split_ratios_fail_closed():
    from cnequity.derive.adj_factors import _unit_split_terms

    frame = pl.DataFrame(
        {"symbol": ["159327.SZ"] * 2, "ex_date": [_DAYS[1]] * 2, "split_factor": [3.0, 2.0]}
    )
    with pytest.raises(ValueError, match="conflicting"):
        _unit_split_terms(frame)


def test_sina_outage_falls_back_to_baostock_and_says_so(tmp_path, monkeypatch):
    """Sina bans by account, so the one vendor behind every return in the lake
    can be taken out by an unrelated sweep. Baostock's raw÷adjusted ratio is
    the same factor, from a different failure domain."""
    import polars as pl

    from cnequity.adapters.sina.adj_factors import SinaAdjFactorUnavailableError
    from cnequity.config import Config
    from cnequity.derive import adj_factors as mod

    cfg = Config(data_root=tmp_path / "data")
    cfg.sources.update({"baostock": True})
    bars = pl.DataFrame(
        {"trade_date": [date(2026, 6, 25), date(2026, 6, 26)]},
        schema={"trade_date": pl.Date},
    )

    def _sina_down(*args, **kwargs):
        raise SinaAdjFactorUnavailableError("456")

    monkeypatch.setattr(mod, "fetch_adj_factor_series", _sina_down)
    monkeypatch.setattr(
        "cnequity.adapters.baostock.adj_factors.fetch_adj_factor_series_baostock",
        lambda symbol, start, end, **kwargs: pl.DataFrame(
            {"trade_date": [date(2026, 6, 25), date(2026, 6, 26)], "factor": [0.976883, 1.0]},
            schema={"trade_date": pl.Date, "factor": pl.Float64},
        ),
    )

    factors, vendor = mod._resolve_factors(cfg, "600519.SH", "hfq", bars, force=True, client=None)

    assert vendor == "baostock"
    assert factors["factor"].to_list() == [0.976883, 1.0]


def test_the_backup_stays_off_until_its_source_is_enabled(tmp_path, monkeypatch):
    """A chain that reaches a vendor nobody enabled is a chain that reaches out
    from a unit test."""
    import polars as pl

    from cnequity.adapters.sina.adj_factors import SinaAdjFactorUnavailableError
    from cnequity.config import Config
    from cnequity.derive import adj_factors as mod

    cfg = Config(data_root=tmp_path / "data")
    bars = pl.DataFrame({"trade_date": [date(2026, 6, 25)]}, schema={"trade_date": pl.Date})

    monkeypatch.setattr(
        mod,
        "fetch_adj_factor_series",
        lambda *a, **k: (_ for _ in ()).throw(SinaAdjFactorUnavailableError("456")),
    )

    def _never(*args, **kwargs):
        raise AssertionError("baostock must not be reached when its source is off")

    monkeypatch.setattr(
        "cnequity.adapters.baostock.adj_factors.fetch_adj_factor_series_baostock", _never
    )

    with pytest.raises(mod.AdjFactorsSourceUnavailableError):
        mod._resolve_factors(cfg, "600519.SH", "hfq", bars, force=True, client=None)


def test_a_beijing_symbol_never_asks_baostock_for_factors(tmp_path):
    """The vendor has no Beijing coverage at all."""
    from datetime import date as _date

    import pytest as _pytest

    from cnequity.adapters.baostock.adj_factors import (
        BaostockAdjFactorUnavailableError,
        fetch_adj_factor_series_baostock,
    )

    with _pytest.raises(BaostockAdjFactorUnavailableError):
        fetch_adj_factor_series_baostock(
            "920002.BJ", _date(2026, 6, 1), _date(2026, 6, 30), bs=object()
        )


def test_rows_name_the_vendor_they_came_from(tmp_path):
    """A frame can hold both vendors now; stamping one source over all of it is
    exactly the provenance this lake refuses to write."""
    import polars as pl

    from cnequity.config import Config
    from cnequity.derive import adj_factors as mod

    cfg = Config(data_root=tmp_path / "data")
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2026, 6, 25), date(2026, 6, 25)],
            "adjust_type": ["hfq", "hfq"],
            "factor": [1.0, 1.0],
            "_vendor": ["sina", "baostock"],
        },
        schema_overrides={"trade_date": pl.Date},
    )
    vendors = frame.get_column("_vendor")
    out = mod.with_provenance(
        frame.drop("_vendor"), source=cfg.adj_factors_source, data_version="v1"
    ).with_columns(vendors.alias("source"))
    assert out["source"].to_list() == ["sina", "baostock"]
