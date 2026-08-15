"""Cross-source close verification — the only reliable truncated-capture detector."""

from datetime import date

import polars as pl

from cn_market_lake.config import Config
from cn_market_lake.quality.cross_checks import daily_bars_close_crosscheck_findings

_DAY = date(2026, 7, 6)


def _write_bars(cfg: Config, closes: dict[str, float], amounts: dict[str, float] | None = None):
    part = cfg.curated_root / "daily_bars" / f"trade_date={_DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    syms = list(closes)
    pl.DataFrame(
        {
            "symbol": syms,
            "trade_date": [_DAY] * len(syms),
            "close": [closes[s] for s in syms],
            "amount": [(amounts or {}).get(s, 1.0e9) for s in syms],
            "volume": [1000] * len(syms),
        }
    ).write_parquet(part / "part-merged.parquet")


def _cfg(tmp_path, sina=True):
    return Config(data_root=tmp_path / "data", sources={"sina": sina})


def test_no_network_call_when_sina_is_disabled(tmp_path):
    """Unit tests and sina-less lakes must never reach out."""
    cfg = _cfg(tmp_path, sina=False)
    _write_bars(cfg, {"600519.SH": 1184.98})

    assert daily_bars_close_crosscheck_findings(cfg, _DAY) == []


def test_silent_when_closes_agree(tmp_path):
    cfg = _cfg(tmp_path)
    _write_bars(cfg, {"600519.SH": 1206.91, "601318.SH": 50.10})

    findings = daily_bars_close_crosscheck_findings(
        cfg, _DAY, reference_closes=lambda syms, d: {"600519.SH": 1206.91, "601318.SH": 50.10}
    )
    assert findings == []


def test_rounding_noise_is_not_a_mismatch(tmp_path):
    """Vendors round the last cent differently; that is not a defect."""
    cfg = _cfg(tmp_path)
    _write_bars(cfg, {"399001.SZ": 15416.82})

    findings = daily_bars_close_crosscheck_findings(
        cfg, _DAY, reference_closes=lambda syms, d: {"399001.SZ": 15416.80}
    )
    assert findings == []


def test_one_odd_symbol_is_a_warning(tmp_path):
    cfg = _cfg(tmp_path)
    _write_bars(cfg, {f"60000{i}.SH": 10.0 for i in range(4)})

    findings = daily_bars_close_crosscheck_findings(
        cfg,
        _DAY,
        reference_closes=lambda syms, d: {s: (12.0 if s == "600000.SH" else 10.0) for s in syms},
    )

    assert len(findings) == 1
    assert findings[0]["severity"] == "warning"
    assert findings[0]["check"] == "daily_bars_close_mismatch"
    assert findings[0]["mismatched"] == 1
    assert findings[0]["compared"] == 4


def test_market_wide_disagreement_is_an_error(tmp_path):
    """The real 2026-07-06 shape: every liquid name off in the same direction."""
    cfg = _cfg(tmp_path)
    truncated = {
        "600519.SH": 1184.98,
        "601318.SH": 48.92,
        "000001.SZ": 10.38,
        "600036.SH": 37.30,
    }
    true_closes = {
        "600519.SH": 1206.91,
        "601318.SH": 50.10,
        "000001.SZ": 10.50,
        "600036.SH": 37.73,
    }
    _write_bars(cfg, truncated)

    findings = daily_bars_close_crosscheck_findings(
        cfg, _DAY, reference_closes=lambda syms, d: true_closes
    )

    assert len(findings) == 1
    assert findings[0]["severity"] == "error"
    assert findings[0]["mismatch_ratio"] == 1.0
    assert "before" in findings[0]["message"] and "session closed" in findings[0]["message"]


def test_unreachable_reference_is_info_not_failure(tmp_path):
    """An unavailable second opinion is not evidence of bad data."""
    cfg = _cfg(tmp_path)
    _write_bars(cfg, {"600519.SH": 1206.91})

    def boom(syms, d):
        raise ConnectionError("dns failure")

    findings = daily_bars_close_crosscheck_findings(cfg, _DAY, reference_closes=boom)

    assert len(findings) == 1
    assert findings[0]["severity"] == "info"
    assert findings[0]["check"] == "close_crosscheck_unavailable"


def test_no_bars_for_the_day_is_silent(tmp_path):
    cfg = _cfg(tmp_path)
    _write_bars(cfg, {"600519.SH": 1206.91})

    findings = daily_bars_close_crosscheck_findings(
        cfg, date(2026, 7, 7), reference_closes=lambda syms, d: {}
    )
    assert findings == []


def test_samples_the_most_traded_symbols(tmp_path):
    """Illiquid names carry stale prints; the sample must prefer real liquidity."""
    cfg = _cfg(tmp_path)
    closes = {f"60{i:04d}.SH": 10.0 for i in range(20)}
    amounts = {sym: float(i) for i, sym in enumerate(closes)}
    _write_bars(cfg, closes, amounts)

    seen: list[str] = []

    def capture(syms, d):
        seen.extend(syms)
        return {}

    daily_bars_close_crosscheck_findings(cfg, _DAY, reference_closes=capture)

    assert len(seen) == 12
    assert "600019.SH" in seen, "highest-turnover symbol must be sampled"
    assert "600000.SH" not in seen, "lowest-turnover symbol must not be"
