"""Code-space sweep that reconstructs the delisted universe without a vendor list."""

import json
from datetime import date, timedelta

import httpx
import polars as pl

from cnequity.config import Config
from cnequity.domain.symbols import ISSUED_CODE_BANDS, issued_code_space
from cnequity.steps.delisted import (
    LIVE_RECENCY_DAYS,
    catalog_path,
    classify_catalog,
    delisted_symbols_in_window,
    discover_delisted,
    load_delisted_catalog,
    pending_codes,
)

# Codes verified against Sina during the source investigation.
_DELISTED = {
    "600001.SH": date(2009, 12, 15),
    "600002.SH": date(2006, 4, 6),
    "600005.SH": date(2017, 1, 23),
}
_NEVER_ISSUED = {"600013.SH", "600014.SH", "600024.SH"}


def _cfg(tmp_path, live=("600519.SH", "000001.SZ")):
    # These sweeps use an injected local probe; pacing real wire calls would
    # only make the fixture's 40-code loops wait without testing source policy.
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True},
        source_intervals={"sina": 0, "sina_bars": 0},
    )
    part = cfg.curated_root / "instruments"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": list(live)}).write_parquet(part / "part-merged.parquet")
    return cfg


def _probe(symbol, client):
    if symbol in _DELISTED:
        return _DELISTED[symbol]
    return None


# --- code space -------------------------------------------------------------


def test_code_space_covers_every_band_without_duplicates():
    space = issued_code_space()
    expected = sum(last - first for _e, first, last in ISSUED_CODE_BANDS)

    assert len(space) == len(set(space)) == expected
    assert "600519.SH" in space and "000001.SZ" in space and "300750.SZ" in space
    # Zero-padded to six digits, else the symbol will not match instruments.
    assert "000001.SZ" in space and "1.SZ" not in space
    # Legacy NEEQ numbering — the pool that grows the catalogue toward ~2k.
    assert "430001.BJ" in space and "830001.BJ" in space and "870001.BJ" in space


def test_pending_excludes_symbols_that_are_listed_today(tmp_path):
    cfg = _cfg(tmp_path, live=("600001.SH", "600519.SH"))

    pending = pending_codes(cfg)

    assert "600001.SH" not in pending, "a live symbol is not a delisting candidate"
    assert "600002.SH" in pending


# --- sweep ------------------------------------------------------------------


def test_sweep_classifies_former_listings_and_never_issued(tmp_path):
    cfg = _cfg(tmp_path)

    result = discover_delisted(cfg, limit=40, probe=_probe)

    catalog = load_delisted_catalog(cfg)
    assert {"600001.SH", "600002.SH", "600005.SH"} <= set(catalog)
    assert catalog["600001.SH"] == date(2009, 12, 15)
    assert result.delisted == 3
    assert result.never_issued == result.probed - 3


def test_sweep_resumes_instead_of_reprobing(tmp_path):
    cfg = _cfg(tmp_path)
    first = discover_delisted(cfg, limit=40, probe=_probe)

    seen: list[str] = []

    def counting(symbol, client):
        seen.append(symbol)
        return _probe(symbol, client)

    discover_delisted(cfg, limit=40, probe=counting)

    assert first.probed == 40
    assert not set(seen) & set(load_delisted_catalog(cfg)), "already-classified codes reprobed"


def test_a_failing_probe_stays_pending_rather_than_being_filed(tmp_path):
    """Misfiling an outage as never-issued would shrink the universe permanently."""
    cfg = _cfg(tmp_path)
    calls = {"n": 0}

    def flaky(symbol, client):
        calls["n"] += 1
        if calls["n"] <= 5:
            raise ConnectionError("reset by peer")
        return _probe(symbol, client)

    result = discover_delisted(cfg, limit=20, probe=flaky)

    assert len(result.failed) == 5
    assert result.probed == 15
    still_pending = set(pending_codes(cfg))
    assert set(result.failed) <= still_pending


def test_sweep_retries_transient_sina_rate_limit(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    attempts = 0
    sleeps: list[float] = []
    request = httpx.Request("GET", "https://example.test/sina")

    def flaky(symbol, client):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            response = httpx.Response(456, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return date(2009, 12, 15)

    deferred: list[tuple[str, float]] = []
    monkeypatch.setattr("cnequity.domain.rate_limit.time.sleep", sleeps.append)
    monkeypatch.setattr(cfg, "defer_source", lambda source, s: deferred.append((source, s)))
    result = discover_delisted(cfg, limit=1, probe=flaky)

    assert attempts == 3
    # A rate limit cools every Sina lane; it used to sleep in this thread only
    # and ask again, which is what earns a longer ban.
    assert sleeps == []
    assert deferred == [("sina_bars", 30.0), ("sina_bars", 30.0)]
    assert result.delisted == 1
    assert result.failed == []


def test_catalog_survives_and_accumulates_across_sweeps(tmp_path):
    cfg = _cfg(tmp_path)
    discover_delisted(cfg, limit=5, probe=_probe)
    before = len(load_delisted_catalog(cfg)) + len(
        __import__("json").loads(catalog_path(cfg).read_text())["never_issued"]
    )

    discover_delisted(cfg, limit=5, probe=_probe)
    after = len(load_delisted_catalog(cfg)) + len(
        __import__("json").loads(catalog_path(cfg).read_text())["never_issued"]
    )

    assert before == 5
    assert after == 10


def test_sweep_reports_what_is_left(tmp_path):
    cfg = _cfg(tmp_path)

    result = discover_delisted(cfg, limit=10, probe=_probe)

    assert result.complete is False
    assert result.remaining == len(pending_codes(cfg))
    assert result.remaining > 0


# --- live vs delisted -------------------------------------------------------


def _catalog(cfg, entries: dict[str, str]):
    path = catalog_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"delisted": entries, "never_issued": []}))


def _with_bars_through(cfg, last: date):
    part = cfg.curated_root / "daily_bars" / f"trade_date={last.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [last]}).write_parquet(
        part / "part-merged.parquet"
    )


def test_a_code_still_quoting_today_is_not_a_delisting(tmp_path):
    """The BJ board: 328 live codes the instrument list simply never had."""
    cfg = _cfg(tmp_path)
    _with_bars_through(cfg, date(2026, 7, 21))
    _catalog(cfg, {"920000.BJ": "2026-07-21", "600001.SH": "2009-12-15"})

    delisted, live = classify_catalog(cfg)

    assert set(delisted) == {"600001.SH"}
    assert set(live) == {"920000.BJ"}


def test_a_long_suspension_is_not_read_as_a_delisting(tmp_path):
    """Erring here would write a delist_date for a listed name and freeze it out."""
    cfg = _cfg(tmp_path)
    _with_bars_through(cfg, date(2026, 7, 21))
    _catalog(cfg, {"600123.SH": (date(2026, 7, 21) - timedelta(days=20)).isoformat()})

    delisted, live = classify_catalog(cfg)

    assert delisted == {}
    assert "600123.SH" in live


def test_a_delisting_past_the_recency_window_is_classified(tmp_path):
    cfg = _cfg(tmp_path)
    _with_bars_through(cfg, date(2026, 7, 21))
    stale = date(2026, 7, 21) - timedelta(days=LIVE_RECENCY_DAYS + 5)
    _catalog(cfg, {"600123.SH": stale.isoformat()})

    delisted, live = classify_catalog(cfg)

    assert delisted == {"600123.SH": stale}
    assert live == {}


def test_post_probe_bar_keeps_currently_listed_symbol_out_of_delisted_catalog(tmp_path):
    cfg = _cfg(tmp_path)
    _catalog(cfg, {"920000.BJ": "2026-07-21"})
    part = cfg.curated_root / "daily_bars" / "trade_date=2026-08-21"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["920000.BJ"],
            "trade_date": [date(2026, 8, 21)],
            "volume": [100],
        }
    ).write_parquet(part / "part-merged.parquet")

    delisted, live = classify_catalog(cfg)

    assert delisted == {}
    assert live == {"920000.BJ": date(2026, 8, 21)}


def test_reference_is_the_lake_not_the_wall_clock(tmp_path):
    """A lake that stopped updating must not reclassify its universe as delisted."""
    cfg = _cfg(tmp_path)
    _with_bars_through(cfg, date(2026, 3, 2))
    _catalog(cfg, {"600123.SH": "2026-03-02"})

    delisted, live = classify_catalog(cfg)

    assert delisted == {}
    assert "600123.SH" in live


def test_backfill_targets_only_genuine_delistings(tmp_path):
    cfg = _cfg(tmp_path)
    _with_bars_through(cfg, date(2026, 7, 21))
    _catalog(cfg, {"920000.BJ": "2026-07-21", "600070.SH": "2025-04-10"})

    assert delisted_symbols_in_window(cfg, date(2016, 1, 1)) == ["600070.SH"]


def _with_instruments(cfg, rows: list[dict]):
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        rows,
        schema={"symbol": pl.Utf8, "list_date": pl.Date, "delist_date": pl.Date},
    ).write_parquet(root / "part-merged.parquet")


def test_a_code_listing_after_the_probe_terminal_is_not_a_delisting(tmp_path):
    """A security cannot stop trading before it starts.

    Sina answers the discovery probe the same way for a code that stopped
    trading and for one that was issued days before its first session, so a
    fresh listing swept in that window used to be filed as a delisting: eleven
    stored rows carried a delist_date of 2026-09-01 against list dates of
    2026-09-02..09-07. The security master settles it.
    """
    cfg = _cfg(tmp_path)
    _with_bars_through(cfg, date(2026, 9, 8))
    stale = date(2026, 9, 8) - timedelta(days=LIVE_RECENCY_DAYS + 5)
    _catalog(cfg, {"301688.SZ": stale.isoformat(), "600001.SH": "2009-12-15"})
    _with_instruments(
        cfg,
        [
            # Listed the day after the probe terminal — a new issue, not a retirement.
            {"symbol": "301688.SZ", "list_date": stale + timedelta(days=1), "delist_date": None},
            {"symbol": "600001.SH", "list_date": date(2000, 1, 4), "delist_date": None},
        ],
    )

    delisted, live = classify_catalog(cfg)

    assert "301688.SZ" not in delisted
    assert "301688.SZ" in live
    assert set(delisted) == {"600001.SH"}
