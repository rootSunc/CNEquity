"""Index membership from the publishers, with EastMoney as backup and peer."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.quality.cross_checks import index_constituents_peer_findings
from cnequity.steps import structure

DAY = date(2026, 10, 26)
NOW = datetime(2026, 10, 26, 9, tzinfo=timezone.utc)


def _rows(index_symbol, symbols, source, day=DAY, weight=None):
    return pl.DataFrame(
        {
            "index_symbol": [index_symbol] * len(symbols),
            "symbol": symbols,
            "as_of_date": [day] * len(symbols),
            "weight": [weight] * len(symbols),
            "source": [source] * len(symbols),
            "data_version": ["v1"] * len(symbols),
            "fetched_at": [NOW] * len(symbols),
        },
        schema_overrides={"weight": pl.Float64},
    )


def test_an_official_book_replaces_the_vendor_book_on_its_index_date():
    frame = pl.concat(
        [
            _rows("000001.SH", ["600000.SH", "900901.SH"], "exchange"),
            # EastMoney's stale ST member must not survive beside the official book.
            _rows("000001.SH", ["600000.SH", "600363.SH"], "eastmoney"),
            # No official book for this index-date: the vendor book stays.
            _rows("399006.SZ", ["300750.SZ"], "eastmoney"),
        ]
    )

    out = dedupe_by_primary_key(frame, "index_constituents")

    assert sorted(out.select("index_symbol", "symbol", "source").rows()) == [
        ("000001.SH", "600000.SH", "exchange"),
        ("000001.SH", "900901.SH", "exchange"),
        ("399006.SZ", "300750.SZ", "eastmoney"),
    ]


@pytest.fixture
def cfg(tmp_path):
    c = Config(data_root=tmp_path / "lake", raw_archive_enabled=False)
    c.staging_root.mkdir(parents=True)
    return c


def test_daily_step_uses_publishers_and_eastmoney_only_for_a_failed_index(cfg, monkeypatch):
    from cnequity.adapters.exchange import index_snapshots

    def official(index_symbol, trade_date, previous, *, client, config):
        if index_symbol == "399006.SZ":
            raise ValueError("CNI unreachable")
        members = _rows(index_symbol, [f"60000{i}.SH" for i in range(60)], "exchange")
        weights = [_rows(index_symbol, ["600000.SH"], "exchange", date(2026, 9, 30), 1.0)]
        return members.drop("data_version", "fetched_at"), weights

    vendor = pl.concat(
        [
            _rows("399006.SZ", [f"30000{i}.SZ" for i in range(60)], "eastmoney"),
            _rows("000300.SH", [f"60000{i}.SH" for i in range(60)], "eastmoney"),
        ]
    ).drop("data_version", "fetched_at", "source")
    monkeypatch.setattr(index_snapshots, "fetch_official_books", official)
    monkeypatch.setattr(structure, "fetch_index_constituents", lambda d, config=None: vendor)
    monkeypatch.setattr(structure, "list_trading_dates", lambda c, s, e: [date(2026, 10, 23), DAY])

    def run_once(config, d, run_id, ds, fetch, **kwargs):
        frame = fetch(d)
        return {"rows_written": frame.height, "staged": frame}

    monkeypatch.setattr(structure, "run_incremental_fetched", run_once)
    written = []
    monkeypatch.setattr(
        structure, "write_fetched", lambda c, r, ds, frame, **k: written.append(frame) or {}
    )

    result = structure.step_index_constituents(cfg, DAY, "run-1", {})

    staged = result["staged"]
    by_source = dict(staged.group_by("index_symbol").agg(pl.col("source").first()).iter_rows())
    assert by_source["399006.SZ"] == "eastmoney"
    assert by_source["000300.SH"] == "exchange"
    assert result["status"] == "degraded"
    checks = [f["check"] for f in result["context_updates"]["audit_findings"]]
    assert checks == ["index_constituents_official_unavailable"]
    assert written and written[0]["as_of_date"][0] == date(2026, 9, 30)


def test_peer_finding_lists_each_sides_members(cfg):
    from cnequity.storage.source_snapshots import SnapshotStore

    path = cfg.curated_root / "index_constituents" / "as_of_date=2026-10" / "part-0.parquet"
    path.parent.mkdir(parents=True)
    _rows("000001.SH", ["600000.SH", "900901.SH"], "exchange").write_parquet(path)
    SnapshotStore(cfg.meta_root).write(
        "index_constituents",
        _rows("000001.SH", ["600000.SH", "600363.SH"], "eastmoney"),
        source="eastmoney",
        data_version="v1",
        run_id="run-1",
        trade_date=DAY,
    )

    [finding] = index_constituents_peer_findings(cfg)

    item = finding["indices"]["000001.SH"]
    assert item["official_only"] == ["900901.SH"]
    assert item["eastmoney_only"] == ["600363.SH"]
    assert finding["severity"] == "warning"  # 1 of 2 shared: below the 95% floor
