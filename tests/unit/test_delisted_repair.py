"""Repair path: instruments from existing bars + catalog, no re-fetch."""

import json
from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.domain.schemas import DAILY_BARS_SCHEMA
from cnequity.steps.delisted import (
    _ingested_symbols,
    catalog_path,
    delisted_symbols_in_window,
    repair_delisted_instruments,
)
from cnequity.storage.parquet import StagingWriter

_BAR_COLS = [
    c for c in DAILY_BARS_SCHEMA if c not in ("source", "data_version", "fetched_at", "pre_close")
]


def _write_bars(cfg: Config, symbol: str, first: date, last: date) -> None:
    for d in (first, last):
        part = cfg.curated_root / "daily_bars" / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": [symbol],
                "trade_date": [d],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [10],
                "amount": [None],
            },
            schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS},
        ).write_parquet(part / "part-merged.parquet")


def _cfg(tmp_path, catalog: dict[str, str], live=("600519.SH",)):
    """``live`` entries are symbols, or ``(symbol, name)`` pairs."""
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    path = catalog_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"delisted": catalog, "never_issued": []}))
    inst = cfg.curated_root / "instruments"
    inst.mkdir(parents=True, exist_ok=True)
    symbols, name_list = [], []
    for item in live:
        if isinstance(item, tuple):
            symbols.append(item[0])
            name_list.append(item[1])
        else:
            symbols.append(item)
            name_list.append("live")
    pl.DataFrame(
        {
            "symbol": symbols,
            "name": name_list,
            "exchange": [s.split(".")[1] for s in symbols],
            "asset_type": ["etf" if n == "认购款" else "stock" for n in name_list],
            "list_date": pl.Series([date(2000, 1, 1)] * len(symbols), dtype=pl.Date),
            "delist_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "prev_symbol": [None] * len(symbols),
        }
    ).write_parquet(inst / "part-merged.parquet")
    # Anchor the lake's last session so classify_catalog's recency gate is stable.
    _write_bars(cfg, "600519.SH", date(2026, 7, 20), date(2026, 7, 24))
    return cfg


def _staged(cfg, dataset, run_id) -> pl.DataFrame:
    files = StagingWriter(cfg.staging_root).list_run_files(dataset, run_id)
    if not files:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def test_repair_writes_instruments_from_existing_bar_spans(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10"})
    _write_bars(cfg, "600070.SH", date(2016, 3, 1), date(2025, 4, 10))

    result = repair_delisted_instruments(cfg, "run-1")

    staged = _staged(cfg, "instruments", "run-1")
    row = staged.filter(pl.col("symbol") == "600070.SH")
    assert result["with_bars"] == 1
    assert row["list_date"].item() == date(2016, 3, 1)
    assert row["delist_date"].item() == date(2025, 4, 10)
    assert "600519.SH" in staged["symbol"].to_list()


def test_repair_marks_catalogued_symbols_with_bars_as_ingested(tmp_path):
    cfg = _cfg(tmp_path, {"600070.SH": "2025-04-10", "600071.SH": "2024-01-01"})
    _write_bars(cfg, "600070.SH", date(2016, 3, 1), date(2025, 4, 10))

    result = repair_delisted_instruments(cfg, "run-1")

    assert "600070.SH" in _ingested_symbols(cfg)
    assert "600071.SH" not in _ingested_symbols(cfg)
    assert delisted_symbols_in_window(cfg, date(2016, 1, 1)) == ["600071.SH"]
    assert result["still_need_bars"] == ["600071.SH"]
    assert result["status"] == "warning"


def test_repair_and_compact_publish_a_catalog_without_subscription_placeholders(tmp_path):
    from cnequity.steps.finalize import step_compact
    from cnequity.storage.revisions import RevisionStore

    cfg = _cfg(
        tmp_path,
        {"600070.SH": "2025-04-10"},
        live=("600519.SH", ("515844.SH", "认购款")),
    )
    _write_bars(cfg, "600070.SH", date(2016, 3, 1), date(2025, 4, 10))
    catalog = cfg.curated_root / "instruments" / "part-merged.parquet"
    pl.read_parquet(catalog).with_columns(
        source=pl.lit("tdx_protocol"),
        data_version=pl.lit("v1"),
        fetched_at=pl.lit(datetime(2026, 7, 24, tzinfo=timezone.utc)),
    ).write_parquet(catalog)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    for dataset in ("instruments", "daily_bars"):
        store.commit(
            dataset,
            run_id="seed",
            changed_files=sorted((cfg.curated_root / dataset).rglob("*.parquet")),
            schema_version=1,
            contract_fingerprint="contract",
        )

    repair_delisted_instruments(cfg, "run-1")
    assert "515844.SH" not in _staged(cfg, "instruments", "run-1")["symbol"].to_list()
    step_compact(cfg, date(2026, 7, 24), "run-1", {})

    # The stub must leave the catalog readers see, not only the mutable tree.
    published = pl.read_parquet(store.current_root("instruments"), hive_partitioning=False)
    assert sorted(published["symbol"].to_list()) == ["600070.SH", "600519.SH"]


def test_repair_picks_up_orphan_bars_not_in_the_catalog(tmp_path):
    """Baostock bars without a catalog entry still need a delist_date."""
    cfg = _cfg(tmp_path, {})
    # Last bar well before the lake's 2026-07-24 session (>180d gap).
    _write_bars(cfg, "000018.SZ", date(2016, 1, 4), date(2020, 1, 7))

    result = repair_delisted_instruments(cfg, "run-1")

    staged = _staged(cfg, "instruments", "run-1")
    row = staged.filter(pl.col("symbol") == "000018.SZ")
    assert result["from_orphan_bars"] == 1
    assert row["delist_date"].item() == date(2020, 1, 7)
    assert row["list_date"].item() == date(2016, 1, 4)


def test_repair_ignores_zero_volume_tail_when_finding_orphan_bars(tmp_path):
    cfg = _cfg(tmp_path, {})
    _write_bars(cfg, "000018.SZ", date(2016, 1, 4), date(2020, 1, 7))
    tail = cfg.curated_root / "daily_bars" / "trade_date=2026-07-24"
    tail.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["000018.SZ"],
            "trade_date": [date(2026, 7, 24)],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [0],
            "amount": [None],
        },
        schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS},
    ).write_parquet(tail / "part-placeholder.parquet")

    result = repair_delisted_instruments(cfg, "run-1")

    staged = _staged(cfg, "instruments", "run-1")
    row = staged.filter(pl.col("symbol") == "000018.SZ")
    assert result["from_orphan_bars"] == 1
    assert row["delist_date"].item() == date(2020, 1, 7)


def _write_placeholder_run(cfg: Config, symbol: str, dates: list[date]) -> None:
    for d in dates:
        part = cfg.curated_root / "daily_bars" / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": [symbol],
                "trade_date": [d],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [0],
                "amount": [None],
            },
            schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS},
        ).write_parquet(part / f"part-placeholder-{d.isoformat()}.parquet")


def test_repair_ignores_suspended_stock_with_a_dense_placeholder_run(tmp_path):
    """A merely-halted (still listed) stock must not be inferred as retired.

    Unlike the single trailing placeholder row above (a source's habit of
    tacking a dummy row onto a series after it truly ends), a long,
    near-continuous run of placeholder rows past the last real trade is
    evidence the vendor is still actively tracking the symbol as listed -
    e.g. a real, sometimes year-plus bankruptcy-reorganisation halt - and
    must not be inferred as a delisting from the trading gap alone.
    """
    from datetime import timedelta

    cfg = _cfg(tmp_path, {})
    _write_bars(cfg, "000099.SZ", date(2016, 1, 4), date(2019, 6, 10))
    _write_placeholder_run(
        cfg, "000099.SZ", [date(2026, 6, 1) + timedelta(days=i) for i in range(25)]
    )

    result = repair_delisted_instruments(cfg, "run-1")

    staged = _staged(cfg, "instruments", "run-1")
    assert result["from_orphan_bars"] == 0
    assert staged.is_empty() or "000099.SZ" not in staged["symbol"].to_list()


def test_repair_does_not_count_duplicate_placeholder_rows_twice(tmp_path):
    """Retry fragments must not turn a short tail into an active halt."""
    from datetime import timedelta

    cfg = _cfg(tmp_path, {})
    _write_bars(cfg, "000018.SZ", date(2016, 1, 4), date(2019, 6, 10))
    days = [date(2026, 6, 1) + timedelta(days=i) for i in range(10)]
    _write_placeholder_run(cfg, "000018.SZ", days)
    for day in days:
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        pl.read_parquet(part / f"part-placeholder-{day.isoformat()}.parquet").write_parquet(
            part / f"part-retry-{day.isoformat()}.parquet"
        )

    result = repair_delisted_instruments(cfg, "run-1")

    assert result["from_orphan_bars"] == 1


def test_instruments_rows_fills_null_list_date_from_recovery_spans(tmp_path):
    """A prior repair with no bars must not shadow a later backfill's list_date."""
    from cnequity.steps.delisted import _instruments_rows

    cfg = _cfg(tmp_path, {})
    # Hollow row as left by repair when bars were missing.
    inst = cfg.curated_root / "instruments" / "part-merged.parquet"
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600070.SH"],
            "name": ["live", None],
            "exchange": ["SH", "SH"],
            "asset_type": ["stock", "stock"],
            "list_date": pl.Series([date(2000, 1, 1), None], dtype=pl.Date),
            "delist_date": pl.Series([None, date(2025, 4, 10)], dtype=pl.Date),
            "prev_symbol": [None, None],
        }
    ).write_parquet(inst)

    out = _instruments_rows(cfg, {"600070.SH": (date(2016, 3, 1), date(2025, 4, 10))})
    row = out.filter(pl.col("symbol") == "600070.SH")
    assert row["list_date"].item() == date(2016, 3, 1)
    assert row["delist_date"].item() == date(2025, 4, 10)


def test_repair_respects_since_window_for_catalog(tmp_path):
    cfg = _cfg(
        tmp_path,
        {"600001.SH": "2009-12-15", "600070.SH": "2025-04-10"},
    )
    _write_bars(cfg, "600070.SH", date(2016, 3, 1), date(2025, 4, 10))

    result = repair_delisted_instruments(cfg, "run-1", start=date(2016, 1, 1))

    staged = _staged(cfg, "instruments", "run-1")
    assert "600070.SH" in staged["symbol"].to_list()
    assert "600001.SH" not in staged["symbol"].to_list()
    assert result["from_catalog"] == 1
