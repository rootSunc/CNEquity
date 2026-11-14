"""A derivation writes and publishes the same way on a generation lake and a catalog lake."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest
from tests.unit.test_minute_resample_derive import NEW, OLD, _lake

from cnequity.config import Config
from cnequity.derive.minute_resample import derive_minute_resample
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.query import load
from cnequity.steps.finalize import _publish_derived_revision
from cnequity.storage.derived_output import DerivedOutput
from cnequity.storage.revisions import RevisionStore

DATASET = "futures_continuous"
JAN, FEB, MAR = date(2024, 1, 5), date(2024, 2, 5), date(2024, 3, 5)


def _row(day: date, close: float, fetched: int = 1) -> dict:
    return {
        "symbol": "IF.CFE",
        "series": "main",
        "trade_date": day,
        "contract_symbol": "IF2403.CFE",
        "close": close,
        "source": "derived",
        "data_version": "v1",
        "fetched_at": datetime(2024, 4, fetched, tzinfo=timezone.utc),
    }


def _frame(*rows: dict) -> pl.DataFrame:
    schema = DATASET_SCHEMAS[DATASET]
    blank = dict.fromkeys(schema)
    return pl.DataFrame([{**blank, **row} for row in rows], schema=schema)


def _commit(cfg: Config, dataset: str, layer) -> None:
    RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root).commit(
        dataset,
        run_id="seed",
        changed_files=sorted((layer / dataset).rglob("*.parquet")),
        schema_version=1,
        contract_fingerprint="contract",
    )


@pytest.fixture
def lake(tmp_path, settle, monkeypatch) -> Config:
    # The audit has its own tests; these rows are too bare to pass it.
    monkeypatch.setattr(
        "cnequity.quality.publication.evaluate_publication",
        lambda *args, **kwargs: {"blocked": False, "mode": "block", "report_path": "report.json"},
    )
    cfg = Config(data_root=tmp_path / "data")
    for day, close in ((JAN, 1.0), (FEB, 2.0)):
        part = cfg.derived_root / DATASET / f"trade_date={day:%Y-%m}"
        part.mkdir(parents=True)
        _frame(_row(day, close)).write_parquet(part / "part-000.parquet")
    _commit(cfg, DATASET, cfg.derived_root)
    return settle(cfg)


def _closes(cfg: Config, revision: int | None = None) -> dict[date, float]:
    """The published rows as stored (these bare rows would not pass a validated load)."""
    from cnequity.query.parquet_scan import scan_parquet_root

    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    root = store.current_root(DATASET, revision=revision)
    rows = scan_parquet_root(root, committed=False).collect().sort("trade_date")
    return dict(zip(rows["trade_date"], rows["close"], strict=True))


def test_a_partition_is_merged_into_replaced_and_removed(lake):
    with DerivedOutput(lake, DATASET, "derive-1") as output:
        january = output.partition("trade_date=2024-01")
        stored = pl.concat([pl.read_parquet(path) for path in january.glob("*.parquet")])
        assert stored["close"].to_list() == [1.0]
        # Merge into the published partition in place, beside what it holds.
        _frame(_row(date(2024, 1, 8), 1.5)).select(stored.columns).cast(
            dict(stored.schema)
        ).write_parquet(january / "part-new.parquet")
        output.write("trade_date=2024-03", _frame(_row(MAR, 3.0)), "part-000.parquet")
        assert output.exists("trade_date=2024-02") and not output.exists("trade_date=2024-04")
        output.remove("trade_date=2024-02")
        published = _publish_derived_revision(lake, DATASET, "derive-1", MAR, output)

    assert published["revision"] == 2
    assert _closes(lake) == {JAN: 1.0, date(2024, 1, 8): 1.5, MAR: 3.0}
    assert _closes(lake, revision=1) == {JAN: 1.0, FEB: 2.0}


def test_a_rebuild_of_what_is_published_changes_nothing_on_a_catalog_lake(lake, settle):
    if settle.kind != "catalog":
        pytest.skip("a working tree compares bytes, and the fetch time is among them")
    with DerivedOutput(lake, DATASET, "derive-2") as output:
        # Recomputed later: the same facts with a newer fetch time.
        output.write("trade_date=2024-01", _frame(_row(JAN, 1.0, fetched=9)), "part-000.parquet")
        assert not output.changed()
        assert _publish_derived_revision(lake, DATASET, "derive-2", JAN, output) is None
        output.write("trade_date=2024-01", _frame(_row(JAN, 1.25, fetched=9)), "part-000.parquet")
        assert output.changed()


def test_a_rejected_derivation_publishes_nothing(lake, monkeypatch):
    monkeypatch.setattr(
        "cnequity.quality.publication.evaluate_publication",
        lambda *args, **kwargs: {"blocked": True, "mode": "block", "report_path": "report.json"},
    )
    with (
        pytest.raises(RuntimeError, match="publication gate blocked"),
        DerivedOutput(lake, DATASET, "derive-3") as output,
    ):
        output.write("trade_date=2024-03", _frame(_row(MAR, 3.0)), "part-000.parquet")
        _publish_derived_revision(lake, DATASET, "derive-3", MAR, output)

    assert _closes(lake) == {JAN: 1.0, FEB: 2.0}


def test_minute_resample_publishes_only_the_stale_session(tmp_path, settle):
    cfg = _lake(tmp_path)
    for dataset in ("minute_bars", "minute_bars_5m"):
        _commit(cfg, dataset, cfg.curated_root)
    with DerivedOutput(cfg, "minute_bars_30m", "resample-1") as output:
        derive_minute_resample(cfg, "minute_bars_30m", start=OLD, end=OLD, output=output)
        first = _publish_derived_revision(cfg, "minute_bars_30m", "resample-1", OLD, output)
    assert first["revision"] == 1
    settle(cfg)

    with DerivedOutput(cfg, "minute_bars_30m", "resample-2") as output:
        summary = derive_minute_resample(cfg, "minute_bars_30m", output=output)
        second = _publish_derived_revision(cfg, "minute_bars_30m", "resample-2", NEW, output)

    assert summary["sessions"] == 1
    assert second["revision"] == 2
    rows = load("minute_bars_30m", config=cfg).sort("trade_date")
    assert rows.select("trade_date", "resampled_from", "volume").rows() == [
        (OLD, "5m", 600),
        (NEW, "1m", 210),
    ]


def _factors(rows: list[tuple[str, date, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [row[0] for row in rows],
            "trade_date": [row[1] for row in rows],
            "adjust_type": ["hfq"] * len(rows),
            "factor": [row[2] for row in rows],
            "source": ["sina"] * len(rows),
            "data_version": ["v1"] * len(rows),
            "fetched_at": [datetime(2024, 4, 1, tzinfo=timezone.utc)] * len(rows),
        },
        schema=DATASET_SCHEMAS["adj_factors"],
    )


def test_the_factor_derive_checks_out_only_the_partitions_it_changes(tmp_path, settle, monkeypatch):
    from cnequity.derive.adj_factors import _factor_partition_symbols, _write_adj_partitions

    monkeypatch.setattr(
        "cnequity.quality.publication.evaluate_publication",
        lambda *args, **kwargs: {"blocked": False, "mode": "block", "report_path": "report.json"},
    )
    cfg = Config(data_root=tmp_path / "data")
    stored = [("600519.SH", JAN, 1.0), ("600519.SH", FEB, 2.0)]
    for symbol, day, factor in stored:
        part = cfg.derived_root / "adj_factors" / f"trade_date={day:%Y-%m}"
        part.mkdir(parents=True)
        _factors([(symbol, day, factor)]).write_parquet(part / "part-0.parquet")
    _commit(cfg, "adj_factors", cfg.derived_root)
    settle(cfg)

    # What a derive starts from is the published rows, with or without a working tree.
    assert _factor_partition_symbols(cfg, FEB) == {"600519.SH"}
    with DerivedOutput(cfg, "adj_factors", "adj-1") as output:
        # A daily derive hands over the whole history again, plus what is new.
        out = _factors([*stored, ("000001.SZ", FEB, 5.0), ("600519.SH", MAR, 3.0)])
        _write_adj_partitions(cfg, out, replace=False, output=output)
        if settle.kind == "catalog":
            # January held every row already: it was compared, never checked out.
            assert sorted(path.name for path in output.root.iterdir()) == [
                "trade_date=2024-02",
                "trade_date=2024-03",
            ]
        published = _publish_derived_revision(cfg, "adj_factors", "adj-1", MAR, output)

    assert published["revision"] == 2
    assert sorted(published["changed_partitions"]) == [
        "adj_factors/trade_date=2024-02",
        "adj_factors/trade_date=2024-03",
    ]
    rows = load("adj_factors", config=cfg).sort("symbol", "trade_date")
    assert rows.select("symbol", "trade_date", "factor").rows() == [
        ("000001.SZ", FEB, 5.0),
        ("600519.SH", JAN, 1.0),
        ("600519.SH", FEB, 2.0),
        ("600519.SH", MAR, 3.0),
    ]
