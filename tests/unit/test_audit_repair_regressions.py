from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.quality.dataset_checks import _partitioned_pk_duplicate_count


@pytest.mark.parametrize("layout", ["root_overlap", "misplaced", "non_key_partition"])
def test_pk_audit_checks_cross_partition_duplicates(tmp_path, layout):
    dataset = "daily_bars"
    pcol = "trade_date"
    frame = pl.DataFrame({"symbol": ["600000.SH"], "trade_date": [date(2026, 1, 1)]})
    if layout == "non_key_partition":
        dataset = "announcement_index"
        pcol = "announce_date"
        frame = pl.DataFrame({"announcement_id": ["same"], "announce_date": [date(2026, 1, 1)]})
    first = tmp_path / f"{pcol}=2026-01-01" / "part.parquet"
    second = tmp_path / (
        "legacy.parquet" if layout == "root_overlap" else f"{pcol}=2026-01-02/part.parquet"
    )
    for path in (first, second):
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
    assert _partitioned_pk_duplicate_count([first, second], dataset, pcol, tmp_path) == 1


def test_offline_audit_never_calls_live_close_check(tmp_path, monkeypatch):
    import cnequity.quality.audit as audit

    def forbidden(*args, **kwargs):
        pytest.fail("offline audit attempted a live close crosscheck")

    monkeypatch.setattr(audit, "daily_bars_close_crosscheck_findings", forbidden)
    audit._collect_lake_findings(Config(data_root=tmp_path), date(2026, 1, 1), offline=True)


@pytest.mark.parametrize("retained_revision", [False, True])
def test_scoped_full_audit_reads_changed_partitions_but_checks_foreign_keys_globally(
    tmp_path, retained_revision
):
    from datetime import datetime, timezone

    from cnequity.quality.dataset_checks import audit_curated_dataset

    # event_id is the whole key, so a changed partition can collide with an
    # unchanged one; the scoped audit must still see both.
    config = Config(data_root=tmp_path)
    root = config.curated_root / "regulatory_events"
    for day in ("2026-01-01", "2026-01-02"):
        path = root / f"event_date={day[:4]}" / f"part-{day}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "event_id": ["same"],
                "symbol": ["600000.SH"],
                "event_date": [date.fromisoformat(day)],
                "event_type": ["inquiry"],
                "title": ["notice"],
                "source": ["sse"],
                "data_version": ["v1"],
                "fetched_at": [datetime(2026, 1, 3, tzinfo=timezone.utc)],
            }
        ).write_parquet(path)
    other = root / "event_date=2025" / "part.parquet"
    other.parent.mkdir(parents=True)
    pl.read_parquet(root / "event_date=2026" / "part-2026-01-01.parquet").with_columns(
        pl.lit(date(2025, 6, 1)).alias("event_date")
    ).write_parquet(other)
    if retained_revision:
        from tests.legacy_lake import commit_generation

        from cnequity.storage.revisions import RevisionStore

        store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        commit_generation(
            store, "regulatory_events", run_id="test", changed_files=list(root.rglob("*.parquet"))
        )

    findings = audit_curated_dataset(
        "regulatory_events",
        "event_date",
        root,
        date(2026, 1, 2),
        full=True,
        partitions=frozenset({"event_date=2026"}),
    )
    rows = next(f for f in findings if f["check"] == "row_count")
    assert rows["file_count"] == 2
    duplicate = next(f for f in findings if f["check"] == "pk_unique")
    assert duplicate["duplicate_rows"] == 2


def test_lake_findings_scope_skips_datasets_outside_it(tmp_path, monkeypatch):
    import cnequity.quality.audit as audit

    seen = []

    def record(dataset, *args, partitions=None, **kwargs):
        seen.append((dataset, partitions))
        return []

    monkeypatch.setattr(audit, "audit_curated_dataset", record)
    for name in ("daily_bars", "fund_flow"):
        (tmp_path / "curated" / name).mkdir(parents=True)
    scope = {"daily_bars": frozenset({"trade_date=2026-01-02"})}
    audit._collect_lake_findings(
        Config(data_root=tmp_path), date(2026, 1, 2), full=True, offline=True, scope=scope
    )
    assert seen == [("daily_bars", frozenset({"trade_date=2026-01-02"}))]


def test_publication_scope_names_changed_partitions(tmp_path):
    from pathlib import Path

    from cnequity.quality.publication import audit_scope

    root = tmp_path / "daily_bars"
    changed = [root / "trade_date=2026-01-02/part-merged.parquet"]
    assert audit_scope({"daily_bars": root}, {"daily_bars": changed}) == {
        "daily_bars": frozenset({"trade_date=2026-01-02"})
    }
    assert audit_scope({"daily_bars": root}, {"daily_bars": [root / "merged.parquet"]}) == {
        "daily_bars": None
    }
    assert audit_scope({"daily_bars": Path(root)}, None) == {"daily_bars": None}


def test_source_label_check_reads_only_scoped_datasets(tmp_path, monkeypatch):
    import cnequity.quality.cross_checks as checks

    seen = []
    monkeypatch.setattr(checks, "dataset_has_parquet", lambda root: seen.append(root.name))
    checks.undeclared_source_findings(Config(data_root=tmp_path), {"daily_bars"})
    assert seen == ["daily_bars"]


def test_share_count_lagging_the_vendor_is_reported(tmp_path):
    from datetime import datetime, timezone

    from cnequity.quality.cross_checks import share_structure_vendor_findings

    cfg = Config(data_root=tmp_path)
    day = date(2026, 9, 28)
    fetched = datetime(2026, 9, 28, tzinfo=timezone.utc)
    provenance = {"source": "eastmoney_datacenter", "data_version": "v1", "fetched_at": fetched}

    def write(dataset, partition, rows):
        path = cfg.curated_root / dataset / partition / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows).write_parquet(path)

    write(
        "valuation_metrics",
        f"trade_date={day}",
        [
            {
                "symbol": s,
                "trade_date": day,
                "total_mv": mv,
                "total_mv_basis": "vendor_reported",
                **provenance,
            }
            for s, mv in (("603014.SH", 31.15 * 694_798_783), ("600000.SH", 10.0 * 1e9))
        ],
    )
    write(
        "daily_bars",
        f"trade_date={day}",
        [
            {"symbol": s, "trade_date": day, "close": c, **provenance}
            for s, c in (("603014.SH", 31.15), ("600000.SH", 10.0))
        ],
    )
    write(
        "share_structure",
        "change_date=2026",
        [
            {
                "symbol": "603014.SH",
                "change_date": date(2026, 5, 19),
                "total_shares": 417_754_066.0,
                "announce_date": date(2026, 5, 12),
                **provenance,
            },
            {
                "symbol": "600000.SH",
                "change_date": date(2026, 1, 5),
                "total_shares": 1e9,
                "announce_date": date(2026, 1, 2),
                **provenance,
            },
        ],
    )
    findings = share_structure_vendor_findings(cfg, day)
    assert [f["check"] for f in findings] == ["share_structure_vendor_mismatch"]
    assert list(findings[0]["mismatches"]) == ["603014.SH"]
    assert findings[0]["mismatches"]["603014.SH"]["lake_change_date"] == "2026-05-19"


def test_scoped_audit_skips_cross_checks_whose_inputs_did_not_change(tmp_path, monkeypatch):
    import cnequity.quality.audit as audit

    def forbidden(*args, **kwargs):
        pytest.fail("a check with no candidate input ran in a scoped audit")

    for name in (
        "adj_factor_reconciliation_findings",
        "adj_factor_coverage_findings",
        "adj_factor_arbitration_findings",
        "valuation_bars_coverage_findings",
        "daily_bars_arbitration_findings",
    ):
        monkeypatch.setattr(audit, name, forbidden)
    audit._collect_lake_findings(
        Config(data_root=tmp_path),
        date(2026, 1, 2),
        full=True,
        offline=True,
        scope={"fund_flow": None},
    )
