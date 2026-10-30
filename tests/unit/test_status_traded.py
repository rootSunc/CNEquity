"""A print establishes trading, but never establishes a negative ST designation."""

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.domain.trading_status import (
    DERIVED_TRADED_SOURCE,
    EVIDENCE_DERIVED,
    evidence_rank,
    evidence_rank_expr,
    evidence_rank_sql,
    last_first_hand_status,
)
from cnequity.quality.trading_status_coverage import trading_state_coverage_report
from cnequity.query import load
from cnequity.storage.repairs.status_traded import repair_status_traded
from cnequity.storage.revisions import RevisionStore

D1, D2 = date(2026, 7, 1), date(2026, 7, 2)
FETCHED = datetime(2026, 7, 3, 8, tzinfo=timezone.utc)


def _commit(cfg, dataset):
    return RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root).commit(
        dataset,
        run_id="seed",
        changed_files=sorted((cfg.curated_root / dataset).rglob("*.parquet")),
        schema_version=1,
        contract_fingerprint="contract",
    )


def _status(symbol, day, source="baostock", status="normal"):
    return {
        "symbol": symbol,
        "trade_date": day,
        "source": source,
        "status": status,
        "is_trading": status == "normal",
        "risk_warning": None,
        "data_version": "v1",
        "fetched_at": FETCHED,
    }


@pytest.fixture
def lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    inst = cfg.curated_root / "instruments"
    inst.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [
                "510001.SH",
                "920001.BJ",
                "000001.SZ",
                "000002.SZ",
                "000003.SZ",
                "000004.SZ",
            ],
            "asset_type": ["etf", "stock", "stock", "stock", "stock", "stock"],
            "list_date": [date(2020, 1, 1), D2, D1, D1, D1, None],
            "delist_date": [None, None, None, D2, None, None],
        },
        schema_overrides={"list_date": pl.Date, "delist_date": pl.Date},
    ).write_parquet(inst / "part.parquet")
    _commit(cfg, "instruments")
    symbols = [
        "510001.SH",
        "920001.BJ",
        "000001.SZ",
        "000002.SZ",
        "000003.SZ",
        "000004.SZ",
        "519999.SH",
    ]
    bars = [
        dict(
            symbol=s,
            trade_date=d,
            open=10.0,
            high=10.0,
            low=10.0,
            close=10.0,
            pre_close=10.0,
            volume=100 if s != "000003.SZ" else 0,
            amount=1000.0,
            source="exchange",
            data_version="v2",
            fetched_at=FETCHED,
        )
        for s in symbols
        for d in (D1, D2)
    ]
    root = cfg.curated_root / "daily_bars" / "trade_date=2026-07"
    root.mkdir(parents=True)
    pl.DataFrame(bars, schema=DATASET_SCHEMAS["daily_bars"]).write_parquet(root / "part.parquet")
    _commit(cfg, "daily_bars")
    root = cfg.curated_root / "trading_status" / "trade_date=2026-07"
    root.mkdir(parents=True)
    pl.DataFrame(
        [_status("000001.SZ", D1, status="suspended"), _status("510001.SH", D2)],
        schema=DATASET_SCHEMAS["trading_status"],
    ).write_parquet(root / "part.parquet")
    _commit(cfg, "trading_status")
    return cfg


def _tree(root: Path):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_preview_is_read_only_and_names_only_missing_listed_prints(lake):
    before = _tree(lake.data_root)
    plan = repair_status_traded(lake, start=D1, end=D2)
    assert _tree(lake.data_root) == before
    assert plan["rows"] == 4
    assert plan["by_class"] == {"etf": 1, "bse_stock": 1, "sh_sz_stock": 2}
    assert plan["exclusions"] == {
        "before_listing": 1,
        "on_or_after_delisting": 1,
        "unknown_listing_date": 2,
        "unsupported_or_missing_instrument": 2,
    }


def test_preview_of_an_empty_lake_creates_nothing(tmp_path):
    cfg = Config(data_root=tmp_path / "missing-lake")
    assert repair_status_traded(cfg, start=D1, end=D2)["rows"] == 0
    assert not cfg.data_root.exists()


def test_cli_previews_and_rejects_a_reversed_window(lake, monkeypatch):
    import json

    from click.testing import CliRunner

    from cnequity.cli.main import cli

    monkeypatch.setattr("cnequity.cli.repair_cmds._cfg", lambda _: lake)
    result = CliRunner().invoke(
        cli, ["repair", "status-traded", "--start", str(D1), "--end", str(D2)]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["rows"] == 4
    result = CliRunner().invoke(
        cli, ["repair", "status-traded", "--start", str(D2), "--end", str(D1)]
    )
    assert result.exit_code != 0


def test_apply_publishes_missing_month_keys_preserves_conflicts_and_old_revision(lake):
    store = RevisionStore(lake.meta_root, lake.curated_root, lake.derived_root)
    before = store.current_pointer("trading_status")["revision"]
    report = repair_status_traded(lake, start=D1, end=D2, apply=True)
    assert report["applied"]
    rows = load("trading_status", config=lake)
    added = rows.filter(pl.col("source") == DERIVED_TRADED_SOURCE)
    assert added.height == 4
    assert added["is_trading"].all()
    # ST does not apply to funds; stocks stay unknown.
    assert added["risk_warning"].null_count() == 3
    assert added.filter(pl.col("symbol") == "510001.SH")["risk_warning"].to_list() == [False]
    assert (
        rows.filter((pl.col("symbol") == "000001.SZ") & (pl.col("trade_date") == D1))["status"][0]
        == "suspended"
    )
    old = store.current_root("trading_status", revision=before)
    assert sum(pl.read_parquet(p).height for p in old.rglob("*.parquet")) == 2
    assert repair_status_traded(lake, start=D1, end=D2, apply=True)["rows"] == 0


def test_coverage_counts_history_before_first_status_and_separates_st(lake):
    before = trading_state_coverage_report(lake)
    assert before["traded_rows"] == 6
    assert before["missing_rows"] == 4
    assert before["contradictory_rows"] == 1
    repair_status_traded(lake, start=D1, end=D2, apply=True)
    after = trading_state_coverage_report(lake)
    assert after["missing_rows"] == 0
    assert after["derived_traded_rows"] == 4
    assert after["risk_warning_unknown_rows"] == 5  # the derived fund row is not ST


def test_preview_reads_committed_inputs_instead_of_unpublished_edits(lake):
    root = lake.curated_root / "daily_bars" / "trade_date=2026-07"
    frame = pl.read_parquet(root / "part.parquet").with_columns(pl.lit(0).alias("volume"))
    frame.write_parquet(root / "part.parquet")
    assert repair_status_traded(lake, start=D1, end=D2)["rows"] == 4


def test_apply_works_without_any_prior_status_partition(lake):
    cfg = Config(data_root=lake.data_root.parent / "fresh")
    import shutil

    for name in ("daily_bars", "instruments"):
        shutil.copytree(lake.curated_root / name, cfg.curated_root / name)
        _commit(cfg, name)
    assert repair_status_traded(cfg, start=D1, end=D2, apply=True)["rows"] == 6
    assert load("trading_status", config=cfg).height == 6


def test_a_blocked_publication_never_moves_the_committed_pointer(lake, monkeypatch):
    store = RevisionStore(lake.meta_root, lake.curated_root, lake.derived_root)
    pointer = store.current_pointer("trading_status")

    def block(*args, **kwargs):
        raise RuntimeError("repair publication blocked")

    monkeypatch.setattr("cnequity.storage.repairs.status_traded.check_repair_publication", block)
    with pytest.raises(RuntimeError, match="blocked"):
        repair_status_traded(lake, start=D1, end=D2, apply=True)
    assert store.current_pointer("trading_status") == pointer
    assert load("trading_status", config=lake).height == 2


def test_beijing_otc_prints_do_not_become_exchange_status(lake):
    root = lake.curated_root / "instruments"
    inst = pl.read_parquet(root / "part.parquet").with_columns(
        pl.when(pl.col("symbol") == "920001.BJ")
        .then(pl.lit(date(2010, 1, 1)))
        .otherwise(pl.col("list_date"))
        .alias("list_date")
    )
    inst.write_parquet(root / "part.parquet")
    _commit(lake, "instruments")
    root = lake.curated_root / "daily_bars" / "trade_date=2019-07"
    root.mkdir()
    bars = pl.read_parquet(lake.curated_root / "daily_bars/trade_date=2026-07/part.parquet")
    bars.filter(pl.col("symbol") == "920001.BJ").head(1).with_columns(
        pl.lit(date(2019, 7, 1)).alias("trade_date")
    ).write_parquet(root / "part.parquet")
    _commit(lake, "daily_bars")
    plan = repair_status_traded(lake, start=date(2019, 7, 1), end=date(2019, 7, 1))
    assert plan["rows"] == 0
    assert plan["exclusions"] == {"before_listing": 1}


def test_only_full_audit_requests_historical_state_coverage(tmp_path, monkeypatch):
    from cnequity.quality import audit

    cfg = Config(data_root=tmp_path / "empty")
    calls = []

    def coverage(*args):
        calls.append(True)
        return []

    monkeypatch.setattr(audit, "trading_state_coverage_findings", coverage)
    audit._collect_lake_findings(cfg, D2, offline=True)
    assert not calls
    audit._collect_lake_findings(cfg, D2, offline=True, full=True)
    assert len(calls) == 1
    audit._collect_lake_findings(cfg, D2, offline=True, full=True, scope={"trading_status": None})
    assert len(calls) == 1


def test_null_volume_and_newer_zero_volume_are_not_print_evidence(lake):
    root = lake.curated_root / "daily_bars" / "trade_date=2026-07"
    frame = pl.read_parquet(root / "part.parquet")
    newer = frame.with_columns(
        pl.lit(0, dtype=pl.Int64).alias("volume"),
        pl.lit(datetime(2026, 7, 4, tzinfo=timezone.utc)).alias("fetched_at"),
    )
    pl.concat([frame, newer]).write_parquet(root / "part.parquet")
    _commit(lake, "daily_bars")
    # Rounded-zero quantities with positive turnover cannot erase actual
    # prints. A genuine zero-volume/zero-amount observation remains distinct.
    assert repair_status_traded(lake, start=D1, end=D2)["rows"] == 4
    newer = newer.with_columns(pl.lit(0.0).alias("amount"))
    pl.concat([frame, newer]).write_parquet(root / "part.parquet")
    _commit(lake, "daily_bars")
    assert repair_status_traded(lake, start=D1, end=D2)["rows"] == 0
    frame.with_columns(pl.lit(None, dtype=pl.Int64).alias("volume")).write_parquet(
        root / "part.parquet"
    )
    _commit(lake, "daily_bars")
    assert repair_status_traded(lake, start=D1, end=D2)["rows"] == 0


def test_derived_trading_has_consistent_rank_and_cannot_be_carried_as_first_hand():
    derived = _status("510001.SH", D1, DERIVED_TRADED_SOURCE)
    frame = pl.DataFrame([derived], schema=DATASET_SCHEMAS["trading_status"])
    assert evidence_rank(derived) == EVIDENCE_DERIVED
    assert frame.select(evidence_rank_expr(frame.schema)).item() == EVIDENCE_DERIVED
    with duckdb.connect() as con:
        con.register("rows", frame.to_arrow())
        assert (
            con.execute(f"SELECT {evidence_rank_sql(frame.columns)} FROM rows").fetchone()[0]
            == EVIDENCE_DERIVED
        )
    assert last_first_hand_status(frame).is_empty()
    authority = _status("510001.SH", D1, "baostock", "suspended")
    canonical = dedupe_by_primary_key(
        pl.DataFrame([authority, derived], schema=frame.schema), "trading_status"
    )
    assert canonical["source"][0] == "baostock"


def test_fund_rows_written_unknown_are_marked_not_st_and_stocks_stay_unknown(lake):
    root = lake.curated_root / "trading_status" / "trade_date=2026-07"
    seeded = pl.DataFrame(
        [
            _status("000001.SZ", D1, status="suspended"),
            _status("510001.SH", D2),
            _status("510001.SH", D1, source=DERIVED_TRADED_SOURCE),
            _status("000002.SZ", D1, source=DERIVED_TRADED_SOURCE),
        ],
        schema=DATASET_SCHEMAS["trading_status"],
    )
    seeded.write_parquet(root / "part.parquet")
    _commit(lake, "trading_status")

    plan = repair_status_traded(lake, start=D1, end=D2)
    assert plan["fund_rows_marked_not_st"] == 1
    repair_status_traded(lake, start=D1, end=D2, apply=True)

    rows = load("trading_status", config=lake)

    def rw(symbol, day):
        hit = rows.filter((pl.col("symbol") == symbol) & (pl.col("trade_date") == day))
        assert hit.height == 1
        return hit["risk_warning"][0]

    assert rw("510001.SH", D1) is False
    assert rw("000002.SZ", D1) is None
    # A first-hand fund row is never rewritten by this repair.
    assert rows.filter((pl.col("symbol") == "510001.SH") & (pl.col("trade_date") == D2))[
        "source"
    ].to_list() == ["baostock"]
    assert repair_status_traded(lake, start=D1, end=D2)["fund_rows_marked_not_st"] == 0


def test_the_daily_derive_fills_printed_keys_and_never_fails_on_it(tmp_path, monkeypatch):
    from cnequity.steps import finalize
    from cnequity.storage.repairs import status_traded

    calls = []

    def fill(config, *, start, end, apply):
        calls.append((start, end, apply))
        return {"rows": 2, "sample": [{"symbol": "158036.SZ", "trade_date": "2026-10-23"}]}

    monkeypatch.setattr(status_traded, "repair_status_traded", fill)
    cfg = Config(data_root=tmp_path / "data")
    finding = finalize._fill_traded_status(cfg, date(2026, 10, 30))
    assert calls == [(date(2026, 10, 20), date(2026, 10, 30), True)]
    assert finding["check"] == "trading_status_traded_fill" and finding["severity"] == "info"

    def broken(*_a, **_k):
        raise OSError("lake busy")

    monkeypatch.setattr(status_traded, "repair_status_traded", broken)
    assert finalize._fill_traded_status(cfg, date(2026, 10, 30))["severity"] == "warning"
