"""compact merging of trading_status must preserve evidence rank.

A newer ordinary EastMoney current-state snapshot (rank 2) must not overwrite
a derived bar-gap suspension (rank 1), while an authority (rank 0: Baostock,
a finalized same-session snapshot, or a delisting) must still be able to
correct the derived row. Other datasets keep the old fetched_at-then-source
precedence.
"""

from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.domain.trading_status import (
    DERIVED_BAR_GAP_SOURCE,
    evidence_rank_expr,
    status_evidence_rank,
)
from cnequity.query.canonical import dedupe_by_primary_key
from cnequity.storage import StagingWriter, compact_dataset

DAY = date(2024, 6, 28)
MOMENT = datetime(2024, 6, 28, 15, 30, tzinfo=timezone.utc)


def _status(symbol: str, is_trading: bool, source: str, fetched: str | None) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [symbol],
            "trade_date": [DAY],
            "is_trading": [is_trading],
            "status": ["normal" if is_trading else "suspended"],
            "risk_warning": [None],
            "source": [source],
            "data_version": ["v1"],
            "fetched_at": pl.Series([fetched], dtype=pl.Utf8).str.to_datetime(
                time_unit="us", time_zone="UTC"
            ),
        }
    )


def _compact(cfg: Config, run_id: str, month: str, rows: list[pl.DataFrame]) -> pl.DataFrame:
    """Stage *rows* for trading_status, compact the run, return the merged partition."""
    part = cfg.curated_root / "trading_status" / f"trade_date={month}"
    part.mkdir(parents=True, exist_ok=True)
    writer = StagingWriter(cfg.staging_root)
    for i, frame in enumerate(rows):
        writer.write_batch("trading_status", run_id, f"batch-{i}", frame)
    compact_dataset(
        cfg.staging_root,
        cfg.curated_root,
        "trading_status",
        run_id,
        partition_col="trade_date",
    )
    return pl.read_parquet(part / "part-merged.parquet")


def test_newer_snapshot_does_not_overwrite_derived_suspension(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    # Existing committed snapshot is the newest observation (July, late stamp),
    # but it is a current-state snapshot stamped onto an older date (rank 2).
    (cfg.curated_root / "trading_status" / "trade_date=2024-06").mkdir(parents=True)
    _status("600519.SH", True, "eastmoney", "2024-07-01T08:00:00").write_parquet(
        cfg.curated_root / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    out = _compact(
        cfg,
        "run-1",
        "2024-06",
        [_status("600519.SH", False, DERIVED_BAR_GAP_SOURCE, "2024-06-28T07:00:00")],
    )
    row = out.to_dicts()[0]
    assert row["is_trading"] is False
    assert row["source"] == DERIVED_BAR_GAP_SOURCE


def test_derived_existing_survives_newer_snapshot_in_staging(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    (cfg.curated_root / "trading_status" / "trade_date=2024-06").mkdir(parents=True)
    _status("600519.SH", False, DERIVED_BAR_GAP_SOURCE, "2024-06-28T07:00:00").write_parquet(
        cfg.curated_root / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    out = _compact(
        cfg,
        "run-2",
        "2024-06",
        [_status("600519.SH", True, "eastmoney", "2024-07-01T08:00:00")],
    )
    row = out.to_dicts()[0]
    assert row["is_trading"] is False
    assert row["source"] == DERIVED_BAR_GAP_SOURCE


def test_authority_source_corrects_derived_suspension(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    (cfg.curated_root / "trading_status" / "trade_date=2024-06").mkdir(parents=True)
    _status("600519.SH", False, DERIVED_BAR_GAP_SOURCE, "2024-06-28T07:00:00").write_parquet(
        cfg.curated_root / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    out = _compact(
        cfg,
        "run-3",
        "2024-06",
        [_status("600519.SH", True, "baostock", "2024-06-28T07:00:00")],
    )
    row = out.to_dicts()[0]
    assert row["is_trading"] is True
    assert row["source"] == "baostock"


def test_finalized_same_session_snapshot_corrects_derived_suspension(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    (cfg.curated_root / "trading_status" / "trade_date=2024-06").mkdir(parents=True)
    _status("600519.SH", False, DERIVED_BAR_GAP_SOURCE, "2024-06-28T07:00:00").write_parquet(
        cfg.curated_root / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    # Fetched on the same session after 15:00 Shanghai (07:30 UTC) -> rank 0.
    out = _compact(
        cfg,
        "run-4",
        "2024-06",
        [_status("600519.SH", True, "eastmoney", "2024-06-28T07:30:00")],
    )
    row = out.to_dicts()[0]
    assert row["is_trading"] is True
    assert row["source"] == "eastmoney"


def test_evidence_rank_expr_matches_row_function(tmp_path):
    """The columnar compact expression must agree with the row-wise authority."""
    df = pl.DataFrame(
        {
            "symbol": ["A", "A", "A", "A", "A"],
            "trade_date": [DAY] * 5,
            "is_trading": [False] * 5,
            "status": ["suspended"] * 5,
            "risk_warning": [None] * 5,
            "source": [
                "baostock",
                "eastmoney",
                "eastmoney",
                DERIVED_BAR_GAP_SOURCE,
                "derived_delisted",
            ],
            "data_version": ["v1"] * 5,
            "fetched_at": pl.Series(
                [None, "2024-06-28T07:30:00", "2024-07-01T08:00:00", None, None],
                dtype=pl.Utf8,
            ).str.to_datetime(time_unit="us", time_zone="UTC"),
        }
    )
    row_ranks = [status_evidence_rank(r) for r in df.to_dicts()]
    col_ranks = df.with_columns(evidence_rank_expr("UTC").alias("rank")).get_column("rank")
    assert col_ranks.to_list() == row_ranks == [0, 0, 2, 1, 0]


def test_dedupe_by_primary_key_trading_status_rank(tmp_path):
    """The shared canonical dedupe used by compact picks rank 1 over rank 2."""
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [DAY, DAY],
            "is_trading": [True, False],
            "status": ["normal", "suspended"],
            "risk_warning": [None, None],
            "source": ["eastmoney", DERIVED_BAR_GAP_SOURCE],
            "data_version": ["v1", "v1"],
            "fetched_at": pl.Series(
                ["2024-07-01T08:00:00", "2024-06-28T07:00:00"], dtype=pl.Utf8
            ).str.to_datetime(time_unit="us", time_zone="UTC"),
        }
    )
    out = dedupe_by_primary_key(frame, "trading_status")
    row = out.to_dicts()[0]
    assert row["is_trading"] is False
    assert row["source"] == DERIVED_BAR_GAP_SOURCE


def test_non_trading_status_keeps_fetched_at_semantics(tmp_path):
    """Regression: other datasets still prefer the freshest observation."""
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [DAY, DAY],
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
            "volume": [100, 100],
            "amount": [100.0, 100.0],
            "source": ["tdx_protocol", "tdx_protocol"],
            "data_version": ["v1", "v1"],
            "fetched_at": pl.Series(
                ["2024-07-01T00:00:00", "2024-06-28T00:00:00"], dtype=pl.Utf8
            ).str.to_datetime(time_unit="us", time_zone="UTC"),
        }
    )
    out = dedupe_by_primary_key(frame, "daily_bars")
    assert out.height == 1
    assert str(out["fetched_at"][0]).startswith("2024-07-01 00:00:00")
