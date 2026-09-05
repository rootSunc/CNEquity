"""Which trading_status row wins a primary-key collision, and why it matters.

`derived_bar_gap` rows are the only evidence the lake has that a security was
halted on most historical sessions. They used to be written straight into the
mutable curated directory, so committed readers never saw them and the next
compact rebuilt the partition from the committed generation without them —
`cne derive trading_status` reported thousands of rows and published none.

Routing them through staging fixes the visibility half. This file pins the
other half: once published, a restated EastMoney board must not quietly erase
them, while a genuine authority still must be able to correct them.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb
import polars as pl
import pytest

from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.trading_status import (
    DERIVED_BAR_GAP_SOURCE,
    EVIDENCE_DERIVED,
    EVIDENCE_POINT_IN_TIME,
    EVIDENCE_RESTATED,
    evidence_rank,
    evidence_rank_expr,
    evidence_rank_sql,
)
from cnequity.storage.parquet import StagingWriter, compact_dataset

DAY = date(2024, 6, 27)
# 15:30 and 16:30 Asia/Shanghai on the session itself.
SAME_SESSION = datetime(2024, 6, 27, 7, 30, tzinfo=timezone.utc)
# The board read four days later, still answering "is it halted *now*".
RESTATED = datetime(2024, 7, 1, 8, 0, tzinfo=timezone.utc)


def _status_row(source: str, fetched_at: datetime | None, **overrides) -> dict:
    row = {
        "symbol": "600984.SH",
        "trade_date": DAY,
        "is_trading": True,
        "status": "normal",
        "risk_warning": None,
        "source": source,
        "data_version": "v1",
        "fetched_at": fetched_at,
    }
    row.update(overrides)
    return row


def _frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "symbol": pl.Utf8,
            "trade_date": pl.Date,
            "is_trading": pl.Boolean,
            "status": pl.Utf8,
            "risk_warning": pl.Boolean,
            "source": pl.Utf8,
            "data_version": pl.Utf8,
            "fetched_at": pl.Datetime("us", "UTC"),
        },
    )


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (_status_row("baostock", RESTATED), EVIDENCE_POINT_IN_TIME),
        (_status_row("eastmoney", SAME_SESSION), EVIDENCE_POINT_IN_TIME),
        # 14:30 Asia/Shanghai: the session had not finished happening yet.
        (
            _status_row("eastmoney", datetime(2024, 6, 27, 6, 30, tzinfo=timezone.utc)),
            EVIDENCE_RESTATED,
        ),
        (_status_row("eastmoney", RESTATED), EVIDENCE_RESTATED),
        (_status_row("tdx_protocol", RESTATED), EVIDENCE_RESTATED),
        (_status_row(DERIVED_BAR_GAP_SOURCE, RESTATED), EVIDENCE_DERIVED),
        (_status_row("derived_delisted", None), EVIDENCE_POINT_IN_TIME),
        # An unclassified source wins conservatively rather than being
        # silently demoted below a derived row.
        (_status_row("some_new_exchange_feed", None), EVIDENCE_POINT_IN_TIME),
    ],
)
def test_evidence_rank_classifies_each_source(row, expected):
    assert evidence_rank(row) == expected


def test_the_three_implementations_agree():
    """Row-wise, polars and DuckDB must not disagree about the same lake."""
    rows = [
        _status_row("baostock", RESTATED),
        _status_row("eastmoney", SAME_SESSION),
        _status_row("eastmoney", RESTATED),
        _status_row(DERIVED_BAR_GAP_SOURCE, RESTATED),
        _status_row("derived_delisted", None),
        _status_row("tdx_protocol", None),
    ]
    frame = _frame(rows)
    row_wise = [evidence_rank(row) for row in rows]
    columnar = frame.select(evidence_rank_expr(frame.columns).alias("r"))["r"].to_list()
    con = duckdb.connect()
    con.register("rows", frame.to_arrow())
    ranked = con.execute(f"SELECT {evidence_rank_sql(frame.columns)} FROM rows").fetchall()
    sql = [value for (value,) in ranked]
    assert row_wise == columnar == sql


def test_a_frame_without_provenance_has_no_opinion():
    """Missing inputs must fall back, not invent a precedence."""
    assert evidence_rank_expr({"symbol", "trade_date"}) is None
    assert evidence_rank_sql({"symbol", "trade_date"}) is None


def test_a_text_fetched_at_makes_both_readers_decline():
    """The two readers have to agree about when they cannot answer.

    polars declines a `fetched_at` it cannot read as an instant and lets the
    caller fall back to its ordinary ordering. A DuckDB view that only knew
    the column *names* applied the CASE to the same fragment and ordered it
    differently — which is the one thing this pair exists to prevent.
    """
    typed = {"symbol": "VARCHAR", "source": "VARCHAR", "trade_date": "DATE"}
    assert evidence_rank_sql({**typed, "fetched_at": "VARCHAR"}) is None
    assert evidence_rank_expr({**typed, "fetched_at": pl.Date}) is None

    for timestamp_type in ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS"):
        assert evidence_rank_sql({**typed, "fetched_at": timestamp_type}) is not None
    # Bare names carry no type to object to, and neither form invents one.
    assert evidence_rank_sql(set(typed) | {"fetched_at"}) is not None


def test_a_restated_snapshot_does_not_overwrite_a_derived_halt():
    derived = _status_row(
        DERIVED_BAR_GAP_SOURCE, SAME_SESSION, is_trading=False, status="suspended"
    )
    restated = _status_row("eastmoney", RESTATED)
    canonical = dedupe_by_primary_key(_frame([derived, restated]), "trading_status")
    assert canonical.height == 1
    assert canonical["source"][0] == DERIVED_BAR_GAP_SOURCE
    assert canonical["status"][0] == "suspended"


def test_an_authority_still_corrects_a_derived_halt():
    """The point of ranking evidence is that real evidence can still win."""
    derived = _status_row(DERIVED_BAR_GAP_SOURCE, RESTATED, is_trading=False, status="suspended")
    for authority in (
        _status_row("baostock", SAME_SESSION),
        _status_row("eastmoney", SAME_SESSION),
    ):
        canonical = dedupe_by_primary_key(_frame([derived, authority]), "trading_status")
        assert canonical.height == 1
        assert canonical["source"][0] == authority["source"]
        assert canonical["status"][0] == "normal"


def test_derived_rows_survive_the_next_compact(tmp_path):
    """The regression: a second compact used to rebuild the day without them."""
    staging, curated = tmp_path / "staging", tmp_path / "curated"
    writer = StagingWriter(staging)

    writer.write_batch(
        "trading_status",
        "run-1",
        "derive-0",
        _frame(
            [
                _status_row(
                    DERIVED_BAR_GAP_SOURCE,
                    SAME_SESSION,
                    is_trading=False,
                    status="suspended",
                )
            ]
        ),
    )
    assert compact_dataset(staging, curated, "trading_status", "run-1") == 1

    # The next day's ordinary EastMoney sweep restates the same session.
    writer.write_batch(
        "trading_status", "run-2", "batch-0", _frame([_status_row("eastmoney", RESTATED)])
    )
    compact_dataset(staging, curated, "trading_status", "run-2")

    published = pl.read_parquet(
        curated / "trading_status" / "trade_date=2024-06" / "part-merged.parquet"
    )
    assert published.height == 1
    assert published["source"][0] == DERIVED_BAR_GAP_SOURCE
    assert published["status"][0] == "suspended"


def test_a_legacy_text_timestamp_is_still_ranked():
    """Old fragments stored `fetched_at` as an ISO string, not a timestamp."""
    frame = _frame([_status_row("eastmoney", SAME_SESSION)]).with_columns(
        pl.col("fetched_at").dt.to_string("%Y-%m-%dT%H:%M:%S%:z")
    )
    ranked = frame.select(evidence_rank_expr(frame.schema).alias("r"))["r"].to_list()
    assert ranked == [EVIDENCE_POINT_IN_TIME]


def test_an_unreadable_timestamp_falls_back_instead_of_raising():
    """A malformed fragment is the schema check's problem, not a query crash."""
    frame = _frame([_status_row("eastmoney", SAME_SESSION)]).with_columns(
        pl.col("fetched_at").dt.date()
    )
    assert evidence_rank_expr(frame.schema) is None
    # The ordinary recency ordering still collapses the key.
    assert dedupe_by_primary_key(frame, "trading_status").height == 1
