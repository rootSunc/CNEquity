"""A PIT load scans only the partitions its window needs, and returns the same rows.

``load()`` collapses each fact to the vintage current on ``as_of`` before
``start``/``end`` apply. It used to read every partition first: one
``financial_statement_items`` quarter held 5.9 GB. Each case here compares the
windowed read with that read-everything-then-filter result.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from itertools import product

import polars as pl
import pytest

import cnequity.query.reader as reader
from cnequity.config import Config
from cnequity.query import load
from cnequity.query.reader import pit_scan_window, scan

D = date
_FETCHED = datetime(2026, 1, 5, tzinfo=timezone.utc)


def _write(cfg: Config, dataset: str, column: str, value: str, frame: pl.DataFrame) -> None:
    part = cfg.curated_root / dataset / f"{column}={value}"
    part.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(part / "part-merged.parquet")


def _garbage(cfg: Config, dataset: str, column: str, value: str) -> None:
    """A partition no windowed read may open: reading it raises."""
    part = cfg.curated_root / dataset / f"{column}={value}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "part-merged.parquet").write_bytes(b"not parquet")


def _fsi(cfg: Config) -> None:
    # Each quarter: an original and a restatement, one restated after the
    # cutoffs below, and a second symbol announced the same day.
    for year, quarter in product((2023, 2024), (1, 2, 3, 4)):
        period = f"{year}Q{quarter}"
        first = D(year + (quarter == 4), (quarter % 4) * 3 + 1, 20)
        rows = [
            ("000001.SZ", "revenue", 100.0, first),
            ("000001.SZ", "revenue", 90.0, D(year + 1, 3, 15)),
            ("000001.SZ", "net_profit", 10.0, first),
            ("000001.SZ", "net_profit", 11.0, D(year + 2, 4, 1)),
            ("600000.SH", "revenue", 50.0, first),
        ]
        _write(
            cfg,
            "financial_statement_items",
            "report_period",
            period,
            pl.DataFrame(
                {
                    "symbol": [r[0] for r in rows],
                    "report_period": [period] * len(rows),
                    "statement_type": ["income"] * len(rows),
                    "item_code": [r[1] for r in rows],
                    "item_value": [r[2] for r in rows],
                    "announce_date": [r[3] for r in rows],
                    "source": ["eastmoney"] * len(rows),
                    "data_version": ["v1"] * len(rows),
                    "fetched_at": [_FETCHED] * len(rows),
                }
            ),
        )


def _top_holders(cfg: Config) -> None:
    for year in (2023, 2024, 2025):
        rows = []
        for rank, name in enumerate(("甲", "乙", "丙"), start=1):
            # Three holders share announce_date and symbol: the sort must
            # still give one order.
            rows.append((D(year, 12, 31), rank, name, 100.0 - rank, D(year + 1, 3, 30)))
            rows.append((D(year, 12, 31), rank, name, 99.0 - rank, D(year + 1, 8, 20)))
        rows.append((D(year, 6, 30), 1, "甲", 80.0, D(year, 8, 25)))
        _write(
            cfg,
            "top_holders",
            "record_date",
            str(year),
            pl.DataFrame(
                {
                    "symbol": ["000001.SZ"] * len(rows),
                    "record_date": [r[0] for r in rows],
                    "holder_scope": ["top10"] * len(rows),
                    "holder_rank": pl.Series([r[1] for r in rows], dtype=pl.Int32),
                    "holder_name": [r[2] for r in rows],
                    "holding_shares": [r[3] for r in rows],
                    "holding_pct": [1.0] * len(rows),
                    "is_institution": [False] * len(rows),
                    "holder_type": ["person"] * len(rows),
                    "announce_date": [r[4] for r in rows],
                    "source": ["eastmoney"] * len(rows),
                    "data_version": ["v1"] * len(rows),
                    "fetched_at": [_FETCHED] * len(rows),
                }
            ),
        )


def _announcements(cfg: Config) -> None:
    # A-1 is announced on 06-10 and again on 06-20: the second vintage lives in
    # another partition. A-2's second vintage comes after every cutoff used.
    by_day = {
        D(2024, 6, 3): [("A-0", "first")],
        D(2024, 6, 10): [("A-1", "first"), ("A-2", "first"), ("A-3", "only")],
        D(2024, 6, 20): [("A-1", "corrected"), ("A-4", "only")],
        D(2024, 7, 2): [("A-2", "corrected")],
    }
    for day, rows in by_day.items():
        _write(
            cfg,
            "announcement_index",
            "announce_date",
            day.isoformat(),
            pl.DataFrame(
                {
                    "announcement_id": [r[0] for r in rows],
                    "symbol": ["000001.SZ"] * len(rows),
                    "title": [r[1] for r in rows],
                    "announce_date": [day] * len(rows),
                    "category": ["notice"] * len(rows),
                    "url": ["https://example.invalid"] * len(rows),
                    "source": ["cninfo"] * len(rows),
                    "data_version": ["v1"] * len(rows),
                    "fetched_at": [_FETCHED] * len(rows),
                }
            ),
        )


@pytest.fixture
def lake(tmp_path) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    _fsi(cfg)
    _top_holders(cfg)
    _announcements(cfg)
    return cfg


def _read_everything(monkeypatch, dataset: str, cfg: Config, **kwargs) -> pl.DataFrame:
    with monkeypatch.context() as patch:
        patch.setattr(reader, "pit_scan_window", lambda *_a, **_k: (None, None))
        return load(dataset, config=cfg, **kwargs)


_WINDOWS = {
    "financial_statement_items": [
        (D(2024, 4, 1), D(2024, 6, 30)),
        (D(2024, 5, 15), D(2024, 5, 15)),
        (D(2023, 7, 1), None),
        (None, D(2023, 12, 31)),
    ],
    "top_holders": [
        (D(2024, 1, 1), D(2024, 12, 31)),
        (D(2024, 12, 31), D(2025, 6, 30)),
        (None, D(2023, 12, 31)),
    ],
    "announcement_index": [
        (D(2024, 6, 10), D(2024, 6, 10)),
        (D(2024, 6, 1), D(2024, 6, 15)),
        (D(2024, 6, 15), None),
        (D(2024, 6, 4), D(2024, 6, 30)),
    ],
}
_AS_OF = [D(2024, 6, 15), D(2024, 6, 25), D(2025, 6, 1), D(2026, 11, 9)]


@pytest.mark.parametrize("dataset", sorted(_WINDOWS))
@pytest.mark.parametrize("all_vintages", [False, True])
def test_windowed_read_equals_reading_everything(lake, monkeypatch, dataset, all_vintages):
    compared = nonempty = 0
    for (start, end), as_of, mode in product(_WINDOWS[dataset], _AS_OF, ("best_effort", "strict")):
        kwargs = dict(start=start, end=end, as_of=as_of, pit_mode=mode, all_vintages=all_vintages)
        old = _read_everything(monkeypatch, dataset, lake, **kwargs)
        new = load(dataset, config=lake, **kwargs)
        assert new.equals(old), (dataset, kwargs)
        compared += 1
        nonempty += not new.is_empty()
    assert nonempty >= compared // 4


def test_a_later_vintage_in_another_partition_still_supersedes(lake):
    # A-1 was corrected on 06-20; on 06-25 the 06-10 window no longer holds it.
    before = load(
        "announcement_index", start="2024-06-10", end="2024-06-10", as_of="2024-06-15", config=lake
    )
    after = load(
        "announcement_index", start="2024-06-10", end="2024-06-10", as_of="2024-06-25", config=lake
    )

    assert sorted(before["announcement_id"]) == ["A-1", "A-2", "A-3"]
    assert sorted(after["announcement_id"]) == ["A-2", "A-3"]


@pytest.mark.parametrize(
    ("dataset", "column", "outside", "kwargs"),
    [
        (
            "financial_statement_items",
            "report_period",
            "2022Q4",
            dict(start="2024-04-01", end="2024-06-30", as_of="2026-11-09"),
        ),
        (
            "top_holders",
            "record_date",
            "2022",
            dict(start="2024-01-01", end="2024-12-31", as_of="2026-11-09"),
        ),
        (
            "announcement_index",
            "announce_date",
            "2024-06-01",
            dict(start="2024-06-10", end="2024-06-10", as_of="2024-06-25"),
        ),
        (
            # Nothing announced after as_of is visible, so it is never read.
            "announcement_index",
            "announce_date",
            "2024-06-26",
            dict(start="2024-06-10", end="2024-06-30", as_of="2024-06-25"),
        ),
    ],
)
def test_partitions_outside_the_window_are_not_read(lake, dataset, column, outside, kwargs):
    _garbage(lake, dataset, column, outside)

    assert not load(dataset, config=lake, **kwargs).is_empty()
    with pytest.raises(Exception):  # noqa: B017 - any read of the bad file
        load(dataset, config=lake, as_of="2026-11-09")


def test_scan_window_follows_the_partition_key():
    assert pit_scan_window(
        "financial_statement_items", D(2024, 4, 1), D(2024, 6, 30), D(2025, 1, 1)
    ) == (
        D(2024, 4, 1),
        D(2024, 6, 30),
    )
    assert pit_scan_window("top_holders", None, D(2024, 6, 30), D(2025, 1, 1)) == (
        None,
        D(2024, 6, 30),
    )
    # A correction announced after `end` but by `as_of` replaces the row.
    assert pit_scan_window("announcement_index", D(2024, 6, 1), D(2024, 6, 30), D(2025, 1, 1)) == (
        D(2024, 6, 1),
        D(2025, 1, 1),
    )
    assert pit_scan_window(
        "announcement_index", D(2024, 6, 1), D(2024, 6, 30), D(2025, 1, 1), all_vintages=True
    ) == (D(2024, 6, 1), D(2024, 6, 30))


def test_scan_compares_report_period_labels(lake):
    periods = (
        scan("financial_statement_items", start="2024-05-15", end="2024-09-30", config=lake)
        .select(pl.col("report_period").unique().sort())
        .collect()["report_period"]
        .to_list()
    )

    assert periods == ["2024Q2", "2024Q3"]
