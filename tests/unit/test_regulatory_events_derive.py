"""regulatory_events is a projection of announcement_index, not a second fetch.

Both datasets used to issue the identical CNINFO request — same endpoint, same
day, no server-side filter — and keep the rows whose title matched a keyword.
Measured 2026-01-01: 46 pages and 1,375 announcements fetched twice to keep 6
events. The two fetches also ran an hour apart, so they could disagree about
the same day.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.derive.regulatory_events import (
    derive_regulatory_events,
    regulatory_events_from_announcements,
)
from cnequity.steps.macro_risk import step_regulatory_events
from cnequity.storage.state import StateStore

DAY = date(2024, 6, 28)


def _announcements(rows: list[tuple[str, str]], day: date = DAY) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "announcement_id": [identifier for identifier, _ in rows],
            "symbol": ["600519.SH"] * len(rows),
            "title": [title for _, title in rows],
            "announce_date": [day] * len(rows),
        }
    )


@pytest.mark.parametrize(
    ("title", "event_type"),
    [
        ("关于收到行政处罚决定书的公告", "penalty"),
        ("关于收到中国证监会处罚决定的公告", "penalty"),
        ("关于收到立案告知书的公告", "investigation"),
        ("关于收到监管函的公告", "regulatory_letter"),
        ("关于收到警示函的公告", "warning_letter"),
        ("关于公司董事被公开处分的公告", "disciplinary"),
    ],
)
def test_a_title_is_classified_by_its_first_matching_keyword(title, event_type):
    events = regulatory_events_from_announcements(_announcements([("A1", title)]))
    assert events["event_type"].to_list() == [event_type]


def test_ordinary_disclosures_are_not_events():
    events = regulatory_events_from_announcements(
        _announcements([("A1", "2024年半年度报告摘要"), ("A2", "关于召开股东大会的通知")])
    )
    assert events.is_empty()


def test_an_event_keeps_the_identity_of_the_filing_it_came_from():
    """`reg-<announcement_id>` is what keeps an event joinable to its filing."""
    events = regulatory_events_from_announcements(
        _announcements([("1225532884", "关于收到行政处罚决定书的公告")])
    )
    assert events["event_id"].to_list() == ["reg-1225532884"]
    assert events["event_date"].to_list() == [DAY]
    assert events.schema["event_date"] == pl.Date


def test_an_empty_input_keeps_the_dataset_schema():
    empty = regulatory_events_from_announcements(pl.DataFrame())
    assert empty.is_empty()
    assert empty.schema["event_date"] == pl.Date


def _seed_announcements(cfg: Config, rows: list[tuple[str, str]], day: date = DAY) -> None:
    part = cfg.curated_root / "announcement_index" / f"announce_date={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    frame = _announcements(rows, day).with_columns(
        pl.lit("其他").alias("category"),
        pl.lit("/a.pdf").alias("url"),
        pl.lit("cninfo").alias("source"),
        pl.lit("v1").alias("data_version"),
        pl.lit(datetime(2024, 6, 28, tzinfo=timezone.utc)).alias("fetched_at"),
    )
    frame.write_parquet(part / "part-merged.parquet")
    StateStore(cfg.meta_root).set_date("announcement_index", day)


def test_the_derive_reads_the_committed_announcements(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_announcements(cfg, [("A1", "关于收到监管函的公告"), ("A2", "半年度报告")])

    derived = derive_regulatory_events(cfg, start=DAY, end=DAY)
    assert derived.announcements == 2
    assert derived.events["event_id"].to_list() == ["reg-A1"]


def test_the_step_stages_events_without_a_single_request(tmp_path, monkeypatch):
    """The point of the change: no CNINFO call is made for this dataset."""
    import httpx

    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    _seed_announcements(cfg, [("A1", "关于收到行政处罚决定书的公告")])

    def _no_network(*args, **kwargs):
        raise AssertionError("regulatory_events must not fetch")

    monkeypatch.setattr(httpx.Client, "post", _no_network)
    monkeypatch.setattr(httpx.Client, "get", _no_network)

    result = step_regulatory_events(cfg, DAY, "run-derive-reg", {})
    assert result["rows_written"] == 1
    assert result["announcements_scanned"] == 1

    staged = list((cfg.staging_root / "regulatory_events").rglob("*.parquet"))
    assert len(staged) == 1
    rows = pl.read_parquet(staged[0])
    assert rows["event_id"].to_list() == ["reg-A1"]
    # The evidence is still CNINFO's; only the path to it changed.
    assert rows["source"].to_list() == ["cninfo"]


def test_a_window_the_announcements_do_not_cover_yet_is_reported(tmp_path):
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    _seed_announcements(cfg, [("A1", "关于收到监管函的公告")], day=date(2024, 6, 20))

    result = step_regulatory_events(cfg, DAY, "run-behind", {})

    assert result["status"] == "degraded"
    assert result["window"]["end"] == "2024-06-20"
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "pending_source_coverage"
    assert finding["covered_through"] == "2024-06-20"


def test_deriving_from_nothing_is_refused(tmp_path):
    """An empty window is only an answer when there was something to search."""
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    StateStore(cfg.meta_root).set_date("announcement_index", DAY)

    with pytest.raises(RuntimeError, match="no rows in 2024"):
        step_regulatory_events(cfg, DAY, "run-empty", {})


def test_without_any_announcement_coverage_the_step_says_so(tmp_path):
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)

    with pytest.raises(RuntimeError, match="no coverage yet"):
        step_regulatory_events(cfg, DAY, "run-bare", {})
