"""TDX share-change events as share_structure's peer and backup."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl

from cnequity.adapters.tdx_protocol.corporate_actions import share_events_from_xdxr
from cnequity.config import Config
from cnequity.steps import share_peer

DAY = date(2026, 10, 30)


def _events(rows):
    return pl.DataFrame(
        rows,
        schema={
            "symbol": pl.Utf8,
            "change_date": pl.Date,
            "total_shares": pl.Float64,
            "float_a_shares": pl.Float64,
        },
        orient="row",
    )


def test_xdxr_share_events_are_counts_after_each_change():
    pdf = pl.DataFrame(
        {
            "year": [2026, 2026, 2026],
            "month": [5, 5, 6],
            "day": [28, 28, 1],
            "category": [1, 5, 5],
            "panhouliutong": [None, 125008.15625, 125000.0],
            "houzongguben": [None, 125008.15625, 125000.0],
        }
    )

    events = share_events_from_xdxr("600519.SH", pdf)

    # The dividend row carries no counts; 万股 become shares, rounded.
    assert events == [
        {
            "symbol": "600519.SH",
            "change_date": date(2026, 5, 28),
            "total_shares": 1250081562.0,
            "float_a_shares": 1250081562.0,
        },
        {
            "symbol": "600519.SH",
            "change_date": date(2026, 6, 1),
            "total_shares": 1250000000.0,
            "float_a_shares": 1250000000.0,
        },
    ]


def test_compare_names_differences_tdx_only_events_and_a_only_companies():
    events = _events(
        [
            ("600519.SH", date(2026, 9, 1), 1.25e9, 1.25e9),
            ("600188.SH", date(2026, 9, 1), 1.0e10, 5.96e9),
            ("000001.SZ", date(2026, 9, 1), 2.0e10, 1.9e10),
            ("000002.SZ", date(2026, 9, 2), 9.7e9, 9.7e9),
        ]
    )
    stored = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600188.SH", "000001.SZ"],
            "change_date": [date(2026, 9, 1)] * 3,
            "total_shares": [1.25e9, 1.0e10, 2.1e10],
            "float_shares": [1.25e9, 1.0e10, 1.9e10],
            "float_a_shares": [None, 5.96e9, None],
        }
    )

    finding, tdx_only, a_only = share_peer.compare(events, stored)

    assert finding["check"] == "share_structure_peer"
    assert finding["severity"] == "warning"
    assert finding["compared"] == 3
    assert finding["total_differ"] == ["000001.SZ@2026-09-01"]
    assert finding["float_a_differ"] == []
    assert tdx_only["symbol"].to_list() == ["000002.SZ"]
    # 600188's float counts its H shares, so TDX's A-only figure cannot stand in.
    assert a_only == {"600519.SH", "000001.SZ"}


def test_compare_sets_aside_restatements_and_changes_dated_apart():
    events = _events(
        [
            # Period-end refresh: the counts in effect since August, off by float32 rounding.
            ("300891.SZ", date(2026, 9, 30), 400008438.0, 333474375.0),
            # The same buyback EastMoney dates a day later.
            ("002935.SZ", date(2026, 8, 7), 421837188.0, 415090898.0),
            # Same-dated, but TDX's total is EastMoney's for the next day.
            ("688981.SH", date(2026, 9, 9), 8.5612e9, 1.9996e9),
            # A real unlock EastMoney has not recorded.
            ("300572.SZ", date(2026, 9, 30), 228988809.0, 225715215.0),
        ]
    )
    stored = pl.DataFrame(
        {
            "symbol": ["300891.SZ", "002935.SZ", "688981.SH", "688981.SH", "300572.SZ"],
            "change_date": [
                date(2026, 8, 14),
                date(2026, 8, 8),
                date(2026, 8, 31),
                date(2026, 9, 9),
                date(2026, 6, 30),
            ],
            "total_shares": [400008440.0, 421837182.0, 8.5612e9, 8.5623e9, 228988812.0],
            "float_shares": [333474370.0, 415090883.0, 8.014e9, 8.0141e9, 181339690.0],
            "float_a_shares": [None, 415090883.0, 1.9996e9, 1.9996e9, 181339690.0],
        }
    )

    finding, tdx_only, _ = share_peer.compare(events, stored)

    assert finding["restated"] == 1
    assert finding["date_shifted"] == ["002935.SZ@2026-08-07", "688981.SH@2026-09-09"]
    assert finding["total_differ"] == []
    assert finding["severity"] == "info"
    assert tdx_only["symbol"].to_list() == ["300572.SZ"]


def test_stored_prefers_the_later_fetch_of_a_restated_row(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    part = cfg.curated_root / "share_structure" / "change_date=2026"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["002134.SZ", "002134.SZ"],
            "change_date": [date(2026, 10, 9)] * 2,
            "total_shares": [248479170.0, 248288987.0],
            "float_shares": [247366229.0, 247176046.0],
            "announce_date": [date(2026, 10, 14)] * 2,
            "fetched_at": [
                datetime(2026, 10, 28, tzinfo=timezone.utc),
                datetime(2026, 10, 16, tzinfo=timezone.utc),
            ],
        }
    ).write_parquet(part / "part-merged.parquet")

    stored = share_peer._stored(cfg, ["002134.SZ"])

    assert stored["total_shares"].to_list() == [248479170.0]


def test_backup_rows_are_disclosed_as_of_the_sweep_and_keep_unknowns_null():
    events = _events(
        [
            ("600519.SH", date(2026, 9, 1), 1.25e9, 1.25e9),
            ("600188.SH", date(2026, 9, 1), 1.0e10, 5.96e9),
        ]
    )

    rows = share_peer.backup_rows(events, {"600519.SH"}, DAY).sort("symbol")

    assert rows["announce_date"].to_list() == [DAY, DAY]
    assert rows["float_shares"].to_list() == [None, 1.25e9]
    assert rows["float_a_shares"].to_list() == [5.96e9, 1.25e9]
    assert rows["restricted_shares"].null_count() == 2


def _stored_lake(cfg, fetched_at):
    part = cfg.curated_root / "share_structure" / "change_date=2026"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "change_date": [date(2026, 9, 1)],
            "total_shares": [1.25e9],
            "float_shares": [1.25e9],
            "announce_date": [date(2026, 8, 30)],
            "source": ["eastmoney"],
            "fetched_at": [fetched_at],
        }
    ).write_parquet(part / "part-merged.parquet")


def _sweep(cfg, monkeypatch, written):
    from cnequity.adapters.tdx_protocol import corporate_actions as tdx

    backup = _events([("000002.SZ", date(2026, 9, 2), 9.7e9, 9.7e9)])
    monkeypatch.setattr(tdx, "fetch_share_events_tdx", lambda *a, **k: backup)
    monkeypatch.setattr(
        "cnequity.steps.http_common.write_fetched",
        lambda c, r, d, frame, **k: written.append(frame) or {"rows_written": frame.height},
    )
    monkeypatch.setattr(cfg, "should_archive_raw", lambda d: False, raising=False)
    events = [
        {
            "symbol": "600519.SH",
            "change_date": date(2026, 9, 1),
            "total_shares": 1.25e9,
            "float_a_shares": 1.25e9,
        },
        {
            "symbol": "000002.SZ",
            "change_date": date(2026, 9, 2),
            "total_shares": 9.7e9,
            "float_a_shares": 9.7e9,
        },
    ]
    return share_peer.share_structure_from_sweep(cfg, "run", DAY, events)


def test_a_fresh_eastmoney_gets_a_peer_check_and_no_backup(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    _stored_lake(cfg, datetime.now(timezone.utc))
    written: list[pl.DataFrame] = []

    findings = _sweep(cfg, monkeypatch, written)

    assert [f["check"] for f in findings] == ["share_structure_peer"]
    assert written == []


def test_a_stale_eastmoney_is_backed_up_with_the_tdx_only_events(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    _stored_lake(cfg, datetime.now(timezone.utc) - timedelta(days=10))
    written: list[pl.DataFrame] = []

    findings = _sweep(cfg, monkeypatch, written)

    assert [f["check"] for f in findings] == ["share_structure_peer", "share_structure_backup"]
    rows = pl.concat(written)
    assert rows.select("symbol", "change_date", "announce_date", "source").rows() == [
        ("000002.SZ", date(2026, 9, 2), DAY, "tdx_protocol")
    ]


def test_the_sql_view_reads_float_a_shares_as_null_on_older_files(tmp_path):
    from cnequity.query.views import views_connection

    cfg = Config(data_root=tmp_path / "lake")
    _stored_lake(cfg, datetime(2026, 9, 1, tzinfo=timezone.utc))
    part = cfg.curated_root / "share_structure" / "change_date=2026" / "part-merged.parquet"
    pl.read_parquet(part).with_columns(pl.lit("v1").alias("data_version")).write_parquet(part)

    con = views_connection(cfg)
    assert con.execute("SELECT symbol, float_a_shares FROM share_structure").fetchall() == [
        ("600519.SH", None)
    ]
