"""The exchange board's daily read as historical ST evidence.

A lake that watches the North Exchange every session accumulates exactly the
evidence a research claim needs — one row per symbol per session, from the
exchange itself — but until this it was invisible to the gate, which only read
receipts written by the historical sweeps. A lake could observe BJ for a year
and still be told its BJ status was unverifiable.
"""

from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.quality.st_coverage import (
    BSE_ST_SOURCE,
    bse_st_observed_window,
    publish_bse_st_observation_receipt,
    st_evidence_source_symbols,
    st_evidence_unsupported_symbols,
)
from cnequity.storage.layout import init_data_layout

BJ = ["920001.BJ", "920002.BJ"]
SESSIONS = [date(2026, 9, 16), date(2026, 9, 17), date(2026, 9, 18)]
FETCHED = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)


def _write(root, partition: str, rows: list[dict]) -> None:
    target = root / partition if partition else root
    target.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target / "part-0.parquet")


def _lake(tmp_path, *, answered: dict[date, list[str]]) -> Config:
    """A lake where every BJ name traded every session, answered as given."""
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    # `current_st_universe` reads instruments: without it the lake has no
    # universe to certify and the publisher has nothing to say.
    _write(
        cfg.curated_root / "instruments",
        "",
        [
            {
                "symbol": symbol,
                "name": f"BJ-{symbol[:6]}",
                "exchange": "BJ",
                "asset_type": "stock",
                "list_date": date(2021, 11, 15),
                "delist_date": None,
                "prev_symbol": None,
                "source": "bse",
                "data_version": "v1",
                "fetched_at": FETCHED,
            }
            for symbol in BJ
        ],
    )
    for session in SESSIONS:
        _write(
            cfg.curated_root / "daily_bars",
            f"trade_date={session.isoformat()}",
            [
                {
                    "symbol": symbol,
                    "trade_date": session,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "volume": 1000,
                    "amount": 10_500.0,
                    "source": "tdx_protocol",
                    "data_version": "v1",
                    "fetched_at": FETCHED,
                }
                for symbol in BJ
            ],
        )
        rows = [
            {
                "symbol": symbol,
                "trade_date": session,
                "is_trading": True,
                "status": "normal",
                "risk_warning": False,
                "source": BSE_ST_SOURCE,
                "data_version": "v1",
                "fetched_at": FETCHED,
            }
            for symbol in answered.get(session, [])
        ]
        if rows:
            _write(
                cfg.curated_root / "trading_status",
                f"trade_date={session.isoformat()}",
                rows,
            )
    return cfg


def test_the_window_is_the_run_of_sessions_answered_for_every_name(tmp_path):
    cfg = _lake(tmp_path, answered={s: BJ for s in SESSIONS})

    assert bse_st_observed_window(cfg, BJ) == (SESSIONS[0], SESSIONS[-1])


def test_a_session_answered_for_only_some_names_ends_the_window(tmp_path):
    """343 of 344 proves nothing about the 344th, and the run stops there.

    Drawing the window straight through would claim a status nobody observed —
    the real 2026-09-16 came back one name short.
    """
    cfg = _lake(
        tmp_path,
        answered={SESSIONS[0]: BJ, SESSIONS[1]: BJ[:1], SESSIONS[2]: BJ},
    )

    assert bse_st_observed_window(cfg, BJ) == (SESSIONS[2], SESSIONS[2])


def test_a_lake_that_never_read_the_board_has_no_window(tmp_path):
    cfg = _lake(tmp_path, answered={})

    assert bse_st_observed_window(cfg, BJ) is None


def test_bj_is_supported_inside_the_observed_window_and_not_before(tmp_path):
    """The deep-history answer and the recent one must stay distinguishable."""
    cfg = _lake(tmp_path, answered={s: BJ for s in SESSIONS})

    inside = st_evidence_unsupported_symbols(BJ, config=cfg, start=SESSIONS[1], end=SESSIONS[2])
    earlier = st_evidence_unsupported_symbols(
        BJ, config=cfg, start=date(2016, 1, 4), end=SESSIONS[2]
    )

    assert inside == [], "observed sessions are evidence"
    assert earlier == sorted(BJ), "the board cannot answer for a date it never saw"
    assert st_evidence_source_symbols(
        BJ, BSE_ST_SOURCE, config=cfg, start=SESSIONS[1], end=SESSIONS[2]
    ) == sorted(BJ)
    assert (
        st_evidence_source_symbols(
            BJ, BSE_ST_SOURCE, config=cfg, start=date(2016, 1, 4), end=SESSIONS[2]
        )
        == []
    )


def test_the_observation_becomes_a_receipt(tmp_path):
    import json

    cfg = _lake(tmp_path, answered={s: BJ for s in SESSIONS})

    path = publish_bse_st_observation_receipt(cfg)

    assert path is not None
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert receipt["status"] == "complete"
    assert receipt["scope"]["source"] == BSE_ST_SOURCE
    assert receipt["scope"]["start"] == SESSIONS[0].isoformat()
    assert receipt["scope"]["end"] == SESSIONS[-1].isoformat()
    assert receipt["completed_symbols"] == sorted(BJ)


def test_no_observation_publishes_no_receipt(tmp_path):
    cfg = _lake(tmp_path, answered={})

    assert publish_bse_st_observation_receipt(cfg) is None


def test_the_lake_reports_the_window_it_can_back(tmp_path):
    """A gate that only says "not the whole history" tells a new lake nothing.

    Every install that keeps BJ and buys no vendor fails the full-history
    question forever, so the verdict alone is a permanently red light. The
    supported window is the part that grows: it is what the evidence covers
    today, and a research window inside it is backed.
    """
    from cnequity.quality.st_coverage import (
        publish_bse_st_observation_receipt,
        st_evidence_supported_window,
    )

    cfg = _lake(tmp_path, answered={s: BJ for s in SESSIONS})
    publish_bse_st_observation_receipt(cfg)

    supported = st_evidence_supported_window(cfg, universe="all_a")

    assert supported["window"] == {
        "start": SESSIONS[0].isoformat(),
        "end": SESSIONS[-1].isoformat(),
    }
    assert supported["by_source"][BSE_ST_SOURCE]["start"] == SESSIONS[0].isoformat()


def test_a_source_with_no_receipt_means_no_window_and_says_which(tmp_path):
    """Naming the source turns "no window" into something to act on."""
    from cnequity.quality.st_coverage import st_evidence_supported_window

    cfg = _lake(tmp_path, answered={})

    supported = st_evidence_supported_window(cfg, universe="all_a")

    assert supported["window"] is None
    assert supported["missing_source"] == BSE_ST_SOURCE


def test_every_source_interval_is_reported_even_when_one_cannot_back_its_symbols(tmp_path):
    """A gap in one source must not hide the evidence the others hold.

    The loop returned at the first source with no covering receipt, and `bse`
    sorts last: a lake holding a complete 349/349 BJ receipt was told
    "可背书窗口：无" with nothing said about BJ at all. The verdict is
    unchanged — every source backs its own symbols or there is no window — but
    naming what each one does cover is what makes it actionable.
    """
    from cnequity.quality.st_coverage import st_evidence_supported_window

    cfg = _lake(tmp_path, answered={s: BJ for s in SESSIONS})
    publish_bse_st_observation_receipt(cfg)
    # A SH name with no baostock receipt anywhere: that source cannot answer.
    # Appended beside the BJ rows rather than replacing them, and given a bar,
    # because the universe is instruments seen trading.
    pl.DataFrame(
        [
            {
                "symbol": "600519.SH",
                "name": "SH-600519",
                "exchange": "SH",
                "asset_type": "stock",
                "list_date": date(2001, 8, 27),
                "delist_date": None,
                "prev_symbol": None,
                "source": "tdx_protocol",
                "data_version": "v1",
                "fetched_at": FETCHED,
            }
        ]
    ).write_parquet(cfg.curated_root / "instruments" / "part-1.parquet")
    for session in SESSIONS:
        pl.DataFrame(
            [
                {
                    "symbol": "600519.SH",
                    "trade_date": session,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "volume": 1000,
                    "amount": 10_500.0,
                    "source": "tdx_protocol",
                    "data_version": "v1",
                    "fetched_at": FETCHED,
                }
            ]
        ).write_parquet(
            cfg.curated_root / "daily_bars" / f"trade_date={session.isoformat()}" / "part-1.parquet"
        )

    supported = st_evidence_supported_window(cfg, universe="all_a")

    assert supported["window"] is None
    assert supported["missing_source"] == "baostock"
    assert supported["by_source"][BSE_ST_SOURCE] == {
        "start": SESSIONS[0].isoformat(),
        "end": SESSIONS[-1].isoformat(),
    }
