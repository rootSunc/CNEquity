"""The ths_official daily-bar adapter and the deep-history repair guard."""

from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from cnequity.adapters.ths_official.bars import (
    HISTORY_FLOOR,
    MAX_WINDOW_DAYS,
    fetch_daily_bars,
    split_windows,
)

CST = timezone(timedelta(hours=8))


def _ms(value: date) -> int:
    return int(datetime(value.year, value.month, value.day, tzinfo=CST).timestamp() * 1000)


class _StubClient:
    def __init__(self, items=None, fail=False):
        self.items = items if items is not None else []
        self.fail = fail
        self.calls = []

    def get(self, path, **params):
        self.calls.append(params)
        if self.fail:
            raise RuntimeError("Unknown thscode")
        return {"item": self.items}


def _bar(day=date(2010, 6, 1), close=10.0):
    return {
        "date_ms": _ms(day),
        "open_price": 9.5,
        "high_price": 10.5,
        "low_price": 9.4,
        "close_price": close,
        "volume": 1_234_500.0,
        "turnover": 12_000_000.0,
    }


def test_a_span_over_ten_years_is_cut_into_accepted_windows():
    """The endpoint answers code=1003 for a wider span, so split before sending."""
    windows = split_windows(date(2005, 1, 1), date(2015, 12, 31))
    assert len(windows) == 2
    assert windows[0][0] == date(2005, 1, 1)
    assert windows[-1][1] == date(2015, 12, 31)
    assert all((stop - begin).days <= MAX_WINDOW_DAYS for begin, stop in windows)
    # Contiguous, no gap and no overlap.
    assert windows[1][0] == windows[0][1] + timedelta(days=1)


def test_only_unadjusted_bars_are_ever_requested():
    """The lake derives hfq from sina factors; a vendor's own adjustment breaks it."""
    client = _StubClient([_bar()])
    fetch_daily_bars(["600519.SH"], date(2010, 1, 1), date(2010, 12, 31), client=client)
    assert {call["adjust"] for call in client.calls} == {"none"}


def test_volume_and_turnover_are_taken_as_given():
    """Both measured at exactly 1.0000 against curated — 股 and yuan, no conversion."""
    frame, counters = fetch_daily_bars(
        ["600519.SH"], date(2010, 1, 1), date(2010, 12, 31), client=_StubClient([_bar()])
    )
    row = frame.to_dicts()[0]
    assert row["volume"] == 1_234_500
    assert row["amount"] == pytest.approx(12_000_000.0)
    assert counters["bars"] == 1


def test_a_refused_security_is_counted_not_raised():
    """594 of the lake's securities are delisted and answer code=1002."""
    frame, counters = fetch_daily_bars(
        ["600213.SH"], date(2010, 1, 1), date(2010, 12, 31), client=_StubClient(fail=True)
    )
    assert frame.is_empty()
    assert counters["failed"] == 1


def test_duplicate_dates_across_windows_collapse():
    day = date(2010, 6, 1)
    frame, _ = fetch_daily_bars(
        ["600519.SH"],
        date(2005, 1, 1),
        date(2015, 12, 31),
        client=_StubClient([_bar(day), _bar(day)]),
    )
    assert frame.height == 1


def test_the_repair_defaults_to_a_dry_run(tmp_path):
    """Switching an existing canonical owner is never automatic (ADR-0005)."""
    import inspect

    from cnequity.steps.bars import repair_deep_history_ths_official

    signature = inspect.signature(repair_deep_history_ths_official)
    assert signature.parameters["dry_run"].default is True


def test_the_repair_never_reaches_below_the_service_floor():
    """949,815 rows predate 2005 and have no licensed counterpart; leave them."""
    assert HISTORY_FLOOR == date(2005, 1, 1)
    windows = split_windows(HISTORY_FLOOR, date(2015, 12, 31))
    assert windows[0][0] == HISTORY_FLOOR


def _snapshot(store, source, rows):
    from cnequity.domain.schemas import data_version_for, with_provenance

    frame = pl.DataFrame(
        rows, schema={"symbol": pl.Utf8, "trade_date": pl.Date, "close": pl.Float64}
    )
    for column, dtype in (
        ("open", pl.Float64),
        ("high", pl.Float64),
        ("low", pl.Float64),
        ("volume", pl.Int64),
        ("amount", pl.Float64),
    ):
        frame = frame.with_columns(pl.col("close").cast(dtype).alias(column))
    store.write(
        "daily_bars",
        with_provenance(frame, source=source, data_version=data_version_for("daily_bars")),
        source=source,
        data_version=data_version_for("daily_bars"),
        run_id=f"run-{source}",
    )


def test_arbitration_names_the_side_a_third_source_supports(tmp_path):
    """A binary disagreement gives the revision gate nothing to decide with."""
    from cnequity.config import Config
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.quality.cross_checks import daily_bars_arbitration_findings
    from cnequity.storage.source_snapshots import SnapshotStore

    config = Config(data_root=tmp_path)
    day = date(2026, 6, 1)
    curated = tmp_path / "curated" / "daily_bars" / f"trade_date={day.isoformat()}"
    curated.mkdir(parents=True)
    primary = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [day, day],
            "open": [10.0, 20.0],
            "high": [10.0, 20.0],
            "low": [10.0, 20.0],
            "close": [10.0, 20.0],
            "volume": [1, 1],
            "amount": [10.0, 20.0],
        }
    )
    with_provenance(
        primary, source="tdx_protocol", data_version=data_version_for("daily_bars")
    ).write_parquet(curated / "part-merged.parquet")

    store = SnapshotStore(config.meta_root)
    # The backup disagrees on both names, well past the 10bps tolerance.
    _snapshot(
        store,
        "eastmoney",
        {"symbol": ["600519.SH", "000001.SZ"], "trade_date": [day, day], "close": [11.0, 22.0]},
    )
    # The third source sides with the primary on one and the backup on the other.
    _snapshot(
        store,
        "ths_official",
        {"symbol": ["600519.SH", "000001.SZ"], "trade_date": [day, day], "close": [10.0, 22.0]},
    )

    finding = daily_bars_arbitration_findings(config)[0]
    assert finding["disputed"] == 2
    assert finding["arbitrated"] == 2
    assert finding["supports_primary"] == 1
    assert finding["supports_backup"] == 1
    assert finding["severity"] == "warning"


def test_arbitration_is_silent_without_a_third_source(tmp_path):
    from cnequity.config import Config
    from cnequity.quality.cross_checks import daily_bars_arbitration_findings

    assert daily_bars_arbitration_findings(Config(data_root=tmp_path)) == []


def _joined(rows):
    return pl.DataFrame(
        rows,
        schema={
            "symbol": pl.Utf8,
            "trade_date": pl.Date,
            "close": pl.Float64,
            "close_peer": pl.Float64,
        },
    )


def test_a_dispute_the_third_source_backs_is_switched():
    from cnequity.steps.bars import _withhold_unbacked_disputes

    day = date(2010, 6, 1)
    joined = _joined(
        {"symbol": ["600519.SH"], "trade_date": [day], "close": [10.0], "close_peer": [11.0]}
    )
    frame = pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [day], "close": [11.0]})
    third = pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [day], "close": [11.0]})
    totals: dict = {}

    assert _withhold_unbacked_disputes(frame, joined, third, totals).height == 1
    assert totals["disputes_backed"] == 1
    assert totals["disputes_withheld"] == 0


def test_a_dispute_the_third_source_contradicts_is_withheld():
    """Taking the peer blindly would import 178 known regressions."""
    from cnequity.steps.bars import _withhold_unbacked_disputes

    day = date(2010, 6, 1)
    joined = _joined(
        {"symbol": ["600519.SH"], "trade_date": [day], "close": [10.0], "close_peer": [11.0]}
    )
    frame = pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [day], "close": [11.0]})
    third = pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [day], "close": [10.0]})
    totals: dict = {}

    assert _withhold_unbacked_disputes(frame, joined, third, totals).is_empty()
    assert totals["disputes_withheld"] == 1


def test_a_dispute_with_no_third_opinion_keeps_the_existing_value():
    """Silence is not consent: an unjudged dispute is not worth a provenance win."""
    from cnequity.steps.bars import _withhold_unbacked_disputes

    day = date(2010, 6, 1)
    joined = _joined(
        {"symbol": ["600519.SH"], "trade_date": [day], "close": [10.0], "close_peer": [11.0]}
    )
    frame = pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [day], "close": [11.0]})
    empty = pl.DataFrame(
        {"symbol": [], "trade_date": [], "close": []},
        schema={"symbol": pl.Utf8, "trade_date": pl.Date, "close": pl.Float64},
    )
    totals: dict = {}

    assert _withhold_unbacked_disputes(frame, joined, empty, totals).is_empty()
    assert totals["disputes_withheld"] == 1


def test_an_undisputed_row_switches_for_provenance_alone():
    from cnequity.steps.bars import _withhold_unbacked_disputes

    day = date(2010, 6, 1)
    joined = _joined(
        {"symbol": ["600519.SH"], "trade_date": [day], "close": [10.0], "close_peer": [10.0]}
    )
    frame = pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [day], "close": [10.0]})
    empty = pl.DataFrame(
        {"symbol": [], "trade_date": [], "close": []},
        schema={"symbol": pl.Utf8, "trade_date": pl.Date, "close": pl.Float64},
    )
    totals: dict = {}

    assert _withhold_unbacked_disputes(frame, joined, empty, totals).height == 1
    assert totals.get("disputes_withheld", 0) == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [("low_price", 11.0), ("high_price", 9.0), ("close_price", -1.0)],
)
def test_a_bar_whose_envelope_cannot_hold_its_prices_is_dropped(field, value):
    """The lake's schema refuses these; one bad row must not abort the sweep.

    Measured in the 2005-2015 re-source: two bars were inconsistent by a single
    tick. Dropped rather than repaired — widening the envelope would invent a
    price that never printed.
    """
    item = _bar()
    item[field] = value
    frame, counters = fetch_daily_bars(
        ["600519.SH"], date(2010, 1, 1), date(2010, 12, 31), client=_StubClient([item])
    )
    assert frame.is_empty()
    assert counters["impossible_candles"] == 1
    assert counters["bars"] == 0


def test_a_well_formed_bar_survives_the_envelope_check():
    frame, counters = fetch_daily_bars(
        ["600519.SH"], date(2010, 1, 1), date(2010, 12, 31), client=_StubClient([_bar()])
    )
    assert frame.height == 1
    assert counters["impossible_candles"] == 0


def test_etf_bars_use_the_fund_endpoint_and_send_no_adjust():
    """ETFs are refused by the A-share endpoint, and the fund one takes no adjust."""
    from cnequity.adapters.ths_official.bars import ETF_ENDPOINT

    client = _StubClient([_bar()])
    fetch_daily_bars(["510300.SH"], date(2024, 1, 1), date(2024, 12, 31), client=client, etf=True)
    assert client.calls
    # `_StubClient` records params only; the endpoint is asserted via the module
    # constant the adapter routes on.
    assert ETF_ENDPOINT == "/api/fund/market/historical"
    assert "adjust" not in client.calls[0]


def test_the_etf_window_stays_under_the_silent_empty_limit():
    """Over the limit the endpoint returns an empty list, not an error.

    Measured against 510300.SH ending 2025-12-31: 1,552 days gives 1,030 bars and
    1,644 days gives zero. An over-wide window therefore reads exactly like a
    fund with no history, which is why the cap is four years rather than the five
    the contract claims.
    """
    from cnequity.adapters.ths_official.bars import ETF_MAX_WINDOW_DAYS

    assert ETF_MAX_WINDOW_DAYS < 1552
    windows = split_windows(date(2016, 1, 1), date(2025, 12, 31), ETF_MAX_WINDOW_DAYS)
    assert all((stop - begin).days <= ETF_MAX_WINDOW_DAYS for begin, stop in windows)
    assert windows[0][0] == date(2016, 1, 1)
    assert windows[-1][1] == date(2025, 12, 31)


def test_an_unanswered_symbol_is_named_not_just_counted():
    """Absent from the frame is how "the vendor said nothing" and "the request
    never landed" look alike; only the second is a failed request."""

    class _PartlyFailing:
        def get(self, path, **params):
            if params.get("thscode", "").startswith("600519"):
                raise RuntimeError("transport failed — nodename nor servname provided")
            return {"item": []}

    frame, counters = fetch_daily_bars(
        ["600519.SH", "000001.SZ"],
        date(2010, 1, 4),
        date(2010, 1, 8),
        client=_PartlyFailing(),
    )

    assert frame.is_empty()
    assert counters["unanswered_symbols"] == ["600519.SH"]
    # The one that answered "nothing" is evidence; it must not be in there.
    assert "000001.SZ" not in counters["unanswered_symbols"]
    assert counters["empty"] == 1


def test_a_failed_request_is_not_counted_as_the_peer_lacking_the_rows(tmp_path, monkeypatch):
    """`only_curated` is the claim "the peer does not have these". A symbol
    whose request never landed says nothing of the kind, and counting it there
    inflated the number the operator reads before deciding to switch."""
    import polars as pl

    from cnequity.config import Config
    from cnequity.steps import bars as bars_mod
    from cnequity.storage.layout import init_data_layout

    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    cfg.sources.update({"ths_official": True})
    cfg.ths_official_api_key = "k"

    day = date(2010, 1, 4)
    curated = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [day, day],
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
            "volume": [1, 1],
            "amount": [1.0, 1.0],
        },
        schema_overrides={"trade_date": pl.Date, "volume": pl.Int64},
    )

    def _fetch(chunk, start, end, *, client, workers):
        # 600519 never answered; 000001 answered with nothing.
        from cnequity.adapters.ths_official.bars import _OUTPUT_SCHEMA

        return (
            pl.DataFrame(schema=_OUTPUT_SCHEMA),
            {
                "requests": 2,
                "empty": 1,
                "failed": 1,
                "bars": 0,
                "unanswered_symbols": ["600519.SH"],
            },
        )

    monkeypatch.setattr("cnequity.adapters.ths_official.bars.fetch_daily_bars", _fetch)
    monkeypatch.setattr(
        "cnequity.adapters.ths_official.client_from_config",
        lambda config: type(
            "C", (), {"close": lambda self: None, "get": lambda self, *a, **k: {}}
        )(),
    )

    captured = {}

    def _scan(*args, **kwargs):
        captured["scanned"] = True
        return curated.with_columns(pl.lit("ths").alias("source")).lazy()

    monkeypatch.setattr("cnequity.query.parquet_scan.scan_parquet_root", _scan)
    monkeypatch.setattr("cnequity.query.canonical.dedupe_lazy_by_primary_key", lambda lf, ds: lf)

    out = bars_mod.repair_deep_history_ths_official(cfg, "run-1", start=day, end=day, dry_run=True)

    assert out["unanswered"] == 1
    assert out["unanswered_symbols"] == ["600519.SH"]
    # Only the symbol that actually answered contributes to the claim.
    assert out["only_curated"] == 1


def test_every_chunk_gets_its_own_archive_receipt(tmp_path, monkeypatch):
    """A receipt is consumed by the publish it backs. One scope spanning the
    whole sweep was spent by the first chunk, and every request after it failed
    with "raw archive capture was already consumed"."""

    from cnequity.config import Config
    from cnequity.steps import fundamentals as fund

    cfg = Config(data_root=tmp_path / "data")
    cfg.sources.update({"ths_official": True})
    cfg.ths_official_api_key = "k"
    cfg.ths_official_backfill_enabled = True
    monkeypatch.setattr(Config, "should_archive_raw", lambda self, dataset: True, raising=False)

    scopes: list[str] = []
    monkeypatch.setattr(
        "cnequity.storage.raw_archive.begin_capture",
        lambda owner, dataset, run_id, *, source, request_scope: (
            scopes.append(request_scope) or "nonce"
        ),
    )
    monkeypatch.setattr("cnequity.storage.raw_archive.RawPayloadArchive", lambda *a, **k: object())
    monkeypatch.setattr(
        "cnequity.adapters.ths_official.ThsOfficialClient",
        lambda *a, **k: type("C", (), {"close": lambda self: None})(),
    )
    monkeypatch.setattr(
        "cnequity.adapters.ths_official.financials.fetch_statements",
        lambda chunk, start, end, **kw: ([], {"requests": len(chunk)}),
    )
    monkeypatch.setattr(
        fund,
        "_borrowable_announce_dates",
        lambda config, s, e: {(f"{i:06d}.SZ", "2016Q1") for i in range(5)},
    )

    fund.backfill_statement_gap_ths_official(
        cfg, "run-1", start=date(2016, 1, 1), end=date(2016, 12, 31), chunk_size=2
    )

    # Five symbols at two per chunk: three scopes, all distinct.
    assert len(scopes) == 3
    assert len(set(scopes)) == 3


def test_the_sector_sweep_reports_progress(caplog):
    """432 boards of requests printed nothing between "starting" and "done"."""
    import logging
    from datetime import date

    import polars as pl

    from cnequity.adapters.ths_official.sectors import HISTORY_FLOOR, fetch_sector_bars

    class _Client:
        def get(self, path, **params):
            return {"item": []}

    catalog = pl.DataFrame(
        {
            "thscode": [f"88{i:04d}.TI" for i in range(30)],
            "sector_code": [f"88{i:04d}" for i in range(30)],
            "sector_name": [f"board {i}" for i in range(30)],
            "board_type": ["industry"] * 30,
        }
    )

    with caplog.at_level(logging.INFO, logger="cnequity.adapters.ths_official.sectors"):
        _, counters = fetch_sector_bars(
            catalog, HISTORY_FLOOR, date(2026, 9, 16), client=_Client(), workers=1
        )

    lines = [r.message for r in caplog.records if "boards" in r.message]
    assert lines, "the sweep reported nothing"
    # A board the peer has no rows for is still progress through the sweep.
    assert counters["boards"] == 0
    assert "30/30 boards" in lines[-1]
