from datetime import date, datetime, timedelta, timezone

import polars as pl

from cnequity.config import Config
from cnequity.storage.staged_contracts import (
    bj_price_limit_breaches,
    bj_rows_missing_amount,
    enforce_daily_bar_contracts,
)

FETCHED = datetime(2026, 9, 30, tzinfo=timezone.utc)
LISTED = date(2023, 1, 3)


def _days(first: date, n: int) -> list[date]:
    out, day = [], first
    while len(out) < n:
        if day.weekday() < 5:
            out.append(day)
        day += timedelta(days=1)
    return out


def _bars(symbol: str, days: list[date], closes: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [symbol] * len(days),
            "trade_date": days,
            "close": closes,
            "volume": [100] * len(days),
            "amount": [1000.0] * len(days),
        }
    )


INSTRUMENTS = pl.DataFrame({"symbol": ["920001.BJ"], "list_date": [LISTED]})
NO_ACTIONS = pl.DataFrame(schema={"symbol": pl.Utf8, "ex_date": pl.Date})
DAYS = _days(date(2024, 3, 4), 6)


# Every weekday is a session in these fixtures.
CALENDAR = _days(date(2018, 1, 1), 2000)


def _breaches(staged, committed, actions=NO_ACTIONS, instruments=INSTRUMENTS):
    return bj_price_limit_breaches(staged, committed, actions, instruments, CALENDAR)


def test_a_move_past_the_limit_with_no_reason_is_a_breach():
    committed = _bars("920001.BJ", DAYS[:5], [10.0] * 5)
    staged = _bars("920001.BJ", DAYS[5:], [14.0])  # +40% on a 30% board
    assert _breaches(staged, committed)["trade_date"].to_list() == [DAYS[5]]
    # A limit-up close (+30%, rounded to the fen) is not.
    assert _breaches(_bars("920001.BJ", DAYS[5:], [13.0]), committed).is_empty()


def test_a_corporate_action_explains_the_move():
    committed = _bars("920001.BJ", DAYS[:5], [10.0] * 5)
    staged = _bars("920001.BJ", DAYS[5:], [5.0])
    actions = pl.DataFrame({"symbol": ["920001.BJ"], "ex_date": [DAYS[5]]})
    assert _breaches(staged, committed, actions).is_empty()


def test_listing_day_resumption_and_neeq_quotes_are_not_judged():
    # First exchange session: the previous close is a NEEQ quote.
    instruments = pl.DataFrame({"symbol": ["920001.BJ"], "list_date": [DAYS[5]]})
    committed = _bars("920001.BJ", DAYS[:5], [10.0] * 5)
    assert _breaches(
        _bars("920001.BJ", DAYS[5:], [20.0]), committed, instruments=instruments
    ).is_empty()
    # Resumption after a month-long halt.
    later = DAYS[4] + timedelta(days=30)
    assert _breaches(_bars("920001.BJ", [later], [20.0]), committed).is_empty()
    # Before the select tier opened there is no limit to hold a quote to.
    old = _days(date(2019, 3, 4), 2)
    old_instruments = pl.DataFrame({"symbol": ["920001.BJ"], "list_date": [date(2018, 1, 2)]})
    assert _breaches(
        _bars("920001.BJ", old[1:], [20.0]),
        _bars("920001.BJ", old[:1], [10.0]),
        instruments=old_instruments,
    ).is_empty()
    # A security the instruments table does not know is not judged either.
    assert _breaches(
        _bars("920002.BJ", DAYS[5:], [20.0]), _bars("920002.BJ", DAYS[:5], [10.0] * 5)
    ).is_empty()


def test_exchange_rows_without_turnover_are_reported():
    staged = _bars("920001.BJ", DAYS[:2], [10.0, 10.0]).with_columns(
        pl.Series("amount", [None, 1000.0], dtype=pl.Float64)
    )
    assert bj_rows_missing_amount(staged, INSTRUMENTS)["trade_date"].to_list() == [DAYS[0]]


def test_enforcement_quarantines_the_row_and_keeps_the_rest(tmp_path):
    cfg = Config(data_root=tmp_path)
    committed = cfg.curated_root / "daily_bars"
    # Monthly partitions, as daily_bars is stored: the market's sessions must
    # come from the calendar, not from day-named directories.
    for index, day in enumerate(DAYS[:5]):
        part = committed / f"trade_date={day:%Y-%m}"
        part.mkdir(parents=True, exist_ok=True)
        _bars("920001.BJ", [day], [10.0]).with_columns(
            pl.lit("tdx_protocol").alias("source"),
            pl.lit("v1").alias("data_version"),
            pl.lit(FETCHED).alias("fetched_at"),
        ).write_parquet(part / f"part-{index}.parquet")
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": ["920001.BJ"], "list_date": [LISTED], "name": ["x"], "exchange": ["BJ"]}
    ).write_parquet(instruments / "part-0.parquet")
    staged_dir = cfg.staging_root / "daily_bars" / "run_id=r1" / "trade_date=x"
    staged_dir.mkdir(parents=True)
    staged_file = staged_dir / "part-ths.parquet"
    pl.concat(
        [
            _bars("920001.BJ", DAYS[5:], [14.0]),
            _bars("600519.SH", DAYS[5:], [1500.0]),
        ]
    ).write_parquet(staged_file)

    findings = enforce_daily_bar_contracts(
        cfg,
        "r1",
        [staged_file],
        committed_root=committed,
        actions_root=None,
        instruments_root=instruments,
    )
    (finding,) = [f for f in findings if f["check"] == "daily_bars_bj_price_limit_quarantined"]
    assert finding["rows_rejected"] == 1
    assert pl.read_parquet(staged_file)["symbol"].to_list() == ["600519.SH"]
    assert list((tmp_path / "_quarantine").glob("daily_bars-r1-bj-limit-*/rejected.parquet"))


def test_a_move_across_a_session_the_lake_is_missing_is_not_judged():
    # 920826.BJ 2021-11-02: the lake lacks 11-01, so +49.5% is two sessions.
    committed = _bars("920001.BJ", DAYS[:4], [10.0] * 4)
    staged = _bars("920001.BJ", DAYS[5:], [14.9])
    assert _breaches(staged, committed).is_empty()


def _roster(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [f"92{i:04d}.BJ" for i in range(n)],
            "list_date": [LISTED] * n,
            "delist_date": [None] * n,
        },
        schema={"symbol": pl.Utf8, "list_date": pl.Date, "delist_date": pl.Date},
    )


def _status(symbols, day, status, source):
    return pl.DataFrame(
        {
            "symbol": symbols,
            "trade_date": [day] * len(symbols),
            "status": [status] * len(symbols),
            "source": [source] * len(symbols),
        }
    )


def test_a_session_missing_listed_names_is_short():
    from cnequity.storage.staged_contracts import bj_session_shortfall

    roster = _roster(100)
    day = DAYS[0]
    present = pl.DataFrame({"symbol": roster["symbol"][:90], "trade_date": [day] * 90})
    none = _status([], day, "normal", "bse")
    (row,) = bj_session_shortfall([day], present, roster, none).iter_rows(named=True)
    assert (row["expected"], row["missing"]) == (100, 10)
    # An independent suspension excuses a missing bar...
    bse = _status(roster["symbol"][90:].to_list(), day, "suspended", "bse")
    assert bj_session_shortfall([day], present, roster, bse).is_empty()
    # ...a suspension inferred from that very missing bar does not.
    derived = _status(roster["symbol"][90:].to_list(), day, "suspended", "derived_bar_gap")
    assert bj_session_shortfall([day], present, roster, derived).height == 1
    # A name or two short is roster drift, not a failed leg.
    nearly = pl.DataFrame({"symbol": roster["symbol"][:98], "trade_date": [day] * 98})
    assert bj_session_shortfall([day], nearly, roster, none).is_empty()


def test_a_run_whose_beijing_leg_failed_records_the_session(tmp_path):
    from cnequity.storage.state import StateStore

    cfg = Config(data_root=tmp_path)
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    _roster(20).with_columns(
        pl.lit("x").alias("name"), pl.lit("BJ").alias("exchange")
    ).write_parquet(instruments / "part-0.parquet")
    staged_dir = cfg.staging_root / "daily_bars" / "run_id=r2" / "trade_date=x"
    staged_dir.mkdir(parents=True)
    staged_file = staged_dir / "part-tdx.parquet"
    # Shanghai landed for the session; no Beijing row did.
    _bars("600519.SH", DAYS[:1], [1500.0]).write_parquet(staged_file)
    findings = enforce_daily_bar_contracts(
        cfg,
        "r2",
        [staged_file],
        committed_root=None,
        actions_root=None,
        instruments_root=instruments,
    )
    (finding,) = [f for f in findings if f["check"] == "daily_bars_bj_session_incomplete"]
    assert finding["sessions"][0]["missing"] == 20
    ranges = StateStore(cfg.meta_root).get_payload("daily_bars")["missing_ranges"]
    assert ranges[0]["start"] == DAYS[0].isoformat()
    assert ranges[0]["last_failure"] == "bj_session_incomplete"
