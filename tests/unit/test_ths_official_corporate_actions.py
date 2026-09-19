"""The adjustment-factor dump parser and the arbitration it enables.

Column names come from a dump downloaded 2026-09-09; the caliber limits are
measured facts, so they are asserted rather than assumed.
"""

from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from cnequity.adapters.ths_official.corporate_actions import (
    ThsOfficialDumpError,
    parse_adjustment_factor_dump,
)

CST = timezone(timedelta(hours=8))


def _ms(value: date) -> int:
    return int(datetime(value.year, value.month, value.day, tzinfo=CST).timestamp() * 1000)


def _dump(tmp_path, rows):
    path = tmp_path / "dump.parquet"
    pl.DataFrame(
        rows,
        schema={
            "thscode": pl.Utf8,
            "ticker": pl.Utf8,
            "ex_date_ms": pl.Int64,
            "dividend_per_share": pl.Float64,
            "per_share_bonus": pl.Float64,
            "allotment_ratio": pl.Float64,
            "allotment_price": pl.Float64,
            "currency": pl.Utf8,
        },
    ).write_parquet(path)
    return path


def _row(symbol="600519.SH", ex=date(2024, 6, 20), div=0.0, bonus=0.0, allot=None, price=None):
    return {
        "thscode": symbol,
        "ticker": symbol[:6],
        "ex_date_ms": _ms(ex),
        "dividend_per_share": div,
        "per_share_bonus": bonus,
        "allotment_ratio": allot,
        "allotment_price": price,
        "currency": "CNY",
    }


def test_one_dated_event_expands_to_one_row_per_action_type(tmp_path):
    out = parse_adjustment_factor_dump(_dump(tmp_path, [_row(div=2.5, bonus=0.4)]))
    assert set(out["action_type"].to_list()) == {"cash_dividend", "bonus"}
    by_type = dict(zip(out["action_type"].to_list(), out["cash_dividend"].to_list(), strict=True))
    assert by_type["cash_dividend"] == pytest.approx(2.5)


def test_transfer_ratio_is_never_inferred(tmp_path):
    """`per_share_bonus` is 送股 alone — 33 of 34 comparable events said so.

    Folding it into a transfer would manufacture a caliber the upstream does not
    report, and would over-state dilution on every plain bonus issue.
    """
    out = parse_adjustment_factor_dump(_dump(tmp_path, [_row(bonus=0.4)]))
    assert out["transfer_ratio"].null_count() == out.height
    assert out.filter(pl.col("action_type") == "bonus")["bonus_ratio"].to_list() == [0.4]


def test_allotment_survives_because_only_the_dump_carries_it(tmp_path):
    """The REST event stream has no allotment fields; the lake holds 1,164 events."""
    out = parse_adjustment_factor_dump(_dump(tmp_path, [_row(allot=0.3, price=5.5)]))
    row = out.filter(pl.col("action_type") == "allotment").to_dicts()[0]
    assert row["allotment_ratio"] == pytest.approx(0.3)
    assert row["allotment_price"] == pytest.approx(5.5)


def test_zero_amount_events_produce_no_rows(tmp_path):
    assert parse_adjustment_factor_dump(_dump(tmp_path, [_row()])).is_empty()


def test_ex_dates_are_read_in_shanghai_time(tmp_path):
    out = parse_adjustment_factor_dump(_dump(tmp_path, [_row(ex=date(2024, 1, 2), div=1.0)]))
    assert out["ex_date"].to_list() == [date(2024, 1, 2)]


def test_a_dump_missing_documented_columns_is_refused(tmp_path):
    path = tmp_path / "bad.parquet"
    pl.DataFrame({"thscode": ["600519.SH"]}).write_parquet(path)
    with pytest.raises(ThsOfficialDumpError, match="missing columns"):
        parse_adjustment_factor_dump(path)


def test_arbitration_is_silent_without_a_peer_snapshot(tmp_path):
    """A lake with no key keeps its existing sources and gains no findings."""
    from cnequity.config import Config
    from cnequity.quality.cross_checks import adj_factor_arbitration_findings

    assert adj_factor_arbitration_findings(Config(data_root=tmp_path)) == []


def _lake_with_one_confirmed_gap(tmp_path, ex_date: date):
    """A lake whose factor stepped on *ex_date* with no action, and a peer that has it."""
    from cnequity.config import Config
    from cnequity.storage.layout import init_data_layout
    from cnequity.storage.source_snapshots import SnapshotStore

    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    fetched = datetime(2026, 9, 19, tzinfo=timezone.utc)
    before = ex_date - timedelta(days=1)
    factors = cfg.derived_root / "adj_factors"
    for day, factor in ((before, 1.0), (ex_date, 1.2)):
        part = factors / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": ["600110.SH"],
                "trade_date": [day],
                "adjust_type": ["hfq"],
                "factor": [factor],
                "source": ["sina"],
                "data_version": ["v1"],
                "fetched_at": [fetched],
            }
        ).write_parquet(part / "part-0.parquet")
    # corporate_actions exists but says nothing about that date.
    actions = cfg.curated_root / "corporate_actions" / f"ex_date={before.year}"
    actions.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600110.SH"],
            "ex_date": [date(before.year, 1, 5)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [0.2],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
            "split_factor": [1.0],
            "source": ["tdx_protocol"],
            "data_version": ["v1"],
            "fetched_at": [fetched],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    ).write_parquet(actions / "part-0.parquet")
    SnapshotStore(cfg.meta_root).write(
        "corporate_actions",
        pl.DataFrame(
            {
                "symbol": ["600110.SH"],
                "ex_date": [ex_date],
                "action_type": ["cash_dividend"],
                "cash_dividend": [0.1],
                "bonus_ratio": [0.0],
                "transfer_ratio": [0.0],
                "allotment_ratio": [None],
                "allotment_price": [None],
                "source": ["ths_official"],
                "data_version": ["v1"],
                "fetched_at": [fetched],
            },
            schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
        ),
        source="ths_official",
        data_version="v1",
        run_id="peer-1",
    )
    return cfg


def test_arbitration_names_the_repair_for_a_pre_floor_gap(tmp_path):
    from cnequity.quality.cross_checks import adj_factor_arbitration_findings

    cfg = _lake_with_one_confirmed_gap(tmp_path, date(2004, 6, 10))

    (finding,) = adj_factor_arbitration_findings(cfg)

    assert finding["missing_recorded_action"] == 1
    assert finding["missing_recorded_action_reachable_dates"] == ["2004-06-10"]
    assert "--eastmoney-date-repair --ex-dates 2004-06-10" in finding["remediation"]
    assert finding["remediation"] in finding["message"], (
        "a hint nobody prints is a hint nobody runs"
    )


def test_a_gap_after_the_floor_gets_no_command_because_the_sweep_already_walked_it(tmp_path):
    """Pointing the repair at a date the normal sources already read would
    spend requests to confirm the source has nothing."""
    from cnequity.quality.cross_checks import adj_factor_arbitration_findings

    cfg = _lake_with_one_confirmed_gap(tmp_path, date(2025, 12, 29))

    (finding,) = adj_factor_arbitration_findings(cfg)

    assert finding["missing_recorded_action"] == 1
    assert finding["missing_recorded_action_reachable_dates"] == []
    assert finding["remediation"] == ""
