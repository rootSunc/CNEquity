from datetime import date, datetime, timezone

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.quality.bar_coverage import KEYS, missing_bar_keys

DAYS = [date(2026, 9, 14), date(2026, 9, 15)]


def test_per_security_gaps_include_wholly_absent_series_and_respect_spans():
    instruments = pl.DataFrame(
        {
            "symbol": ["600001.SH", "600002.SH", "600003.SH", "600004.SH"],
            "list_date": [DAYS[0], DAYS[0], DAYS[1], DAYS[0]],
            "delist_date": [None, None, None, DAYS[0]],
        }
    )
    bars = pl.DataFrame({"symbol": ["600001.SH"], "trade_date": [DAYS[1]]})
    status = pl.DataFrame(
        {
            "symbol": ["600001.SH"],
            "trade_date": [DAYS[0]],
            "is_trading": [False],
            "source": ["baostock"],
        }
    )
    gaps = missing_bar_keys(instruments, bars, status, DAYS)
    assert set(gaps.iter_rows()) == {
        ("600002.SH", DAYS[0]),
        ("600002.SH", DAYS[1]),
        ("600003.SH", DAYS[1]),
    }


def _status(flags: list[bool | None], sources: list[str] | None = None) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600001.SH"] * len(flags),
            "trade_date": [DAYS[0]] * len(flags),
            "is_trading": flags,
            "source": sources or (["baostock"] * len(flags)),
        },
        schema_overrides={"is_trading": pl.Boolean},
    )


@pytest.mark.parametrize(
    ("flags", "missing"),
    [
        # Two trusted sources disagreeing is a conflict: withhold the verdict.
        ([False, True], 1),
        # No trusted row states a halt at all.
        ([None], 1),
        # A trusted halt beside a row that says nothing. Silence is not
        # contradiction, so the halt stands.
        ([False, None], 0),
    ],
)
def test_only_a_trusted_disagreement_withholds_the_verdict(flags, missing):
    instruments = pl.DataFrame(
        {"symbol": ["600001.SH"], "list_date": [DAYS[0]], "delist_date": [None]},
        schema_overrides={"delist_date": pl.Date},
    )
    bars = pl.DataFrame(schema=KEYS)
    assert missing_bar_keys(instruments, bars, _status(flags), DAYS[:1]).height == missing


def test_an_untrusted_row_cannot_veto_a_vendors_explicit_halt():
    """The lake's own gap-derived inference must not outvote the source it was
    a stand-in for. The two land on the same key as a vendor backfill catches
    up with what the lake had already inferred."""
    instruments = pl.DataFrame(
        {"symbol": ["600001.SH"], "list_date": [DAYS[0]], "delist_date": [None]},
        schema_overrides={"delist_date": pl.Date},
    )
    bars = pl.DataFrame(schema=KEYS)
    status = _status([False, False], sources=["derived_bar_gap", "baostock"])

    assert missing_bar_keys(instruments, bars, status, DAYS[:1]).height == 0

    # The circular row on its own still proves nothing.
    alone = _status([False], sources=["derived_bar_gap"])
    assert missing_bar_keys(instruments, bars, alone, DAYS[:1]).height == 1


@pytest.mark.parametrize(
    ("source", "observed", "expected_missing"),
    [
        (None, None, 1),
        ("derived_bar_gap", None, 1),
        ("eastmoney", None, 1),
        ("eastmoney", datetime(2026, 9, 15, 8, tzinfo=timezone.utc), 1),
        ("eastmoney", datetime(2026, 9, 14, 6, tzinfo=timezone.utc), 1),
        ("eastmoney", datetime(2026, 9, 14, 7, tzinfo=timezone.utc), 0),
        ("baostock", None, 0),
    ],
)
def test_suspension_requires_independent_session_evidence(source, observed, expected_missing):
    instruments = pl.DataFrame(
        {"symbol": ["600001.SH"], "list_date": [DAYS[0]], "delist_date": [None]},
        schema_overrides={"delist_date": pl.Date},
    )
    status = pl.DataFrame(
        {
            "symbol": ["600001.SH"],
            "trade_date": [DAYS[0]],
            "is_trading": [False],
            "source": [source],
            "fetched_at": [observed],
        },
        schema_overrides={"source": pl.Utf8, "fetched_at": pl.Datetime("us", "UTC")},
    )
    assert (
        missing_bar_keys(instruments, pl.DataFrame(schema=KEYS), status, DAYS[:1]).height
        == expected_missing
    )


def test_verify_bars_exits_nonzero_for_unresolved_coverage(monkeypatch):
    monkeypatch.setattr("cnequity.cli.quality_cmds._cfg", lambda path: object())
    monkeypatch.setattr(
        "cnequity.quality.bar_coverage.daily_bar_coverage",
        lambda *args: {"complete": False, "unresolved_keys": 94},
    )
    result = CliRunner().invoke(
        cli, ["verify", "--bars", "--start", "2026-09-07", "--end", "2026-09-15"]
    )
    assert result.exit_code == 1
    assert '"unresolved_keys": 94' in result.output
    result = CliRunner().invoke(
        cli, ["verify", "--bars", "--start", "2026-09-16", "--end", "2026-09-15"]
    )
    assert result.exit_code != 0
    assert "--start must be" in result.output
    # The mode flags are the whole point of the merge: an option from another
    # mode has to be refused, not quietly ignored.
    result = CliRunner().invoke(cli, ["verify", "--bars", "--repair"])
    assert result.exit_code != 0
    assert "--repair belongs to" in result.output
    result = CliRunner().invoke(cli, ["verify", "--bars"])
    assert result.exit_code != 0
    assert "--bars needs --start" in result.output
