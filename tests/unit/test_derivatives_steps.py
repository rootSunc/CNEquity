"""Futures/option steps: opt-in, partial exchanges, resumable backfill, contracts."""

from __future__ import annotations

import dataclasses
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from cnequity.adapters.futures_exchange.cffex import parse_daily_csv
from cnequity.adapters.futures_exchange.common import ExchangeDay, FuturesDayUnavailable
from cnequity.config import Config
from cnequity.derive.derivative_contracts import build_futures_contracts, build_option_contracts
from cnequity.domain.datasets import is_dataset_enabled
from cnequity.run_scope import set_backfill, set_run_flags
from cnequity.steps import derivatives
from cnequity.storage.parquet import compact_dataset

FIXTURE = Path(__file__).parents[1] / "fixtures" / "futures" / "cffex_20260924.csv"


def _day(d: date) -> ExchangeDay:
    """The fixture file, re-dated: every session lists the same contracts."""
    parsed = parse_daily_csv(FIXTURE.read_bytes(), date(2026, 9, 24))
    redate = pl.lit(d).alias("trade_date")
    return ExchangeDay(
        "CFE", d, parsed.futures.with_columns(redate), parsed.options.with_columns(redate)
    )


@pytest.fixture
def cfg(tmp_path) -> Config:
    config = Config(data_root=tmp_path / "data")
    config.futures_enabled = True
    config.futures_exchanges = ["CFE"]
    return config


@pytest.fixture
def reader(monkeypatch):
    calls: list[date] = []
    missing: set[date] = set()

    def fetch_day(d, *, config=None):
        calls.append(d)
        if d in missing:
            raise FuturesDayUnavailable("not published")
        return _day(d)

    patched = dataclasses.replace(
        derivatives.READERS["CFE"], fetch_day=fetch_day, fetch_reference=None
    )
    monkeypatch.setitem(derivatives.READERS, "CFE", patched)
    return calls, missing


def _staged(cfg: Config, dataset: str) -> pl.DataFrame:
    files = sorted((cfg.staging_root / dataset).rglob("*.parquet"))
    return pl.concat([pl.read_parquet(p) for p in files], how="diagonal_relaxed")


def test_the_family_is_off_until_configured(tmp_path):
    config = Config(data_root=tmp_path / "data")
    assert not is_dataset_enabled("futures_bars", config)
    result = derivatives.step_futures_bars(config, date(2026, 9, 24), "run", {})
    assert result["rows_written"] == 0
    assert "disabled" in result["note"]
    config.futures_enabled = True
    config.futures_options = False
    assert is_dataset_enabled("futures_bars", config)
    assert not is_dataset_enabled("option_bars", config)


def test_daily_run_writes_the_reconciliation_tail(cfg, reader):
    calls, _missing = reader
    result = derivatives.step_futures_bars(cfg, date(2026, 9, 24), "run", {})
    staged = _staged(cfg, "futures_bars")
    # Three sessions: the lookback includes the run day.
    assert sorted(set(calls)) == [date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)]
    assert result["rows_written"] == staged.height == 7 * 3
    assert set(staged["source"]) == {"futures_exchange"}


def test_a_session_no_exchange_published_is_a_failed_day_not_a_crash(cfg, reader):
    _calls, missing = reader
    missing.add(date(2026, 9, 24))
    result = derivatives.step_futures_bars(cfg, date(2026, 9, 24), "run", {})
    staged = _staged(cfg, "futures_bars")
    assert date(2026, 9, 24) not in set(staged["trade_date"])
    checks = {f["check"] for f in result["context_updates"]["audit_findings"]}
    assert "futures_exchange_missing" in checks


def test_one_exchange_missing_leaves_the_others_written(cfg, monkeypatch, reader):
    """A second exchange that did not publish must not hold the first one back."""

    def never(d, *, config=None):
        raise FuturesDayUnavailable("not yet")

    monkeypatch.setitem(
        derivatives.READERS,
        "XXX",
        dataclasses.replace(derivatives.READERS["CFE"], exchange="XXX", fetch_day=never),
    )
    monkeypatch.setattr(
        "cnequity.adapters.futures_exchange.registry.SUPPORTED_EXCHANGES", ("CFE", "XXX")
    )
    cfg.futures_exchanges = []
    findings: list[dict] = []
    frame = derivatives.fetch_session(cfg, date(2026, 9, 24), "futures", findings=findings)
    assert frame.height == 7
    assert [f["exchange"] for f in findings] == ["XXX"]
    assert findings[0]["severity"] == "warning"


def test_backfill_walks_sessions_and_resumes_by_exchange(cfg, reader):
    calls, _missing = reader
    set_backfill(cfg, active=True, start=date(2026, 9, 21), end=date(2026, 9, 24))
    result = derivatives.step_option_bars(cfg, date(2026, 9, 24), "bf", {})
    assert result["rows_written"] == 5 * 4
    compact_dataset(
        cfg.staging_root,
        cfg.curated_root,
        "option_bars",
        "bf",
        partition_col="trade_date",
        granularity="day",
    )
    calls.clear()
    again = derivatives.step_option_bars(cfg, date(2026, 9, 24), "bf2", {})
    assert calls == []
    assert again["rows_written"] == 0


def test_backfill_never_asks_before_the_exchange_existed(cfg, reader):
    calls, _missing = reader
    set_backfill(cfg, active=True, start=date(2019, 12, 19), end=date(2019, 12, 24))
    derivatives.step_option_bars(cfg, date(2019, 12, 24), "bf", {})
    assert min(calls) == date(2019, 12, 23)


def _bars(rows: list[tuple[str, str, date]]) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "exchange": ["CFE"] * len(rows),
            "exchange_code": [r[1] for r in rows],
            "product": ["IF"] * len(rows),
            "trade_date": [r[2] for r in rows],
        }
    ).lazy()


def test_observation_bounds_never_claim_contractual_dates():
    bars = _bars(
        [
            # Already live on the first observed day: listing date unknown.
            ("IF2501.CFE", "IF2501", date(2025, 1, 2)),
            ("IF2501.CFE", "IF2501", date(2025, 1, 17)),
            # Listed and expired inside observed history.
            ("IF2503.CFE", "IF2503", date(2025, 1, 3)),
            ("IF2503.CFE", "IF2503", date(2025, 3, 21)),
            # Still live on the last observed day.
            ("IF2506.CFE", "IF2506", date(2025, 1, 20)),
            ("IF2506.CFE", "IF2506", date(2025, 3, 24)),
        ]
    )
    table = {r["symbol"]: r for r in build_futures_contracts(bars).to_dicts()}
    assert table["IF2501.CFE"]["dates_basis"] == "observed_truncated"
    assert table["IF2503.CFE"]["dates_basis"] == "observed"
    assert table["IF2503.CFE"]["list_date"] is None
    assert table["IF2503.CFE"]["first_seen_date"] == date(2025, 1, 3)
    assert table["IF2503.CFE"]["last_trade_date"] is None
    assert table["IF2503.CFE"]["last_seen_date"] == date(2025, 3, 21)
    assert table["IF2506.CFE"]["dates_basis"] == "open"
    assert table["IF2506.CFE"]["last_trade_date"] is None
    assert table["IF2506.CFE"]["multiplier"] == 300.0
    assert table["IF2506.CFE"]["delivery_month"] == date(2025, 6, 1)


def test_a_reference_file_names_a_live_contract_end():
    bars = _bars(
        [
            ("IF2506.CFE", "IF2506", date(2025, 1, 20)),
            ("IF2506.CFE", "IF2506", date(2025, 3, 24)),
        ]
    )
    reference = pl.DataFrame(
        {
            "symbol": ["IF2506.CFE"],
            "list_date": [date(2024, 10, 21)],
            "last_trade_date": [date(2025, 6, 20)],
        }
    )
    row = build_futures_contracts(bars, reference).row(0, named=True)
    assert row["dates_basis"] == "exchange"
    assert row["list_date"] == date(2024, 10, 21)
    assert row["last_trade_date"] == date(2025, 6, 20)


def test_option_contracts_carry_series_and_style():
    parsed = parse_daily_csv(FIXTURE.read_bytes(), date(2026, 9, 24))
    table = build_option_contracts(parsed.options.lazy())
    row = table.filter(pl.col("symbol") == "MO2610C6200.CFE").row(0, named=True)
    assert row["underlying_symbol"] == "000852.SH"
    assert row["underlying_kind"] == "index"
    assert row["exercise_style"] == "european"
    assert row["expiry_month"] == date(2026, 10, 1)
    assert row["multiplier"] == 100.0


def test_an_impossible_row_is_quarantined_not_the_session(cfg, monkeypatch):
    def fetch_day(d, *, config=None):
        day = _day(d)
        broken = day.futures.with_columns(
            pl.when(pl.col("symbol") == "IF2610.CFE")
            .then(-1)
            .otherwise(pl.col("open_interest"))
            .alias("open_interest")
        )
        return ExchangeDay("CFE", d, broken, day.options)

    monkeypatch.setitem(
        derivatives.READERS,
        "CFE",
        dataclasses.replace(derivatives.READERS["CFE"], fetch_day=fetch_day),
    )
    findings: list[dict] = []
    frame = derivatives.fetch_session(cfg, date(2026, 9, 24), "futures", findings=findings)
    assert frame.height == 6
    assert "IF2610.CFE" not in frame["symbol"].to_list()
    assert [f["check"] for f in findings] == ["futures_row_rejected"]


@pytest.mark.parametrize("listed", [True, False])
def test_a_listed_exchange_anomaly_is_quarantined_without_owing_the_session(
    cfg, monkeypatch, listed
):
    """INE kx20210728.dat publishes LU2108 with a close above its high; refetching
    serves the same bytes, so a listed row must not keep the session owed."""
    from cnequity.storage.derivative_evidence import read_json, receipt_path

    day = date(2026, 9, 24)

    def fetch_day(d, *, config=None):
        base = _day(d)
        broken = base.futures.with_columns(
            pl.when(pl.col("symbol") == "IF2610.CFE")
            .then(pl.col("high") + 100)
            .otherwise(pl.col("close"))
            .alias("close")
        )
        return ExchangeDay("CFE", d, broken, base.options)

    monkeypatch.setitem(
        derivatives.READERS,
        "CFE",
        dataclasses.replace(derivatives.READERS["CFE"], fetch_day=fetch_day),
    )
    known = {day: frozenset({"IF2610.CFE"})} if listed else {}
    monkeypatch.setattr(derivatives, "PUBLISHED_FUTURES_ANOMALIES", known)
    findings: list[dict] = []
    frame = derivatives.fetch_session(cfg, day, "futures", findings=findings)

    assert "IF2610.CFE" not in frame["symbol"].to_list()
    receipt = read_json(receipt_path(cfg, "futures_bars", day, "CFE"))
    assert receipt["rejected_rows"] == 1
    assert receipt["state"] == ("captured" if listed else "owed")


def test_real_file_shapes_pass_the_invariants():
    """Shapes the history sweep found: a close with no open/high/low on a
    traded row, an expiring option with no settlement, delta at -1.000001."""
    from cnequity.domain.schemas import derivative_bar_violations

    rows = pl.DataFrame(
        {
            "open": [None, None],
            "high": [None, None],
            "low": [None, None],
            "close": [92.45, 1.0],
            "settle": [92.45, None],
            "pre_settle": [90.0, 2.0],
            "volume": [1, 438],
            "amount": [None, 0.0],
            "open_interest": [13, 0],
            "strike": [435.0, 12200.0],
            "option_type": ["C", "C"],
            "delta": [-1.000001, 0.05],
            "exercise_volume": [0, 0],
            "implied_vol": [None, 0.3],
            "series_implied_vol": [None, None],
        }
    )
    assert rows.filter(derivative_bar_violations("option_bars")).is_empty()


def test_a_zero_settlement_is_a_price_only_for_options():
    from cnequity.domain.schemas import derivative_bar_violations

    none = pl.Series([None], dtype=pl.Float64)
    base = {
        "open": none,
        "high": none,
        "low": none,
        "close": none,
        "settle": [0.0],
        "pre_settle": [3.4],
        "volume": [0],
        "amount": [0.0],
        "open_interest": [5],
    }
    option = pl.DataFrame(
        {
            **base,
            "strike": [4600.0],
            "option_type": ["P"],
            "delta": none,
            "exercise_volume": [0],
            "implied_vol": none,
            "series_implied_vol": none,
        }
    )
    assert option.filter(derivative_bar_violations("option_bars")).is_empty()
    assert (
        option.with_columns(pl.lit(-0.2).alias("settle"))
        .filter(derivative_bar_violations("option_bars"))
        .height
        == 1
    )
    assert pl.DataFrame(base).filter(derivative_bar_violations("futures_bars")).height == 1


def test_contract_step_consumes_current_run_staging_and_excludes_other_runs(cfg, reader):
    from cnequity.domain.schemas import with_provenance
    from cnequity.storage.parquet import StagingWriter

    day = date(2026, 9, 24)
    writer = StagingWriter(cfg.staging_root)
    frame = with_provenance(_day(day).futures, source="futures_exchange", data_version="v1")
    writer.write_batch("futures_bars", "current", "b", frame)
    writer.write_batch(
        "futures_bars", "other", "b", frame.with_columns(pl.lit("IF9901.CFE").alias("symbol"))
    )
    out = derivatives.step_futures_contracts(cfg, day, "current", {})
    assert out["rows_written"] == frame["symbol"].n_unique()
    contracts = _staged(cfg, "futures_contracts")
    assert "IF9901.CFE" not in contracts["symbol"]
    assert contracts["last_trade_date"].null_count() == contracts.height


def test_receipt_does_not_allow_a_partial_committed_session_to_be_skipped(cfg, reader):
    from cnequity.storage.derivative_evidence import session_matches

    day = date(2026, 9, 24)
    frame = derivatives.fetch_session(cfg, day, "futures", findings=[])
    assert session_matches(cfg, "futures_bars", day, "CFE", frame)
    assert not session_matches(cfg, "futures_bars", day, "CFE", frame.head(1))
    root = cfg.curated_root / "futures_bars" / "trade_date=2026-09"
    root.mkdir(parents=True)
    frame.head(1).write_parquet(root / "part.parquet")
    assert derivatives._completed_sessions(cfg, "futures", [day]) == set()
    frame.write_parquet(root / "part.parquet")
    assert derivatives._completed_sessions(cfg, "futures", [day]) == {day}
    set_run_flags(cfg, derivatives_refresh=True)
    assert derivatives._completed_sessions(cfg, "futures", [day]) == set()


def test_ine_only_receipt_coexists_with_combined_shfe_receipt(cfg):
    from cnequity.domain.schemas import with_provenance
    from cnequity.storage.derivative_evidence import (
        confirm_receipts,
        owed_sessions,
        read_json,
        receipt_path,
        record_session,
    )

    day = date(2026, 9, 24)
    base = with_provenance(_day(day).futures.head(1), source="futures_exchange", data_version="v1")
    shf = base.with_columns(pl.lit("CU2610.SHF").alias("symbol"), pl.lit("SHF").alias("exchange"))
    ine = base.with_columns(pl.lit("SC2610.INE").alias("symbol"), pl.lit("INE").alias("exchange"))
    combined = pl.concat([shf, ine])
    target = cfg.curated_root / "futures_bars" / "trade_date=2026-09"
    target.mkdir(parents=True)
    combined.write_parquet(target / "part.parquet")
    record_session(cfg, "futures_bars", day, "SHF", combined)
    record_session(cfg, "futures_bars", day, "INE", ine)

    confirm_receipts(cfg, "futures_bars")

    assert not owed_sessions(cfg, "futures_bars", day)
    for exchange in ["SHF", "INE"]:
        assert read_json(receipt_path(cfg, "futures_bars", day, exchange))["state"] == "committed"


def test_quarantined_ine_row_remains_owed_after_accepted_rows_publish(cfg):
    from cnequity.domain.schemas import with_provenance
    from cnequity.storage.derivative_evidence import (
        confirm_receipts,
        owed_sessions,
        read_json,
        receipt_path,
        record_session,
        session_matches,
    )

    day = date(2018, 9, 24)
    frame = with_provenance(
        _day(day).futures.head(1), source="futures_exchange", data_version="v1"
    ).with_columns(pl.lit("SC1812.INE").alias("symbol"), pl.lit("INE").alias("exchange"))
    target = cfg.curated_root / "futures_bars" / "trade_date=2018-09"
    target.mkdir(parents=True)
    frame.write_parquet(target / "part.parquet")
    record_session(
        cfg, "futures_bars", day, "INE", frame, error="rows quarantined", original_rows=2
    )

    confirm_receipts(cfg, "futures_bars")

    receipt = read_json(receipt_path(cfg, "futures_bars", day, "INE"))
    assert receipt["state"] == "owed" and receipt["rejected_rows"] == 1
    assert owed_sessions(cfg, "futures_bars", day) == [day]
    assert not session_matches(cfg, "futures_bars", day, "INE", frame)


def test_pre_ine_route_receipt_requires_refresh_only_for_2018_shf_futures(cfg):
    from cnequity.domain.schemas import with_provenance
    from cnequity.storage.atomic import write_json_atomic
    from cnequity.storage.derivative_evidence import (
        PRE_2004_ARCHIVE_PARSER,
        PRE_INE_2018_ROUTE_PARSER,
        read_json,
        receipt_path,
        record_session,
        session_matches,
    )

    for day, exchange, valid in [
        (date(2018, 3, 26), "SHF", False),
        (date(2018, 3, 26), "INE", True),
        (date(2019, 1, 2), "SHF", True),
    ]:
        frame = with_provenance(
            _day(day).futures.head(1), source="futures_exchange", data_version="v1"
        ).with_columns(pl.lit(exchange).alias("exchange"))
        record_session(cfg, "futures_bars", day, exchange, frame)
        path = receipt_path(cfg, "futures_bars", day, exchange)
        write_json_atomic(path, {**read_json(path), "parser": PRE_INE_2018_ROUTE_PARSER})
        assert session_matches(cfg, "futures_bars", day, exchange, frame) is valid
        if day.year == 2018 and exchange == "SHF":
            write_json_atomic(path, {**read_json(path), "parser": PRE_2004_ARCHIVE_PARSER})
            assert not session_matches(cfg, "futures_bars", day, exchange, frame)


def test_failed_session_remains_retryable_outside_reconciliation_tail(cfg, reader):
    from cnequity.storage.derivative_evidence import owed_sessions

    calls, missing = reader
    old = date(2026, 9, 1)
    missing.add(old)
    derivatives.fetch_session(cfg, old, "futures", findings=[], raise_when_empty=False)
    assert owed_sessions(cfg, "futures_bars", date(2026, 9, 24)) == [old]
    missing.clear()
    derivatives.step_futures_bars(cfg, date(2026, 9, 24), "repair", {})
    assert calls.count(old) == 2
    assert old in _staged(cfg, "futures_bars")["trade_date"]
    assert old in owed_sessions(cfg, "futures_bars", date(2026, 9, 24))
    compact_dataset(
        cfg.staging_root,
        cfg.curated_root,
        "futures_bars",
        "repair",
        partition_col="trade_date",
        granularity="month",
    )
    assert not owed_sessions(cfg, "futures_bars", date(2026, 9, 24))


def test_new_partial_reference_does_not_erase_historical_exact_dates():
    bars = _bars([("IF2506.CFE", "IF2506", date(2025, 3, 24))])
    reference = pl.DataFrame(
        {
            "symbol": ["IF2506.CFE"] * 2,
            "list_date": [date(2025, 1, 20), None],
            "last_trade_date": [date(2025, 6, 20), None],
        }
    )
    row = build_futures_contracts(bars, reference).row(0, named=True)
    assert row["list_date"] == date(2025, 1, 20)
    assert row["last_trade_date"] == date(2025, 6, 20)
    assert row["dates_basis"] == "exchange"


def test_cli_backfill_publishes_contracts_and_derivatives_even_on_weekend(cfg, reader, monkeypatch):
    from click.testing import CliRunner

    import cnequity.orchestrator.engine as engine
    from cnequity.cli import backfill_cmds
    from cnequity.cli.main import cli
    from cnequity.storage.read_context import read_root

    monkeypatch.setattr(backfill_cmds, "_cfg", lambda _: cfg)
    monkeypatch.setattr(engine, "shanghai_today", lambda: date(2026, 9, 27))
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "futures_bars",
            "--exchange",
            "CFE",
            "--start",
            "2026-09-23",
            "--end",
            "2026-09-24",
        ],
    )
    assert result.exit_code == 0, result.output
    assert '"followup"' in result.output
    assert list(read_root(cfg, "futures_contracts").glob("**/*.parquet"))
    assert list(read_root(cfg, "futures_continuous").glob("**/*.parquet"))


def test_exchange_scope_also_limits_reference_requests(cfg, reader, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("unselected exchange must not be requested")

    monkeypatch.setitem(
        derivatives.READERS,
        "SHF",
        dataclasses.replace(derivatives.READERS["SHF"], fetch_reference=forbidden),
    )
    bars = _bars([("IF2506.CFE", "IF2506", date(2025, 3, 24))]).with_columns(
        pl.lit("SHF").alias("exchange")
    )
    assert derivatives._reference(cfg, bars, [], "futures_contracts") is None


def test_reference_backfill_replays_requested_observed_dates(cfg, monkeypatch):
    calls = []

    def fetch(day, *, config):
        calls.append(day)
        return pl.DataFrame(
            {
                "symbol": ["IF2506.CFE"],
                "kind": ["future"],
                "list_date": [date(2025, 1, 1)],
                "last_trade_date": [date(2025, 6, 20)],
                "as_of": [day],
            }
        )

    monkeypatch.setitem(
        derivatives.READERS,
        "CFE",
        dataclasses.replace(derivatives.READERS["CFE"], fetch_reference=fetch),
    )
    set_backfill(cfg, start=date(2025, 3, 24), end=date(2025, 3, 25))
    bars = _bars(
        [
            ("IF2506.CFE", "IF2506", d)
            for d in [date(2025, 3, 24), date(2025, 3, 25), date(2025, 3, 26)]
        ]
    )
    result = derivatives._reference(cfg, bars, [], "futures_contracts")
    assert calls == [date(2025, 3, 24), date(2025, 3, 25)]
    assert set(result["as_of"]) == set(calls)


def test_reference_replay_stops_source_after_refusal(cfg, monkeypatch):
    from cnequity.adapters.futures_exchange.common import FuturesSourceBlocked

    calls = []

    def blocked(day, *, config):
        calls.append(day)
        raise FuturesSourceBlocked("412")

    monkeypatch.setitem(
        derivatives.READERS,
        "CFE",
        dataclasses.replace(derivatives.READERS["CFE"], fetch_reference=blocked),
    )
    set_backfill(cfg, start=date(2025, 3, 24), end=date(2025, 3, 25))
    bars = _bars([("IF2506.CFE", "IF2506", d) for d in [date(2025, 3, 24), date(2025, 3, 25)]])
    findings = []
    assert derivatives._reference(cfg, bars, findings, "futures_contracts") is None
    assert calls == [date(2025, 3, 24)]
    assert findings[0]["check"] == "futures_reference_unavailable"


def test_historical_reference_cannot_override_newer_archive(cfg, monkeypatch):
    original = pl.DataFrame(
        {
            "symbol": ["IF2506.CFE"],
            "kind": ["future"],
            "list_date": [date(2025, 1, 1)],
            "last_trade_date": [date(2025, 6, 20)],
            "as_of": [date(2025, 3, 25)],
        }
    )
    archive = cfg.meta_root / "derivatives/references/CFE/2025-03-25-future.parquet"
    archive.parent.mkdir(parents=True)
    original.write_parquet(archive)

    def fetch(day, *, config):
        return original.with_columns(
            pl.lit(day).alias("as_of"), pl.lit(date(2025, 6, 19)).alias("last_trade_date")
        )

    monkeypatch.setitem(
        derivatives.READERS,
        "CFE",
        dataclasses.replace(derivatives.READERS["CFE"], fetch_reference=fetch),
    )
    set_backfill(cfg, start=date(2025, 3, 24), end=date(2025, 3, 24))
    bars = _bars([("IF2506.CFE", "IF2506", date(2025, 3, 24))])
    ref = derivatives._reference(cfg, bars, [], "futures_contracts")
    rebuilt = build_futures_contracts(bars, ref)
    assert rebuilt["last_trade_date"][0] == date(2025, 6, 20)


def test_reference_replay_skips_snapshot_only_endpoint(cfg, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("historical replay must not fetch a current snapshot")

    monkeypatch.setitem(
        derivatives.READERS,
        "CFE",
        dataclasses.replace(
            derivatives.READERS["CFE"], fetch_reference=forbidden, reference_history=False
        ),
    )
    set_backfill(cfg, start=date(2025, 3, 24), end=date(2025, 3, 24))
    bars = _bars([("IF2506.CFE", "IF2506", date(2025, 3, 24))])
    findings = []
    assert derivatives._reference(cfg, bars, findings, "futures_contracts") is None
    assert findings[0]["check"] == "futures_reference_history_unsupported"


def test_an_unpublished_exchange_session_is_not_owed(cfg):
    """CZCE's archive 404s on 2010-06-21 while both neighbours answer."""
    from cnequity.storage.derivative_evidence import owed_sessions, record_session

    record_session(cfg, "futures_bars", date(2010, 6, 21), "CZC", None, error="not published")
    record_session(cfg, "futures_bars", date(2010, 6, 22), "CZC", None, error="not published")

    assert owed_sessions(cfg, "futures_bars", date(2010, 12, 31)) == [date(2010, 6, 22)]


def test_an_ine_receipt_after_its_own_route_ended_is_not_owed(cfg):
    """From 2019 INE contracts arrive in the SHFE file; a 2021 INE receipt
    left by an older routing must not keep 2021-07-28 owed."""
    from cnequity.storage.derivative_evidence import owed_sessions, record_session

    record_session(cfg, "futures_bars", date(2018, 12, 28), "INE", None, error="x")
    record_session(cfg, "futures_bars", date(2021, 7, 28), "INE", None, error="x")

    assert owed_sessions(cfg, "futures_bars", date(2021, 12, 31)) == [date(2018, 12, 28)]
