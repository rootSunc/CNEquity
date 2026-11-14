"""Backfills carry their derived data along: explicit lineage, not the next daily run."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, timedelta

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config.bootstrap import path_for_toml
from cnequity.derive.adj_factors import AdjFactorsResult
from cnequity.derive.lineage import changed_symbols, verify_factor_sync


def _fp(rows: list[tuple[str, str, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "_key": [r[1] for r in rows],
            "_fp": pl.Series([r[2] for r in rows], dtype=pl.UInt64),
        }
    )


def test_changed_symbols_sees_added_removed_and_retermed_actions():
    before = _fp([("A", "A|d1|div", 1), ("B", "B|d1|div", 2), ("C", "C|d1|div", 3)])
    after = _fp([("A", "A|d1|div", 1), ("B", "B|d1|div", 9), ("D", "D|d2|div", 4)])

    assert changed_symbols(before, after) == ["B", "C", "D"]


def test_nothing_changed_means_nothing_to_derive():
    same = _fp([("A", "A|d1|div", 1)])
    assert changed_symbols(same, same) == []


def _fake_lake(monkeypatch, *, bars: dict[str, date], factors: dict[str, date]):
    def fake_load(config, dataset, **kwargs):
        source = bars if dataset == "daily_bars" else factors
        rows = [(s, d) for s, d in source.items() if s in kwargs.get("symbols", source)]
        frame = pl.DataFrame(
            {"symbol": [r[0] for r in rows], "trade_date": [r[1] for r in rows]},
            schema={"symbol": pl.Utf8, "trade_date": pl.Date},
        )
        return frame.with_columns(pl.lit(100).alias("volume")) if dataset == "daily_bars" else frame

    monkeypatch.setattr("cnequity.derive.lineage._load", fake_load)


def test_verify_sorts_each_symbol_into_one_outcome(monkeypatch):
    day = date(2026, 9, 30)
    _fake_lake(
        monkeypatch,
        bars={"600000.SH": day, "600001.SH": day, "600002.SH": day},
        factors={"600000.SH": day, "600001.SH": day - timedelta(days=1)},
    )

    sync = verify_factor_sync(
        None,
        ["600000.SH", "600001.SH", "600002.SH", "600003.SH", "689009.SH"],
        failed=["600002.SH:hfq"],
        rows=10,
    )

    assert sync.updated == ["600000.SH"]
    assert sync.lagging == ["600001.SH"]
    assert sync.failed == ["600002.SH"]
    assert sync.skipped == {"600003.SH": "no_bars", "689009.SH": "cdr"}
    assert not sync.synchronized


def _config(tmp_path) -> str:
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n\n[orchestrator]\nworkers = 1\n',
        encoding="utf-8",
    )
    return str(path)


def _stub_corporate_actions_backfill(monkeypatch, *, factor_dates: dict[str, date]):
    fingerprints = iter(
        [
            _fp([("600000.SH", "a", 1), ("600001.SH", "b", 2)]),
            _fp([("600000.SH", "a", 1), ("600001.SH", "b", 5), ("600002.SH", "c", 6)]),
        ]
    )
    monkeypatch.setattr(
        "cnequity.derive.lineage.corporate_action_fingerprint", lambda cfg: next(fingerprints)
    )
    monkeypatch.setattr(
        "cnequity.cli.backfill_cmds._backfill_once",
        lambda cfg, dataset: {"run_id": "r1", "status": "success", "rows_written": 3},
    )

    @contextmanager
    def published(cfg, dataset):
        yield {"status": "success", "rows_written": 0}, None

    monkeypatch.setattr("cnequity.cli.maintain_cmds._published_derive", published)
    calls: list[list[str]] = []

    def derive(cfg, refresh_symbols=None, **kwargs):
        calls.append(list(refresh_symbols or []))
        return AdjFactorsResult(20, len(refresh_symbols or []), [], [])

    monkeypatch.setattr("cnequity.derive.adj_factors.compute_adj_factors", derive)
    day = date(2026, 9, 30)
    _fake_lake(monkeypatch, bars={"600001.SH": day, "600002.SH": day}, factors=factor_dates)
    return calls


@pytest.mark.usefixtures("chinese_output")
def test_corporate_actions_backfill_derives_exactly_the_changed_symbols(tmp_path, monkeypatch):
    day = date(2026, 9, 30)
    calls = _stub_corporate_actions_backfill(
        monkeypatch, factor_dates={"600001.SH": day, "600002.SH": day}
    )

    result = CliRunner().invoke(
        cli, ["backfill", "corporate_actions", "--config", _config(tmp_path)]
    )

    assert result.exit_code == 0, result.output
    assert calls == [["600001.SH", "600002.SH"]], "only the symbols whose terms changed"
    assert "受影响标的：      2" in result.output
    assert "✓ 公司行为与复权因子已同步" in result.output
    payload = json.loads(result.stdout[result.stdout.index("{") :])
    assert payload["adj_factors_sync"]["updated"] == 2
    assert payload["status"] == "success"


@pytest.mark.usefixtures("chinese_output")
def test_factors_left_behind_degrade_the_backfill(tmp_path, monkeypatch):
    day = date(2026, 9, 30)
    _stub_corporate_actions_backfill(
        monkeypatch, factor_dates={"600001.SH": day, "600002.SH": day - timedelta(days=3)}
    )

    result = CliRunner().invoke(
        cli, ["backfill", "corporate_actions", "--config", _config(tmp_path)]
    )

    assert "✗ 1 只标的的复权因子没有跟上公司行为" in result.output
    payload = json.loads(result.stdout[result.stdout.index("{") :])
    assert payload["adj_factors_sync"]["lagging"] == ["600002.SH"]
    assert payload["status"] == "degraded"


def _months(rows: list[tuple[str, date, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "month": [r[1] for r in rows],
            "_n": pl.Series([20] * len(rows), dtype=pl.UInt32),
            "_fp": pl.Series([r[2] for r in rows], dtype=pl.UInt64),
        }
    )


def _stub_daily_bars_backfill(monkeypatch, *, after: pl.DataFrame, suspend=None):
    before = _months([("600000.SH", date(2024, 1, 1), 1), ("600001.SH", date(2024, 1, 1), 2)])
    frames = iter([before, after])
    windows: list[tuple[date, date]] = []

    def fingerprint(cfg, window):
        windows.append(window)
        return next(frames)

    monkeypatch.setattr("cnequity.cli.backfill_cmds._bar_fingerprint", fingerprint)
    fetched = {"run_id": "r1", "status": "success", "rows_written": 10}
    monkeypatch.setattr("cnequity.cli.backfill_cmds._backfill_once", lambda cfg, ds: dict(fetched))
    monkeypatch.setattr(
        "cnequity.cli.backfill_cmds._backfill_chunked", lambda cfg, ds, s, e, n: dict(fetched)
    )

    @contextmanager
    def published(cfg, dataset):
        yield {"status": "success", "rows_written": 0}, None

    monkeypatch.setattr("cnequity.cli.maintain_cmds._published_derive", published)
    derive_calls: list[dict] = []

    def derive(cfg, output=None, **kwargs):
        derive_calls.append(kwargs)
        return AdjFactorsResult(5, 1, [], [])

    monkeypatch.setattr("cnequity.derive.adj_factors.compute_adj_factors", derive)
    day = date(2026, 9, 30)
    _fake_lake(
        monkeypatch,
        bars={"600001.SH": day, "600002.SH": day},
        factors={"600001.SH": day, "600002.SH": day},
    )
    suspensions: list[tuple[date, date]] = []

    def derive_status(cfg, *, start, end):
        if suspend is not None:
            raise suspend
        suspensions.append((start, end))
        return {"run_id": "d1", "rows_staged": 4, "status": "success"}

    monkeypatch.setattr("cnequity.cli.maintain_cmds._derive_trading_status", derive_status)
    return windows, derive_calls, suspensions


@pytest.mark.usefixtures("chinese_output")
def test_daily_bars_backfill_realigns_and_rederives_only_what_changed(tmp_path, monkeypatch):
    after = _months(
        [
            ("600000.SH", date(2024, 1, 1), 1),
            ("600001.SH", date(2024, 1, 1), 7),
            ("600002.SH", date(2024, 3, 1), 9),
        ]
    )
    windows, derive_calls, suspensions = _stub_daily_bars_backfill(monkeypatch, after=after)

    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "daily_bars",
            "--start",
            "2023-06-01",
            "--end",
            "2024-06-28",
            "--config",
            _config(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert windows == [(date(2023, 6, 1), date(2024, 6, 28))] * 2
    # Bars changed, the factors did not: realign from cache, never refetch.
    assert derive_calls == [{"realign_symbols": ["600001.SH", "600002.SH"]}]
    # Suspensions over the changed months, reaching back by the derive's tail.
    assert suspensions == [(date(2024, 1, 1) - timedelta(days=90), date(2024, 3, 31))]
    assert "✓ 日线与复权因子已同步" in result.output
    payload = json.loads(result.stdout[result.stdout.index("{") :])
    assert payload["adj_factors_sync"]["updated"] == 2
    assert payload["status"] == "success"


@pytest.mark.usefixtures("chinese_output")
def test_daily_bars_backfill_without_dates_fingerprints_its_default_window(tmp_path, monkeypatch):
    from cnequity.domain.market_time import shanghai_today
    from cnequity.steps.common import BACKFILL_START

    before = _months([("600000.SH", date(2024, 1, 1), 1), ("600001.SH", date(2024, 1, 1), 2)])
    windows, derive_calls, suspensions = _stub_daily_bars_backfill(monkeypatch, after=before)

    result = CliRunner().invoke(cli, ["backfill", "daily_bars", "--config", _config(tmp_path)])

    assert result.exit_code == 0, result.output
    assert windows[0] == (BACKFILL_START, shanghai_today())
    assert "日线没有变化" in result.output
    assert derive_calls == [] and suspensions == []


def test_a_failed_downstream_derive_still_reports_the_fetch(tmp_path, monkeypatch):
    after = _months([("600000.SH", date(2024, 1, 1), 5), ("600001.SH", date(2024, 1, 1), 2)])
    _stub_daily_bars_backfill(monkeypatch, after=after, suspend=RuntimeError("bars unreadable"))

    result = CliRunner().invoke(cli, ["backfill", "daily_bars", "--config", _config(tmp_path)])

    payload = json.loads(result.stdout[result.stdout.index("{") :])
    assert payload["rows_written"] == 10
    assert payload["status"] == "degraded"
    assert "bars unreadable" in payload["trading_status_derive"]["error"]


def test_delisted_profile_follows_the_same_lineage(tmp_path, monkeypatch):
    after = _months([("600000.SH", date(2024, 1, 1), 1), ("600001.SH", date(2024, 1, 1), 9)])
    _, derive_calls, suspensions = _stub_daily_bars_backfill(monkeypatch, after=after)
    monkeypatch.setattr(
        "cnequity.cli.backfill_cmds._run_delisted_profile",
        lambda cfg, since: {"run_id": "r1", "status": "success", "rows_written": 3},
    )

    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "daily_bars",
            "--profile",
            "delisted",
            "--start",
            "2020-01-01",
            "--config",
            _config(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert derive_calls == [{"realign_symbols": ["600001.SH"]}]
    assert len(suspensions) == 1


def test_bar_fingerprint_localises_a_change_to_its_month(tmp_path):
    from cnequity.config import Config
    from cnequity.derive.lineage import changed_bar_scope, daily_bar_fingerprint

    cfg = Config(data_root=tmp_path / "data")

    def write(day: date, close: float):
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"symbol": ["600000.SH"], "close": [close], "volume": [100]}).write_parquet(
            part / "part-0.parquet"
        )

    window = (date(2024, 1, 1), date(2024, 12, 31))
    assert daily_bar_fingerprint(cfg, *window).is_empty()
    write(date(2024, 1, 2), 10.0)
    write(date(2024, 3, 4), 11.0)
    before = daily_bar_fingerprint(cfg, *window)
    write(date(2024, 3, 4), 11.5)
    write(date(2024, 5, 6), 12.0)

    symbols, first, last = changed_bar_scope(before, daily_bar_fingerprint(cfg, *window))

    assert symbols == ["600000.SH"]
    assert (first, last) == (date(2024, 3, 1), date(2024, 5, 1))


def test_fingerprint_reads_the_lake_and_ignores_payment_dates(tmp_path):
    """A payment-date repair changes no factor; a re-termed dividend does."""
    from datetime import datetime, timezone

    from cnequity.config import Config
    from cnequity.derive.lineage import corporate_action_fingerprint

    cfg = Config(data_root=tmp_path / "data")
    assert corporate_action_fingerprint(cfg).is_empty()
    base = {
        "payment_date": None,
        "payment_source": None,
        "action_type": "dividend",
        "cash_dividend": 0.5,
        "bonus_ratio": 0.0,
        "transfer_ratio": 0.0,
        "split_factor": None,
        "allotment_ratio": None,
        "allotment_price": None,
        "reference_price": None,
        "source": "tdx",
        "data_version": "v1",
        "fetched_at": datetime.now(timezone.utc),
    }

    def write(rows):
        for row in rows:
            part = cfg.curated_root / "corporate_actions" / f"ex_date={row['ex_date'].isoformat()}"
            part.mkdir(parents=True, exist_ok=True)
            pl.DataFrame([{k: v for k, v in row.items() if k != "ex_date"}]).write_parquet(
                part / "part-0.parquet"
            )

    write(
        [
            {**base, "symbol": "600000.SH", "ex_date": date(2020, 6, 1)},
            {**base, "symbol": "600001.SH", "ex_date": date(2020, 6, 2)},
        ]
    )
    before = corporate_action_fingerprint(cfg)
    write(
        [
            {
                **base,
                "symbol": "600000.SH",
                "ex_date": date(2020, 6, 1),
                "payment_date": date(2020, 6, 5),
            },
            {**base, "symbol": "600001.SH", "ex_date": date(2020, 6, 2), "cash_dividend": 0.6},
        ]
    )

    assert changed_symbols(before, corporate_action_fingerprint(cfg)) == ["600001.SH"]
