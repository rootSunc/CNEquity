"""Offline coverage for fundamentals step wrappers and valuation backfill."""

from __future__ import annotations

from datetime import date, datetime, timezone

import httpx
import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.steps import fundamentals as fund
from cnequity.steps.common import load_bar_universe


@pytest.fixture
def cfg(tmp_path):
    # Wrapper tests use normalized fakes; opt out explicitly instead of
    # weakening the production exact-wire archive boundary.
    c = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    c.staging_root.mkdir(parents=True)
    return c


def test_financial_statement_items_disabled(cfg):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-1", {})


def test_financial_statement_items_empty(cfg, monkeypatch):
    monkeypatch.setattr(
        fund,
        "fetch_financial_statement_items",
        lambda trade_date, backfill=False, config=None, run_id=None: pl.DataFrame(),
    )
    result = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-1", {})
    assert result == {"rows_read": 0, "rows_written": 0}


def test_financial_statement_items_backfill_surfaces_missing_periods(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 6, 30)
    monkeypatch.setattr(fund, "fetch_financial_statement_items", lambda *a, **k: pl.DataFrame())

    result = fund.step_financial_statement_items(cfg, date(2026, 6, 30), "run-fsi-gap", {})

    assert result["status"] == "warning"
    assert result["missing_periods"] == 2
    assert result["context_updates"]["audit_findings"][0]["check"] == (
        "backfill_missing_report_periods"
    )


def test_financial_statement_items_writes_staging(cfg, monkeypatch):
    seen = {}

    def fake_fetch(trade_date, backfill=False, config=None, run_id=None, **kwargs):
        seen["backfill"] = backfill
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"] * 4,
                "report_period": ["2024Q1"] * 4,
                "statement_type": ["income", "indicator", "balance", "cashflow"],
                "item_code": ["revenue", "roe", "total_assets", "net_cash_operate"],
                "item_value": [1_000_000.0, 0.12, 1_000_000.0, 100_000.0],
                "announce_date": [date(2024, 4, 20)] * 4,
            }
        )

    monkeypatch.setattr(fund, "fetch_financial_statement_items", fake_fetch)
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 3, 31)
    result = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-fsi", {})
    assert seen["backfill"] is True
    assert result["rows_written"] == 4
    assert result.get("status") is None
    files = list(cfg.staging_root.glob("financial_statement_items/**/*.parquet"))
    assert files
    assert pl.read_parquet(files[0])["source"].unique().to_list() == ["eastmoney_backfill"]


def test_financial_report_failure_keeps_valid_unit_for_same_run_retry(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 3, 31)
    unit = "2024-03-31|RPT_LICO_FN_CPD"
    calls = []

    def fake_fetch(_day, *, on_unit, skip_units, failures, **_kwargs):
        calls.append(set(skip_units))
        if unit not in skip_units:
            on_unit(
                unit,
                "backfill:2024-03-31:RPT_LICO_FN_CPD",
                [
                    {
                        "symbol": "600519.SH",
                        "report_period": "2024Q1",
                        "statement_type": "income",
                        "item_code": "revenue",
                        "item_value": 1.0,
                        "announce_date": date(2024, 4, 20),
                    }
                ],
            )
        failures.append(("2024-03-31|RPT_DMSK_FN_BALANCE", "temporary failure"))
        return pl.DataFrame()

    monkeypatch.setattr(fund, "fetch_financial_statement_items", fake_fetch)
    first = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-retry", {})
    assert first["status"] == "degraded"
    assert first["batch_settled"] is True
    assert first["rows_written"] == 1
    assert len(list(cfg.staging_root.glob("financial_statement_items/**/*.parquet"))) == 1

    second = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-retry", {})
    assert calls == [set(), {unit}]
    assert second["rows_written"] == 1
    assert len(list(cfg.staging_root.glob("financial_statement_items/**/*.parquet"))) == 1


def test_financial_statement_items_prefers_qmt_and_falls_back_on_empty(cfg, monkeypatch):
    cfg.qmt_bridge_enabled = True
    monkeypatch.setattr(fund, "load_symbols", lambda _config: ["600519.SH"])
    eastmoney_called = False

    def fake_qmt(symbols, start, end, **kwargs):
        assert symbols == ["600519.SH"]
        assert start == date(2024, 5, 29)
        assert end == date(2024, 6, 28)
        return with_provenance(
            pl.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "report_period": ["2024Q1"],
                    "statement_type": ["income"],
                    "item_code": ["revenue"],
                    "item_value": [1_000_000.0],
                    "announce_date": [date(2024, 4, 20)],
                }
            ),
            source="qmt_bridge",
            data_version=data_version_for("financial_statement_items"),
        )

    def fake_eastmoney(*args, **kwargs):
        nonlocal eastmoney_called
        eastmoney_called = True
        return pl.DataFrame()

    monkeypatch.setattr(fund, "fetch_financial_statement_items_qmt", fake_qmt)
    monkeypatch.setattr(fund, "fetch_financial_statement_items", fake_eastmoney)
    result = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-qmt", {})
    assert result["rows_written"] == 1
    assert eastmoney_called is False
    files = list(cfg.staging_root.glob("financial_statement_items/**/*.parquet"))
    assert files
    assert pl.read_parquet(files[0])["source"].unique().to_list() == ["qmt_bridge"]


def test_shareholder_counts_uses_qmt_when_eastmoney_disabled(cfg, monkeypatch):
    from cnequity.adapters import qmt_bridge

    cfg.sources["eastmoney"] = False
    cfg.qmt_bridge_enabled = True
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 6, 30)
    cfg._backfill_symbols = ["600519.SH"]

    def fake_qmt(symbols, start, end, *, by, config):
        assert symbols == ["600519.SH"]
        assert start == date(2024, 1, 1)
        assert end == date(2024, 6, 30)
        assert by == "change_date"
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "count_date": [date(2024, 3, 31)],
                "holder_count": [100000.0],
                "holder_count_change_pct": [None],
                "avg_float_shares": [None],
                "avg_holding_value": [None],
                "announce_date": [date(2024, 4, 20)],
            }
        )

    monkeypatch.setattr(qmt_bridge, "fetch_shareholder_counts_qmt", fake_qmt)

    result = fund.step_shareholder_counts(cfg, date(2024, 6, 28), "run-qmt", {})
    assert result["rows_written"] == 1
    files = list(cfg.staging_root.glob("shareholder_counts/**/*.parquet"))
    assert files
    assert pl.read_parquet(files[0])["source"].unique().to_list() == ["qmt_bridge"]


def test_financial_statement_items_falls_back_to_eastmoney_when_qmt_empty(cfg, monkeypatch):
    cfg.qmt_bridge_enabled = True
    monkeypatch.setattr(fund, "load_symbols", lambda _config: ["600519.SH"])
    monkeypatch.setattr(
        fund,
        "fetch_financial_statement_items_qmt",
        lambda *args, **kwargs: pl.DataFrame(),
    )

    def fake_eastmoney(trade_date, backfill=False, config=None, run_id=None):
        assert trade_date == date(2024, 6, 28)
        assert backfill is False
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "report_period": ["2024Q1"],
                "statement_type": ["income"],
                "item_code": ["revenue"],
                "item_value": [1_000_000.0],
                "announce_date": [date(2024, 4, 20)],
            }
        )

    monkeypatch.setattr(fund, "fetch_financial_statement_items", fake_eastmoney)
    result = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-fallback", {})
    assert result["rows_written"] == 1
    files = list(cfg.staging_root.glob("financial_statement_items/**/*.parquet"))
    assert pl.read_parquet(files[0])["source"].unique().to_list() == ["eastmoney"]


def test_financial_statement_items_backfill_surfaces_partial_report_families(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 3, 31)
    monkeypatch.setattr(
        fund,
        "fetch_financial_statement_items",
        lambda *args, **kwargs: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "report_period": ["2024Q1"],
                "statement_type": ["income"],
                "item_code": ["revenue"],
                "item_value": [1_000_000.0],
                "announce_date": [date(2024, 4, 20)],
            }
        ),
    )

    result = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-fsi-partial", {})

    assert result["status"] == "warning"
    assert result["missing_statement_periods"] == 1
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "backfill_missing_statement_types"
    assert finding["missing_statement_types"] == [
        {"report_period": "2024Q1", "missing": ["balance", "cashflow", "indicator"]}
    ]


def test_financial_statement_items_backfill_surfaces_missing_income_statement(cfg, monkeypatch):
    """A missing income statement must surface too, not just balance/cashflow.

    fetch_financial_statement_items issues four independent requests -
    income, indicator, balance, cashflow - so a period with only
    balance/cashflow present is still incomplete.
    """
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 3, 31)
    monkeypatch.setattr(
        fund,
        "fetch_financial_statement_items",
        lambda *args, **kwargs: pl.DataFrame(
            {
                "symbol": ["600519.SH", "600519.SH"],
                "report_period": ["2024Q1", "2024Q1"],
                "statement_type": ["balance", "cashflow"],
                "item_code": ["total_assets", "net_cash_operate"],
                "item_value": [1_000_000.0, 100_000.0],
                "announce_date": [date(2024, 4, 20), date(2024, 4, 20)],
            }
        ),
    )

    result = fund.step_financial_statement_items(cfg, date(2024, 6, 28), "run-fsi-income", {})

    assert result["status"] == "warning"
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "backfill_missing_statement_types"
    assert finding["missing_statement_types"] == [
        {"report_period": "2024Q1", "missing": ["income", "indicator"]}
    ]


def test_valuation_metrics_disabled(cfg):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        fund.step_valuation_metrics(cfg, date(2024, 6, 28), "run-1", {})


def test_valuation_metrics_rejects_empty_snapshot(cfg, monkeypatch):
    monkeypatch.setattr(fund, "load_bar_universe", lambda _config: {"600519.SH"})
    monkeypatch.setattr(
        fund,
        "fetch_valuation_metrics",
        lambda *_args, **_kwargs: pl.DataFrame(),
    )
    with pytest.raises(RuntimeError, match="valuation_metrics: no rows returned"):
        fund.step_valuation_metrics(cfg, date(2024, 6, 28), "run-empty", {})


def test_load_bar_universe_ignores_zero_volume_placeholders(cfg):
    part = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "volume": [100, 0],
        }
    ).write_parquet(part / "part-000.parquet")

    assert load_bar_universe(cfg) == {"600519.SH"}


def test_load_bar_universe_uses_newest_retry_before_volume_filter(cfg):
    part = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "volume": [100, 0],
            "fetched_at": [
                datetime(2024, 6, 28, 7, tzinfo=timezone.utc),
                datetime(2024, 6, 28, 8, tzinfo=timezone.utc),
            ],
        }
    ).write_parquet(part / "part-retry.parquet")

    assert load_bar_universe(cfg) == set()


def test_load_bar_universe_keeps_legacy_rows_in_a_mixed_schema_lake(cfg):
    root = cfg.curated_root / "daily_bars"
    root.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 27)]}).write_parquet(
        root / "legacy.parquet"
    )
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "trade_date": [date(2024, 6, 28)],
            "volume": [0],
        }
    ).write_parquet(root / "current.parquet")

    assert load_bar_universe(cfg) == {"600519.SH"}


def test_symbols_needing_backfill_does_not_count_duplicate_rows(cfg):
    part = cfg.curated_root / "valuation_metrics" / "trade_date=2024-06"
    part.mkdir(parents=True)
    dates = [date(2024, 6, day) for day in range(1, 9)]
    rows = []
    for index, day in enumerate(dates):
        rows.append(
            {
                "symbol": "600519.SH",
                "trade_date": day,
                "float_mv": 1.0 if index < 6 else None,
                "total_mv": 1.0 if index < 6 else None,
                "source": "baostock",
                "fetched_at": f"2024-06-{day.day:02d}T00:00:00+00:00",
            }
        )
    rows.extend(
        {**rows[index], "fetched_at": f"2024-06-{index + 1:02d}T01:00:00+00:00"} for index in (0, 1)
    )
    pl.DataFrame(rows).write_parquet(part / "part-000.parquet")

    # Eight unique dates contain only six complete market-cap rows (75%). The
    # two retries must not inflate this to the 80% skip threshold.
    assert fund._symbols_needing_backfill(cfg, ["600519.SH"]) == ["600519.SH"]


def test_backfill_valuation_locked_nothing_to_do(cfg, monkeypatch):
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 0},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH"})
    monkeypatch.setattr(fund, "_symbols_needing_backfill", lambda config, universe, **kwargs: [])
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2024, 6, 1))
    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-v")
    assert result["rows_written"] == 0
    assert "already backfilled" in result["note"]


def test_backfill_valuation_locked_history_end_before_start(cfg, monkeypatch):
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 0},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH"})
    monkeypatch.setattr(
        fund,
        "_symbols_needing_backfill",
        lambda config, universe, **kwargs: ["600519.SH"],
    )
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2010, 1, 1))
    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-v")
    assert "history_end before backfill start" in result["note"]


def test_backfill_valuation_locked_writes_chunks(cfg, monkeypatch):
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 1},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH", "000001.SZ"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH", "000001.SZ"})
    monkeypatch.setattr(
        fund,
        "_symbols_needing_backfill",
        lambda config, universe, **kwargs: ["600519.SH", "000001.SZ"],
    )
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2024, 6, 1))

    def fake_history(batch, start, end, config=None):
        df = pl.DataFrame(
            {
                "symbol": batch,
                "trade_date": [date(2024, 1, 2)] * len(batch),
                "pe_ttm": [10.0] * len(batch),
                "pb": [1.0] * len(batch),
                "ps_ttm": [2.0] * len(batch),
                "total_mv": [1e9] * len(batch),
                "float_mv": [1e9] * len(batch),
            }
        )
        return df, []

    monkeypatch.setattr(
        "cnequity.adapters.baostock.valuation.fetch_valuation_history",
        fake_history,
    )
    # Shrink chunk size so the loop body runs once with our tiny universe.
    monkeypatch.setattr(fund, "_VALUATION_BACKFILL_CHUNK", 50)
    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-chunk")
    assert result["rows_written"] == 2
    assert result["symbols_todo"] == 2
    assert list(cfg.staging_root.glob("valuation_metrics/**/batch-00000/*.parquet")) or list(
        cfg.staging_root.glob("valuation_metrics/**/*.parquet")
    )


def test_backfill_valuation_locked_honors_requested_window(cfg, monkeypatch):
    cfg._backfill_start = date(2024, 5, 1)
    cfg._backfill_end = date(2024, 6, 1)
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 0},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH"})
    monkeypatch.setattr(
        fund,
        "_symbols_needing_backfill",
        lambda config, universe, **kwargs: ["600519.SH"],
    )
    monkeypatch.setattr(
        fund, "_valuation_history_end", lambda config, trade_date: date(2024, 6, 28)
    )
    calls = []

    def fake_history(batch, start, end, config=None):
        calls.append((start, end))
        return (
            pl.DataFrame(
                {
                    "symbol": batch,
                    "trade_date": [date(2024, 5, 2)] * len(batch),
                    "pe_ttm": [10.0] * len(batch),
                    "pb": [1.0] * len(batch),
                    "ps_ttm": [2.0] * len(batch),
                    "total_mv": [1e9] * len(batch),
                    "float_mv": [1e9] * len(batch),
                }
            ),
            [],
        )

    monkeypatch.setattr(
        "cnequity.adapters.baostock.valuation.fetch_valuation_history",
        fake_history,
    )

    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-window")

    assert result["history_start"] == "2024-05-01"
    assert result["history_end"] == "2024-06-01"
    assert calls == [(date(2024, 5, 1), date(2024, 6, 1))]


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"trade_date": date(2015, 12, 31)}, "outside requested window"),
        ({"symbol": "000001.SZ"}, "unexpected symbol"),
    ],
)
def test_backfill_valuation_locked_rejects_out_of_scope_rows(cfg, monkeypatch, update, message):
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 0},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH"})
    monkeypatch.setattr(
        fund,
        "_symbols_needing_backfill",
        lambda config, universe, **kwargs: ["600519.SH"],
    )
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2024, 6, 1))

    def fake_history(batch, start, end, config=None):
        row = {
            "symbol": "600519.SH",
            "trade_date": date(2024, 1, 2),
            "pe_ttm": 10.0,
            "pb": 1.0,
            "ps_ttm": 2.0,
            "total_mv": 1e9,
            "float_mv": 1e9,
        }
        row.update(update)
        return pl.DataFrame([row]), []

    monkeypatch.setattr(
        "cnequity.adapters.baostock.valuation.fetch_valuation_history", fake_history
    )
    with pytest.raises(RuntimeError, match=message):
        fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-invalid")
    assert not list(cfg.staging_root.glob("valuation_metrics/**/*.parquet"))


def test_backfill_valuation_locked_aborts_on_runtime_error(cfg, monkeypatch):
    monkeypatch.setattr(
        "cnequity.storage.repairs.valuation_orphans.purge_valuation_orphan_symbols",
        lambda config: {"purged": 0},
    )
    monkeypatch.setattr(fund, "load_symbols", lambda config: ["600519.SH"])
    monkeypatch.setattr(fund, "load_bar_universe", lambda config: {"600519.SH"})
    monkeypatch.setattr(
        fund,
        "_symbols_needing_backfill",
        lambda config, universe, **kwargs: ["600519.SH"],
    )
    monkeypatch.setattr(fund, "_valuation_history_end", lambda config, trade_date: date(2024, 6, 1))

    def boom(*a, **k):
        raise RuntimeError("baostock banned")

    monkeypatch.setattr(
        "cnequity.adapters.baostock.valuation.fetch_valuation_history",
        boom,
    )
    result = fund._backfill_valuation_metrics_locked(cfg, date(2024, 6, 28), "run-abort")
    assert result["rows_written"] == 0
    assert "baostock banned" in result["aborted"]
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["code"] == "baostock_backfill_incomplete"


def test_shareholder_backfill_surfaces_empty_windows(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 12, 31)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.shareholders.fetch_share_structure",
        lambda *args, **kwargs: pl.DataFrame(),
    )

    result = fund.step_share_structure(cfg, date(2026, 6, 30), "run-empty-window", {})

    assert result["status"] == "warning"
    assert result["empty_windows"] == 1
    assert result["context_updates"]["audit_findings"][0]["check"] == ("backfill_empty_windows")


def test_shareholder_backfill_rows_are_marked_reconstructed(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 12, 31)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.shareholders.fetch_share_structure",
        lambda *args, **kwargs: pl.DataFrame({"symbol": ["600519.SH"]}),
    )
    seen: list[str] = []

    def fake_write(*args, source, **kwargs):
        seen.append(source)
        return {"rows_read": 1, "rows_written": 1}

    monkeypatch.setattr(fund, "write_fetched", fake_write)

    result = fund.step_share_structure(cfg, date(2026, 6, 30), "run-backfill", {})

    assert result["rows_written"] == 1
    assert seen == ["eastmoney_backfill"]


def test_share_structure_later_window_failure_keeps_earlier_valid_year(cfg, monkeypatch):
    from cnequity.storage.state import StateStore

    cfg._backfill = True
    cfg._backfill_start = date(2023, 1, 1)
    cfg._backfill_end = date(2024, 12, 31)

    def fetch(start, end, **_kwargs):
        if start.year == 2024:
            raise fund.EastMoneyDatacenterError("source unavailable")
        return pl.DataFrame({"change_date": [start], "symbol": ["600519.SH"]})

    def write(config, run_id, dataset, frame, *, batch_id, **_kwargs):
        path = config.staging_root / dataset / f"run_id={run_id}" / f"part-{batch_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
        return {"rows_read": frame.height, "rows_written": frame.height}

    monkeypatch.setattr("cnequity.adapters.eastmoney.shareholders.fetch_share_structure", fetch)
    monkeypatch.setattr(fund, "write_fetched", write)
    result = fund.step_share_structure(cfg, date(2024, 12, 31), "run-years", {})
    assert result["status"] == "degraded"
    assert result["batch_settled"] is True
    assert result["rows_written"] == 1
    assert result["missing_units"] == 1
    assert StateStore(cfg.meta_root).get_payload("share_structure")["missing_units"]


def test_shareholder_programming_error_is_not_a_retryable_source_gap(cfg):
    from cnequity.storage.state import StateStore

    def fetch(_start, _end, **_kwargs):
        raise TypeError("invalid adapter argument")

    with pytest.raises(TypeError, match="invalid adapter argument"):
        fund._run_shareholder_step(
            cfg,
            date(2024, 12, 31),
            "run-invalid-adapter",
            "share_structure",
            fetch,
            daily_by="notice_date",
            daily_lookback_days=30,
        )
    assert not StateStore(cfg.meta_root).get_payload("share_structure").get("missing_units")


def test_top_holders_quarter_slices_resume_without_reasking_completed_windows(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 12, 31)
    calls = []

    def fetch(start, end, **_kwargs):
        calls.append((start, end))
        return pl.DataFrame({"record_date": [start], "symbol": ["600519.SH"]})

    def write(config, run_id, dataset, frame, *, batch_id, **_kwargs):
        path = config.staging_root / dataset / f"run_id={run_id}" / f"part-{batch_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
        return {"rows_read": frame.height, "rows_written": frame.height}

    monkeypatch.setattr(fund, "write_fetched", write)
    first = fund._run_shareholder_step(
        cfg,
        date(2024, 12, 31),
        "run-quarter",
        "top_holders",
        fetch,
        daily_by="record_date",
        daily_lookback_days=240,
    )
    second = fund._run_shareholder_step(
        cfg,
        date(2024, 12, 31),
        "run-quarter",
        "top_holders",
        fetch,
        daily_by="record_date",
        daily_lookback_days=240,
    )
    assert len(calls) == 4
    assert first["rows_written"] == second["rows_written"] == 4


def test_top_holders_failure_keeps_complete_windows_and_retries_only_missing(cfg, monkeypatch):
    from cnequity.storage.state import StateStore

    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 12, 31)
    calls = []
    fail_once = {"value": True}

    def fetch(start, end, **_kwargs):
        calls.append((start, end))
        if start.month == 7 and fail_once["value"]:
            fail_once["value"] = False
            raise fund.EastMoneyDatacenterError("page 30 timed out")
        return pl.DataFrame({"record_date": [start], "symbol": ["600519.SH"]})

    def write(config, run_id, dataset, frame, *, batch_id, **_kwargs):
        path = config.staging_root / dataset / f"run_id={run_id}" / f"part-{batch_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
        return {"rows_read": frame.height, "rows_written": frame.height}

    monkeypatch.setattr(fund, "write_fetched", write)
    args = (
        cfg,
        date(2024, 12, 31),
        "run-windows",
        "top_holders",
        fetch,
    )
    kwargs = {"daily_by": "record_date", "daily_lookback_days": 240}
    first = fund._run_shareholder_step(*args, **kwargs)
    assert first["status"] == "degraded"
    assert first["batch_settled"] is True
    assert first["rows_written"] == 2
    assert first["missing_units"] == 2
    assert [start.month for start, _ in calls] == [1, 4, 7]

    second = fund._run_shareholder_step(*args, **kwargs)
    assert second["rows_written"] == 4
    assert "missing_units" not in second
    assert [start.month for start, _ in calls[3:]] == [7, 10]
    state = StateStore(cfg.meta_root)
    state.commit_staged_units("top_holders", "run-windows")
    assert not state.get_payload("top_holders").get("missing_units")


def test_top_holders_partial_page_rows_publish_without_claiming_window_complete(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 3, 31)
    calls = []

    def fetch(start, end, **kwargs):
        calls.append((start, end))
        if len(calls) == 1:
            progress = kwargs["progress"]
            progress.failed_report = "RPT_F10_EH_FREEHOLDERS"
            progress.failure = "page 2 timed out"
        return pl.DataFrame({"record_date": [start], "symbol": ["600519.SH"]})

    def write(config, run_id, dataset, frame, *, batch_id, **_kwargs):
        path = config.staging_root / dataset / f"run_id={run_id}" / f"part-{batch_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
        return {"rows_read": frame.height, "rows_written": frame.height}

    monkeypatch.setattr(fund, "write_fetched", write)
    args = (cfg, date(2024, 3, 31), "run-page-partial", "top_holders", fetch)
    kwargs = {"daily_by": "record_date", "daily_lookback_days": 240}
    first = fund._run_shareholder_step(*args, **kwargs)
    assert first["status"] == "degraded"
    assert first["batch_settled"] is True
    assert first["rows_written"] == 1
    files = list((cfg.staging_root / "top_holders").rglob("*.parquet"))
    assert len(files) == 1 and files[0].name.startswith("part-partial-window-")

    second = fund._run_shareholder_step(*args, **kwargs)
    assert len(calls) == 2
    assert second["rows_written"] == 1
    files = list((cfg.staging_root / "top_holders").rglob("*.parquet"))
    assert len(files) == 2
    assert any(path.name.startswith("part-window-") for path in files)


def test_top_holders_real_archive_partial_page_reaches_curated_with_gap(tmp_path, monkeypatch):
    from cnequity.adapters.eastmoney import shareholders as sh
    from cnequity.query.reader import load
    from cnequity.steps.finalize import step_compact
    from cnequity.storage.state import StateStore

    config = Config(data_root=tmp_path / "isolated-lake", raw_archive_enabled=True)
    config._backfill = True
    config._backfill_start = date(2024, 1, 1)
    config._backfill_end = date(2024, 3, 31)
    row = {
        "SECUCODE": "600519.SH",
        "END_DATE": "2024-03-31",
        "HOLDER_NAME": "holder A",
        "HOLD_NUM": 100,
        "FREE_HOLDNUM_RATIO": 1.0,
        "HOLDER_RANK": 1,
        "IS_HOLDORG": "0",
        "NOTICE_DATE": "2024-04-30",
    }

    class PageThenTimeout:
        def __init__(self, *args, **kwargs):
            self.calls = 0
            self.config = kwargs.get("config")

        def get(self, url):
            self.calls += 1
            if self.calls > 1:
                raise httpx.ReadTimeout("page 2 timed out")
            return httpx.Response(
                200,
                json={"success": True, "result": {"pages": 2, "count": 501, "data": [row] * 500}},
                request=httpx.Request("GET", url),
            )

        def close(self):
            return None

    monkeypatch.setattr(sh, "EastMoneyClient", PageThenTimeout)
    monkeypatch.setattr(sh, "_SWEEP_RETRIES", 1)
    result = fund.step_top_holders(config, date(2024, 3, 31), "partial-wire", {})
    assert result["status"] == "degraded"
    assert result["batch_settled"] is True
    assert result["rows_written"] == 1
    assert StateStore(config.meta_root).get_payload("top_holders")["missing_units"]

    step_compact(config, date(2024, 3, 31), "partial-wire", {})
    published = load("top_holders", config=config, as_of=date(2024, 5, 1), pit_mode="best_effort")
    assert published.height == 1
    assert published["holder_scope"].to_list() == ["float"]
    assert StateStore(config.meta_root).get_payload("top_holders")["missing_units"]

    class CompleteWindow:
        def __init__(self, *args, **kwargs):
            self.config = kwargs.get("config")

        def get(self, url):
            complete = dict(row, HOLD_NUM_RATIO=2.0)
            return httpx.Response(
                200,
                json={"success": True, "result": {"pages": 1, "count": 1, "data": [complete]}},
                request=httpx.Request("GET", url),
            )

        def close(self):
            return None

    monkeypatch.setattr(sh, "EastMoneyClient", CompleteWindow)
    repaired = fund.step_top_holders(config, date(2024, 3, 31), "partial-wire", {})
    assert repaired["rows_written"] == 2
    step_compact(config, date(2024, 3, 31), "partial-wire", {})
    published = load("top_holders", config=config, as_of=date(2024, 5, 1), pit_mode="best_effort")
    assert set(published["holder_scope"].to_list()) == {"float", "total"}
    assert not StateStore(config.meta_root).get_payload("top_holders").get("missing_units")


def test_share_structure_symbol_refresh_is_one_window_scoped_to_the_names(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(1990, 1, 1)
    cfg._backfill_end = date(2026, 9, 29)
    cfg._backfill_symbols = ["603014.SH"]
    calls = []

    def fetch(start, end, **kwargs):
        calls.append((start, end, kwargs.get("symbols")))
        return pl.DataFrame()

    monkeypatch.setattr("cnequity.adapters.eastmoney.shareholders.fetch_share_structure", fetch)
    fund.step_share_structure(cfg, date(2026, 9, 29), "run-symbols", {})
    assert calls == [(date(1990, 1, 1), date(2026, 9, 29), ["603014.SH"])]
