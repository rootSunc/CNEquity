"""`cne backfill` keeps the generic options; dataset-specific modes moved to repair/import."""

from __future__ import annotations

from datetime import date

import pytest
from click.testing import CliRunner

from cnequity.cli import backfill_cmds
from cnequity.cli.main import cli
from cnequity.orchestrator.backfill_request import run_flags, validate
from cnequity.orchestrator.backfill_request import window as request_window


@pytest.fixture
def captured(monkeypatch):
    seen: list = []
    monkeypatch.setattr(
        backfill_cmds, "run_backfill", lambda config_path, request: seen.append(request)
    )
    return seen


@pytest.mark.parametrize(
    ("argv", "dataset", "repair", "flags", "window"),
    [
        (
            ["repair", "payment-dates", "--symbols", "600519.SH"]
            + ["--start", "2024-01-01", "--end", "2024-12-31"],
            "corporate_actions",
            "payment-dates",
            {"ca_payment_repair": True},
            {"symbols": ["600519.SH"], "start": date(2024, 1, 1), "end": date(2024, 12, 31)},
        ),
        (
            ["repair", "payment-dates", "--symbols", "600519.SH"]
            + ["--start", "2024-01-01", "--end", "2024-12-31", "--issuer-notices-only"],
            "corporate_actions",
            "issuer-notices",
            {"ca_payment_repair": True, "ca_issuer_notice_only": True},
            {"symbols": ["600519.SH"]},
        ),
        (
            ["repair", "delisted-actions", "--source", "ths"],
            "corporate_actions",
            "ths-actions",
            {"ca_ths_repair": True},
            {},
        ),
        (
            ["repair", "delisted-actions", "--source", "baostock"],
            "corporate_actions",
            "baostock-actions",
            {"ca_baostock_repair": True},
            {},
        ),
        (
            ["repair", "ex-dates", "--ex-dates", "2014-06-30,2013-05-02,2014-06-30"],
            "corporate_actions",
            "ex-dates",
            {"ca_eastmoney_date_repair": [date(2013, 5, 2), date(2014, 6, 30)]},
            {},
        ),
        (
            ["repair", "bar-amounts", "--start", "2024-01-01", "--end", "2024-01-31"],
            "daily_bars",
            "bar-amounts",
            {"tdx_amount_repair": True},
            {"start": date(2024, 1, 1), "end": date(2024, 1, 31)},
        ),
        (
            ["repair", "bar-turnover", "--start", "2024-01-01", "--end", "2024-01-31"]
            + ["--peer-review"],
            "daily_bars",
            "bar-turnover",
            {"turnover_repair": True, "turnover_peer_review": True},
            {},
        ),
        (
            ["repair", "bse-tip-turnover", "--symbols", "920001.BJ", "--date", "2026-10-23"],
            "daily_bars",
            "bse-tip",
            {"bse_tip_repair": True},
            {"start": date(2026, 10, 23), "end": date(2026, 10, 23)},
        ),
        (
            ["repair", "valuation-outage", "--start", "2026-09-22", "--end", "2026-09-30"],
            "valuation_metrics",
            "valuation-outage",
            {"valuation_fill_em_outage": True},
            {},
        ),
    ],
)
def test_repairs_preview_by_default_and_build_a_backfill_request(
    captured, argv, dataset, repair, flags, window
):
    preview = CliRunner().invoke(cli, argv)
    assert preview.exit_code == 0, preview.output
    applied = CliRunner().invoke(cli, [*argv, "--apply"])
    assert applied.exit_code == 0, applied.output

    first, second = captured
    assert first.dataset == second.dataset == dataset
    assert first.repair == repair
    assert first.plan is True and second.plan is False
    validate(first)
    assert run_flags(first) == flags
    fields = request_window(first)
    for key, value in window.items():
        assert fields[key] == value, key
    assert "Preview only" in preview.output


def test_valuation_outage_preview_runs(config, monkeypatch):
    """It crashed on every call: `ctx.invoke` handed `backfill` a tuple for --symbols."""
    seen = []
    monkeypatch.setattr(
        backfill_cmds, "_backfill_plan", lambda cfg, *args: seen.append(args) or {"plan": True}
    )
    result = CliRunner().invoke(
        cli,
        ["repair", "valuation-outage", "--start", "2026-09-22", "--end", "2026-09-30"]
        + ["--config", str(config.config_path)],
    )
    assert result.exit_code == 0, result.output
    assert seen and seen[0][-1] == ["fill-em-outage"]


def test_import_shfe_archive_runs_the_annual_import(monkeypatch, tmp_path, config):
    archive = tmp_path / "shfe-2018.zip"
    archive.write_bytes(b"PK")
    calls = []

    def fake(cfg, dataset, path, year, **kwargs):
        calls.append((dataset, path, year, kwargs))
        return {"status": "success"}

    monkeypatch.setattr(backfill_cmds, "_run_shfe_annual_archive", fake)
    argv = ["import", "shfe-archive", str(archive), "--dataset", "futures_bars", "--year", "2018"]
    argv += ["--accept-partial-fields", "--config", str(config.config_path)]
    preview = CliRunner().invoke(cli, argv)
    assert preview.exit_code == 0 and not calls
    assert '"network_requests": 0' in preview.output
    result = CliRunner().invoke(cli, [*argv, "--apply"])

    assert result.exit_code == 0, result.output
    ((dataset, path, year, kwargs),) = calls
    assert (dataset, path, year) == ("futures_bars", archive, 2018)
    assert kwargs["start"] is None and kwargs["end"] is None


def test_tdx_fin_plan_does_not_fetch(config, monkeypatch):
    """`--source tdx_fin` used to be read before `--plan`, so a plan ran the backfill."""
    import cnequity.steps.fundamentals as fundamentals

    monkeypatch.setattr(
        fundamentals,
        "stage_financials_tdx_fin",
        lambda *a, **k: pytest.fail("a plan must not fetch"),
    )
    result = CliRunner().invoke(
        cli,
        ["backfill", "financial_statement_items", "--source", "tdx_fin", "--plan"]
        + ["--start", "2024-01-01", "--end", "2024-12-31", "--config", str(config.config_path)],
    )
    assert result.exit_code == 0, result.output
    assert '"periods"' in result.output and "2024Q4" in result.output


def test_backfill_help_lists_only_generic_options():
    page = CliRunner().invoke(cli, ["backfill", "--help"]).output
    assert "--refetch" in page and "--source" in page
    for gone in ("--payment-date-repair", "--shfe-annual-archive", "--margin-source", "--force"):
        assert gone not in page


def test_refetch_maps_per_dataset_and_rejects_others(config):
    result = CliRunner().invoke(
        cli, ["backfill", "daily_bars", "--refetch", "--plan", "--config", str(config.config_path)]
    )
    assert result.exit_code != 0
    assert "--refetch applies to" in result.output
