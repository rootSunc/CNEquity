from __future__ import annotations

import json
from datetime import date, timedelta

import polars as pl
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.config.bootstrap import path_for_toml
from cnequity.orchestrator.manifest import Manifest


def _config(tmp_path):
    data = tmp_path / "lake"
    path = tmp_path / "cnequity.toml"
    path.write_text(f'[data]\nroot = "{path_for_toml(data)}"\n', encoding="utf-8")
    return Config(data_root=data), path


def test_source_resilience_enforce_passes_on_real_independent_backups():
    """`--enforce` is a gate: it has to pass for a reason, not for a label.

    It once passed because `trading_status` named TDX as a primary it never
    used, putting EastMoney on both sides of a pair that looked disjoint.
    Correcting that made it fail; it passes now because every critical dataset
    has a backup in a genuinely different failure domain.
    """
    result = CliRunner().invoke(cli, ["sources", "resilience", "--enforce"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["backup_gate"]["passed"] is True
    assert payload["backup_gate"]["issues"] == []
    # adj_factors used to be the example of a single-source primary here. It
    # is not one any more: Sina bans by account, so the one vendor behind every
    # return in the lake needed a second, and Baostock's raw÷adjusted ratio is
    # that factor.
    adj = next(item for item in payload["datasets"] if item["dataset"] == "adj_factors")
    assert adj["impact"]["single_source_primary"] is False
    assert adj["backup"] == "baostock"
    announcements = next(
        item for item in payload["datasets"] if item["dataset"] == "announcement_index"
    )
    assert announcements["impact"]["single_source_primary"] is True


def test_source_slo_without_history_fails_closed(tmp_path):
    _, path = _config(tmp_path)
    result = CliRunner().invoke(cli, ["sources", "slo", "--config", str(path), "--enforce"])

    assert result.exit_code == 1
    assert json.loads(result.output)["passed"] is False


def test_snapshot_cli_round_trip(tmp_path):
    cfg, path = _config(tmp_path)
    part = cfg.curated_root / "daily_bars" / "trade_date=2026-08-28"
    part.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600000.SH"], "trade_date": ["2026-08-28"]}).write_parquet(
        part / "part.parquet"
    )
    snapshots = tmp_path / "snapshots"
    runner = CliRunner()

    created = runner.invoke(
        cli,
        [
            "snapshot",
            "create",
            "acceptance",
            "--dataset",
            "daily_bars",
            "--config",
            str(path),
            "--snapshot-root",
            str(snapshots),
        ],
    )
    verified = runner.invoke(
        cli,
        [
            "snapshot",
            "verify",
            "acceptance",
            "--config",
            str(path),
            "--snapshot-root",
            str(snapshots),
        ],
    )
    target = tmp_path / "restored"
    restored = runner.invoke(
        cli,
        [
            "snapshot",
            "restore",
            "acceptance",
            str(target),
            "--config",
            str(path),
            "--snapshot-root",
            str(snapshots),
        ],
    )

    assert created.exit_code == 0, created.output
    assert verified.exit_code == 0, verified.output
    assert restored.exit_code == 0, restored.output
    assert (target / "curated/daily_bars/trade_date=2026-08-28/part.parquet").is_file()


def test_snapshot_archive_cli_wraps_expected_errors_without_traceback(tmp_path):
    cfg, path = _config(tmp_path)
    runner = CliRunner()
    bad_archive = tmp_path / "bad.tar"
    bad_archive.write_bytes(b"not a tar archive")

    exported = runner.invoke(
        cli,
        [
            "snapshot",
            "export",
            "missing",
            "--config",
            str(path),
            "--snapshot-root",
            str(tmp_path / "snapshots"),
        ],
    )
    imported = runner.invoke(
        cli,
        [
            "snapshot",
            "import",
            str(bad_archive),
            "--config",
            str(path),
            "--snapshot-root",
            str(tmp_path / "snapshots"),
        ],
    )

    assert exported.exit_code != 0
    assert imported.exit_code != 0
    assert "Traceback" not in exported.output
    assert "Traceback" not in imported.output
    assert "Error:" in exported.output
    assert "Error:" in imported.output


def _stability_lake(tmp_path, *, sessions: int = 20, skip: int | None = None):
    """A lake with `sessions` trading days and a core run on each but `skip`."""
    cfg, path = _config(tmp_path)
    days = [date(2026, 7, 1) + timedelta(days=index) for index in range(sessions)]

    calendar = cfg.curated_root / "trading_calendar" / "trade_date=2026"
    calendar.mkdir(parents=True)
    pl.DataFrame({"trade_date": days, "is_trading": [True] * len(days)}).write_parquet(
        calendar / "part-0.parquet"
    )

    manifest = Manifest(cfg.manifest_path)
    for index, day in enumerate(days):
        if index == skip:
            continue
        run_id = manifest.start_run("daily:core", {"trade_date": day.isoformat()})
        manifest.finish_run(run_id, "success")

    return cfg, path, days


def _stability(path, days, *extra):
    return CliRunner().invoke(
        cli,
        [
            "verify",
            "--runs",
            "--config",
            str(path),
            "--days",
            "20",
            "--as-of",
            days[-1].isoformat(),
            *extra,
        ],
    )


def test_stability_enforce_exits_one_until_the_gate_passes(tmp_path):
    """The gate is the whole point of `--enforce`; without a test, a wrapper that
    stopped raising would report the failure and still exit 0, and the scheduled
    job around it would go green."""
    _, path, days = _stability_lake(tmp_path, skip=17)

    result = _stability(path, days, "--enforce")

    assert result.exit_code == 1
    assert json.loads(result.output)["passed"] is False


def test_stability_without_enforce_reports_the_same_failure_and_exits_zero(tmp_path):
    """`scripts/daily_pipeline.sh` runs it bare, every day, and treats a non-zero
    exit as a pipeline failure. Reporting mode must stay reporting-only."""
    _, path, days = _stability_lake(tmp_path, skip=17)

    result = _stability(path, days)

    assert result.exit_code == 0
    assert json.loads(result.output)["passed"] is False


def test_stability_enforce_exits_zero_on_a_clean_window(tmp_path):
    _, path, days = _stability_lake(tmp_path)

    result = _stability(path, days, "--enforce")

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["passed"] is True
    assert payload["consecutive_passed"] == 20


def test_source_policy_query_fails_closed_for_an_unreviewed_source():
    """An unreviewed source is the literal `unknown`, which never satisfies an
    allow check — so the query exits 1 rather than printing a permissive answer."""
    result = CliRunner().invoke(cli, ["sources", "policy", "eastmoney", "--profile", "personal"])

    assert result.exit_code == 1
    assert json.loads(result.output)["allowed"] is False


def test_source_policy_listing_reports_without_gating():
    """Listing the whole register is inspection, not a check. It must exit 0 even
    though every entry in it is currently unreviewed and would deny on query."""
    result = CliRunner().invoke(cli, ["sources", "policy"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "eastmoney" in payload and "tdx_protocol" in payload


def test_source_policy_rejects_an_unknown_source_distinguishably():
    """A denial and a typo must not look alike: a denial prints a JSON assessment,
    an unknown name prints an error and no payload."""
    result = CliRunner().invoke(cli, ["sources", "policy", "no-such-source"])

    assert result.exit_code != 0
    assert "unknown source policy" in result.output
    assert "{" not in result.output
