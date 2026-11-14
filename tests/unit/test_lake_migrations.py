"""`cne migrate` and meta/state/lake.json: which one-off fixes a lake still owes."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.diagnostics.report import build_report
from cnequity.storage import migrations
from cnequity.storage.layout import init_data_layout
from cnequity.storage.migrations import MIGRATIONS, affected, lake_path, owed, read_lake


@pytest.fixture
def fake_registry(monkeypatch):
    calls: list[tuple[str, bool]] = []
    counts = {"first": 3, "second": 0}

    def make(name):
        def run(config, *, apply):
            calls.append((name, apply))
            return {"applied": apply, "rows": counts[name]}

        return run

    registry = (
        migrations.Migration("first", "First fix", "0.18.0", make("first")),
        migrations.Migration("second", "Second fix", "0.18.0", make("second")),
    )
    monkeypatch.setattr(migrations, "MIGRATIONS", registry)
    return calls, counts


def test_a_new_lake_is_stamped_and_owes_nothing(config):
    init_data_layout(config)

    record = read_lake(config)

    assert record["created_with"] == migrations.package_version()
    assert owed(config) == [
        m
        for m in MIGRATIONS
        if not m.optional
        and migrations._parse(m.introduced_in) > migrations._parse(record["created_with"])
    ]


def test_an_old_lake_re_running_init_is_not_stamped(config):
    import polars as pl

    # Any published data marks the lake as old; a name no DuckDB view reads.
    dataset = config.curated_root / "legacy_dataset"
    dataset.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"]}).write_parquet(dataset / "part.parquet")

    init_data_layout(config)

    assert not lake_path(config).exists()
    assert owed(config) == [m for m in MIGRATIONS if not m.optional]


def test_preview_changes_nothing_and_apply_records(config, fake_registry):
    calls, _ = fake_registry
    args = ["migrate", "--config", str(config.config_path)]

    preview = CliRunner().invoke(cli, args)
    assert preview.exit_code == 0, preview.output
    assert "would change 3 row(s)" in preview.output
    assert calls == [("first", False), ("second", False)]
    assert not lake_path(config).exists()

    applied = CliRunner().invoke(cli, [*args, "--apply"])
    assert applied.exit_code == 0, applied.output
    done = read_lake(config)["migrations"]
    assert done["first"]["status"] == "applied" and done["first"]["affected"] == 3
    assert done["second"]["status"] == "not_needed"

    again = CliRunner().invoke(cli, args)
    assert "Nothing owed" in again.output


def test_only_reruns_a_recorded_migration_and_rejects_unknown_ids(config, fake_registry):
    calls, _ = fake_registry
    CliRunner().invoke(cli, ["migrate", "--apply", "--config", str(config.config_path)])
    calls.clear()

    rerun = CliRunner().invoke(
        cli, ["migrate", "--only", "first", "--config", str(config.config_path)]
    )
    assert rerun.exit_code == 0, rerun.output
    assert calls == [("first", False)]

    bad = CliRunner().invoke(
        cli, ["migrate", "--only", "nope", "--config", str(config.config_path)]
    )
    assert bad.exit_code != 0
    assert "unknown migration" in bad.output


def test_a_failure_stops_and_records_nothing_after_it(config, monkeypatch):
    def boom(config, *, apply):
        raise RuntimeError("exchange source disabled")

    registry = (
        migrations.Migration("broken", "Broken", "0.18.0", boom),
        migrations.Migration("later", "Later", "0.18.0", lambda c, *, apply: {"rows": 0}),
    )
    monkeypatch.setattr(migrations, "MIGRATIONS", registry)

    result = CliRunner().invoke(
        cli, ["migrate", "--apply", "--json", "--config", str(config.config_path)]
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert [item["id"] for item in payload["migrations"]] == ["broken"]
    assert read_lake(config).get("migrations", {}) == {}


def test_doctor_reports_owed_migrations(config, fake_registry):
    config.meta_root.mkdir(parents=True, exist_ok=True)

    report = build_report(config=config)

    assert any("lake migration" in str(f.title) for f in report.findings)


def test_affected_counts_nested_rows_and_settled_keys():
    assert affected({"datasets": {"a": {"rows": 2}, "b": {"rows": 3}}}) == 5
    assert affected({"resolved_sessions": ["d1"], "resolved_prelisting_keys": ["k1", "k2"]}) == 3
    assert affected({"note": "nothing to repair"}) == 0
    # A report that read 100 rows and rewrites 3 counts 3.
    assert affected({"rows": 100, "rows_changed": 3}) == 3


def test_every_migration_still_resolves_its_repair():
    """The one-off repair commands are gone; `cne migrate` is how an old lake reaches them."""
    import inspect

    for migration in MIGRATIONS:
        closure = inspect.getclosurevars(migration.run)
        module = closure.nonlocals.get("module")
        name = closure.nonlocals.get("name")
        if module is None:  # the missing-values pair calls its repair directly
            continue
        import importlib

        assert callable(
            getattr(importlib.import_module(f"cnequity.storage.repairs.{module}"), name)
        ), migration.id
