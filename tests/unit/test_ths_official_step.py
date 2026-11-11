"""The daily snapshot step: it must never fail a day's ingestion.

It sits between `compact` and `audit` because the arbitration checks read what
it writes and nothing else does. A lake without a key still runs the wave.
"""

from datetime import date

import pytest

import cnequity.steps  # noqa: F401 — register steps
from cnequity.config import Config
from cnequity.orchestrator.registry import STEP_REGISTRY


def _run(config):
    return STEP_REGISTRY["ths_official_snapshot"].fn(config, date(2026, 9, 4), "run-1", {})


def test_the_step_runs_after_compact_and_before_audit():
    """Ordering is the whole point: audit arbitrates against what this writes."""
    entry = STEP_REGISTRY["ths_official_snapshot"]
    assert "compact" in entry.depends_on


def test_a_lake_without_the_source_enabled_skips_quietly(tmp_path):
    result = _run(Config(data_root=tmp_path))
    assert result["status"] == "skipped"
    assert result["rows_written"] == 0


def test_a_key_without_verification_skips(tmp_path):
    config = Config(data_root=tmp_path, ths_official_api_key="sk-test")
    config.sources["ths_official"] = True
    config.ths_official_verify_enabled = False
    assert _run(config)["status"] == "skipped"


def test_an_enabled_source_with_no_key_skips(tmp_path):
    config = Config(data_root=tmp_path)
    config.sources["ths_official"] = True
    assert _run(config)["reason"] == "no api key"


def test_a_peer_outage_is_reported_not_raised(tmp_path, monkeypatch):
    """An absent second opinion is not a reason to fail a day's ingestion."""
    config = Config(data_root=tmp_path, ths_official_api_key="sk-test")
    config.sources["ths_official"] = True

    def boom(*args, **kwargs):
        raise RuntimeError("upstream unreachable")

    for module, name in (
        ("cnequity.steps.capital", "snapshot_valuations_ths_official"),
        ("cnequity.steps.bars.ths_official", "snapshot_daily_bars_ths_official"),
        ("cnequity.steps.fundamentals", "snapshot_corporate_actions_ths_official"),
    ):
        monkeypatch.setattr(f"{module}.{name}", boom)

    result = _run(config)
    assert result["rows_written"] == 0
    assert {part["status"] for part in result.values() if isinstance(part, dict)} == {"error"}


def test_statements_are_not_swept_daily(tmp_path, monkeypatch):
    """Statements change on disclosure days; a daily sweep of 300 buys nothing."""
    config = Config(data_root=tmp_path, ths_official_api_key="sk-test")
    config.sources["ths_official"] = True
    called: list[str] = []

    def record(label):
        def inner(*args, **kwargs):
            called.append(label)
            return {"rows_written": 0}

        return inner

    monkeypatch.setattr(
        "cnequity.steps.capital.snapshot_valuations_ths_official", record("valuations")
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.ths_official.snapshot_daily_bars_ths_official", record("daily_bars")
    )
    monkeypatch.setattr(
        "cnequity.steps.fundamentals.snapshot_corporate_actions_ths_official",
        record("corporate_actions"),
    )
    monkeypatch.setattr(
        "cnequity.steps.fundamentals.snapshot_financials_ths_official", record("financials")
    )

    _run(config)
    assert called == ["valuations", "daily_bars", "corporate_actions"]


@pytest.mark.parametrize(
    "config_path",
    # Only the templates that actually ship. configs/cnequity.toml is the user's
    # own config and is gitignored, so naming it here passes on a developer's
    # machine and fails on every clean checkout.
    [
        "configs/cnequity.example.toml",
        "src/cnequity/config/templates/cnequity.example.toml",
    ],
)
def test_the_shipped_configs_run_it_before_audit(config_path):
    from cnequity.config import load_config

    waves = load_config(config_path).daily_waves
    finalize = next(wave for wave in waves if wave.name == "finalize")
    assert finalize.steps.index("ths_official_snapshot") < finalize.steps.index("audit")
    assert finalize.steps.index("compact") < finalize.steps.index("ths_official_snapshot")
