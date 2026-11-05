"""Per-run switches are one frozen value, and Config grows no new private state.

Repair modes, a forced checkpoint reset, an alternate option source and the
MCP live switch used to be sixteen private attributes hung on Config — seven
declared, nine not — read through string ``getattr`` calls nothing checked.
"""

from __future__ import annotations

import ast
import pickle
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import cnequity
from cnequity.config import Config
from cnequity.orchestrator.backfill_scope import capture_backfill_scope, restore_backfill_scope
from cnequity.run_scope import LEGACY_RUN_FLAG_ATTRS, RunFlags, run_flags_of, set_run_flags

PACKAGE_ROOT = Path(cnequity.__file__).resolve().parent


def _config(tmp_path) -> Config:
    return Config(data_root=tmp_path / "data")


def test_flags_start_off_and_are_replaced_whole(tmp_path):
    cfg = _config(tmp_path)
    before = run_flags_of(cfg)
    assert before == RunFlags()
    set_run_flags(cfg, tdx_amount_repair=True, option_source="tushare")
    assert before == RunFlags()
    assert run_flags_of(cfg).tdx_amount_repair is True
    assert run_flags_of(cfg).option_source == "tushare"


def test_unknown_flags_are_rejected(tmp_path):
    with pytest.raises(TypeError, match="unknown run flag"):
        set_run_flags(_config(tmp_path), tdx_repair=True)


def test_repair_dates_are_stored_as_a_tuple(tmp_path):
    cfg = _config(tmp_path)
    dates = [date(2024, 1, 2)]
    set_run_flags(cfg, ca_eastmoney_date_repair=dates)
    dates.append(date(2024, 1, 3))
    assert run_flags_of(cfg).ca_eastmoney_date_repair == (date(2024, 1, 2),)


def test_flags_survive_pickling(tmp_path):
    cfg = _config(tmp_path)
    set_run_flags(cfg, derivatives_refresh=True, derivative_batch="abc")
    assert run_flags_of(pickle.loads(pickle.dumps(cfg))) == run_flags_of(cfg)


@pytest.mark.parametrize("attr", sorted(LEGACY_RUN_FLAG_ATTRS))
def test_every_legacy_name_still_reads_and_writes_the_flag(tmp_path, attr):
    cfg = _config(tmp_path)
    field = LEGACY_RUN_FLAG_ATTRS[attr]
    value = {
        "ca_eastmoney_date_repair": [date(2024, 1, 2)],
        "option_source": "tushare",
        "derivative_batch": "batch-1",
    }.get(field, True)
    setattr(cfg, attr, value)
    assert getattr(cfg, attr) == value
    stored = getattr(run_flags_of(cfg), field)
    assert (list(stored) if isinstance(stored, tuple) else stored) == value


def test_config_shaped_doubles_with_legacy_flags_are_read():
    double = SimpleNamespace(_tdx_amount_repair=True, _option_source="tushare")
    flags = run_flags_of(double)
    assert flags.tdx_amount_repair is True and flags.option_source == "tushare"


def test_class_level_flags_on_doubles_are_read_but_mocks_invent_nothing():
    from unittest.mock import MagicMock

    class Double:
        _commodity_via_eastmoney = True

    assert run_flags_of(Double()).commodity_via_eastmoney is True
    assert run_flags_of(MagicMock()) == RunFlags()


def test_retry_scope_keeps_its_recorded_keys_and_round_trips(tmp_path):
    cfg = _config(tmp_path)
    set_run_flags(
        cfg,
        ca_baostock_repair=True,
        tdx_amount_repair=True,
        ca_eastmoney_date_repair=[date(2024, 1, 2)],
        derivatives_refresh=True,
        sector_bars_force=True,
    )
    scope = capture_backfill_scope(cfg)
    assert scope["baostock_repair"] is True and scope["tdx_amount_repair"] is True
    assert scope["settings"]["_corporate_actions_eastmoney_date_repair"] == ["2024-01-02"]
    assert scope["settings"]["_derivatives_refresh"] is True
    fresh = _config(tmp_path)
    restore_backfill_scope(fresh, scope)
    restored = run_flags_of(fresh)
    assert restored.ca_baostock_repair and restored.tdx_amount_repair
    assert restored.ca_eastmoney_date_repair == (date(2024, 1, 2),)
    assert restored.derivatives_refresh is True
    # A --force checkpoint reset is one-time; a retry resumes instead.
    assert restored.sector_bars_force is False


def test_a_legacy_scope_keeps_unrecorded_flags_as_they_are(tmp_path):
    cfg = _config(tmp_path)
    set_run_flags(cfg, valuation_fill_em_outage=True)
    restore_backfill_scope(cfg, {"start": None, "end": None, "symbols": None})
    assert run_flags_of(cfg).valuation_fill_em_outage is True


# ------------------------------------------------------------------ guard

#: What may still be read or written as a private attribute of a config.
#: The run caches are shared on purpose and initialised atomically through
#: ``run_scope.ensure_run_scoped``; ``_rate_limiters`` is a declared runtime
#: field; ``_validate_source_limits`` is a Config method. Adding to this set
#: means adding loose state to Config — put it in a frozen value instead.
_ALLOWED_PRIVATE = {
    "_derivative_blocked_hosts",
    "_derivative_refreshed_requests",
    "_learned_suspensions",
    "_sina_bar_circuit",
    "_rate_limiters",
    "_validate_source_limits",
}
_CONFIG_NAMES = {"config", "cfg", "step_config", "pipeline_config", "self.config"}
_SKIP = {"run_scope.py", "config/loader.py"}


def _dotted(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return f"{base}.{node.attr}" if base else None
    return None


def _private_config_attributes() -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        rel = path.relative_to(PACKAGE_ROOT).as_posix()
        if rel in _SKIP:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            name = None
            if isinstance(node, ast.Attribute) and _dotted(node.value) in _CONFIG_NAMES:
                name = node.attr
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in {"getattr", "setattr", "hasattr", "delattr"}
                and len(node.args) >= 2
                and _dotted(node.args[0]) in _CONFIG_NAMES
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
            ):
                name = node.args[1].value
            if name and name.startswith("_") and not name.startswith("__"):
                found.setdefault(name, set()).add(rel)
    return found


def test_no_new_private_state_is_hung_on_config():
    unexpected = {
        name: sorted(files)
        for name, files in _private_config_attributes().items()
        if name not in _ALLOWED_PRIVATE
    }
    assert not unexpected, (
        "private attributes on Config are hidden run state; use run_scope.RunFlags, "
        f"run_scope.BackfillWindow, or a declared field: {unexpected}"
    )


def test_the_allowlist_has_no_stale_entries():
    stale = _ALLOWED_PRIVATE - set(_private_config_attributes())
    assert not stale, f"no longer used, drop from _ALLOWED_PRIVATE: {sorted(stale)}"


def test_the_config_fingerprint_does_not_depend_on_hash_order(tmp_path):
    """A set-valued setting (``best_effort_enabled``) was hashed via ``str()``,
    whose order changes per process, so lineage reported a new config on
    every run."""
    import os
    import subprocess
    import sys

    script = (
        "from pathlib import Path\n"
        "from cnequity.config import Config\n"
        "from cnequity.storage.provenance import config_fingerprint\n"
        "cfg = Config(data_root=Path('/tmp/x'))\n"
        "cfg.best_effort_enabled = frozenset({'fund_flow', 'hot_rank', 'news_headlines',"
        " 'broker_forecasts', 'chip_distribution', 'share_repurchases'})\n"
        "print(config_fingerprint(cfg))\n"
    )
    prints = {
        subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
        ).stdout.strip()
        for seed in ("1", "2", "3")
    }
    assert len(prints) == 1
