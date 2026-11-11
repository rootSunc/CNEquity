"""The run's backfill window is one frozen value, not six loose Config attributes.

It used to be ``_backfill``, ``_backfill_start``, ``_backfill_end``,
``_backfill_symbols``, ``_backfill_workers`` and ``_backfill_refetch``, read
through ~170 string ``getattr`` calls that nothing checked and written field by
field, so a reader could see a new start beside an old end.
"""

from __future__ import annotations

import ast
import copy
import pickle
from dataclasses import FrozenInstanceError
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import cnequity
from cnequity.config import Config
from cnequity.orchestrator.backfill_scope import capture_backfill_scope, restore_backfill_scope
from cnequity.run_scope import (
    LEGACY_BACKFILL_ATTRS,
    LEGACY_RUN_FLAG_ATTRS,
    BackfillWindow,
    backfill_of,
    backfill_override,
    set_backfill,
)
from cnequity.storage.provenance import config_fingerprint

PACKAGE_ROOT = Path(cnequity.__file__).resolve().parent


def _config(tmp_path) -> Config:
    return Config(data_root=tmp_path / "data")


def test_a_fresh_config_has_an_empty_window(tmp_path):
    assert backfill_of(_config(tmp_path)) == BackfillWindow()
    assert backfill_of(None) == BackfillWindow()


def test_the_window_is_immutable_and_replaced_whole(tmp_path):
    cfg = _config(tmp_path)
    before = backfill_of(cfg)
    after = set_backfill(cfg, active=True, start=date(2020, 1, 1), end=date(2020, 2, 1))
    assert before == BackfillWindow()  # the old value a reader holds never changes
    assert backfill_of(cfg) is after
    assert (after.active, after.start, after.end) == (True, date(2020, 1, 1), date(2020, 2, 1))
    with pytest.raises(FrozenInstanceError):
        after.start = date(2021, 1, 1)  # type: ignore[misc]


def test_symbols_are_stored_as_a_tuple_and_handed_out_as_a_fresh_list(tmp_path):
    cfg = _config(tmp_path)
    symbols = ["600000.SH"]
    set_backfill(cfg, symbols=symbols)
    symbols.append("000001.SZ")  # the caller's list does not leak in
    assert backfill_of(cfg).symbols == ("600000.SH",)
    handed = backfill_of(cfg).symbol_list()
    handed.append("x")
    assert backfill_of(cfg).symbol_list() == ["600000.SH"]


def test_unknown_fields_are_rejected(tmp_path):
    with pytest.raises(TypeError, match="unknown backfill field"):
        set_backfill(_config(tmp_path), begin=date(2020, 1, 1))


def test_a_copied_config_does_not_share_later_changes(tmp_path):
    cfg = _config(tmp_path)
    set_backfill(cfg, active=True, start=date(2020, 1, 1))
    scoped = copy.copy(cfg)
    set_backfill(scoped, start=date(2024, 1, 1))
    assert backfill_of(cfg).start == date(2020, 1, 1)
    assert backfill_of(scoped).start == date(2024, 1, 1)


def test_override_puts_the_previous_window_back(tmp_path):
    cfg = _config(tmp_path)
    set_backfill(cfg, active=True, start=date(2020, 1, 1))
    with pytest.raises(RuntimeError):
        with backfill_override(cfg, active=False, start=None):
            assert backfill_of(cfg).active is False
            raise RuntimeError
    assert backfill_of(cfg) == BackfillWindow(active=True, start=date(2020, 1, 1))


def test_the_window_survives_pickling_to_worker_processes(tmp_path):
    cfg = _config(tmp_path)
    set_backfill(cfg, active=True, symbols=["600000.SH"], workers=3)
    assert backfill_of(pickle.loads(pickle.dumps(cfg))) == backfill_of(cfg)


def test_config_no_longer_carries_the_legacy_names():
    legacy = set(LEGACY_BACKFILL_ATTRS) | set(LEGACY_RUN_FLAG_ATTRS)
    assert not sorted(name for name in legacy if hasattr(Config, name))


def test_tests_do_not_use_the_legacy_names_as_attributes():
    """On a real Config such an assignment would now be a silent no-op."""
    legacy = set(LEGACY_BACKFILL_ATTRS) | set(LEGACY_RUN_FLAG_ATTRS)
    found = []
    for path in sorted(Path(__file__).parent.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        found += [
            f"{path.name}:{node.lineno} .{node.attr}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute) and node.attr in legacy
        ]
    assert not found, "use backfill_of / set_backfill / run_flags_of / set_run_flags:\n" + (
        "\n".join(found)
    )


def test_config_shaped_doubles_with_legacy_attributes_are_read(tmp_path):
    double = SimpleNamespace(_backfill=True, _backfill_start=date(2020, 1, 1))
    window = backfill_of(double)
    assert (window.active, window.start, window.end) == (True, date(2020, 1, 1), None)


def test_the_config_fingerprint_hashes_the_window_under_its_old_names(tmp_path):
    """Provenance records this fingerprint; moving the fields must not change it."""
    from cnequity.storage import provenance

    cfg = _config(tmp_path)
    set_backfill(cfg, active=True, symbols=["600000.SH"])
    captured: dict = {}
    real = provenance.json.dumps

    def spy(obj, *args, **kwargs):
        captured.update(obj)
        return real(obj, *args, **kwargs)

    provenance.json.dumps = spy
    try:
        config_fingerprint(cfg)
    finally:
        provenance.json.dumps = real
    assert captured["_backfill"] is True
    assert captured["_backfill_symbols"] == ["600000.SH"]
    assert "backfill_window" not in captured


def test_retry_scope_round_trips_the_window(tmp_path):
    cfg = _config(tmp_path)
    set_backfill(
        cfg, start=date(2020, 1, 1), end=date(2020, 2, 1), symbols=["600000.SH"], workers=3
    )
    scope = capture_backfill_scope(cfg)
    assert scope["start"] == "2020-01-01" and scope["symbols"] == ["600000.SH"]
    assert scope["settings"]["_backfill_workers"] == 3  # the recorded key is unchanged
    fresh = _config(tmp_path)
    restore_backfill_scope(fresh, scope)
    assert backfill_of(fresh) == backfill_of(cfg)


def test_a_scope_recorded_before_workers_existed_keeps_the_default(tmp_path):
    fresh = _config(tmp_path)
    restore_backfill_scope(fresh, {"start": "2020-01-01", "end": None, "symbols": None})
    assert backfill_of(fresh) == BackfillWindow(start=date(2020, 1, 1))


# ------------------------------------------------------------------ guard

#: The double reader and the two places that keep the old names on purpose
#: (the fingerprint and the recorded retry-scope key).
_ALLOWED = {"run_scope.py", "storage/provenance.py"}
_ALLOWED_STRINGS = {"orchestrator/backfill_scope.py": {"_backfill_workers"}}


def _legacy_uses() -> list[str]:
    names = set(LEGACY_BACKFILL_ATTRS)
    found = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        rel = path.relative_to(PACKAGE_ROOT).as_posix()
        if rel in _ALLOWED:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in names:
                found.append(f"{rel}:{node.lineno} .{node.attr}")
            elif (
                isinstance(node, ast.Constant)
                and node.value in names
                and node.value not in _ALLOWED_STRINGS.get(rel, set())
            ):
                found.append(f"{rel}:{node.lineno} {node.value!r}")
    return found


def test_src_reads_the_window_through_run_scope_only():
    found = _legacy_uses()
    assert not found, (
        "use run_scope.backfill_of(config) / set_backfill(config, ...) instead of the "
        "loose _backfill* attributes:\n" + "\n".join(found)
    )
