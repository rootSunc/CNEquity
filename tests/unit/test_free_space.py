"""[storage.free_space]: low space shortens retention; a nearly full disk stops writes."""

from __future__ import annotations

from collections import namedtuple
from datetime import date

import pytest

from cnequity.config import Config
from cnequity.config.loader import load_config
from cnequity.storage import free_space as module
from cnequity.storage.free_space import effective_keep_days, free_space, require_free_space
from cnequity.storage.revisions import DiskSpaceError

_Usage = namedtuple("_Usage", "total used free")
GB = 1_000_000_000


def _disk(monkeypatch, free_gb: float) -> None:
    monkeypatch.setattr(module, "_disk_usage", lambda _path: _Usage(500 * GB, 0, int(free_gb * GB)))


def _config_file(tmp_path, body: str = ""):
    from cnequity.config.bootstrap import path_for_toml

    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'extends = "builtin"\n[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n{body}',
        encoding="utf-8",
    )
    return path


def test_thresholds_load_with_defaults_and_reject_bad_values(tmp_path):
    cfg = load_config(_config_file(tmp_path))
    assert (cfg.free_space_warn_gb, cfg.free_space_stop_gb) == (50.0, 10.0)
    cfg = load_config(_config_file(tmp_path, "[storage.free_space]\nwarn_gb = 0\nstop_gb = 2.5\n"))
    assert (cfg.free_space_warn_gb, cfg.free_space_stop_gb) == (0.0, 2.5)
    for body in ("stop_gb = -1", "warn_gb = true", "warn_gb = 5\nstop_gb = 8"):
        with pytest.raises(ValueError, match="free_space"):
            load_config(_config_file(tmp_path, f"[storage.free_space]\n{body}\n"))


def test_levels_follow_the_two_thresholds_and_zero_turns_them_off(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "absent/lake")
    _disk(monkeypatch, 80)
    assert free_space(cfg).level == "ok"
    _disk(monkeypatch, 30)
    assert free_space(cfg).level == "low"
    assert effective_keep_days(cfg, 7) == 1 and effective_keep_days(cfg, 0) == 1
    _disk(monkeypatch, 5)
    assert free_space(cfg).level == "critical"
    with pytest.raises(DiskSpaceError, match="stop_gb"):
        require_free_space(cfg)
    cfg.free_space_warn_gb = cfg.free_space_stop_gb = 0
    assert free_space(cfg).level == "ok"
    require_free_space(cfg)
    assert effective_keep_days(cfg, 7) == 7


def test_a_publish_is_refused_below_stop_and_retention_keeps_one_day_when_low(
    tmp_path, monkeypatch
):
    from tests.catalog_seed import publish_rows
    from tests.unit.test_catalog_bridge import _rows

    from cnequity.storage.catalog_bridge import catalog_retention

    cfg = Config(data_root=tmp_path / "lake")
    day = date(2024, 1, 2)
    publish_rows(cfg, "daily_bars", _rows(day, {"600000.SH": 10.0}), run_id="first")

    _disk(monkeypatch, 5)
    with pytest.raises(DiskSpaceError):
        publish_rows(cfg, "daily_bars", _rows(day, {"600000.SH": 11.0}), run_id="refused")
    assert not list((cfg.staging_root / "_checkout").glob("refused*"))

    _disk(monkeypatch, 30)
    assert catalog_retention(cfg, apply=False)["keep_days"] == 1
    _disk(monkeypatch, 80)
    assert catalog_retention(cfg, apply=False)["keep_days"] == 7


def test_a_config_without_a_data_root_is_not_checked():
    from types import SimpleNamespace

    require_free_space(SimpleNamespace())
    assert free_space(SimpleNamespace()).level == "ok"
