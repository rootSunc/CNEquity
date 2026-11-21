from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.domain.universe_profiles import (
    list_universe_profiles,
    profile_scope_hash,
    resolve_universe_profile,
)
from cnequity.query.reader import (
    ReaderError,
    _require_profile_delisting_evidence,
    _resolve_reader_scope,
)


def test_official_profiles_are_versioned_and_scope_hash_is_stable():
    profiles = list_universe_profiles(include_compatibility=False)
    assert [item["name"] for item in profiles] == [
        "cn_a_sh_sz_research_v1",
        "cn_a_all_experimental_v1",
    ]
    first = profile_scope_hash("cn_a_sh_sz_research_v1", ["000001.SZ", "600000.SH"])
    second = profile_scope_hash("cn_a_sh_sz_research_v1", ["600000.sh", "000001.sz"])
    assert first == second


def test_explicit_profile_enables_strict_scope_and_legacy_all_a_stays_permissive():
    universe, profile, strict = _resolve_reader_scope(
        universe=None,
        profile="cn_a_sh_sz_research_v1",
        strict_universe=False,
    )
    assert universe == "all_a_sh_sz"
    assert profile is not None and strict is True

    universe, profile, strict = _resolve_reader_scope(
        universe=None,
        profile="legacy_all_a",
        strict_universe=False,
    )
    assert universe == "all_a"
    assert profile is not None and strict is False


@pytest.mark.parametrize("profile", [None, "legacy_all_a"])
def test_universe_all_a_is_removed(profile):
    with pytest.raises(ReaderError, match=r"universe='all_a' was removed.*profile='legacy_all_a'"):
        _resolve_reader_scope(
            universe="all_a",
            profile=profile,
            strict_universe=False,
        )


def test_strict_profile_delisting_evidence_fails_closed(tmp_path, monkeypatch):
    frame = pl.DataFrame({"trade_date": [date(2026, 8, 28)]})
    profile = resolve_universe_profile("cn_a_sh_sz_research_v1")
    monkeypatch.setattr(
        "cnequity.quality.delisted_coverage.delisted_coverage_report",
        lambda *args, **kwargs: {
            "verified": False,
            "counts": {"pending_probe": 1},
        },
    )

    with pytest.raises(ReaderError, match="delisting evidence"):
        _require_profile_delisting_evidence(Config(data_root=tmp_path), frame, profile)


def test_profile_cli_is_machine_readable():
    result = CliRunner().invoke(cli, ["profile", "show", "cn_a_sh_sz_research_v1"])
    assert result.exit_code == 0
    assert '"scope_hash"' in result.output


@pytest.mark.parametrize(
    "module", ["cnequity.domain", "cnequity.query", "cnequity.domain.universe_profiles"]
)
@pytest.mark.parametrize(
    "alias", ["get_universe_profile", "list_profiles", "show_profile", "profile_json"]
)
def test_second_profile_names_are_removed(module, alias):
    import importlib

    with pytest.raises(AttributeError):
        getattr(importlib.import_module(module), alias)


def test_universe_profile_argument_is_removed():
    from cnequity.query import load

    with pytest.raises(TypeError, match="universe_profile"):
        load("daily_bars", universe_profile="cn_a_sh_sz_research_v1")
