from __future__ import annotations

import warnings
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


def test_explicit_profile_enables_strict_scope_and_legacy_all_a_only_warns():
    universe, profile, strict = _resolve_reader_scope(
        universe=None,
        profile="cn_a_sh_sz_research_v1",
        universe_profile=None,
        strict_universe=False,
    )
    assert universe == "all_a_sh_sz"
    assert profile is not None and strict is True

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        legacy = _resolve_reader_scope(
            universe="all_a",
            profile=None,
            universe_profile=None,
            strict_universe=False,
        )
    assert legacy == ("all_a", None, False)
    assert any(item.category is DeprecationWarning for item in caught)


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


PROFILE_MODULE = "cnequity.domain.universe_profiles"


@pytest.mark.parametrize("module", ["cnequity.domain", "cnequity.query", PROFILE_MODULE])
@pytest.mark.parametrize(
    ("alias", "canonical"),
    [
        ("get_universe_profile", "resolve_universe_profile"),
        ("list_profiles", "list_universe_profiles"),
        ("show_profile", "show_universe_profile"),
    ],
)
def test_second_profile_names_warn_and_still_work(module, alias, canonical):
    import importlib

    mod = importlib.import_module(module)
    with pytest.warns(DeprecationWarning, match=rf"{alias} is deprecated.*0\.25\.0.*{canonical}"):
        function = getattr(mod, alias)
    assert function is getattr(importlib.import_module(PROFILE_MODULE), canonical)
    assert alias not in mod.__all__


def test_profile_json_warns_and_still_serialises():
    import json

    from cnequity.domain import universe_profiles

    with pytest.warns(DeprecationWarning, match="profile_json is deprecated"):
        profile_json = universe_profiles.profile_json
    payload = json.loads(profile_json("cn_a_sh_sz_research_v1"))
    assert payload == universe_profiles.show_universe_profile("cn_a_sh_sz_research_v1")


def test_universe_profile_argument_warns_and_resolves_like_profile():
    from cnequity.query.reader import _resolve_reader_scope

    with pytest.warns(DeprecationWarning, match=r"universe_profile=.*profile="):
        old = _resolve_reader_scope(
            universe=None,
            profile=None,
            universe_profile="cn_a_sh_sz_research_v1",
            strict_universe=False,
        )
    new = _resolve_reader_scope(
        universe=None,
        profile="cn_a_sh_sz_research_v1",
        universe_profile=None,
        strict_universe=False,
    )
    assert old == new
