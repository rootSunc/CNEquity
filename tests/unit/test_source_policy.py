"""The source policy matrix must cover the dataset registry conservatively."""

from __future__ import annotations

from pathlib import Path

import pytest

from cnequity.compliance.source_policy import (
    PERMISSION_FIELDS,
    REQUIRED_FIELDS,
    SourcePolicyValidationError,
    is_explicitly_allowed,
    load_source_policies,
    policies_for_dataset,
    required_sources,
    usage_profile,
    validate_source_policies,
)

ROOT = Path(__file__).resolve().parents[2]


def test_matrix_covers_every_registered_source():
    policies = load_source_policies()

    assert required_sources() <= policies.keys()
    assert policies["derived"].derived is True
    assert policies_for_dataset("daily_bars", policies).primary.name == "tdx_protocol"


def test_every_policy_has_nonempty_required_fields():
    policies = load_source_policies()

    for source, policy in policies.items():
        assert REQUIRED_FIELDS <= set(policy)
        for field_name in REQUIRED_FIELDS:
            value = policy[field_name]
            assert value is not None, (source, field_name)
            assert not isinstance(value, str) or value.strip(), (source, field_name)


def test_unknown_permission_is_not_an_allow():
    policies = load_source_policies()
    policy = policies["sina"]

    for field_name in PERMISSION_FIELDS:
        assert policy[field_name] == "unknown"
        assert is_explicitly_allowed(policy[field_name]) is False

    assessment = usage_profile(policy, commercial=True, redistribution=True)
    assert assessment.allowed is False
    assert assessment.decision == "blocked"
    assert set(assessment.blocked_fields) == {"commercial_use", "redistribution"}


def test_reviewed_restricted_terms_remain_fail_closed():
    policies = load_source_policies()
    for source in ("eastmoney", "ths"):
        policy = policies[source]
        assert policy["tos_url"].startswith("https://")
        assert policy["tos_reviewed_at"] == "2026-08-29"
        assert policy["legal_status"] == "restricted"
        assert not is_explicitly_allowed(policy["commercial_use"])
        assert not is_explicitly_allowed(policy["redistribution"])
        assessment = usage_profile(policy, commercial=True, redistribution=True)
        assert assessment.decision == "blocked"


def test_commercial_and_redistribution_limits_are_exposed():
    assessment = usage_profile("derived", profile="commercial")
    assert assessment["allowed"] is False
    assert "commercial_use" in assessment["blocked_fields"]
    assert any("commercial" in reason for reason in assessment.reasons)


def test_validator_reports_missing_fields_and_registry_sources():
    errors = validate_source_policies(
        {
            "derived": {
                "owner": "local",
                "access_type": "local_derivation",
                "tos_url": "unknown",
                "tos_reviewed_at": "unknown",
                "authentication": "not_applicable",
                "personal_use": "unknown",
                "commercial_use": "unknown",
                "cache_allowed": "unknown",
                "redistribution": "unknown",
                "rate_limit": "not_applicable",
                "retained_payloads": "unknown",
                "legal_status": "unknown",
                # notes intentionally omitted
                "derived": True,
            }
        }
    )
    assert any("missing required field 'notes'" in error for error in errors)
    assert any("missing policy for registered source 'eastmoney'" in error for error in errors)


def test_load_rejects_incomplete_policy_document(tmp_path):
    path = tmp_path / "SOURCES.yml"
    path.write_text("sources:\n  test:\n    owner: test\n", encoding="utf-8")

    with pytest.raises(SourcePolicyValidationError):
        load_source_policies(path)


def test_an_operator_invoked_repair_is_declared_without_becoming_a_fallback():
    """`cne ths-official resource-sectors` writes licensed rows into sector_bars.

    Its terms apply to those rows, so the compliance matrix has to speak for
    them — that is what `unrouted_source` was reporting. But nothing schedules
    the command, so declaring it as a route would tell the resilience report
    that `sector_bars` has a live fallback it does not have.
    """
    from cnequity.compliance.source_policy import policies_for_dataset
    from cnequity.diagnostics.substitutes import _declared_sources
    from cnequity.domain.datasets import DATASETS

    assert DATASETS["sector_bars"].repair_sources == ("ths_official",)

    policy = policies_for_dataset("sector_bars")
    assert "ths_official" in {item.name for item in policy.all}
    assert [item.name for item in policy.repair] == ["ths_official"]

    assert "ths_official" not in {source for source, _role in _declared_sources("sector_bars")}


def test_the_audit_no_longer_calls_a_declared_repair_source_unrouted(tmp_path):
    from datetime import date

    import polars as pl

    from cnequity.config import Config
    from cnequity.domain.schemas import data_version_for, with_provenance
    from cnequity.quality.cross_checks import undeclared_source_findings

    part = tmp_path / "curated" / "sector_bars" / "trade_date=2026-09-04"
    part.mkdir(parents=True, exist_ok=True)
    with_provenance(
        pl.DataFrame(
            {
                "symbol": ["881101.TI"],
                "trade_date": [date(2026, 9, 4)],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1],
                "amount": [1.0],
            }
        ),
        source="ths_official",
        data_version=data_version_for("sector_bars"),
    ).write_parquet(part / "part-merged.parquet")

    findings = undeclared_source_findings(Config(data_root=tmp_path))

    assert [f for f in findings if f["check"] == "unrouted_source"] == []


def test_a_config_that_still_sets_the_dead_universe_default_keeps_loading(tmp_path, caplog):
    """It was parsed for a long time and read by nothing.

    `load()` resolves its universe from the call and the profile. Silently
    accepting the key let an operator write down an intention the lake never
    honoured, so it is dropped from the template and announced when present —
    but an existing config must not stop loading over it.
    """
    import logging

    from cnequity.config import load_config

    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{(tmp_path / "lake").as_posix()}"\n\n'
        '[universe]\ndefault = "all_a"\ningest = "all_a"\n',
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger="cnequity.config.loader"):
        cfg = load_config(str(path))

    assert cfg.ingest_universe == "all_a"
    assert not hasattr(cfg, "universe_default")
    assert any("[universe].default" in record.message for record in caplog.records)
