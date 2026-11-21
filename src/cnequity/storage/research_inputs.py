"""Explicit, credential-free inputs needed to replay historical universe reads."""

from __future__ import annotations

import json
from pathlib import Path

from cnequity.config import Config
from cnequity.domain.canonical import canonical_policy
from cnequity.domain.universe_profiles import PROFILE_REGISTRY

RESEARCH_DATASETS = frozenset(
    {"daily_bars", "adj_factors", "instruments", "trading_status", "trading_calendar"}
)
RESEARCH_METADATA = (
    "quality/coverage/historical_st_evidence",
    "quality/coverage/delisted_daily_bars_recovery",
    "quality/evidence/delisted_security_identity",
    "source_cache/delisted_catalog.json",
    "source_cache/historical_st_evidence",
    "seeds/trading_calendar.csv",
)
CONTEXT_PATH = Path("research/read-context.json")


def research_context(config: Config, datasets: list[str]) -> dict:
    restored = config.meta_root / CONTEXT_PATH
    if restored.exists():
        return load_research_context(config)
    return {
        "schema_version": 1,
        "tushare_st_enabled": bool(config.sources.get("tushare", False) and config.tushare_token),
        "canonical_policies": {name: canonical_policy(name) for name in datasets},
        "universe_profiles": {
            name: profile.to_dict() for name, profile in PROFILE_REGISTRY.items()
        },
    }


def load_research_context(config: Config) -> dict:
    payload = json.loads((config.meta_root / CONTEXT_PATH).read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1 or not isinstance(
        payload.get("tushare_st_enabled"), bool
    ):
        raise ValueError("invalid frozen research read context")
    return payload


def validate_research_policies(
    config: Config, datasets: set[str], profile_name: str | None = None
) -> None:
    if not (config.meta_root / CONTEXT_PATH).exists():
        return
    context = load_research_context(config)
    for name in datasets:
        expected = context.get("canonical_policies", {}).get(name)
        if expected is not None and expected != canonical_policy(name):
            raise ValueError(f"research snapshot policy mismatch for {name}: {expected}")
    if profile_name is not None:
        profile = context.get("universe_profiles", {}).get(profile_name)
        if profile is not None and (
            profile_name not in PROFILE_REGISTRY
            or PROFILE_REGISTRY[profile_name].to_dict() != profile
        ):
            raise ValueError(f"research snapshot universe profile mismatch: {profile_name}")
