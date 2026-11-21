"""Check that the published catalog and configured sources match the registry."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from cnequity.domain.datasets import DATASETS, datasets_by_tier

DOCS = Path(__file__).resolve().parents[2] / "docs"
CATALOG = DOCS / "zh" / "datasets" / "catalog.md"
# The English catalog is a translation of the same tables; it drifts the same way.
CATALOGS = [CATALOG, DOCS / "en" / "datasets" / "catalog.md"]
_SECTION = re.compile(r"^##\s+(L[0-9])\b")
_ROW = re.compile(r"^\|\s*([a-z][a-z0-9_]*)\s*\|")


def _documented_by_tier(catalog: Path) -> dict[str, list[str]]:
    """Dataset names appearing in each ``## L<n>`` section's tables."""
    documented: dict[str, list[str]] = {}
    tier: str | None = None
    for line in catalog.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            match = _SECTION.match(line)
            tier = match.group(1) if match else None
            if tier:
                documented.setdefault(tier, [])
            continue
        row = _ROW.match(line)
        if tier and row and row.group(1) in DATASETS:
            documented[tier].append(row.group(1))
    return documented


@pytest.mark.parametrize("catalog", CATALOGS, ids=["zh", "en"])
def test_catalog_tier_tables_match_the_registry(catalog):
    documented = _documented_by_tier(catalog)
    registered = {tier: names for tier, names in datasets_by_tier().items() if names}

    assert set(documented) == set(registered), "catalog tier sections differ from registry tiers"
    for tier in sorted(registered):
        assert sorted(documented[tier]) == sorted(registered[tier]), (
            f"{tier}: catalog lists {sorted(documented[tier])}, "
            f"registry has {sorted(registered[tier])}"
        )


def test_failover_sources_match_the_registry():
    """Configured failover precedence must match canonical query precedence."""
    import sys
    from unittest.mock import patch

    from cnequity.config import load_config

    example = Path(__file__).resolve().parents[2] / "configs" / "cnequity.example.toml"
    with patch.object(sys, "platform", "linux"):
        cfg = load_config(example)

    mismatches = []
    for failover in cfg.failover_datasets:
        spec = DATASETS.get(failover.name)
        if spec is None:
            mismatches.append(f"{failover.name}: missing from registry")
            continue
        if (spec.primary_source, spec.backup_source) != (failover.primary, failover.backup):
            mismatches.append(
                f"{failover.name}: registry={spec.primary_source}/{spec.backup_source} "
                f"config={failover.primary}/{failover.backup}"
            )
    assert mismatches == []


def test_every_dataset_declares_a_primary_source():
    """An empty primary_source would leave the catalog 主源 cell without a feed."""
    assert [n for n, s in DATASETS.items() if not s.primary_source] == []
