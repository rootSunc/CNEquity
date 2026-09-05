"""The published catalog must list what the registry actually holds.

``docs/datasets/catalog.md`` carries per-dataset prose (主源, 备注) that has no
home in ``DatasetSpec``, so the document is written by hand rather than
generated. That leaves it free to drift: before this guard existed the L7 table
was missing ``flash_news_wire`` and ``economic_calendar``, and ``industry_index``
appeared in no tier table at all.
"""

from __future__ import annotations

import re
from pathlib import Path

from cnequity.domain.datasets import DATASETS, datasets_by_tier

CATALOG = Path(__file__).resolve().parents[2] / "docs" / "datasets" / "catalog.md"

_SECTION = re.compile(r"^##\s+(L[0-8])\b")
_ROW = re.compile(r"^\|\s*([a-z][a-z0-9_]*)\s*\|")


def _documented_by_tier() -> dict[str, list[str]]:
    """Dataset names appearing in each ``## L<n>`` section's tables."""
    documented: dict[str, list[str]] = {}
    tier: str | None = None
    for line in CATALOG.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            match = _SECTION.match(line)
            # Any other h2 (采集模式, 主备配置, Step → 数据集映射) closes the
            # tier section; those tables mention datasets too.
            tier = match.group(1) if match else None
            if tier:
                documented.setdefault(tier, [])
            continue
        row = _ROW.match(line)
        if tier and row and row.group(1) in DATASETS:
            documented[tier].append(row.group(1))
    return documented


def test_catalog_tier_tables_match_the_registry():
    documented = _documented_by_tier()
    registered = {tier: names for tier, names in datasets_by_tier().items() if names}

    assert set(documented) == set(registered), "catalog tier sections differ from registry tiers"
    for tier in sorted(registered):
        assert sorted(documented[tier]) == sorted(registered[tier]), (
            f"{tier}: catalog lists {sorted(documented[tier])}, "
            f"registry has {sorted(registered[tier])}"
        )


def test_catalog_lists_every_dataset_exactly_once():
    listed = [name for names in _documented_by_tier().values() for name in names]
    assert len(listed) == len(set(listed)), "a dataset is documented under two tiers"
    assert set(listed) == set(DATASETS)


def test_catalog_header_states_the_registered_count():
    """The intro sentence hard-codes the total; keep it honest."""
    header = CATALOG.read_text(encoding="utf-8").split("---", 1)[0]
    assert f"**{len(DATASETS)} 个注册数据集**" in header


def test_catalog_history_floor_table_has_no_duplicate_datasets():
    """The measured source-history table must not repeat a dataset row."""
    text = CATALOG.read_text(encoding="utf-8")
    history = text.split("### 历史视野", 1)[1].split("### `trade_ticks` 的容量", 1)[0]
    names = [
        match.group(1) for match in re.finditer(r"^\|\s*([a-z][a-z0-9_]*)\s*\|", history, re.M)
    ]
    assert len(names) == len(set(names)), f"duplicate history-floor rows: {names}"


# --- sources.md group labels -------------------------------------------------
# Same failure mode, different file: sources.md tags each dataset with the
# schedule group and start time it runs in. Both drifted — fund_flow was labelled
# core@16:30 and margin_trading signals@17:00 when both run in capital — and one
# of them had been wrong since before the start times moved at all.

SOURCES = Path(__file__).resolve().parents[2] / "docs" / "datasets" / "sources.md"
_GROUP_ROW = re.compile(r"\|\s*分组\s*\|\s*([^|]+?)\s*\|")


def _shipped_step_groups() -> dict[str, str]:
    import sys
    from unittest.mock import patch

    from cnequity.config import load_config

    example = Path(__file__).resolve().parents[2] / "configs" / "cnequity.example.toml"
    with patch.object(sys, "platform", "linux"):
        cfg = load_config(example)
    # Events groups are scheduled the same way and documented in the same
    # column; leaving them out here is how a doc row would quietly stop being
    # checked the moment a feed moved between the two jobs.
    groups = {**cfg.schedule_groups, **cfg.events_groups}
    return {
        step: f"{name}@{group.at}"
        for name, group in groups.items()
        for step in group.steps
        if step != "compact"
    }


def test_sources_group_labels_match_the_shipped_schedule():
    step_groups = _shipped_step_groups()
    mismatches = []
    for block in SOURCES.read_text(encoding="utf-8").split("\n#### ")[1:]:
        heading = block.split("\n", 1)[0].strip()
        row = _GROUP_ROW.search(block)
        if row is None:
            continue
        for dataset in (part.strip() for part in heading.split("/")):
            if dataset in step_groups:
                if row.group(1) != step_groups[dataset]:
                    mismatches.append(
                        f"{dataset}: doc={row.group(1)} config={step_groups[dataset]}"
                    )
                break
    assert mismatches == []


# --- README data table -------------------------------------------------------
# The README's dataset table names a primary and backup source per dataset. It
# is written from DatasetSpec.primary_source / .backup_source precisely because
# the hand-written source tables drifted — sector_bars sat on EastMoney long
# after it moved to 同花顺, and fund_flow was filed under the wrong group.

README = Path(__file__).resolve().parents[2] / "README.md"
_DATA_ROW = re.compile(r"^\|\s*`([a-z][a-z0-9_]*)`\s*○?\s*\|([^|]*)\|([^|]*)\|([^|]*)\|")


def _readme_rows() -> dict[str, tuple[str, str]]:
    rows: dict[str, tuple[str, str]] = {}
    for line in README.read_text(encoding="utf-8").splitlines():
        m = _DATA_ROW.match(line.strip())
        if m:
            rows[m.group(1)] = (m.group(3).strip(), m.group(4).strip())
    return rows


def test_readme_table_lists_every_registered_dataset():
    rows = _readme_rows()
    assert set(rows) == set(DATASETS), (
        f"missing from README: {sorted(set(DATASETS) - set(rows))}; "
        f"unknown in README: {sorted(set(rows) - set(DATASETS))}"
    )


def test_readme_sources_match_the_registry():
    mismatches = []
    for name, (primary, backup) in _readme_rows().items():
        spec = DATASETS[name]
        want_backup = spec.backup_source or "—"
        if primary != spec.primary_source or backup != want_backup:
            mismatches.append(
                f"{name}: README={primary}/{backup} registry={spec.primary_source}/{want_backup}"
            )
    assert mismatches == []


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
    """An empty primary_source would render as a blank cell in the README."""
    assert [n for n, s in DATASETS.items() if not s.primary_source] == []
