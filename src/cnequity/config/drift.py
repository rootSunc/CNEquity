"""Report how a user config has fallen behind the packaged example.

``cne config init`` writes the config once. Every release after that may add a
section, a key, or — the case that actually loses data — a step to a schedule
group. Nothing tells the operator: the file is gitignored, missing sections
silently take their defaults, and ``cne config validate`` answers
``Configuration OK`` for a config whose ``core`` group is missing a step the
shipped example has been running for weeks.

A missing *key* is usually harmless (the default applies). A missing *step* is
not: a step that appears in no group and no wave is never scheduled, so the
feature it implements is installed and never runs.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10
    import tomli as tomllib  # type: ignore

from cnequity.config.bootstrap import example_toml_text

# Local by construction: reporting these as drift would be noise on every run.
_LOCAL_ONLY_KEYS = frozenset(
    {
        "data.root",
        "orchestrator.workers",
        "orchestrator.tdx_daily_backend",
    }
)


@dataclass
class ConfigDrift:
    """What the packaged example has that this config does not."""

    missing_sections: list[str] = field(default_factory=list)
    missing_keys: list[str] = field(default_factory=list)
    unscheduled_steps: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not (self.missing_sections or self.missing_keys or self.unscheduled_steps)


def _flatten(table: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten nested tables to dotted paths, keeping arrays-of-tables opaque.

    An array of tables (``[[job.daily.waves]]``) is compared as a whole by the
    step check below; walking into it would report every wave index as drift
    for an operator who merely reordered them.
    """
    out: dict[str, Any] = {}
    for key, value in table.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            out[path] = value
            out.update(_flatten(value, f"{path}."))
        else:
            out[path] = value
    return out


def _scheduled_steps(raw: dict) -> set[str]:
    """Every step name this config would ever run, from waves and both group maps."""
    steps: set[str] = set()
    job = raw.get("job", {})
    for wave in job.get("daily", {}).get("waves", []) or []:
        steps.update(wave.get("steps", []) or [])
    for scope in ("daily", "events"):
        for group in (job.get(scope, {}).get("groups", {}) or {}).values():
            steps.update(group.get("steps", []) or [])
    return steps


def config_drift(config_path: Path | str) -> ConfigDrift:
    """Compare *config_path* against the packaged example template."""
    with open(config_path, "rb") as handle:
        mine = tomllib.load(handle)
    theirs = tomllib.loads(example_toml_text())

    mine_flat = _flatten(mine)
    theirs_flat = _flatten(theirs)

    drift = ConfigDrift()
    for path, value in theirs_flat.items():
        if path in mine_flat or path in _LOCAL_ONLY_KEYS:
            continue
        if isinstance(value, dict):
            # Report the outermost missing table only; its children add nothing.
            if not any(path.startswith(f"{parent}.") for parent in drift.missing_sections):
                drift.missing_sections.append(path)
        elif not any(path.startswith(f"{parent}.") for parent in drift.missing_sections):
            drift.missing_keys.append(path)

    drift.unscheduled_steps = sorted(_scheduled_steps(theirs) - _scheduled_steps(mine))
    drift.missing_sections.sort()
    drift.missing_keys.sort()
    return drift


def render_drift(drift: ConfigDrift, config_path: Path | str) -> list[str]:
    """Human-readable report lines, most consequential first."""
    if drift.clean:
        return [f"{config_path} 与打包的示例配置一致。"]

    lines: list[str] = []
    if drift.unscheduled_steps:
        lines.append(f"未被调度的 step（{len(drift.unscheduled_steps)}）—— 装了但从来不会跑：")
        lines.extend(f"  {step}" for step in drift.unscheduled_steps)
        lines.append("  → 把它们加进 [job.daily.groups.*] 或 [[job.daily.waves]] 的 steps")
        lines.append("")
    if drift.missing_sections:
        lines.append(f"缺少的配置段（{len(drift.missing_sections)}）—— 当前使用内置默认值：")
        lines.extend(f"  [{section}]" for section in drift.missing_sections)
        lines.append("")
    if drift.missing_keys:
        lines.append(f"缺少的配置项（{len(drift.missing_keys)}）—— 当前使用内置默认值：")
        lines.extend(f"  {key}" for key in drift.missing_keys)
        lines.append("")
    lines.append(f"对照：{Path(__file__).parent / 'templates' / 'cnequity.example.toml'}")
    return lines
