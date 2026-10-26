"""Turn a full config into an ``extends = "builtin"`` one: `cne config upgrade --minimize`.

What stays in the file is what differs from the built-in defaults:

* a key equal to its default is dropped, so it follows later releases;
* a key the defaults do not have is kept as written;
* a schedule group whose steps differ is written as ``steps_add`` /
  ``steps_remove``. The order inside a group carries no meaning — the engine
  levels steps by dependency, finalize order and name
  (`orchestrator.deps.step_execution_levels`) — so the edits always reproduce
  what the group runs, and a list that differs only in order is dropped;
* a default schedule group the config does not have becomes ``enabled = false``,
  since today it does not run;
* ``[data].root`` is written as the absolute path the config resolves to now.

Steps and groups are therefore kept exactly as the config schedules them today;
a step the config was merely missing is listed for review rather than silently
added. Keys the old config left out are the one place behaviour can change —
they now take the template's value instead of the loader's fallback — so the
two configs are loaded and every differing setting is reported before writing.
"""

from __future__ import annotations

import dataclasses
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10
    import tomli as tomllib  # type: ignore

from cnequity.config.layers import BUILTIN, EXTENDS_KEY, GROUP_SCOPES, builtin_defaults, is_layered
from cnequity.config.toml_out import dumps

HEADER = """\
# CNEquity lake config.
#
# Only settings that differ from the built-in defaults belong here. The defaults
# ship with the installed version, so a later release's new schedule steps and
# settings apply without editing this file.
#
#   cne config show             the effective configuration, marking what this file sets
#   cne config show --defaults  every setting with its default and an explanation
#
# A schedule group can swap one step without freezing the rest of it:
#   [job.daily.groups.capital]
#   steps_remove = ["fund_flow"]
#   steps_add = ["fund_flow_ths"]
"""


@dataclass
class MinimizePlan:
    """What `--minimize` would write, and what a reviewer should look at first."""

    text: str = ""
    already_layered: bool = False
    #: ``job.daily.groups.capital`` → (added, removed) relative to the defaults.
    step_edits: dict[str, tuple[list[str], list[str]]] = field(default_factory=dict)
    disabled_groups: list[str] = field(default_factory=list)
    #: ``(setting, before, after)`` where loading the new file changes the lake's behaviour.
    behaviour_changes: list[tuple[str, str, str]] = field(default_factory=list)
    kept_keys: int = 0
    total_keys: int = 0


def render_minimal(data_root: str | Path) -> str:
    """The config `cne config create` writes: defaults plus where the lake lives."""
    return HEADER + dumps({EXTENDS_KEY: BUILTIN, "data": {"root": Path(data_root).as_posix()}})


def _is_group(path: list[str]) -> bool:
    return len(path) == 4 and path[0] == "job" and path[1] in GROUP_SCOPES and path[2] == "groups"


def _group_diff(mine: dict, theirs: dict, table: str, plan: MinimizePlan) -> dict:
    out = {k: v for k, v in mine.items() if k != "steps" and (k not in theirs or theirs[k] != v)}
    steps, default = mine.get("steps"), list(theirs.get("steps", []) or [])
    if steps is None or set(steps) == set(default):
        return out
    added = [s for s in dict.fromkeys(steps) if s not in default]
    removed = [s for s in default if s not in steps]
    if added:
        out["steps_add"] = added
    if removed:
        out["steps_remove"] = removed
    plan.step_edits[table] = (added, removed)
    return out


def _diff(mine: dict, theirs: dict, path: list[str], plan: MinimizePlan) -> dict:
    out: dict[str, Any] = {}
    for name, item in mine.items():
        here = [*path, name]
        if name not in theirs:
            out[name] = item
            continue
        default = theirs[name]
        if _is_group(here) and isinstance(item, dict) and isinstance(default, dict):
            diff = _group_diff(item, default, ".".join(here), plan)
            if diff:
                out[name] = diff
        elif isinstance(item, dict) and isinstance(default, dict):
            diff = _diff(item, default, here, plan)
            if diff:
                out[name] = diff
        elif item != default:
            out[name] = item
    return out


def _count(table: dict) -> int:
    return sum(_count(v) if isinstance(v, dict) else 1 for v in table.values())


def _settings(cfg) -> dict[str, tuple[Any, bool]]:
    out = {
        f.name: (getattr(cfg, f.name), f.repr)
        for f in dataclasses.fields(cfg)
        if f.name != "config_path"
    }
    # Step order inside a group is not behaviour (see the module docstring).
    for name in ("schedule_groups", "events_groups"):
        if name in out:
            groups, visible = out[name]
            out[name] = (
                {k: dataclasses.replace(g, steps=sorted(g.steps)) for k, g in groups.items()},
                visible,
            )
    return out


def _show(item: Any, visible: bool) -> str:
    if not visible:
        return "(hidden)"
    text = repr(item)
    return text if len(text) <= 120 else text[:117] + "..."


def plan_minimize(config_path: Path | str) -> MinimizePlan:
    """Work out the minimal file for *config_path* and how loading it would differ."""
    from cnequity.config.loader import load_config

    path = Path(config_path)
    text = path.read_text(encoding="utf-8")
    mine = tomllib.loads(text)
    plan = MinimizePlan()
    if is_layered(mine):
        plan.already_layered = True
        return plan

    before = load_config(path)
    theirs = builtin_defaults()
    overrides = _diff(mine, theirs, [], plan)
    for scope in GROUP_SCOPES:
        defaults = theirs.get("job", {}).get(scope, {}).get("groups", {}) or {}
        own = mine.get("job", {}).get(scope, {}).get("groups", {}) or {}
        for name in defaults:
            if name not in own:
                groups = overrides.setdefault("job", {}).setdefault(scope, {})
                groups.setdefault("groups", {})[name] = {"enabled": False}
                plan.disabled_groups.append(f"job.{scope}.groups.{name}")
    overrides.setdefault("data", {})["root"] = before.data_root.as_posix()
    # A full config's group lists turn its best-effort datasets on; the layered
    # file drops those lists, so it has to say so explicitly.
    enabled = sorted(before.best_effort_enabled or ())
    if enabled:
        overrides.setdefault("datasets", {})["best_effort"] = enabled

    body = {EXTENDS_KEY: BUILTIN, "data": overrides.pop("data"), **overrides}
    plan.text = HEADER + dumps(body)
    plan.total_keys = _count(mine)
    plan.kept_keys = _count(body) - 1

    # Load the candidate beside the original, so relative paths read the same.
    with tempfile.NamedTemporaryFile(
        "w", suffix=".toml", dir=path.parent, delete=False, encoding="utf-8"
    ) as handle:
        handle.write(plan.text)
        candidate = Path(handle.name)
    try:
        after = load_config(candidate)
    finally:
        candidate.unlink(missing_ok=True)
    old, new = _settings(before), _settings(after)
    for name, (value, visible) in old.items():
        changed = new[name][0]
        if changed == value:
            continue
        if isinstance(value, dict) and isinstance(changed, dict):
            # Name the entries that differ, not two near-identical dicts.
            for entry in sorted(set(value) | set(changed), key=str):
                if value.get(entry) != changed.get(entry):
                    plan.behaviour_changes.append(
                        (
                            f"{name}[{entry}]",
                            _show(value.get(entry), visible),
                            _show(changed.get(entry), visible),
                        )
                    )
        else:
            plan.behaviour_changes.append((name, _show(value, visible), _show(changed, visible)))
    return plan
