from __future__ import annotations

from cn_market_lake.orchestrator.registry import FINALIZE_STEP_GROUPS, STEP_REGISTRY, get_step

# Hard ordering for finalize steps — do not rely on registration or alphabet sort alone.
FINALIZE_STEP_ORDER = ("compact", "derive_adj_factors", "derive_industry_index", "audit")


class CyclicDependencyError(ValueError):
    """Raised when step dependencies form a cycle within a wave."""


class UnknownStepError(KeyError):
    """Raised when a configured step is not registered."""


def validate_steps_registered(step_names: list[str]) -> None:
    unknown = [name for name in step_names if name not in STEP_REGISTRY]
    if unknown:
        raise UnknownStepError(f"Unknown steps: {', '.join(unknown)}")


def _levels_for(
    step_names: list[str],
    *,
    names_set: set[str],
    already_done: set[str],
) -> list[list[str]]:
    remaining = set(step_names)
    done = set(already_done)
    levels: list[list[str]] = []

    while remaining:
        ready: list[str] = []
        for name in sorted(remaining):
            entry = get_step(name)
            internal_deps = [dep for dep in entry.depends_on if dep in names_set]
            if all(dep in done for dep in internal_deps):
                ready.append(name)

        if not ready:
            raise CyclicDependencyError(
                f"Cyclic or unsatisfied dependencies among steps: {sorted(remaining)}"
            )

        levels.append(ready)
        done.update(ready)
        remaining -= set(ready)

    return levels


def _finalize_execution_levels(finalize_steps: list[str]) -> list[list[str]]:
    """Return one step per level in canonical finalize order."""
    names = set(finalize_steps)
    ordered = [s for s in FINALIZE_STEP_ORDER if s in names]
    ordered.extend(sorted(names - set(FINALIZE_STEP_ORDER)))
    return [[step] for step in ordered]


def step_execution_levels(step_names: list[str]) -> list[list[str]]:
    """Group steps into dependency levels that may run in parallel within each level.

    Dependencies on steps outside *step_names* are treated as already satisfied
    (typically fulfilled by earlier waves or schedule groups).

    Steps in finalize groups (``compact``, ``derive_adj_factors``,
    ``derive_industry_index``, ``audit``) are deferred until every non-finalize
    step in *step_names* has completed, so ``compact`` never runs against an
    empty staging run.
    """
    validate_steps_registered(step_names)

    names_set = set(step_names)
    fetch_steps = [n for n in step_names if get_step(n).group not in FINALIZE_STEP_GROUPS]
    finalize_steps = [n for n in step_names if get_step(n).group in FINALIZE_STEP_GROUPS]

    levels: list[list[str]] = []
    if fetch_steps:
        levels.extend(_levels_for(fetch_steps, names_set=names_set, already_done=set()))

    if finalize_steps:
        levels.extend(_finalize_execution_levels(finalize_steps))

    return levels
