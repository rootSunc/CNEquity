from __future__ import annotations

from cnequity.orchestrator.registry import FINALIZE_STEP_GROUPS, STEP_REGISTRY, get_step

# Hard ordering for finalize steps — do not rely on registration or alphabet sort alone.
FINALIZE_STEP_ORDER = (
    "compact",
    "derive_adj_factors",
    "derive_industry_index",
    "derive_futures_continuous",
    "derive_option_greeks",
    "ths_official_snapshot",
    "audit",
)


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
    """Serialize finalization, respecting dependencies before preferred order."""
    names = set(finalize_steps)
    preferred = [s for s in FINALIZE_STEP_ORDER if s in names]
    preferred.extend(sorted(names - set(FINALIZE_STEP_ORDER)))
    remaining = set(names)
    levels = []
    while remaining:
        ready = next(
            (
                name
                for name in preferred
                if name in remaining and not (set(get_step(name).depends_on) & remaining)
            ),
            None,
        )
        if ready is None:
            raise CyclicDependencyError(f"Cyclic finalize dependencies: {sorted(remaining)}")
        levels.append([ready])
        remaining.remove(ready)
    return levels


def _deferred_steps(step_names: list[str]) -> set[str]:
    """Finalize-group steps and everything downstream of them in *step_names*."""
    deferred = {n for n in step_names if get_step(n).group in FINALIZE_STEP_GROUPS}
    while True:
        downstream = {n for n in step_names if set(get_step(n).depends_on) & deferred}
        if downstream <= deferred:
            return deferred
        deferred.update(downstream)


def committed_reads_in_run(step_names: list[str]) -> list[tuple[str, str, str]]:
    """``(consumer, dataset, producer)`` where a step reads last run's generation.

    ``input_datasets`` are read from the *committed* lake. When the step that
    writes such a dataset runs in the same run, before ``compact``, the
    consumer sees the previous generation, not this run's rows. That is
    sometimes the intent (``trading_status_derive`` cannot see a halt on the
    newest session either way) and sometimes a surprise, and ``depends_on``
    alone does not say which: it orders the steps, it does not publish
    anything. This makes the situation visible; it changes no plan.
    """
    validate_steps_registered(step_names)
    deferred = _deferred_steps(step_names)
    found: list[tuple[str, str, str]] = []
    for consumer in step_names:
        if consumer in deferred:
            continue  # runs after compact: sees this run's publication
        for dataset in get_step(consumer).input_datasets:
            for producer in step_names:
                if producer == consumer or producer in deferred:
                    continue
                if dataset in get_step(producer).outputs:
                    found.append((consumer, dataset, producer))
    return found


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
    # A core step may consume compacted data. Defer its transitive dependents
    # as well instead of treating the fetch/finalize boundary as a cycle.
    deferred = _deferred_steps(step_names)
    fetch_steps = [n for n in step_names if n not in deferred]
    finalize_steps = [n for n in step_names if n in deferred]

    levels: list[list[str]] = []
    if fetch_steps:
        levels.extend(_levels_for(fetch_steps, names_set=names_set, already_done=set()))

    if finalize_steps:
        levels.extend(_finalize_execution_levels(finalize_steps))

    return levels
