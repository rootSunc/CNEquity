from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS

StepFn = Callable[[Config, date, str, dict], dict]

# Steps in these groups always run after all other steps in the same wave/group.
FINALIZE_STEP_GROUPS = frozenset({"finalize"})


class DuplicateStepError(ValueError):
    """Raised when a second step registers a name that is already taken.

    Registration is a side effect of import, and the registry used to keep
    whichever module happened to import last — a silent swap of the function
    every run executes under that name.
    """


@dataclass
class StepEntry:
    fn: StepFn
    depends_on: list[str] = field(default_factory=list)
    group: str = "core"
    description: str = ""
    parallelizable: bool = True
    requires_workers: bool = False
    # Datasets whose *committed* generation the step reads. A producer that
    # runs in the same run is not visible here until compact publishes it;
    # see ``deps.committed_reads_in_run``.
    input_datasets: tuple[str, ...] = ()
    # Datasets the step writes (staging for curated datasets, a published
    # generation for derived ones). Empty for steps that only check or
    # publish, such as ``compact`` and ``audit``.
    outputs: tuple[str, ...] = ()


STEP_REGISTRY: OrderedDict[str, StepEntry] = OrderedDict()


def _default_outputs(name: str) -> tuple[str, ...]:
    return (name,) if name in DATASETS else ()


def register_step(
    name: str,
    *,
    depends_on: list[str] | None = None,
    group: str = "core",
    description: str = "",
    parallelizable: bool = True,
    requires_workers: bool = False,
    input_datasets: tuple[str, ...] = (),
    outputs: tuple[str, ...] | None = None,
) -> Callable[[StepFn], StepFn]:
    """Register a step under *name*.

    *outputs* defaults to the dataset of the same name, when there is one —
    the convention almost every fetch step follows. A step that writes a
    differently named dataset (``derive_adj_factors``, ``daily_bars_history``)
    says so explicitly, so the step → dataset mapping lives here rather than
    in tables kept by each consumer.
    """
    resolved_outputs = _default_outputs(name) if outputs is None else tuple(outputs)
    unknown = [
        dataset for dataset in (*resolved_outputs, *input_datasets) if dataset not in DATASETS
    ]
    if unknown:
        raise KeyError(f"step {name!r} names unregistered dataset(s): {', '.join(unknown)}")

    def decorator(fn: StepFn) -> StepFn:
        existing = STEP_REGISTRY.get(name)
        if existing is not None and existing.fn is not fn:
            raise DuplicateStepError(
                f"step {name!r} is already registered by "
                f"{existing.fn.__module__}.{existing.fn.__qualname__}"
            )
        STEP_REGISTRY[name] = StepEntry(
            fn=fn,
            depends_on=depends_on or [],
            group=group,
            description=description or fn.__doc__ or name,
            parallelizable=parallelizable,
            requires_workers=requires_workers,
            input_datasets=input_datasets,
            outputs=resolved_outputs,
        )
        return fn

    return decorator


def get_step(name: str) -> StepEntry:
    if name not in STEP_REGISTRY:
        raise KeyError(f"Unknown step: {name}")
    return STEP_REGISTRY[name]


def receipt_dataset(name: str) -> str:
    """The dataset key a step's manifest receipts are recorded under.

    A finalize-group step with exactly one output records under that output
    (``derive_adj_factors`` → ``adj_factors``): it is the only writer of its
    dataset in the run. Every other step keeps its own name, because a second
    writer of the same dataset in the run (``trading_status_derive`` beside
    the ``trading_status`` fetch, ``daily_bars_history`` beside
    ``daily_bars``) would otherwise overwrite that step's receipt.
    """
    entry = STEP_REGISTRY.get(name)
    if entry is not None and entry.group in FINALIZE_STEP_GROUPS and len(entry.outputs) == 1:
        return entry.outputs[0]
    return name
