from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

from cn_market_lake.config import Config

StepFn = Callable[[Config, date, str, dict], dict]

# Steps in these groups always run after all other steps in the same wave/group.
FINALIZE_STEP_GROUPS = frozenset({"finalize"})


@dataclass
class StepEntry:
    fn: StepFn
    depends_on: list[str] = field(default_factory=list)
    group: str = "core"
    description: str = ""
    parallelizable: bool = True
    requires_workers: bool = False


STEP_REGISTRY: OrderedDict[str, StepEntry] = OrderedDict()


def register_step(
    name: str,
    *,
    depends_on: list[str] | None = None,
    group: str = "core",
    description: str = "",
    parallelizable: bool = True,
    requires_workers: bool = False,
) -> Callable[[StepFn], StepFn]:
    def decorator(fn: StepFn) -> StepFn:
        STEP_REGISTRY[name] = StepEntry(
            fn=fn,
            depends_on=depends_on or [],
            group=group,
            description=description or fn.__doc__ or name,
            parallelizable=parallelizable,
            requires_workers=requires_workers,
        )
        return fn

    return decorator


def get_step(name: str) -> StepEntry:
    if name not in STEP_REGISTRY:
        raise KeyError(f"Unknown step: {name}")
    return STEP_REGISTRY[name]
