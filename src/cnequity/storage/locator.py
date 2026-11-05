"""Where a dataset's logical directory lives: one rule, not one copy per caller.

A registered dataset sits under ``curated/`` or ``derived/`` according to
``DatasetSpec.layer``. That one-line rule used to be re-typed at every call
site (``derived_root if spec.layer == "derived" else curated_root``) — about
twenty of them across storage, query, quality, serve and the CLI — so a new
layer or a moved root would have had to find each one.

These helpers return the *logical* (mutable, compatibility) directory. A
reader that needs the committed generation resolves it with
``storage.read_context.read_root`` or a scanner's ``committed=True``; this
module deliberately does not, so it stays usable for writers, repairs and
candidate views as well.

``roots`` is anything carrying ``curated_root`` and ``derived_root``: a
``Config``, or the candidate view the publication gate builds.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Protocol

from cnequity.domain.datasets import DATASETS, DatasetSpec

LayerName = Literal["curated", "derived"]


class LayerRoots(Protocol):
    curated_root: Path
    derived_root: Path


def _spec(dataset: str | DatasetSpec) -> DatasetSpec:
    return dataset if isinstance(dataset, DatasetSpec) else DATASETS[dataset]


def layer_name(dataset: str | DatasetSpec) -> LayerName:
    """``"derived"`` for a derived dataset, ``"curated"`` for everything else."""
    return "derived" if _spec(dataset).layer == "derived" else "curated"


def layer_root(roots: LayerRoots, dataset: str | DatasetSpec) -> Path:
    """The layer directory (``curated_root`` or ``derived_root``) holding *dataset*."""
    return roots.derived_root if layer_name(dataset) == "derived" else roots.curated_root


def logical_root(roots: LayerRoots, dataset: str | DatasetSpec) -> Path:
    """``<layer root>/<dataset>``: the dataset's logical directory."""
    return layer_root(roots, dataset) / _spec(dataset).name
