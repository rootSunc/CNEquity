"""Imports may only point down the layer stack — a ratchet over today's violations.

Every module gets a layer rank (``_RANKS``, longest prefix wins). Importing a
module of a *higher* rank is a layering violation, whether the import sits at
module level or inside a function: a function-level import hides a cycle from
the interpreter, not from the architecture.

The lake grew without this rule, so ``import_layer_baseline.txt`` lists every
violation that existed when it was introduced, one ``importer -> imported``
pair per line. The test fails on any pair not in that file — a new upward
import — and on any listed pair that no longer occurs, so the baseline can
only shrink. Removing a violation means deleting its line; adding one means
fixing the import instead.

Regenerate the file (after a deliberate reshuffle of ``_RANKS`` only) with::

    python tests/unit/test_import_layers.py > tests/unit/import_layer_baseline.txt
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src" / "cnequity"
BASELINE = Path(__file__).with_name("import_layer_baseline.txt")

# Lower layers must not import higher ones. Ranks leave gaps for later splits.
_RANKS: dict[str, int] = {
    # Standalone utilities with no lake knowledge.
    "cnequity": 0,
    "cnequity.i18n": 0,
    "cnequity.file_lock": 0,
    "cnequity.file_replace": 0,
    "cnequity.fd_limit": 0,
    "cnequity.keep_awake": 0,
    "cnequity.windows_process": 0,
    "cnequity.terminal_log": 0,
    "cnequity.progress": 0,
    "cnequity.run_scope": 0,
    # Registry, schemas, contracts, reference data: pure policy.
    "cnequity.domain": 10,
    "cnequity.universe_profiles": 10,  # compatibility re-export of domain
    "cnequity.config": 20,
    "cnequity.compliance": 20,
    # Write primitives every layer persists through: atomic replace and the
    # raw-response archive. They import only root utilities, so adapters may
    # use them; ranked here, the test keeps them from growing lake knowledge.
    "cnequity.storage.atomic": 25,
    # Outcome vocabulary (error types, reason codes) adapters raise and steps
    # report; no first-party imports. The run lock wraps file_lock only.
    "cnequity.orchestrator.outcomes": 25,
    "cnequity.orchestrator.run_lock": 25,
    "cnequity.storage.raw_archive": 25,
    # Vendor clients.
    "cnequity.adapters": 30,
    # The step SPI every step and helper is written against.
    "cnequity.orchestrator.registry": 35,
    "cnequity.orchestrator.source_gaps": 35,
    # Schedule policy: which group runs a step, and when a dataset is due.
    "cnequity.orchestrator.cadence": 35,
    "cnequity.storage": 40,
    # Read primitives storage itself needs (partition scans, canonical row
    # order, the trading calendar). They sit beside storage, below the
    # load()/views read API that the rest of ``query`` provides.
    "cnequity.query.parquet_scan": 40,
    "cnequity.query.canonical": 40,
    "cnequity.query.calendar": 40,
    "cnequity.query": 50,
    "cnequity.quality": 60,
    "cnequity.derive": 60,
    # Offline repairs and the migrations that replay them: each publishes
    # through the quality gate (``check_repair_publication``) and some re-run
    # derives, so they sit above quality and derive, below the steps that call
    # them. Readers of what they record (``storage.never_listed``,
    # ``storage.lake_record``) stay in storage.
    "cnequity.storage.repairs": 65,
    "cnequity.storage.migrations": 65,
    "cnequity.steps": 70,
    "cnequity.orchestrator": 80,
    "cnequity.research": 90,
    "cnequity.diagnostics": 90,
    # Entry points.
    "cnequity.cli": 100,
    "cnequity.serve": 100,
    "cnequity.mcp_server": 100,
    "cnequity.__main__": 100,
}


def _rank(module: str) -> int:
    parts = module.split(".")
    for end in range(len(parts), 0, -1):
        rank = _RANKS.get(".".join(parts[:end]))
        if rank is not None:
            return rank
    raise KeyError(f"no layer rank for {module}; add it to _RANKS")


def _module_name(path: Path) -> str:
    relative = path.relative_to(PACKAGE_ROOT.parent).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve(module: str, node: ast.ImportFrom, is_package: bool) -> str | None:
    if node.level == 0:
        return node.module
    base = module.split(".")
    if not is_package:
        base = base[:-1]
    base = base[: len(base) - (node.level - 1)]
    return ".".join([*base, node.module] if node.module else base)


def _imports(path: Path) -> set[str]:
    module = _module_name(path)
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            target = _resolve(module, node, path.name == "__init__.py")
            if target:
                found.add(target)
    return {name for name in found if name == "cnequity" or name.startswith("cnequity.")}


def layer_violations() -> set[str]:
    violations: set[str] = set()
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        if "_wire" in path.parts:  # vendored tdxpy protocol code
            continue
        importer = _module_name(path)
        for imported in _imports(path):
            if _rank(importer) < _rank(imported):
                violations.add(f"{importer} -> {imported}")
    return violations


def _baseline() -> set[str]:
    lines = BASELINE.read_text(encoding="utf-8").splitlines()
    return {line.strip() for line in lines if line.strip() and not line.startswith("#")}


def test_every_module_has_a_layer():
    for path in PACKAGE_ROOT.rglob("*.py"):
        _rank(_module_name(path))


def test_no_new_upward_imports():
    new = sorted(layer_violations() - _baseline())
    assert new == [], (
        "these imports point up the layer stack (see _RANKS); invert the dependency "
        "or move the code instead:\n  " + "\n  ".join(new)
    )


def test_the_baseline_only_shrinks():
    gone = sorted(_baseline() - layer_violations())
    assert gone == [], (
        "these violations no longer occur; delete them from "
        f"{BASELINE.name}:\n  " + "\n  ".join(gone)
    )


if __name__ == "__main__":
    sys.stdout.write(
        "# importer -> imported pairs that point up the layer stack; see test_import_layers.py\n"
    )
    for line in sorted(layer_violations()):
        sys.stdout.write(line + "\n")


def test_the_domain_layer_has_no_exceptions():
    """``domain`` is the registry and its rules: pure, the root everything else
    stands on. Its exceptions were paid off, so it gets none back — a new edge
    out of ``domain`` cannot be absorbed into the baseline."""
    grandfathered = sorted(pair for pair in _baseline() if pair.startswith("cnequity.domain."))
    upward = sorted(pair for pair in layer_violations() if pair.startswith("cnequity.domain."))
    assert not grandfathered and not upward, grandfathered + upward


def test_every_rank_is_declared_once():
    """A dict literal keeps the last of two equal keys, so a duplicate rank
    silently overrides the one a reader sees first."""
    import ast

    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "_RANKS":
            keys = [key.value for key in node.value.keys]
            duplicates = sorted({key for key in keys if keys.count(key) > 1})
            assert not duplicates, duplicates
            return
    raise AssertionError("_RANKS not found")
