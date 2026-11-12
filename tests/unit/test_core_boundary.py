"""The engine core may not know which market it serves — a ratchet.

``CORE`` lists the modules that publish, store, read back and schedule
datasets: the part of the lake a second market could reuse unchanged. To stay
reusable they may not

* name a registered dataset in a string literal (``if dataset == "daily_bars"``);
* name a data vendor in a string literal (``"tushare"``, ``"eastmoney_dc"``);
* import a vendor adapter, a step, a derivation, or one of the
  market-specific ``domain`` modules.

What a dataset needs from the core is declared on its ``DatasetSpec``; the
registry (``domain.datasets``, ``domain.schemas``) is the one seam the core
reads the market through.

The core grew without this rule, so ``core_boundary_baseline.txt`` lists every
violation that existed when it was introduced, one ``module -> kind value``
pair per line. The test fails on any pair not in that file, and on any listed
pair that no longer occurs, so the baseline can only shrink. Each line is one
piece of market knowledge still to move out of the core.

Regenerate the file (after a deliberate change to ``CORE`` only) with::

    python tests/unit/test_core_boundary.py > tests/unit/core_boundary_baseline.txt
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src" / "cnequity"
BASELINE = Path(__file__).with_name("core_boundary_baseline.txt")

# Modules another market would take as they are. A package name covers every
# module below it.
CORE: tuple[str, ...] = (
    # Process and file utilities.
    "cnequity.file_lock",
    "cnequity.file_replace",
    "cnequity.fd_limit",
    "cnequity.json_codec",
    # Publication: atomic writes, generations, receipts, watermarks, raw archive.
    "cnequity.storage.atomic",
    "cnequity.storage.file_copy",
    "cnequity.storage.revisions",
    "cnequity.storage.state",
    "cnequity.storage.raw_archive",
    "cnequity.storage.provenance",
    "cnequity.storage.parquet",
    "cnequity.storage.merge_files",
    "cnequity.storage.read_context",
    "cnequity.storage.partition_cache",
    "cnequity.storage.snapshots",
    # Scheduling: the step registry, the run ledger, the job engine.
    "cnequity.orchestrator.registry",
    "cnequity.orchestrator.manifest",
    "cnequity.orchestrator.outcomes",
    "cnequity.orchestrator.run_lock",
    "cnequity.orchestrator.engine",
    "cnequity.orchestrator.worker_pool",
    # Reading: partition scans, the load() core, SQL views.
    "cnequity.query.parquet_scan",
    "cnequity.query.reader",
    "cnequity.query.views",
    # Policy with no market in it: partition naming, pacing, findings, row order.
    "cnequity.domain.partitions",
    "cnequity.domain.rate_limit",
    "cnequity.domain.http_policy",
    "cnequity.domain.findings",
    "cnequity.domain.canonical",
)

# Packages that exist only for this market.
_MARKET_PACKAGES: tuple[str, ...] = (
    "cnequity.adapters",
    "cnequity.steps",
    "cnequity.derive",
)

# ``domain`` modules that encode this market's rules. Everything else in
# ``domain`` is registry or market-neutral policy.
_MARKET_DOMAIN: frozenset[str] = frozenset(
    {
        "action_evidence",
        "action_sessions",
        "announced_halts",
        "bse_codes",
        "cash_entitlements",
        "central_banks",
        "decision_evidence",
        "derivatives",
        "financial_statements",
        "futures_products",
        "holidays_cn",
        "industry_levels",
        "market_profile",
        "market_time",
        "membership",
        "option_pricing",
        "security_identifiers",
        "sentiment",
        "symbols",
        "trading_status",
        "universe_profiles",
        "valuation",
    }
)

# Vendor labels as they appear in specs, rate-limit lanes and row provenance.
# A literal counts when it is the label itself or starts with ``<label>_``.
_VENDORS: tuple[str, ...] = (
    "baostock",
    "bse",
    "cni",
    "cninfo",
    "eastmoney",
    "jin10",
    "nbs",
    "pboc",
    "sina",
    "sw",
    "tdx",
    "ths",
    "tushare",
)


def _module_name(path: Path) -> str:
    relative = path.relative_to(PACKAGE_ROOT.parent).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _in_core(module: str) -> bool:
    return any(module == name or module.startswith(name + ".") for name in CORE)


def _core_files() -> list[Path]:
    return sorted(path for path in PACKAGE_ROOT.rglob("*.py") if _in_core(_module_name(path)))


def _imported(path: Path, tree: ast.AST) -> set[str]:
    """Every first-party module *path* imports, at module level or inside a function."""
    module = _module_name(path)
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package.split(".")
                base = base[: len(base) - (node.level - 1)]
                target = ".".join([*base, node.module] if node.module else base)
            else:
                target = node.module or ""
            found.add(target)
            # ``from cnequity.domain import symbols`` imports a module too.
            found.update(f"{target}.{alias.name}" for alias in node.names)
    return {name for name in found if name.startswith("cnequity")}


def _market_import(name: str) -> str | None:
    for package in _MARKET_PACKAGES:
        if name == package or name.startswith(package + "."):
            return package
    prefix = "cnequity.domain."
    if name.startswith(prefix):
        module = name[len(prefix) :].split(".")[0]
        if module in _MARKET_DOMAIN:
            return prefix + module
    return None


def _vendor(text: str) -> str | None:
    for vendor in _VENDORS:
        if text == vendor or text.startswith(vendor + "_"):
            return vendor
    return None


def boundary_violations() -> set[str]:
    from cnequity.domain.datasets import DATASETS

    datasets = set(DATASETS)
    violations: set[str] = set()
    for path in _core_files():
        module = _module_name(path)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for name in _imported(path, tree):
            target = _market_import(name)
            if target is not None:
                violations.add(f"{module} -> imports {target}")
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if node.value in datasets:
                violations.add(f"{module} -> names dataset {node.value}")
            elif (vendor := _vendor(node.value)) is not None:
                violations.add(f"{module} -> names vendor {vendor}")
    return violations


def _baseline() -> set[str]:
    lines = BASELINE.read_text(encoding="utf-8").splitlines()
    return {line.strip() for line in lines if line.strip() and not line.startswith("#")}


def test_every_core_entry_is_a_module():
    """A rename must move the entry, not silently drop the module from the check."""
    modules = {_module_name(path) for path in PACKAGE_ROOT.rglob("*.py")}
    missing = [name for name in CORE if name not in modules]
    assert not missing, f"CORE names modules that do not exist: {missing}"


def test_the_core_gains_no_market_knowledge():
    new = sorted(boundary_violations() - _baseline())
    assert not new, (
        "the engine core named a dataset or vendor, or imported market code. "
        "Declare the fact on the DatasetSpec, or move the code out of the core:\n  "
        + "\n  ".join(new)
    )


def test_the_baseline_only_shrinks():
    gone = sorted(_baseline() - boundary_violations())
    assert not gone, (
        f"these entries no longer occur; delete them from {BASELINE.name}:\n  " + "\n  ".join(gone)
    )


if __name__ == "__main__":
    sys.stdout.write("\n".join(sorted(boundary_violations())) + "\n")
