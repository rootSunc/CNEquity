"""Guards that keep the TDX client free of its former upstreams.

The wire protocol is vendored under ``adapters/tdx_protocol/_wire`` precisely so
that mootdx and tdxpy — both last released in 2024, both unmaintained — are no
longer installed. A stray ``import mootdx`` would still pass on a developer
machine that happens to have it left over, and only fail once a user installs
into a clean environment. These tests turn that into a CI failure instead.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

import pytest

# `akshare` joined this list in the issue #3 cleanup: both call sites reached
# endpoints the project already queries directly (the ST board via push2 clist,
# PMI / 货币供应量 via the datacenter reports), so the wrapper only added a
# parsing layer, 14 transitive dependencies, and the mini-racer install
# friction. Re-adding an import would bring all three back.
RETIRED_PACKAGES = ("mootdx", "tdxpy", "py_mini_racer", "mini_racer", "akshare")

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "cn_market_lake"
VENDORED = SRC_ROOT / "adapters" / "tdx_protocol" / "_wire"


def _project_sources() -> list[Path]:
    """Every first-party module — the vendored tree is upstream code, not ours."""
    return [p for p in SRC_ROOT.rglob("*.py") if VENDORED not in p.parents and p != VENDORED]


def test_no_source_file_imports_a_retired_package():
    offenders: list[str] = []
    for path in _project_sources():
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue  # provenance comments are fine; imports are not
            for pkg in RETIRED_PACKAGES:
                if f"import {pkg}" in stripped or f"from {pkg}" in stripped:
                    offenders.append(f"{path.relative_to(SRC_ROOT)}: {stripped}")
    assert not offenders, "retired packages must not be imported:\n" + "\n".join(offenders)


def _unguarded_imports(tree: ast.Module) -> list[str]:
    """Top-level module names imported outside a try/except.

    Upstream soft-imports two optional accelerators (``hexdump`` for a debug
    dump, ``cython`` for compiled mode), both wrapped in ``try/except
    ImportError`` with working fallbacks. Those create no dependency, so only
    unguarded imports count.
    """
    guarded: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            for child in ast.walk(node):
                guarded.add(id(child))

    names: list[str] = []
    for node in ast.walk(tree):
        if id(node) in guarded:
            continue
        if isinstance(node, ast.Import):
            names.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.append(node.module.split(".")[0])
    return names


def test_vendored_wire_client_needs_no_third_party_package():
    """The vendored tree must stay installable with zero dependencies.

    Its whole value is that it adds none; a stray `import pandas` at module
    level would quietly put one back.
    """
    stdlib = set(sys.stdlib_module_names)
    offenders: list[str] = []

    for path in VENDORED.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for name in _unguarded_imports(tree):
            if name in stdlib or name == "cn_market_lake" or name == "__future__":
                continue
            offenders.append(f"{path.name}: {name}")

    assert not offenders, "vendored wire client must need no third-party package:\n" + "\n".join(
        offenders
    )


def test_importing_the_lake_does_not_load_a_retired_package():
    """A subprocess, so a package already imported by the suite cannot mask this."""
    code = (
        "import sys;"
        "import cn_market_lake.steps;"
        "import cn_market_lake.adapters.tdx_protocol.client;"
        "import cn_market_lake.derive.sector_routing;"
        "import cn_market_lake.adapters.tdx_protocol.quotes;"
        f"bad=[m for m in sys.modules if m.split('.')[0] in {RETIRED_PACKAGES!r}];"
        "print(','.join(sorted(bad)))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    loaded = result.stdout.strip()
    assert not loaded, f"importing the lake pulled in retired packages: {loaded}"


@pytest.mark.parametrize("package", RETIRED_PACKAGES)
def test_retired_packages_are_not_declared_as_dependencies(package):
    """Parsed, not grepped — the vendored LICENSE filename mentions tdxpy legitimately."""
    pyproject = tomllib.loads(
        (Path(__file__).resolve().parents[2] / "pyproject.toml").read_bytes().decode("utf-8")
    )
    project = pyproject["project"]
    declared = list(project.get("dependencies", []))
    # Extras are gone, but keep reading them so a reintroduced one is still caught.
    for extra_reqs in project.get("optional-dependencies", {}).values():
        declared.extend(extra_reqs)
    for group_reqs in pyproject.get("dependency-groups", {}).values():
        declared.extend(r for r in group_reqs if isinstance(r, str))

    needle = package.replace("_", "-")
    offenders = [req for req in declared if needle in req.replace("_", "-")]
    assert not offenders, f"{package} reappeared as a dependency: {offenders}"


def test_vendored_client_satisfies_every_callback_the_wire_expects():
    """The trimmed client must still answer everything vendored code calls by name.

    `TdxWireClient` keeps 5 of tdxpy's 22 methods. Anything the vendored modules
    reach for on the client — `HeartBeatThread` calls `api.do_heartbeat()` on a
    timer — is invisible to both imports and unit tests unless something asserts
    it, and only surfaces as a background thread throwing every interval.
    """
    import ast

    from cn_market_lake.adapters.tdx_protocol._wire import TdxWireClient

    expected: set[str] = set()
    for path in VENDORED.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            # `self.api.<name>` — how the vendored helpers reach the client.
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "api"
            ):
                expected.add(node.attr)

    assert expected, "no api.* callbacks found — the scan stopped working"
    # An instance, not the class: the socket layer sets `last_ack_time` in
    # __init__. Constructing does not connect, so this stays offline.
    client = TdxWireClient()
    missing = sorted(n for n in expected if not hasattr(client, n))
    assert not missing, f"vendored code calls these, but the client lacks them: {missing}"
