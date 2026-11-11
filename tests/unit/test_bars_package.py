"""``cnequity.steps.bars`` is a package; patch helpers where they are defined.

Its modules call one another through the module (``gapfill.helper(...)``), so
replacing ``bars.gapfill.helper`` reaches every caller. Replacing a name on the
package itself would only change the re-export, never what the code calls, and
the test would pass without exercising its fake.
"""

from __future__ import annotations

import ast
from pathlib import Path

TESTS = Path(__file__).resolve().parent
PACKAGE = "cnequity.steps.bars"
MODULES = {p.stem for p in (TESTS.parents[1] / "src/cnequity/steps/bars").glob("*.py")} - {
    "__init__"
}


def _own_nodes(scope: ast.AST):
    """Nodes in *scope*, not descending into nested functions."""
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop()
        yield node
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            stack.extend(ast.iter_child_nodes(node))


def _package_aliases(scope: ast.AST) -> set[str]:
    aliases = set()
    for node in _own_nodes(scope):
        if isinstance(node, ast.ImportFrom) and node.module == "cnequity.steps":
            aliases |= {a.asname or a.name for a in node.names if a.name == "bars"}
        if isinstance(node, ast.Import):
            aliases |= {a.asname for a in node.names if a.name == PACKAGE and a.asname}
    return aliases


def _offences(scope: ast.AST, inherited: set[str], name: str) -> list[str]:
    aliases = inherited | _package_aliases(scope)
    found = []
    for node in _own_nodes(scope):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            found += _offences(node, aliases, name)
        elif isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "setattr":
            first = node.args[0] if node.args else None
            if isinstance(first, ast.Name) and first.id in aliases:
                found.append(f"{name}:{node.lineno} setattr({first.id}, ...)")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            # Dotted targets for monkeypatch.setattr, wherever they are written.
            rest = node.value.removeprefix(PACKAGE + ".")
            if rest != node.value and rest.split(".")[0] not in MODULES:
                found.append(f"{name}:{node.lineno} {node.value!r}")
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) and t.value.id in aliases
            for t in node.targets
        ):
            found.append(f"{name}:{node.lineno} assignment on the package")
    return found


def test_no_test_patches_a_name_on_the_package_itself():
    found = []
    for path in sorted(TESTS.rglob("*.py")):
        found += _offences(ast.parse(path.read_text(encoding="utf-8")), set(), path.name)
    assert not found, "patch the submodule that defines or looks up the name:\n" + "\n".join(found)


def test_code_outside_the_package_imports_from_the_defining_module():
    """A lazy ``from cnequity.steps.bars import f`` binds the re-export, which a
    test patching ``bars.<module>.f`` does not replace."""
    src = TESTS.parents[1] / "src" / "cnequity"
    found = [
        f"{path.relative_to(src)}:{node.lineno}"
        for path in sorted(src.rglob("*.py"))
        # steps/__init__ imports the package so its steps register.
        if "steps/bars/" not in path.as_posix()
        and path.relative_to(src).as_posix() != "steps/__init__.py"
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if (isinstance(node, ast.ImportFrom) and node.module == PACKAGE)
        or (
            isinstance(node, ast.ImportFrom)
            and node.module == "cnequity.steps"
            and any(a.name == "bars" for a in node.names)
        )
    ]
    assert not found, "import from cnequity.steps.bars.<module>:\n" + "\n".join(found)
