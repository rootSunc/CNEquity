"""Extension lookup order: installed, local file, official repository, mirror."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from equitylake_core.extensions import (
    CATALOG_EXTENSIONS,
    LOCAL_DIR_ENV,
    MIRROR_ENV,
    ExtensionUnavailableError,
    ensure_extension,
    ensure_extensions,
)

CORE_ROOT = Path(__file__).resolve().parents[2] / "src" / "equitylake_core"


class FakeConnection:
    """Records statements; ``works`` names the install statements that succeed."""

    def __init__(self, *, installed: set[str] = frozenset(), works=lambda statement: False):
        self.installed = set(installed)
        self.works = works
        self.statements: list[str] = []

    def execute(self, query: str):
        self.statements.append(query)
        verb, _, rest = query.partition(" ")
        if verb in {"SET", "RESET"}:
            return None
        if verb == "LOAD":
            if rest not in self.installed:
                raise RuntimeError(f'Extension "{rest}" not found\nsecond line')
            return None
        if not self.works(query):
            raise RuntimeError("download failed")
        self.installed.add(rest.split()[0] if not rest.startswith("'") else self._from_path(rest))
        return None

    @staticmethod
    def _from_path(rest: str) -> str:
        stem = Path(rest.strip("'")).name.removesuffix(".duckdb_extension")
        return stem


@pytest.fixture(autouse=True)
def _no_environment(monkeypatch):
    monkeypatch.delenv(LOCAL_DIR_ENV, raising=False)
    monkeypatch.delenv(MIRROR_ENV, raising=False)


def test_an_installed_extension_is_only_loaded():
    con = FakeConnection(installed={"ducklake"})
    assert ensure_extension(con, "ducklake") == "installed"
    assert con.statements == [
        "SET autoinstall_known_extensions = false",
        "LOAD ducklake",
        "RESET autoinstall_known_extensions",
    ]


def test_a_local_file_wins_over_the_network(tmp_path):
    (tmp_path / "ducklake.duckdb_extension").write_bytes(b"x")
    con = FakeConnection(works=lambda s: s.startswith("INSTALL '"))
    assert ensure_extension(con, "ducklake", local_dir=tmp_path) == "local"
    assert not any(s == "INSTALL ducklake" for s in con.statements)


def test_the_sqlite_alias_finds_the_scanner_file(tmp_path):
    (tmp_path / "sqlite_scanner.duckdb_extension").write_bytes(b"x")
    con = FakeConnection(works=lambda s: s.startswith("INSTALL '"))
    assert ensure_extension(con, "sqlite_scanner", local_dir=tmp_path) == "local"


def test_the_official_repository_is_next(tmp_path):
    con = FakeConnection(works=lambda s: s == "INSTALL ducklake")
    assert ensure_extension(con, "ducklake", local_dir=tmp_path) == "official"


def test_the_mirror_is_the_last_resort():
    con = FakeConnection(works=lambda s: "FROM" in s)
    assert ensure_extension(con, "ducklake", mirror="https://mirror.example/ext") == "mirror"
    assert con.statements[-2] == "INSTALL ducklake FROM 'https://mirror.example/ext'"


def test_environment_variables_supply_the_defaults(tmp_path, monkeypatch):
    (tmp_path / "ducklake.duckdb_extension").write_bytes(b"x")
    monkeypatch.setenv(LOCAL_DIR_ENV, str(tmp_path))
    con = FakeConnection(works=lambda s: s.startswith("INSTALL '"))
    assert ensure_extension(con, "ducklake") == "local"


def test_every_attempt_is_reported_when_nothing_works(tmp_path):
    con = FakeConnection()
    with pytest.raises(ExtensionUnavailableError) as caught:
        ensure_extension(con, "ducklake", local_dir=tmp_path, mirror="https://mirror.example")
    assert [source for source, _ in caught.value.attempts] == [
        "installed",
        "local",
        "official",
        "mirror",
    ]
    assert LOCAL_DIR_ENV in str(caught.value) and "second line" not in str(caught.value)


def test_a_quote_in_a_path_cannot_end_the_statement(tmp_path):
    odd = tmp_path / "it's here"
    odd.mkdir()
    (odd / "ducklake.duckdb_extension").write_bytes(b"x")
    con = FakeConnection(works=lambda s: s.startswith("INSTALL '"))
    ensure_extension(con, "ducklake", local_dir=odd)
    assert any("it''s here" in statement for statement in con.statements)


def test_a_name_that_is_not_an_identifier_is_refused():
    with pytest.raises(ValueError, match="not an extension name"):
        ensure_extension(FakeConnection(), "ducklake; DROP TABLE t")


def test_all_catalog_extensions_are_ensured():
    con = FakeConnection(installed=set(CATALOG_EXTENSIONS))
    assert ensure_extensions(con) == dict.fromkeys(CATALOG_EXTENSIONS, "installed")


def test_the_core_imports_no_market_package():
    """EquityLake Core is the dependency, never the dependent."""
    offenders = []
    for path in sorted(CORE_ROOT.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and not node.level:
                names = [node.module or ""]
            offenders += [
                f"{path.name}: {name}" for name in names if name.split(".")[0] == "cnequity"
            ]
    assert not offenders, offenders
