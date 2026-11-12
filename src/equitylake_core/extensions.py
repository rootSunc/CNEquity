"""Make the DuckDB extensions the catalog needs available on a connection.

DuckDB downloads an extension the first time it is installed. That download
is the one network dependency of an otherwise local lake, and the official
repository is not reachable from every network. Each extension is therefore
looked for in this order, stopping at the first that works:

1. already installed for this DuckDB version: just load it;
2. a local file, ``<local_dir>/<name>.duckdb_extension``: offline installs and
   packaged copies;
3. the official DuckDB extension repository;
4. a mirror repository, when one is configured.

``local_dir`` and ``mirror`` default to the ``EQUITYLAKE_EXTENSION_DIR`` and
``EQUITYLAKE_EXTENSION_MIRROR`` environment variables.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol

LOCAL_DIR_ENV = "EQUITYLAKE_EXTENSION_DIR"
MIRROR_ENV = "EQUITYLAKE_EXTENSION_MIRROR"

# What the catalog needs: the lake format itself and its SQLite metadata store.
CATALOG_EXTENSIONS: tuple[str, ...] = ("ducklake", "sqlite_scanner")

# DuckDB accepts ``sqlite`` for ``sqlite_scanner``; a downloaded file carries the long name.
_FILE_NAMES: dict[str, tuple[str, ...]] = {
    "sqlite": ("sqlite_scanner", "sqlite"),
    "sqlite_scanner": ("sqlite_scanner", "sqlite"),
}


class Connection(Protocol):
    def execute(self, query: str) -> Any: ...


class ExtensionUnavailableError(RuntimeError):
    """No source could provide an extension; the message lists every attempt."""

    def __init__(self, name: str, attempts: list[tuple[str, str]]):
        self.name = name
        self.attempts = attempts
        tried = "; ".join(f"{source}: {reason}" for source, reason in attempts)
        super().__init__(
            f"DuckDB extension {name!r} is not available ({tried}). "
            f"Put {name}.duckdb_extension for this DuckDB version and platform in a "
            f"directory and set {LOCAL_DIR_ENV}, or set {MIRROR_ENV} to a reachable "
            "extension repository."
        )


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _identifier(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"not an extension name: {name!r}")
    return name


def _reason(exc: Exception) -> str:
    return (str(exc).strip().splitlines() or [type(exc).__name__])[0][:200]


def _local_file(name: str, local_dir: Path | None) -> Path | None:
    if local_dir is None:
        return None
    for stem in _FILE_NAMES.get(name, (name,)):
        candidate = local_dir / f"{stem}.duckdb_extension"
        if candidate.is_file():
            return candidate
    return None


def ensure_extension(
    con: Connection,
    name: str,
    *,
    local_dir: str | Path | None = None,
    mirror: str | None = None,
) -> str:
    """Load *name* on *con*, installing it if needed. Returns where it came from.

    The return value is ``"installed"``, ``"local"``, ``"official"`` or ``"mirror"``.
    """
    name = _identifier(name)
    if local_dir is None:
        local_dir = os.environ.get(LOCAL_DIR_ENV) or None
    if mirror is None:
        mirror = os.environ.get(MIRROR_ENV) or None
    directory = Path(local_dir).expanduser() if local_dir else None
    attempts: list[tuple[str, str]] = []

    # A bare LOAD of a known extension downloads it from the official
    # repository. Switch that off for the probe, or the network would be tried
    # before the local file and a blocked network would stall every start.
    try:
        con.execute("SET autoinstall_known_extensions = false")
        con.execute(f"LOAD {name}")
        return "installed"
    except Exception as exc:  # noqa: BLE001 - any failure means "not installed yet"
        attempts.append(("installed", _reason(exc)))
    finally:
        con.execute("RESET autoinstall_known_extensions")

    sources: list[tuple[str, str]] = []
    file = _local_file(name, directory)
    if file is not None:
        sources.append(("local", f"INSTALL {_quote(file.as_posix())}"))
    elif directory is not None:
        attempts.append(("local", f"no {name}.duckdb_extension in {directory}"))
    sources.append(("official", f"INSTALL {name}"))
    if mirror:
        sources.append(("mirror", f"INSTALL {name} FROM {_quote(mirror)}"))

    for source, statement in sources:
        try:
            con.execute(statement)
            con.execute(f"LOAD {name}")
            return source
        except Exception as exc:  # noqa: BLE001 - fall through to the next source
            attempts.append((source, _reason(exc)))
    raise ExtensionUnavailableError(name, attempts)


def ensure_extensions(
    con: Connection,
    names: Iterable[str] = CATALOG_EXTENSIONS,
    *,
    local_dir: str | Path | None = None,
    mirror: str | None = None,
) -> dict[str, str]:
    """Load every extension in *names*; map each to the source that provided it."""
    return {name: ensure_extension(con, name, local_dir=local_dir, mirror=mirror) for name in names}
