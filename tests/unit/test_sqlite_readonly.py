"""Read-only SQLite opens work under any directory name, and go through one helper."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from cnequity.sqlite_readonly import connect_readonly

SRC = Path(__file__).resolve().parents[2] / "src" / "cnequity"


@pytest.mark.parametrize("directory", ["lake#1", "lake?x", "lake 2", "湖"])
def test_reads_the_file_it_was_given(tmp_path, directory):
    path = tmp_path / directory / "manifest.db"
    path.parent.mkdir()
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE ingestion_runs (run_id TEXT)")
        conn.execute("INSERT INTO ingestion_runs VALUES ('r1')")
        conn.commit()

    with closing(connect_readonly(path)) as conn:
        assert conn.execute("SELECT run_id FROM ingestion_runs").fetchall() == [("r1",)]
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("INSERT INTO ingestion_runs VALUES ('r2')")


def test_a_missing_file_is_not_created(tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        connect_readonly(tmp_path / "absent.db")
    assert not (tmp_path / "absent.db").exists()


def test_no_module_builds_its_own_read_only_uri():
    found = [
        f"{path.relative_to(SRC)}:{number}"
        for path in sorted(SRC.rglob("*.py"))
        if path.name != "sqlite_readonly.py"
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if "mode=ro" in line
    ]
    assert not found, "use cnequity.sqlite_readonly.connect_readonly:\n" + "\n".join(found)
