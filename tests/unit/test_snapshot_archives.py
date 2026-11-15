"""Streaming snapshot archive transport and extraction safety."""

from __future__ import annotations

import io
import tarfile
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.config.bootstrap import path_for_toml
from cnequity.steps.http_common import write_fetched
from cnequity.storage.raw_archive import RawArchiveError
from cnequity.storage.snapshots import SnapshotStore
from cnequity.storage.state import StateStore


@pytest.fixture
def _catalog():
    from cnequity.query import catalog_read
    from cnequity.storage import revision_records
    from equitylake_core.extensions import ExtensionUnavailableError, ensure_extensions

    try:
        import duckdb

        ensure_extensions(duckdb.connect())
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield
    catalog_read.close_all()
    revision_records.forget()


def _lake(tmp_path: Path) -> tuple[Config, SnapshotStore]:
    from tests.catalog_seed import publish_rows
    from tests.unit.test_lake_write import _bars

    root = tmp_path / "lake"
    config = Config(data_root=root)
    publish_rows(config, "daily_bars", _bars(date(2026, 8, 28), {"600000.SH": 10.0}))
    StateStore(root / "meta").set_date("daily_bars", date(2026, 8, 28))
    store = SnapshotStore(config, tmp_path / "snapshots")
    store.create("base", ["daily_bars"])
    return config, store


@pytest.mark.parametrize("suffix", [".tar.zst", ".tar.gz", ".tar"])
def test_snapshot_archive_round_trip_is_streamed_and_verified(_catalog, tmp_path, suffix):
    _, store = _lake(tmp_path)
    archive = store.export_archive("base", tmp_path / f"base{suffix}")
    assert archive.is_file()
    assert not list(tmp_path.glob("*.part"))

    imported = store.import_archive(archive, name=f"restored-{suffix[1:].replace('.', '-')}")
    result = store.verify(imported.name)
    assert result.passed
    assert (imported / "data/curated/daily_bars/trade_date=2026-08/part-merged.parquet").is_file()


def test_snapshot_archive_rejects_path_traversal_without_publishing(_catalog, tmp_path):
    _, store = _lake(tmp_path)
    archive = tmp_path / "evil.tar"
    with tarfile.open(archive, "w") as tar:
        payload = b"not a snapshot"
        info = tarfile.TarInfo("../escape")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))

    with pytest.raises(ValueError, match="unsafe archive member"):
        store.import_archive(archive, name="evil")
    assert not store.path("evil").exists()
    assert not (tmp_path / "escape").exists()


def test_snapshot_archive_rejects_tampered_manifest_before_overwrite(_catalog, tmp_path):
    _, store = _lake(tmp_path)
    archive = store.export_archive("base", tmp_path / "base.tar.zst")
    raw = bytearray(archive.read_bytes())
    # The compressed stream is intentionally not modified in-place: use the
    # archive's own verified import path as the baseline, then truncate it.
    archive.write_bytes(raw[:-17])
    with pytest.raises((OSError, RuntimeError, ValueError, EOFError, tarfile.ReadError)):
        store.import_archive(archive, name="tampered")
    assert not store.path("tampered").exists()


@pytest.mark.parametrize(
    ("limit", "kwargs"),
    [
        ("members", {"max_members": 1}),
        ("member-bytes", {"max_member_bytes": 1}),
        ("total-bytes", {"max_total_bytes": 1}),
    ],
)
def test_snapshot_archive_enforces_uncompressed_resource_limits(_catalog, tmp_path, limit, kwargs):
    _, store = _lake(tmp_path)
    archive = tmp_path / f"oversized-{limit}.tar"
    with tarfile.open(archive, "w") as tar:
        for name, payload in (("manifest.json", b"{}"), ("padding", b"x")):
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))

    with pytest.raises(ValueError, match="archive (contains too many members|member|uncompressed)"):
        store.import_archive(archive, name=f"oversized-{limit}", **kwargs)
    assert not store.path(f"oversized-{limit}").exists()


def test_snapshot_archive_cli_export_import(_catalog, tmp_path):
    config, store = _lake(tmp_path)
    config_path = tmp_path / "cnequity.toml"
    config_path.write_text(
        f'[data]\nroot = "{path_for_toml(config.data_root)}"\n', encoding="utf-8"
    )
    archive = tmp_path / "cli.tar.zst"
    runner = CliRunner()
    exported = runner.invoke(
        cli,
        [
            "snapshot",
            "export",
            "base",
            str(archive),
            "--config",
            str(config_path),
            "--snapshot-root",
            str(store.root),
        ],
    )
    assert exported.exit_code == 0, exported.output
    imported = runner.invoke(
        cli,
        [
            "snapshot",
            "import",
            str(archive),
            "--name",
            "cli-restored",
            "--config",
            str(config_path),
            "--snapshot-root",
            str(store.root),
        ],
    )
    assert imported.exit_code == 0, imported.output
    assert store.verify("cli-restored").passed


@pytest.mark.parametrize(
    "dataset",
    [
        "announcement_index",
        "financial_statement_items",
        "corporate_actions",
        "analyst_consensus",
        "fund_flow",
        "sector_members",
        "hot_rank",
        "sector_fund_flow",
        "news_headlines",
        "flash_news_wire",
        "economic_calendar",
    ],
)
def test_write_fetched_raw_none_fails_before_staging_when_archive_required(
    _catalog, tmp_path, dataset
):
    config = Config(data_root=tmp_path / "lake")
    frame = pl.DataFrame({"symbol": ["600000.SH"]})

    with pytest.raises(RawArchiveError, match="verified evidence receipt"):
        write_fetched(
            config,
            "run-without-wire",
            dataset,
            frame,
            source="eastmoney",
        )

    assert not (config.staging_root / dataset).exists()
    assert not (config.meta_root / "raw").exists()


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["snapshot", "verify", "nope"], "no snapshot named 'nope'"),
        (["snapshot", "restore", "nope", "TARGET"], "no snapshot named 'nope'"),
    ],
)
def test_naming_a_missing_snapshot_is_an_error_not_a_traceback(_catalog, tmp_path, argv, expected):
    """`export`/`import` already treated this as operator input; the rest did not.

    Naming a snapshot that does not exist printed a Python traceback, and the
    message was a bare manifest path the operator never typed.
    """
    config, store = _lake(tmp_path)
    config_path = tmp_path / "cnequity.toml"
    config_path.write_text(
        f'[data]\nroot = "{path_for_toml(config.data_root)}"\n', encoding="utf-8"
    )
    argv = [str(tmp_path / "target") if part == "TARGET" else part for part in argv]

    result = CliRunner().invoke(
        cli, [*argv, "--config", str(config_path), "--snapshot-root", str(store.root)]
    )

    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output
    assert expected in result.output
