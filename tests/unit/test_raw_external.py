"""External raw payloads preserve evidence paths and portable snapshots."""

import gzip
import hashlib
import json
import shutil
from datetime import date, datetime, timezone

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive
from cnequity.storage.snapshots import SnapshotStore
from cnequity.storage.state import StateStore

DATASET = "financial_statement_items"
PDF = b"%PDF-1.4\noriginal financial disclosure\n%%EOF"
STAMP = datetime(2026, 10, 1, tzinfo=timezone.utc)


def external(tmp_path):
    archive = RawPayloadArchive(tmp_path / "lake/meta")
    root = tmp_path / "external"
    root.mkdir()
    archive.register_external(DATASET, "cninfo", root)
    return archive, root


def capture(archive, payload=PDF, fmt="pdf"):
    return archive.archive(DATASET, payload, source="cninfo", payload_format=fmt, captured_at=STAMP)


def test_pdf_external_sidecar_local_and_query_local(tmp_path):
    archive, root = external(tmp_path)
    record = capture(archive)
    assert (root / record.payload_path).read_bytes() == PDF
    assert not (archive.meta_root / record.payload_path).exists()
    assert (archive.meta_root / record.metadata_path).is_file()
    query = capture(archive, {"records": []}, "json")
    assert (archive.meta_root / query.payload_path).is_file()
    assert not (root / query.payload_path).exists()
    assert archive.record(record.metadata_path) == record
    assert archive.read(record.to_dict()) == PDF
    assert archive.read(record.payload_path) == PDF
    assert len(archive.records(DATASET)) == 2


def test_existing_reader_and_gzip_receipt_survive_relocation(tmp_path):
    archive = RawPayloadArchive(tmp_path / "lake/meta")
    digest = hashlib.sha256(PDF).hexdigest()
    path = archive.root / DATASET / "source=cninfo/captured_date=2026-10-01" / f"{digest}.gz"
    path.parent.mkdir(parents=True)
    path.write_bytes(gzip.compress(PDF, mtime=0))
    record = capture(archive)
    assert record.compression == "gzip"
    original_sidecar = (archive.meta_root / record.metadata_path).read_bytes()
    root = tmp_path / "external"
    root.mkdir()
    archive.register_external(DATASET, "cninfo", root)
    target = root / record.payload_path
    target.parent.mkdir(parents=True)
    shutil.copy2(path, target)
    assert archive.read(record) == PDF  # Local remains usable during copying.
    path.unlink()
    assert archive.read(record) == PDF
    assert capture(archive) == record  # Reuses gzip rather than making another format.
    assert (archive.meta_root / record.metadata_path).read_bytes() == original_sidecar


def test_missing_external_volume_is_not_recreated_or_fetched(tmp_path):
    archive, root = external(tmp_path)
    record = capture(archive)
    root.rename(tmp_path / "unmounted")
    with pytest.raises(RawArchiveError, match="unavailable"):
        archive.read(record)
    with pytest.raises(RawArchiveError, match="unavailable"):
        capture(archive, PDF + b"new")
    assert not root.exists()


def test_corruption_does_not_fall_back_to_another_copy(tmp_path):
    archive, root = external(tmp_path)
    record = capture(archive)
    target = root / record.payload_path
    target.write_bytes(b"X" * len(PDF))
    with pytest.raises(RawArchiveError, match="digest|mismatch|changed"):
        archive.read(record)
    with pytest.raises(RawArchiveError, match="digest|mismatch|changed"):
        capture(archive)
    target.write_bytes(PDF)
    local = archive.meta_root / record.payload_path
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(b"X" * len(PDF))
    with pytest.raises(RawArchiveError, match="digest|mismatch|changed"):
        archive.read(record)
    with pytest.raises(RawArchiveError, match="digest|mismatch|changed"):
        capture(archive)


def test_read_retries_a_verified_location_change_during_open(tmp_path, monkeypatch):
    from cnequity.storage import raw_archive

    archive = RawPayloadArchive(tmp_path / "lake/meta")
    record = capture(archive)
    root = tmp_path / "external"
    root.mkdir()
    archive.register_external(DATASET, "cninfo", root)
    local = archive.meta_root / record.payload_path
    target = root / record.payload_path
    target.parent.mkdir(parents=True)
    verify = raw_archive._verify_stored_payload

    def moved_while_opening(path, *args, **kwargs):
        if path == local:
            local.rename(target)
        return verify(path, *args, **kwargs)

    monkeypatch.setattr(raw_archive, "_verify_stored_payload", moved_while_opening)
    assert archive.read(record) == PDF


def test_external_paths_and_registry_reject_links(tmp_path):
    archive, root = external(tmp_path)
    record = capture(archive)
    target = root / record.payload_path
    other = tmp_path / "other"
    other.write_bytes(PDF)
    target.unlink()
    target.symlink_to(other)
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.read(record)
    registry = archive.meta_root / "raw_storage.json"
    data = registry.read_bytes()
    registry.unlink()
    other.write_bytes(data)
    registry.symlink_to(other)
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.external_locations()


@pytest.mark.parametrize("prefix", ["../escape", "raw/../source=cninfo", "raw/x/source=.."])
def test_invalid_location_prefix_fails_closed(tmp_path, prefix):
    archive, _ = external(tmp_path)
    path = archive.meta_root / "raw_storage.json"
    value = json.loads(path.read_text())
    value["locations"][0]["prefix"] = prefix
    path.write_text(json.dumps(value))
    with pytest.raises(RawArchiveError, match="invalid"):
        capture(archive)


def test_location_cannot_change_or_point_into_lake(tmp_path):
    archive, root = external(tmp_path)
    assert archive.register_external(DATASET, "cninfo", root)["root"] == str(root)
    other = tmp_path / "other"
    other.mkdir()
    with pytest.raises(RawArchiveError, match="already registered"):
        archive.register_external(DATASET, "cninfo", other)
    with pytest.raises(RawArchiveError, match="outside"):
        archive.register_external("daily_bars", "cninfo", archive.meta_root)


def test_snapshot_materializes_external_payloads_without_machine_paths(tmp_path):
    archive, root = external(tmp_path)
    record = capture(archive)
    config = Config(data_root=archive.meta_root.parent)
    from tests.catalog_seed import publish_rows

    from cnequity.domain.schemas import DATASET_SCHEMAS
    from cnequity.query import catalog_read

    schema = DATASET_SCHEMAS[DATASET]
    row = {
        "symbol": "600000.SH",
        "report_period": "2025-12-31",
        "announce_date": date(2026, 10, 1),
        "source": "test",
        "data_version": "v1",
        "fetched_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
    }
    frame = pl.DataFrame([{name: row.get(name) for name in schema}], schema=schema)
    publish_rows(config, DATASET, frame)
    catalog_read.close_all()
    StateStore(config.meta_root).set_date(DATASET, date(2025, 12, 31))
    store = SnapshotStore(config, tmp_path / "snapshots")
    store.create("external", [DATASET])
    snapshot = store.path("external")
    assert store.verify("external").passed
    manifest = json.loads((snapshot / "manifest.json").read_text())
    assert str(root) not in json.dumps(manifest)
    assert not (snapshot / "meta/raw_storage.json").exists()
    # The standalone copy is readable after the external drive disappears.
    shutil.rmtree(root)
    copied_meta = snapshot / "meta"
    assert RawPayloadArchive(copied_meta).read(record) == PDF
    with pytest.raises(ValueError, match="unavailable"):
        store.create("missing", [DATASET])
    assert not store.path("missing").exists()


def test_export_rejects_divergent_duplicates_and_missing_evidence(tmp_path):
    archive, root = external(tmp_path)
    record = capture(archive)
    local = archive.meta_root / record.payload_path
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(PDF)
    assert archive.export_files(DATASET)
    local.write_bytes(b"X" * len(PDF))
    with pytest.raises(RawArchiveError, match="conflicting"):
        archive.export_files(DATASET)
    local.unlink()
    (root / record.payload_path).unlink()
    with pytest.raises(RawArchiveError, match="missing"):
        archive.export_files(DATASET)


def test_cli_registers_only_and_does_not_move_local_bytes(tmp_path):
    lake = tmp_path / "lake"
    config = tmp_path / "config.toml"
    config.write_text(f'[data]\nroot = "{lake.as_posix()}"\n')
    archive = RawPayloadArchive(lake / "meta")
    record = capture(archive)
    root = tmp_path / "external"
    root.mkdir()
    result = CliRunner().invoke(
        cli,
        [
            "storage",
            "raw-external",
            "--config",
            str(config),
            "--dataset",
            DATASET,
            "--source",
            "cninfo",
            "--root",
            str(root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (archive.meta_root / record.payload_path).is_file()
    assert not list(root.iterdir())
    assert archive.read(record) == PDF
