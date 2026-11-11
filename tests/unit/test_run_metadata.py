from datetime import date, datetime, timedelta, timezone

import pytest

from cnequity.orchestrator.manifest import Manifest


@pytest.mark.parametrize("write_path", ["start", "mutate"])
def test_metadata_dates_are_encoded_only_at_storage_boundary(tmp_path, write_path):
    manifest = Manifest(tmp_path / "manifest.db")
    listed = date(2024, 6, 28)
    observed = datetime(2024, 6, 28, 9, 30, tzinfo=timezone(timedelta(hours=8)))
    metadata = {
        "results": [{"new_instruments": [{"list_date": listed, "delist_date": None}]}],
        "observed_at": observed,
        "local_time": datetime(2024, 6, 28, 9, 30),
        "label": "2024-06-28",
    }
    if write_path == "start":
        run_id = manifest.start_run("init", metadata)
    else:
        run_id = manifest.start_run("init")
        returned = manifest.mutate_run_metadata(run_id, lambda _: metadata)
        assert returned is metadata

    saved = Manifest(manifest.db_path).get_run_metadata(run_id)
    assert saved == {
        "results": [{"new_instruments": [{"list_date": "2024-06-28", "delist_date": None}]}],
        "observed_at": "2024-06-28T09:30:00+08:00",
        "local_time": "2024-06-28T09:30:00",
        "label": "2024-06-28",
    }
    assert metadata["results"][0]["new_instruments"][0]["list_date"] is listed
    assert metadata["observed_at"] is observed


@pytest.mark.parametrize("write_path", ["start", "mutate"])
def test_unsupported_metadata_does_not_commit(tmp_path, write_path):
    manifest = Manifest(tmp_path / "manifest.db")
    saved = {"existing": "keep"}
    metadata = {"nested": [date(2024, 6, 28), object()]}
    run_id = "invalid-metadata" if write_path == "start" else manifest.start_run("init", saved)
    with pytest.raises(TypeError, match="Object of type object is not JSON serializable"):
        if write_path == "start":
            manifest.start_run("init", metadata, run_id=run_id)
        else:
            manifest.mutate_run_metadata(run_id, lambda current: current.update(metadata))
    if write_path == "start":
        assert manifest.get_run(run_id) is None
    else:
        assert manifest.get_run_metadata(run_id) == saved
