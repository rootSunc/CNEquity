"""Compressed JSON with a stable logical name, and expiry of CNINFO checkpoints.

CNINFO pagination checkpoints were plain JSON rewritten after every page; one
reached 239 MB and the directory 7.2 GB, though no checkpoint older than its
7-day TTL can ever be reused.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from cnequity import json_codec
from cnequity.config import Config
from cnequity.steps.events import prune_expired_cninfo_checkpoints

PAYLOAD = {"identity": "abc", "slices": {"s": {"rows": [{"announcement_id": "1", "标题": "年报"}]}}}


def test_a_write_round_trips_through_a_compressed_file(tmp_path):
    path = tmp_path / "ckpt.json"
    stored = json_codec.write_json(path, PAYLOAD)
    assert stored.name in {"ckpt.json.zst", "ckpt.json.gz"}
    assert not path.exists()
    assert json_codec.read_json(path) == PAYLOAD
    assert json_codec.exists(path)


def test_gzip_is_used_where_no_zstd_binding_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(json_codec, "_zstd", lambda: None)
    path = tmp_path / "ckpt.json"
    assert json_codec.write_json(path, PAYLOAD).name == "ckpt.json.gz"
    assert json_codec.read_json(path) == PAYLOAD


def test_a_plain_legacy_file_is_read_and_replaced_by_the_next_write(tmp_path):
    path = tmp_path / "ckpt.json"
    path.write_text(json.dumps({"identity": "old"}), encoding="utf-8")
    assert json_codec.read_json(path) == {"identity": "old"}
    json_codec.write_json(path, PAYLOAD)
    assert not path.exists(), "the plain variant must not shadow or outlive the new one"
    assert json_codec.read_json(path) == PAYLOAD


def test_compressed_json_is_much_smaller_than_the_plain_text(tmp_path):
    rows = [
        {"announcement_id": str(i), "title": "关于召开股东大会的通知", "plate": "sz"}
        for i in range(5000)
    ]
    path = tmp_path / "ckpt.json"
    stored = json_codec.write_json(path, {"rows": rows})
    plain = len(json.dumps({"rows": rows}, ensure_ascii=False).encode("utf-8"))
    assert stored.stat().st_size * 8 < plain


def test_a_corrupt_file_is_reported_not_returned(tmp_path):
    path = tmp_path / "ckpt.json"
    stored = json_codec.write_json(path, PAYLOAD)
    stored.write_bytes(b"not compressed json")
    with pytest.raises(ValueError):
        json_codec.read_json(path)


def test_remove_clears_every_variant(tmp_path):
    path = tmp_path / "ckpt.json"
    path.write_text("{}", encoding="utf-8")
    json_codec.write_json(path, PAYLOAD)
    (tmp_path / "ckpt.json.gz").write_bytes(b"x")
    json_codec.remove(path)
    assert not json_codec.exists(path)
    with pytest.raises(FileNotFoundError):
        json_codec.read_json(path)


def _age(path, days: float) -> None:
    stamp = time.time() - days * 86400
    os.utime(path, (stamp, stamp))


def test_checkpoints_past_their_ttl_are_pruned_and_fresh_ones_kept(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    base = cfg.meta_root / "checkpoints" / "announcement_index"
    old = json_codec.write_json(base / "2016-01-01_2016-01-31.json", PAYLOAD)
    fresh = json_codec.write_json(base / "2026-11-01_2026-11-05.json", PAYLOAD)
    temp_leftover = base / ".2016-02-01_2016-02-29.json.zst.ab12cd"
    temp_leftover.write_bytes(b"partial")
    _age(old, 30)
    _age(temp_leftover, 30)
    _age(fresh, 2)
    assert prune_expired_cninfo_checkpoints(cfg) == 2
    assert not old.exists() and not temp_leftover.exists()
    assert fresh.exists()


def test_nothing_is_pruned_when_the_ttl_is_disabled(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    cfg.cninfo_checkpoint_ttl_days = None
    old = json_codec.write_json(cfg.meta_root / "checkpoints" / "x" / "w.json", PAYLOAD)
    _age(old, 400)
    assert prune_expired_cninfo_checkpoints(cfg) == 0
    assert old.exists()
