"""The cross-process recovery probe that follows a source cooldown."""

from __future__ import annotations

import json
import time

import pytest

from cnequity.domain import http_policy


def _cooled_and_due_for_probe(root, source="sse"):
    path = http_policy._path(root, source)
    path.parent.mkdir(parents=True, exist_ok=True)
    expired = time.time() - 1
    path.write_text(json.dumps({"until": expired, "kind": "blacklist", "needs_probe": True}))
    return path, expired


def test_ctrl_c_during_the_probe_hands_it_back_without_re_cooling(tmp_path):
    path, expired = _cooled_and_due_for_probe(tmp_path)

    with pytest.raises(KeyboardInterrupt):
        with http_policy.source_probe_slot(tmp_path, "sse"):
            raise KeyboardInterrupt

    state = json.loads(path.read_text())
    assert state["needs_probe"] is True  # the next run probes at once
    assert state["until"] == expired
    assert state["kind"] == "blacklist"
    assert not [key for key in state if key.startswith("probe_")]
    with http_policy.source_probe_slot(tmp_path, "sse"):
        pass  # not refused as "a probe is already running"
    assert json.loads(path.read_text())["needs_probe"] is False


def test_a_failed_probe_still_re_cools_the_source(tmp_path):
    path, _ = _cooled_and_due_for_probe(tmp_path)

    with pytest.raises(ConnectionError):
        with http_policy.source_probe_slot(tmp_path, "sse"):
            raise ConnectionError("refused again")

    state = json.loads(path.read_text())
    assert state["kind"] == "probe_failed"
    assert state["until"] > time.time() + 200
