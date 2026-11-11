"""Dashboard jobs read a command's result instead of scraping its output."""

from __future__ import annotations

import json

from cnequity.cli._shared import emit_json
from cnequity.ops.job import classify, read_result


def test_emit_json_prints_as_before_and_hands_the_result_over(tmp_path, monkeypatch, capsys):
    payload = {"status": "success", "rows_written": 3, "day": "2024-06-03"}
    monkeypatch.delenv("CNE_RESULT_PATH", raising=False)
    emit_json(payload, indent=2)
    assert capsys.readouterr().out == json.dumps(payload, indent=2) + "\n"
    assert not list(tmp_path.iterdir())

    target = tmp_path / "job.result"
    monkeypatch.setenv("CNE_RESULT_PATH", str(target))
    emit_json({"status": "failed"}, indent=2)
    emit_json(payload, indent=2)
    capsys.readouterr()
    assert read_result(target) == payload, "the last result wins, as the last printed did"


def test_a_handed_over_result_outranks_whatever_was_printed_after_it():
    """A later JSON line (a library's log, a nested report) used to become the result."""
    stdout = '{"status": "success"}\n{"status": "failed", "note": "from a later print"}\n'
    assert classify("run_json", 0, [], stdout)[0] == "failed"
    assert classify("run_json", 0, [], stdout, {"status": "success"})[0] == "succeeded"


def test_an_unreadable_result_falls_back_to_the_output(tmp_path):
    broken = tmp_path / "job.result"
    broken.write_text("{not json", encoding="utf-8")
    assert read_result(broken) is None
    assert read_result(tmp_path / "missing.result") is None
