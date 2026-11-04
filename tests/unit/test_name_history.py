"""Offline tests for name_history (Tushare namechange)."""

from __future__ import annotations

import json
from datetime import date

import httpx
import polars as pl

from cnequity.adapters.tushare import name_history as nh
from cnequity.adapters.tushare.client import TushareClient
from cnequity.config import Config
from cnequity.domain.schemas import NAME_HISTORY_SCHEMA, validate_dataframe, with_provenance

FIELDS = nh._FIELDS.split(",")


def _cfg(tmp_path, **kw):
    return Config(
        data_root=tmp_path,
        sources={"tushare": True},
        source_intervals={"tushare": 0},
        tushare_token="tok",
        **kw,
    )


def test_normalize_reads_the_st_label_from_the_name():
    rows = [
        ["000613.SZ", "大东海A", "19970128", "19990503", None, "其他"],
        ["000613.SZ", "ST东海A", "19990504", "20010426", None, "ST"],
        ["000613.SZ", "PT东海A", "20010427", "20021017", None, "PT"],
        ["000613.SZ", "S*ST东海", "20061009", "20070322", None, "未股改加S"],
        ["600530.SH", "G*ST昂立", "20060227", None, "20060220", "*ST"],
        ["600530.SH", "SST昂立", "20060301", None, None, "其他"],
        ["600530.SH", "", "20060301", None, None, "其他"],
        ["600530.SH", "交大昂立", None, None, None, "其他"],
    ]
    frame = nh.normalize(FIELDS, rows)
    assert frame.height == 6
    labels = dict(zip(frame["name"], frame["risk_warning"], strict=True))
    assert labels == {
        "大东海A": False,
        "ST东海A": True,
        "PT东海A": False,
        "S*ST东海": True,
        "G*ST昂立": True,
        "SST昂立": True,
    }
    assert frame.filter(pl.col("name") == "G*ST昂立")["announce_date"][0] == date(2006, 2, 20)
    stored = with_provenance(frame, source="tushare", data_version="v1")
    assert dict(stored.schema) == NAME_HISTORY_SCHEMA
    validate_dataframe(stored, "name_history")


def test_fetch_pages_through_the_whole_history(tmp_path):
    cfg = _cfg(tmp_path)
    calls = []

    def respond(request):
        body = json.loads(request.content)
        calls.append(body["params"])
        offset = body["params"]["offset"]
        items = [
            [f"{600000 + offset + i:06d}.SH", "某某", "20200101", None, None, "其他"]
            for i in range(2000 if offset == 0 else 1)
        ]
        return httpx.Response(
            200, json={"code": 0, "msg": "", "data": {"fields": FIELDS, "items": items}}
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as http:
        frame = nh.fetch_name_history(cfg, client=TushareClient(cfg, client=http))
    assert frame.height == 2001
    assert [c["offset"] for c in calls] == [0, 2000]


def test_the_step_sends_nothing_without_a_token(tmp_path, monkeypatch):
    from cnequity.steps.reference import step_name_history

    def boom(*_a, **_k):  # pragma: no cover - must not be called
        raise AssertionError("no request without a token")

    monkeypatch.setattr(httpx.Client, "post", boom)
    result = step_name_history(Config(data_root=tmp_path), date(2026, 11, 4), "run-1", {})
    assert result["status"] == "skipped"
    assert result["rows_written"] == 0
