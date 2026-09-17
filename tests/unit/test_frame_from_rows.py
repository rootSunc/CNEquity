"""Dtypes come from the declared schema, not from the first hundred rows.

`news_headlines` and `flash_news_wire` failed 22 times between 2026-09-12 and
2026-09-16 this way: a session whose first hundred flashes named no security
typed `related_symbols` as Null, and the hundred-and-first raised.
"""

from __future__ import annotations

import polars as pl
import pytest

from cnequity.domain.schemas import DATASET_SCHEMAS, frame_from_rows


def _headlines(n_blank: int) -> list[dict]:
    rows = [
        {
            "news_id": str(i),
            "publish_date": None,
            "publish_time": "09:25:00",
            "title": "标题",
            "summary": None,
            "related_symbols": None,
            "channel": "fast_news",
        }
        for i in range(n_blank)
    ]
    rows.append({**rows[0], "news_id": str(n_blank), "related_symbols": "000063.SZ"})
    return rows


def test_a_column_blank_past_the_inference_window_still_takes_its_value():
    rows = _headlines(100)
    with pytest.raises(pl.exceptions.ComputeError):
        pl.DataFrame(rows)

    frame = frame_from_rows(rows, "news_headlines")

    assert frame.height == 101
    assert frame.schema["related_symbols"] == pl.Utf8
    assert frame["related_symbols"][-1] == "000063.SZ"


def test_a_column_blank_in_every_row_still_takes_its_declared_type():
    """Otherwise it lands as Null and the write has to guess."""
    rows = [r for r in _headlines(3)[:3]]
    frame = frame_from_rows(rows, "news_headlines")
    assert frame.schema["summary"] == pl.Utf8


def test_a_column_the_stored_schema_does_not_name_is_left_alone():
    """Adapters build intermediate frames; pinning only what is declared keeps
    this from dictating their shape."""
    rows = [{"news_id": "1", "scratch": 1}, {"news_id": "2", "scratch": 2}]
    frame = frame_from_rows(rows, "news_headlines")
    assert frame.schema["scratch"] == pl.Int64
    assert frame.schema["news_id"] == DATASET_SCHEMAS["news_headlines"]["news_id"]


def test_an_unknown_dataset_still_reads_every_row():
    rows = [{"a": None}] * 100 + [{"a": "x"}]
    assert frame_from_rows(rows, "not_a_dataset")["a"][-1] == "x"


def test_no_rows_is_an_empty_frame():
    assert frame_from_rows([], "news_headlines").is_empty()
