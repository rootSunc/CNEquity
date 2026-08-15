"""Offline coverage for CNI index adjustment helpers."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl

from cn_market_lake.adapters.cni import index_constituents_history as cni


def test_member_symbol_filters_non_a():
    assert cni._member_symbol("600519") == "600519.SH"
    assert cni._member_symbol("000001") == "000001.SZ"
    # Not an A-share equity code
    assert cni._member_symbol("999999") is None


def test_fetch_cni_empty_payload_and_parse_error(monkeypatch):
    class Resp:
        content = b"x" * 10  # too short

        def raise_for_status(self):
            return None

    client = SimpleNamespace(get=lambda *a, **k: Resp(), close=lambda: None)
    empty = cni.fetch_cni_index_adjustments("399001.SZ", client=client)
    assert empty.is_empty()
    assert "index_symbol" in empty.schema

    class BadResp:
        content = b"x" * 200

        def raise_for_status(self):
            return None

    import pandas as pd

    monkeypatch.setattr(
        pd, "read_excel", lambda *a, **k: (_ for _ in ()).throw(ValueError("bad xlsx"))
    )
    bad = cni.fetch_cni_index_adjustments(
        "399001.SZ",
        client=SimpleNamespace(get=lambda *a, **k: BadResp(), close=lambda: None),
    )
    assert bad.is_empty()


def test_fetch_cni_parses_xlsx_rows(monkeypatch):
    import pandas as pd

    pdf = pd.DataFrame(
        [
            {
                "开始日期": "2024-01-01",
                "结束日期": "2025-01-01",
                "样本代码": "000001",
                "调整类型": "OLD",
            },
            {
                "开始日期": "2024-01-01",
                "结束日期": "2025-01-01",
                "样本代码": "600519",
                "调整类型": "-",  # removal — skipped
            },
            {
                "开始日期": "2024-06-01",
                "结束日期": "2025-06-01",
                "样本代码": "000002",
                "调整类型": "+",
            },
        ]
    )

    class Resp:
        content = b"x" * 200

        def raise_for_status(self):
            return None

    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: pdf)
    df = cni.fetch_cni_index_adjustments(
        "399001.SZ",
        client=SimpleNamespace(get=lambda *a, **k: Resp(), close=lambda: None),
    )
    assert df.height == 2
    assert set(df["symbol"].to_list()) == {"000001.SZ", "000002.SZ"}


def test_expand_cni_constituents_as_of():
    adjustments = pl.DataFrame(
        {
            "index_symbol": ["399001.SZ", "399001.SZ"],
            "symbol": ["000001.SZ", "000002.SZ"],
            "start_date": [date(2024, 1, 1), date(2024, 6, 1)],
            "end_date": [date(2025, 1, 1), date(2025, 6, 1)],
            "adjust_type": ["OLD", "+"],
        }
    )
    out = cni.expand_cni_constituents_as_of(adjustments, [date(2024, 3, 1), date(2024, 7, 1)])
    assert out.height == 3  # 000001 on both dates; 000002 only on Jul
    assert cni.expand_cni_constituents_as_of(adjustments, []).is_empty()
    assert cni.expand_cni_constituents_as_of(
        pl.DataFrame(schema=adjustments.schema), [date(2024, 1, 2)]
    ).is_empty()
