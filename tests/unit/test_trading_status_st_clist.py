"""ST clist uses shared pagination + host failover (not hard-coded push2)."""

from __future__ import annotations

import cn_market_lake.adapters.eastmoney.trading_status as ts


def test_fetch_st_symbols_uses_clist_failover(monkeypatch):
    calls: list[dict] = []

    def _fake_clist(client, *, fields, fs, page_size):
        calls.append({"fields": fields, "fs": fs, "page_size": page_size})
        return [
            {"f12": "600519", "f13": 1, "f14": "贵州茅台"},
            {"f12": "000001", "f13": 0, "f14": "平安银行"},
        ]

    monkeypatch.setattr(ts, "fetch_clist_pages", _fake_clist)
    out = ts._fetch_st_symbols(client=object())  # type: ignore[arg-type]
    assert calls == [
        {
            "fields": "f12,f13,f14",
            "fs": ts._ST_FS,
            "page_size": ts._ST_PAGE_SIZE,
        }
    ]
    assert out == {"600519.SH", "000001.SZ"}


def test_fetch_st_symbols_returns_empty_on_clist_failure(monkeypatch):
    monkeypatch.setattr(
        ts,
        "fetch_clist_pages",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("all hosts 502")),
    )
    assert ts._fetch_st_symbols(client=object()) == set()  # type: ignore[arg-type]
