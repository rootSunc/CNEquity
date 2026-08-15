"""Offline coverage for OnDemandService cache + unimplemented stubs."""

from __future__ import annotations

import pytest

from cn_market_lake.config import Config
from cn_market_lake.query.on_demand import OnDemandService


def test_fetch_cache_roundtrip(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data", on_demand_datasets=["stock_news"])
    svc = OnDemandService(cfg)

    monkeypatch.setattr(
        "cn_market_lake.query.on_demand.fetch_stock_news",
        lambda symbol, **k: {"symbol": symbol, "items": [{"title": "t"}]},
    )
    monkeypatch.setattr(Config, "rate_limit", lambda self, name: None)

    first = svc.fetch("stock_news", "600519.SH", limit=5)
    assert first["items"][0]["title"] == "t"
    assert first["data_version"] == "v1"

    # Second hit uses cache (remote would raise if called).
    monkeypatch.setattr(
        "cn_market_lake.query.on_demand.fetch_stock_news",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("cache miss")),
    )
    second = svc.fetch("stock_news", "600519.SH")
    assert second["items"][0]["title"] == "t"

    with pytest.raises(ValueError, match="not enabled"):
        svc.fetch("unknown_ds", "600519.SH")


def test_unimplemented_datasets_raise_and_do_not_cache(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        on_demand_datasets=["announcement_body", "financial_reports"],
    )
    svc = OnDemandService(cfg)

    with pytest.raises(NotImplementedError, match="announcement_body"):
        svc.fetch("announcement_body", "600519.SH")
    assert not (cfg.meta_root / "on_demand" / "announcement_body").exists()

    with pytest.raises(NotImplementedError, match="financial_reports"):
        svc.fetch("financial_reports", "600519.SH")
    assert not (cfg.meta_root / "on_demand" / "financial_reports").exists()


def test_research_reports_error_path_skips_cache(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data", on_demand_datasets=["research_reports"])
    svc = OnDemandService(cfg)
    monkeypatch.setattr(Config, "rate_limit", lambda self, name: None)

    class BoomClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def get(self, url):
            raise RuntimeError("offline")

    monkeypatch.setattr("cn_market_lake.query.on_demand.EastMoneyClient", BoomClient)
    out = svc.fetch("research_reports", "600519.SH")
    assert out["items"] == []
    assert "error" in out
    assert not (cfg.meta_root / "on_demand" / "research_reports").exists()

    with pytest.raises(NotImplementedError, match="mystery"):
        svc._fetch_remote("mystery", "600519.SH")
