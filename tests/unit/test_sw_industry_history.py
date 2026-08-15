"""Offline coverage for Shenwan industry interval helpers."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from cn_market_lake.adapters.sw import industry_history as sw


def test_exchange_and_code_to_symbol():
    assert sw.exchange_from_code("600519") == "SH"
    assert sw.exchange_from_code("000001") == "SZ"
    assert sw.exchange_from_code("920001") == "BJ"
    assert sw._code_to_symbol("600519") == "600519.SH"
    assert sw._code_to_symbol("999999") is None


def test_fetch_sw_industry_intervals(monkeypatch):
    pdf = pd.DataFrame(
        [
            {
                "股票代码": "600519",
                "计入日期": "2021-01-01",
                "行业代码": "801780",
                "更新日期": "2021-01-02",
            },
            {
                "股票代码": "000001",
                "计入日期": "2022-06-01",
                "行业代码": "801780",
                "更新日期": "2022-06-02",
            },
            {
                "股票代码": "999999",
                "计入日期": "2021-01-01",
                "行业代码": "801780",
                "更新日期": "2021-01-02",
            },
        ]
    )

    class Resp:
        content = b"xls-bytes"

        def raise_for_status(self):
            return None

    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: pdf)
    df = sw.fetch_sw_industry_intervals(
        client=SimpleNamespace(get=lambda *a, **k: Resp(), close=lambda: None)
    )
    assert df.height == 2
    assert set(df["symbol"].to_list()) == {"600519.SH", "000001.SZ"}

    monkeypatch.setattr(pd, "read_excel", lambda *a, **k: pd.DataFrame())
    with pytest.raises(RuntimeError, match="no rows"):
        sw.fetch_sw_industry_intervals(
            client=SimpleNamespace(get=lambda *a, **k: Resp(), close=lambda: None)
        )


def test_expand_sw_industry_as_of():
    intervals = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH", "000001.SZ"],
            "start_date": [date(2020, 1, 1), date(2023, 1, 1), date(2021, 1, 1)],
            "industry_code": ["A", "B", "C"],
        }
    )
    assert sw.expand_sw_industry_as_of(intervals, []).is_empty()
    out = sw.expand_sw_industry_as_of(intervals, [date(2022, 6, 1), date(2023, 6, 1)])
    # 2022: 600519→A, 000001→C; 2023: 600519→B, 000001→C
    assert out.height == 4
    latest = out.filter(
        (pl.col("symbol") == "600519.SH") & (pl.col("as_of_date") == date(2023, 6, 1))
    )
    assert latest["industry_code"][0] == "B"


# --- TLS trust ---------------------------------------------------------------
# swsresearch.com sends its leaf certificate and no intermediate, so certifi
# alone cannot build a path to a root it trusts and every fetch failed with
# "unable to get local issuer certificate" (httpx 0/5, curl_cffi 0/6 measured).


def test_shipped_intermediate_completes_the_chain():
    import ssl

    assert sw._SW_INTERMEDIATE_PEM.exists(), "intermediate must ship in the wheel"
    pem = sw._SW_INTERMEDIATE_PEM.read_text(encoding="ascii")
    assert pem.startswith("-----BEGIN CERTIFICATE-----")

    # Loadable, and it is the CA the leaf's AIA extension names.
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=str(sw._SW_INTERMEDIATE_PEM))
    subjects = {
        tuple(part for rdn in cert["subject"] for part in rdn) for cert in ctx.get_ca_certs()
    }
    flat = {v for subj in subjects for _, v in [p for p in subj]}
    assert "GeoTrust G2 TLS CN RSA4096 SHA256 2022 CA1" in flat


def test_sw_ssl_context_keeps_verification_on():
    ctx = sw.sw_ssl_context()
    assert ctx.verify_mode != 0, "must not fall back to an unverified context"
    assert ctx.check_hostname is True
    # Cached: building the context per request would re-read the PEM every time.
    assert sw.sw_ssl_context() is ctx


def test_sw_client_uses_that_context(monkeypatch):
    seen = {}

    def _fake_client(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(sw.httpx, "Client", _fake_client)
    sw.sw_client(timeout=7.0)
    assert seen["verify"] is sw.sw_ssl_context()
    assert seen["timeout"] == 7.0
    assert seen["follow_redirects"] is True
