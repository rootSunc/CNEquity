"""Offline coverage for 通达信 quarterly financial packages and their routing."""

from __future__ import annotations

import hashlib
import io
import struct
import zipfile
from datetime import date

import httpx
import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError
from cnequity.adapters.tdx_fin import packages as tdx
from cnequity.config import Config
from cnequity.steps import fundamentals as fund

FIELDS = 584


def _package(report_date: int, securities: dict[str, dict[int, float]]) -> bytes:
    """A gpcw zip: header, 11-byte directory entries, float32 records."""
    header = struct.Struct("<hIH3L")
    entry = struct.Struct("<6scL")
    record = struct.Struct(f"<{FIELDS}f")
    base = header.size + entry.size * len(securities)
    body = bytearray(header.pack(1, report_date, len(securities), 0, record.size, 0))
    records = bytearray()
    for i, (code, values) in enumerate(securities.items()):
        body += entry.pack(code.encode(), b"0", base + i * record.size)
        fields = [0.0] * FIELDS
        for column, value in values.items():
            fields[column - 1] = value
        records += record.pack(*fields)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr(f"gpcw{report_date}.dat", bytes(body + records))
    return out.getvalue()


MOUTAI = {96: 45_402_963_968.0, 40: 292_257_792_000.0, 314: 250813.0}
OLD = {96: 1_000_000.0, 314: 250401.0}


def test_package_rows_maps_fields_and_labels_by_disclosure_age():
    content = _package(
        20250630, {"600519": MOUTAI, "000001": OLD, "900901": MOUTAI, "300001": {96: 5.0}}
    )
    rows = tdx.package_rows("2025Q2", content, observed_on=date(2025, 8, 20))

    moutai = rows.filter(pl.col("symbol") == "600519.SH")
    assert set(moutai["item_code"]) == {"net_profit", "total_assets"}
    assert moutai["announce_date"].unique().to_list() == [date(2025, 8, 13)]
    assert moutai["source"].unique().to_list() == ["tdx_fin"]
    # Disclosed before the period closed: an impossible date, so skipped.
    assert rows.filter(pl.col("symbol") == "000001.SZ").is_empty()
    # B shares are not A-shares; no disclosure date means no row.
    assert rows["symbol"].unique().sort().to_list() == ["600519.SH"]

    later = tdx.package_rows("2025Q2", content, observed_on=date(2026, 10, 26))
    assert later["source"].unique().to_list() == ["tdx_fin_backfill"]


def test_package_for_the_wrong_period_is_rejected():
    with pytest.raises(tdx.TdxFinPayloadError, match="reports"):
        tdx.package_rows(
            "2025Q1", _package(20250630, {"600519": MOUTAI}), observed_on=date(2025, 9, 1)
        )


def test_latest_periods():
    assert tdx.latest_periods(date(2026, 10, 26)) == ["2026Q3", "2026Q2"]
    assert tdx.latest_periods(date(2026, 1, 5)) == ["2025Q4", "2025Q3"]
    assert tdx.latest_periods(date(2026, 3, 31)) == ["2026Q1", "2025Q4"]


def _client(packages: dict[str, bytes], *, corrupt: bool = False) -> httpx.Client:
    index = "\n".join(
        f"{name},{hashlib.md5(b'x' if corrupt else data).hexdigest()},{len(data)}"
        for name, data in packages.items()
    )

    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if name == tdx.INDEX_NAME:
            return httpx.Response(200, text=index)
        return httpx.Response(200, content=packages[name])

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_package_checks_the_index_md5():
    packages = {"gpcw20250630.zip": _package(20250630, {"600519": MOUTAI})}
    client = _client(packages, corrupt=True)
    index = tdx.fetch_index(client=client)
    with pytest.raises(tdx.TdxFinPayloadError, match="MD5"):
        tdx.fetch_package("2025Q2", index=index, client=client)
    assert tdx.fetch_package("2024Q4", index=index, client=client) is None


@pytest.fixture
def cfg(tmp_path):
    c = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    c.staging_root.mkdir(parents=True)
    return c


def test_daily_step_falls_back_to_tdx_fin_when_eastmoney_fails(cfg, monkeypatch):
    packages = {
        "gpcw20250630.zip": _package(
            20250630, {"600519": MOUTAI, "000002": {96: 7.0, 314: 250420.0}}
        ),
        "gpcw20250331.zip": _package(20250331, {}),
    }
    client = _client(packages)

    def failing(*_a, **_k):
        raise EastMoneyDatacenterError("datacenter breaker open")

    monkeypatch.setattr(fund, "fetch_financial_statement_items", failing)
    real_stage = fund.stage_financials_tdx_fin
    monkeypatch.setattr(
        fund,
        "stage_financials_tdx_fin",
        lambda *a, **k: real_stage(*a, **{**k, "client": client}),
    )

    result = fund.step_financial_statement_items(cfg, date(2025, 8, 20), "run-tdx", {})

    assert result["status"] == "degraded"
    assert result["fallback_source"] == "tdx_fin"
    assert result["rows_written"] == 2
    staged = pl.concat(
        [pl.read_parquet(p) for p in cfg.staging_root.rglob("*.parquet")], how="diagonal_relaxed"
    )
    assert staged["source"].unique().to_list() == ["tdx_fin"]
    assert staged["symbol"].unique().to_list() == ["600519.SH"]


def test_fallback_failure_names_both_sources(cfg, monkeypatch):
    def failing(*_a, **_k):
        raise EastMoneyDatacenterError("datacenter breaker open")

    def offline(*_a, **_k):
        raise httpx.ConnectError("no route")

    monkeypatch.setattr(fund, "fetch_financial_statement_items", failing)
    monkeypatch.setattr(fund, "stage_financials_tdx_fin", offline)
    with pytest.raises(RuntimeError, match="eastmoney failed.*tdx_fin failed"):
        fund.step_financial_statement_items(cfg, date(2025, 8, 20), "run-tdx", {})


def test_gap_fill_only_writes_items_the_lake_lacks(cfg, monkeypatch):
    held = pl.DataFrame(
        {"symbol": ["600519.SH"], "statement_type": ["income"], "item_code": ["net_profit"]}
    ).with_columns(pl.lit("2025Q2").alias("report_period"))
    monkeypatch.setattr(
        fund,
        "_without_held_keys",
        lambda _c, frame, _p: frame.join(held, on=list(held.columns), how="anti"),
    )
    client = _client({"gpcw20250630.zip": _package(20250630, {"600519": MOUTAI})})

    result = fund.stage_financials_tdx_fin(
        cfg, "run-gap", date(2026, 10, 26), periods=["2025Q2"], gap_only=True, client=client
    )

    assert result["rows_written"] == 1
    staged = pl.read_parquet(next(cfg.staging_root.rglob("*.parquet")))
    assert staged.select("item_code", "source").rows() == [("total_assets", "tdx_fin_backfill")]


def test_a_short_read_is_retried_before_the_backup(cfg, monkeypatch):
    calls = []

    def flaky(*_a, **_k):
        calls.append(1)
        if len(calls) == 1:
            raise EastMoneyDatacenterError(
                "EastMoney datacenter RPT_LICO_FN_CPD returned 5568 rows but page 1 "
                "declared count=5577; refusing truncated result"
            )
        return pl.DataFrame()

    def backup(*_a, **_k):
        raise AssertionError("the backup must not run after a successful retry")

    monkeypatch.setattr(fund, "_SHORT_READ_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(fund, "fetch_financial_statement_items", flaky)
    monkeypatch.setattr(fund, "_financials_from_tdx_fin", backup)

    fund.step_financial_statement_items(cfg, date(2025, 8, 20), "run-retry", {})
    assert len(calls) == 2


def test_only_short_reads_are_retried(cfg, monkeypatch):
    calls = {"short": 0, "breaker": 0}

    def short(*_a, **_k):
        calls["short"] += 1
        raise EastMoneyDatacenterError(
            "returned 1 rows but page 1 declared count=2; refusing truncated result"
        )

    def breaker(*_a, **_k):
        calls["breaker"] += 1
        raise EastMoneyDatacenterError("datacenter breaker open")

    monkeypatch.setattr(fund, "_SHORT_READ_BACKOFF_SECONDS", 0.0)
    monkeypatch.setattr(fund, "_financials_from_tdx_fin", lambda *a, **k: {"status": "degraded"})
    monkeypatch.setattr(fund, "fetch_financial_statement_items", short)
    assert (
        fund.step_financial_statement_items(cfg, date(2025, 8, 20), "r1", {})["status"]
        == "degraded"
    )
    monkeypatch.setattr(fund, "fetch_financial_statement_items", breaker)
    fund.step_financial_statement_items(cfg, date(2025, 8, 20), "r2", {})
    assert calls == {"short": fund._SHORT_READ_RETRIES + 1, "breaker": 1}
