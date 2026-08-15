"""daily_bars.volume is 股 from every adapter, and the audit says so if it isn't."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from cn_market_lake.adapters.baostock.delisted_bars import _fetch_one
from cn_market_lake.adapters.eastmoney import bars as em_bars
from cn_market_lake.adapters.sina import bars as sina_bars
from cn_market_lake.adapters.tdx_protocol.bars import _parse_bar_rows
from cn_market_lake.adapters.ths.stock_bars import _parse_stock_kline
from cn_market_lake.domain.schemas import data_version_for
from cn_market_lake.domain.units import SHARES_PER_LOT, lots_to_shares
from cn_market_lake.quality.unit_checks import (
    UNIT_CHECK_MIN_ROWS,
    daily_bars_volume_unit_findings,
)

# One bar, priced so amount = close × shares exactly. 40,000 shares = 400 手
# at 12.5 元 turns over 500,000 元 — the invariant the whole check rests on.
CLOSE = 12.5
SHARES = 40_000
LOTS = SHARES // SHARES_PER_LOT
AMOUNT = CLOSE * SHARES


def _ratio(row: dict) -> float:
    return row["amount"] / row["close"] / row["volume"]


# --- the conversion itself ---------------------------------------------------


def test_lots_to_shares_scales_and_handles_none():
    assert lots_to_shares(LOTS) == SHARES
    assert lots_to_shares(0) == 0
    assert lots_to_shares(None) == 0


# --- per-adapter: every path lands on 股 -------------------------------------


def test_tdx_daily_bars_convert_lots_to_shares():
    pdf = pl.DataFrame(
        [
            {
                "datetime": date(2024, 6, 28),
                "open": CLOSE,
                "high": CLOSE,
                "low": CLOSE,
                "close": CLOSE,
                "volume": LOTS,
                "amount": AMOUNT,
            }
        ]
    )
    row = _parse_bar_rows(pdf, "600519.SH", date(2024, 6, 1), date(2024, 6, 30))[0]
    assert row["volume"] == SHARES
    assert _ratio(row) == pytest.approx(1.0)


def test_tdx_index_bars_keep_the_vendor_unit():
    """index_bars is a separate contract whose unit is not pinned down; the
    daily conversion must not leak onto it."""
    pdf = pl.DataFrame(
        [
            {
                "datetime": date(2024, 6, 28),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": LOTS,
                "amount": AMOUNT,
            }
        ]
    )
    row = _parse_bar_rows(
        pdf, "000001.SH", date(2024, 6, 1), date(2024, 6, 30), volume_in_lots=False
    )[0]
    assert row["volume"] == LOTS


def test_eastmoney_kline_converts_lots_to_shares():
    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": {"klines": [f"20240628,{CLOSE},{CLOSE},{CLOSE},{CLOSE},{LOTS},{AMOUNT}"]}
            }

    class _Client:
        def get(self, url, params=None):
            return _Resp()

        def close(self):
            return None

    df = em_bars.fetch_daily_bars(
        ["600519.SH"], date(2024, 6, 1), date(2024, 6, 30), client=_Client()
    )
    row = df.row(0, named=True)
    assert row["volume"] == SHARES
    assert _ratio(row) == pytest.approx(1.0)


def test_eastmoney_clist_converts_lots_to_shares(monkeypatch):
    monkeypatch.setattr(em_bars, "fetch_clist_pages", lambda client, fields: [{}])
    monkeypatch.setattr(
        em_bars,
        "clist_rows_to_symbols",
        lambda rows: [
            (
                "600519.SH",
                {"f17": CLOSE, "f15": CLOSE, "f16": CLOSE, "f2": CLOSE, "f5": LOTS, "f6": AMOUNT},
            )
        ],
    )

    class _Client:
        def close(self):
            return None

    df = em_bars.fetch_daily_bars_clist(date(2024, 6, 28), client=_Client())
    row = df.row(0, named=True)
    assert row["volume"] == SHARES
    assert _ratio(row) == pytest.approx(1.0)


def test_sina_passes_shares_through(monkeypatch):
    """Sina reports 股 natively — converting here is what broke it before."""
    monkeypatch.setattr(
        sina_bars,
        "_request",
        lambda symbol, datalen, client: [
            {
                "day": "2024-06-28",
                "open": CLOSE,
                "high": CLOSE,
                "low": CLOSE,
                "close": CLOSE,
                "volume": SHARES,
            }
        ],
    )
    df = sina_bars.fetch_daily_bars_sina("600519.SH")
    assert df["volume"][0] == SHARES


def test_ths_passes_shares_through():
    payload = {"data": f"20150105,{CLOSE},{CLOSE},{CLOSE},{CLOSE},{SHARES},{AMOUNT}"}
    row = _parse_stock_kline(payload, "600519.SH")[0]
    assert row["volume"] == SHARES
    assert _ratio(row) == pytest.approx(1.0)


def test_baostock_passes_shares_through():
    class _Rs:
        error_code = "0"

        def __init__(self):
            self._left = 1

        def next(self):
            self._left -= 1
            return self._left >= 0

        def get_row_data(self):
            return ["2020-06-30", CLOSE, CLOSE, CLOSE, CLOSE, SHARES, AMOUNT, "1"]

    class _Bs:
        def query_history_k_data_plus(self, *a, **k):
            return _Rs()

    row = _fetch_one(_Bs(), "600519.SH", date(2020, 6, 1), date(2020, 6, 30))[0]
    assert row["volume"] == SHARES
    assert _ratio(row) == pytest.approx(1.0)


# --- data_version ------------------------------------------------------------


def test_daily_bars_is_on_v2_and_other_datasets_are_not():
    assert data_version_for("daily_bars") == "v2"
    assert data_version_for("index_bars") == "v1"
    assert data_version_for("anything_else") == "v1"


def test_both_worker_paths_stamp_the_dataset_version():
    """The pooled and serial fetch paths must agree on ``data_version``.

    ``worker_pool`` fetches daily_bars two ways — one worker process per batch,
    or serially in-process — and they differ only in that argument. When the
    pooled one omitted it, every run with ``workers > 1`` wrote correctly
    converted 股 stamped ``v1``, so the stamp stopped meaning "股 guaranteed"
    and no ratio check could notice: the numbers were right, only the label
    was wrong.
    """
    import inspect

    from cn_market_lake.orchestrator import worker_pool

    source = inspect.getsource(worker_pool)
    calls = [line.strip() for line in source.splitlines() if "normalize_with_source(df" in line]
    assert calls, "no normalize_with_source call found in worker_pool"
    assert all("dataset=dataset" in call for call in calls), calls


# --- the audit check ---------------------------------------------------------


def _write_bars(config, rows: list[dict]) -> None:
    root = config.curated_root / "daily_bars"
    by_day: dict[date, list[dict]] = {}
    for row in rows:
        by_day.setdefault(row["trade_date"], []).append(row)
    for day, day_rows in by_day.items():
        part = root / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(day_rows).write_parquet(part / "part-0.parquet")


def _rows(source: str, *, volume: int, n: int, anchor: date) -> list[dict]:
    """*n* rows spread over the days before *anchor*, all with the same unit."""
    return [
        {
            "symbol": f"{600000 + i}.SH",
            "trade_date": anchor - timedelta(days=i % 5),
            "close": CLOSE,
            "volume": volume,
            "amount": AMOUNT,
            "source": source,
            "data_version": "v2",
        }
        for i in range(n)
    ]


def test_no_finding_when_every_source_is_in_shares(config):
    anchor = date(2024, 6, 28)
    _write_bars(
        config,
        _rows("tdx_protocol", volume=SHARES, n=UNIT_CHECK_MIN_ROWS + 10, anchor=anchor)
        + _rows("ths", volume=SHARES, n=UNIT_CHECK_MIN_ROWS + 10, anchor=anchor),
    )
    assert daily_bars_volume_unit_findings(config, anchor) == []


def test_flags_the_source_that_wrote_lots(config):
    anchor = date(2024, 6, 28)
    _write_bars(
        config,
        _rows("tdx_protocol", volume=LOTS, n=UNIT_CHECK_MIN_ROWS + 10, anchor=anchor)
        + _rows("ths", volume=SHARES, n=UNIT_CHECK_MIN_ROWS + 10, anchor=anchor),
    )
    findings = daily_bars_volume_unit_findings(config, anchor)
    assert len(findings) == 1
    finding = findings[0]
    assert finding["source"] == "tdx_protocol"
    assert finding["severity"] == "error"
    assert finding["check"] == "daily_bars_volume_unit"
    assert finding["median_ratio"] == pytest.approx(SHARES_PER_LOT)
    assert "手" in finding["message"]


def test_thin_sources_are_not_judged(config):
    """A gap-fill contributing a handful of rows is noise, not evidence."""
    anchor = date(2024, 6, 28)
    _write_bars(
        config,
        _rows("eastmoney", volume=LOTS, n=UNIT_CHECK_MIN_ROWS - 1, anchor=anchor),
    )
    assert daily_bars_volume_unit_findings(config, anchor) == []


def test_rows_without_amount_are_skipped_not_flagged(config):
    """Sina serves no amount — unmeasurable must not read as broken."""
    anchor = date(2024, 6, 28)
    rows = _rows("sina", volume=SHARES, n=UNIT_CHECK_MIN_ROWS + 10, anchor=anchor)
    for row in rows:
        row["amount"] = None
    _write_bars(config, rows)
    assert daily_bars_volume_unit_findings(config, anchor) == []


def test_no_curated_data_is_not_a_finding(config):
    assert daily_bars_volume_unit_findings(config, date(2024, 6, 28)) == []
