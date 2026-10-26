"""Exchange day k-lines fill the sub-lot fund sessions TDX leaves unpublished."""

from __future__ import annotations

import json
from datetime import date

import httpx

from cnequity.adapters.exchange.fund_volume import fetch_fund_dayk
from cnequity.config import Config
from cnequity.steps import bars

PAYLOAD = {
    "code": "501217",
    "total": 3,
    "begin": 0,
    "end": 3,
    "kline": [
        [20261022, 0.923, 0.923, 0.923, 0.923, 100, 92],
        [20261023, 0, 0, 0, 0, 0, 0],
        [20261026, 0.915, 0.915, 0.915, 0.915, 15, 14],
    ],
}


def _client(payload=PAYLOAD) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/sh1/dayk/501217")
        return httpx.Response(200, content=json.dumps(payload).encode())

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_fund_dayk_keeps_share_quantities():
    frame = fetch_fund_dayk("501217.SH", trade_dates={date(2026, 10, 26)}, client=_client())

    assert frame.select("trade_date", "close", "volume", "amount").rows() == [
        (date(2026, 10, 26), 0.915, 15, 14.0)
    ]


def test_gapfill_asks_only_exchange_funds_and_stages_their_keys(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    cfg.sources["exchange"] = True
    asked, staged = [], {}

    def fake_fetch(symbol, *, trade_dates, client, config):
        asked.append((symbol, sorted(trade_dates)))
        return fetch_fund_dayk(symbol, trade_dates=trade_dates, client=_client())

    def fake_stage(config, run_id, *, batch_id, source, frame, symbols, start, end):
        staged.update(source=source, rows=frame.select("symbol", "trade_date", "volume").rows())
        return frame.height

    monkeypatch.setattr("cnequity.adapters.exchange.fund_volume.fetch_fund_dayk", fake_fetch)
    monkeypatch.setattr(bars, "_stage_daily_gap_batch", fake_stage)
    day = date(2026, 10, 26)

    result = bars._gapfill_fund_keys_via_exchange_dayk(
        cfg,
        "run-1",
        missing={("501217.SH", day), ("600519.SH", day)},
        start=day,
        end=day,
    )

    assert asked == [("501217.SH", [day])]
    assert staged == {"source": "exchange", "rows": [("501217.SH", day, 15)]}
    assert result["rows_written"] == 1


def test_gapfill_is_off_with_the_exchange_source(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    cfg.sources["exchange"] = False
    day = date(2026, 10, 26)

    result = bars._gapfill_fund_keys_via_exchange_dayk(
        cfg, "run-1", missing={("501217.SH", day)}, start=day, end=day
    )

    assert result["rows_written"] == 0
