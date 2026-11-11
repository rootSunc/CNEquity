"""融资融券 read from the exchanges instead of a redistributor.

The values were verified against the curated EastMoney day for 2026-08-26 —
every compared field matched exactly over 3,522 shared symbols — so what these
cover is the parsing, the two publishers' differing horizons, and the one field
the SSE does not publish.
"""

from __future__ import annotations

import io
from datetime import date

import polars as pl
import pytest

from cnequity.adapters.exchange import margin_trading as em
from cnequity.config import Config
from cnequity.run_scope import set_backfill
from cnequity.steps.capital import _fetch_margin_via_exchange, _margin_source

TD = date(2026, 8, 27)


class _Resp:
    def __init__(self, payload=None, content=b""):
        self._payload = payload
        self.content = content

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _serve(monkeypatch, resp):
    monkeypatch.setattr(
        em, "_client", lambda: type("C", (), {"get": staticmethod(lambda *a, **k: resp)})
    )


def _sse_payload(rows, *, total=None):
    return {"pageHelp": {"data": rows, "total": len(rows) if total is None else total}}


_SSE_ROW = {
    "stockCode": "600000",
    "rzye": 1452735156,
    "rzmre": 49295712,
    "rqyl": 38171440,
    "rqmcl": 2274700,
    "rqylje": None,
}


def test_sse_margin_parses_and_leaves_short_balance_null(monkeypatch):
    """SSE publishes 融券余量 but not 融券余额; the gap is carried, not filled."""
    _serve(monkeypatch, _Resp(_sse_payload([_SSE_ROW])))
    out = em.fetch_sse_margin_trading(TD)
    row = out.to_dicts()[0]
    assert row["symbol"] == "600000.SH"
    assert row["margin_balance"] == 1452735156
    assert row["margin_buy"] == 49295712
    assert row["short_sell_volume"] == 2274700
    assert row["short_balance"] is None


def test_sse_fills_short_balance_if_the_publisher_ever_does(monkeypatch):
    """Parsed from the real field rather than hardcoded to null."""
    _serve(monkeypatch, _Resp(_sse_payload([{**_SSE_ROW, "rqylje": 12345.0}])))
    assert em.fetch_sse_margin_trading(TD).to_dicts()[0]["short_balance"] == 12345.0


def test_a_truncated_sse_page_is_not_written_as_a_day(monkeypatch):
    """A short page would read downstream as securities leaving the list."""
    _serve(monkeypatch, _Resp(_sse_payload([_SSE_ROW], total=1999)))
    assert em.fetch_sse_margin_trading(TD).is_empty()


@pytest.mark.parametrize("second_page", ["complete", "repeated", "empty", "changed_total", "error"])
def test_sse_margin_paginates_without_accepting_partial_days(monkeypatch, second_page):
    from urllib.parse import parse_qs, urlparse

    calls = []
    first = {**_SSE_ROW, "stockCode": "600000"}
    second = {**_SSE_ROW, "stockCode": "600001"}
    last = {**_SSE_ROW, "stockCode": "600002"}

    def get(url, **kwargs):
        query = parse_qs(urlparse(url).query)
        calls.append(query)
        if len(calls) == 1:
            # A server may cap a page below the requested size.
            return _Resp({"pageHelp": {"data": [first, second], "total": 3, "pageSize": 2}})
        if second_page == "error":
            raise OSError("connection reset on second page")
        rows = {
            "complete": [last],
            "repeated": [first, second],
            "empty": [],
            "changed_total": [last],
        }[second_page]
        return _Resp(_sse_payload(rows, total=4 if second_page == "changed_total" else 3))

    monkeypatch.setattr(em, "_client", lambda: type("C", (), {"get": staticmethod(get)}))
    out = em.fetch_sse_margin_trading(TD)
    assert len(calls) == 2
    assert calls[1]["pageHelp.pageNo"] == ["2"]
    assert calls[1]["pageHelp.pageSize"] == ["2"]
    assert out.height == (3 if second_page == "complete" else 0)


def test_sse_skips_codes_outside_the_lake_universe(monkeypatch):
    _serve(monkeypatch, _Resp(_sse_payload([{**_SSE_ROW, "stockCode": "900902"}])))
    assert em.fetch_sse_margin_trading(TD).is_empty()


@pytest.mark.parametrize("failure", ["error", "repeated", "changed_total", "empty", "oversized"])
def test_command_retains_valid_sse_pages_before_a_pagination_failure(
    tmp_path, monkeypatch, failure
):
    from cnequity.orchestrator.source_gaps import source_gap_scope

    calls = []

    def get(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            return _Resp(_sse_payload([_SSE_ROW], total=2))
        if failure == "error":
            raise OSError("connection reset")
        rows = {
            "repeated": [_SSE_ROW],
            "changed_total": [{**_SSE_ROW, "stockCode": "600001"}],
            "empty": [],
            "oversized": [
                {**_SSE_ROW, "stockCode": "600001"},
                {**_SSE_ROW, "stockCode": "600002"},
            ],
        }[failure]
        return _Resp(_sse_payload(rows, total=3 if failure == "changed_total" else 2))

    monkeypatch.setattr(em, "_client", lambda: type("C", (), {"get": staticmethod(get)}))
    with source_gap_scope(_cfg(tmp_path)) as gaps:
        frame = em.fetch_sse_margin_trading(TD)
    assert frame.get_column("symbol").to_list() == ["600000.SH"]
    assert frame.get_column("margin_balance").to_list() == [float(_SSE_ROW["rzye"])]
    assert gaps[0]["dates"] == [TD.isoformat()]


def _szse_workbook(rows):
    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False)
    return buf.getvalue()


_SZSE_ROW = {
    "证券代码": "000001",
    "证券简称": "平安银行",
    "融资买入额(元)": "90,795,122",
    "融资余额(元)": "4,651,849,607",
    "融券卖出量(股/份)": "684,865",
    "融券余量(股/份)": "6,827,165",
    "融券余额(元)": "79,126,842",
    "融资融券余额(元)": "4,730,976,449",
}


def test_szse_margin_parses_raw_units_and_separators(monkeypatch):
    """The xlsx export states 元 and 股 directly — unlike its JSON twin."""
    _serve(monkeypatch, _Resp(content=_szse_workbook([_SZSE_ROW])))
    row = em.fetch_szse_margin_trading(TD).to_dicts()[0]
    assert row["symbol"] == "000001.SZ"
    assert row["margin_balance"] == pytest.approx(4_651_849_607.0)
    assert row["margin_buy"] == pytest.approx(90_795_122.0)
    assert row["short_balance"] == pytest.approx(79_126_842.0)
    assert row["short_sell_volume"] == pytest.approx(684_865.0)


def test_a_header_only_szse_export_means_not_published_yet(monkeypatch):
    """How SZSE represents a session it has not compiled — seen for 2026-08-28."""
    _serve(monkeypatch, _Resp(content=_szse_workbook([])))
    assert em.fetch_szse_margin_trading(TD).is_empty()


def test_a_renamed_szse_column_is_reported_not_guessed(monkeypatch):
    _serve(monkeypatch, _Resp(content=_szse_workbook([{"证券代码": "000001"}])))
    assert em.fetch_szse_margin_trading(TD).is_empty()


def test_combined_result_names_the_exchange_that_published(monkeypatch):
    monkeypatch.setattr(em, "fetch_sse_margin_trading", lambda *a, **k: em._EMPTY_MARGIN.clone())
    monkeypatch.setattr(
        em,
        "fetch_szse_margin_trading",
        lambda *a, **k: em._finish(
            [{"symbol": "000001.SZ", "trade_date": TD, "margin_balance": 1.0}]
        ),
    )
    result = em.fetch_exchange_margin_trading(TD)
    assert result.covered == frozenset({"szse"})
    assert result.failures == {"sse": "no usable rows"}


def test_combined_exchange_does_not_hide_a_gap_ledger_storage_failure(tmp_path, monkeypatch):
    from cnequity.orchestrator.source_gaps import source_gap_scope
    from cnequity.storage.state import StateStore

    responses = iter([_Resp(_sse_payload([_SSE_ROW], total=2)), OSError("connection reset")])

    def get(*args, **kwargs):
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return response

    def fail_ledger(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(em, "_client", lambda: type("C", (), {"get": staticmethod(get)}))
    monkeypatch.setattr(StateStore, "record_missing_dates", fail_ledger)
    with source_gap_scope(_cfg(tmp_path)), pytest.raises(OSError, match="disk full"):
        em.fetch_exchange_margin_trading(TD)


# --- step wiring -------------------------------------------------------------


def _cfg(tmp_path, **kw) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    for key, value in kw.items():
        setattr(cfg, key, value)
    return cfg


def test_a_half_published_session_retains_rows_and_records_the_gap(tmp_path, monkeypatch):
    """Command scope retains SH facts; strict callers reject incomplete coverage."""
    import cnequity.adapters.exchange.margin_trading as adapter
    from cnequity.orchestrator.outcomes import SourceUnavailableError
    from cnequity.orchestrator.source_gaps import source_gap_scope

    monkeypatch.setattr(
        adapter,
        "fetch_exchange_margin_trading",
        lambda *a, **k: adapter.ExchangeMarginResult(
            rows=adapter._finish([{"symbol": "600000.SH", "trade_date": TD}]),
            covered=frozenset({"sse"}),
            failures={"szse": "no usable rows"},
        ),
    )
    cfg = _cfg(tmp_path)
    with pytest.raises(SourceUnavailableError, match="missing publisher.*szse"):
        _fetch_margin_via_exchange(TD, config=cfg)
    with source_gap_scope(cfg) as gaps:
        assert _fetch_margin_via_exchange(TD, config=cfg).height == 1
    assert gaps[0]["dates"] == [TD.isoformat()]


def test_a_fully_published_session_is_returned(tmp_path, monkeypatch):
    import cnequity.adapters.exchange.margin_trading as adapter

    rows = adapter._finish(
        [
            {"symbol": "600000.SH", "trade_date": TD, "margin_balance": 1.0},
            {"symbol": "000001.SZ", "trade_date": TD, "margin_balance": 2.0},
        ]
    )
    monkeypatch.setattr(
        adapter,
        "fetch_exchange_margin_trading",
        lambda *a, **k: adapter.ExchangeMarginResult(
            rows=rows, covered=frozenset({"sse", "szse"}), failures={}
        ),
    )
    assert _fetch_margin_via_exchange(TD, config=_cfg(tmp_path)).height == 2


@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("job", ["backfill", "daily"])
@pytest.mark.parametrize("source", ["exchange", "eastmoney"])
def test_partial_exchange_publishes_and_the_retry_fetches_the_missing_exchange(
    tmp_path, monkeypatch, workers, job, source
):
    from cnequity.orchestrator.engine import JobEngine
    from cnequity.storage.revisions import RevisionStore
    from cnequity.storage.state import StateStore

    cfg = _cfg(tmp_path)
    cfg.margin_trading_source = source
    set_backfill(cfg, start=TD, end=TD, workers=workers)
    calls = []

    def fetch(day, **kwargs):
        calls.append(day)
        rows = [
            {"symbol": f"{600000 + i:06d}.SH", "trade_date": day, "margin_balance": 1.0}
            for i in range(60)
        ]
        if len(calls) > 1:
            rows += [
                {"symbol": f"{i + 1:06d}.SZ", "trade_date": day, "margin_balance": 2.0}
                for i in range(60)
            ]
        return em.ExchangeMarginResult(
            rows=em._finish(
                [
                    {"margin_buy": 0.0, "short_balance": None, "short_sell_volume": 0.0, **row}
                    for row in rows
                ]
            ),
            covered=frozenset({"sse"}) if len(calls) == 1 else frozenset({"sse", "szse"}),
            failures={"szse": "connection reset"} if len(calls) == 1 else {},
        )

    monkeypatch.setattr(em, "fetch_exchange_margin_trading", fetch)
    if source == "eastmoney":
        monkeypatch.setattr(
            "cnequity.steps.capital.fetch_margin_trading", lambda day, **kwargs: fetch(day).rows
        )
        monkeypatch.setattr(
            "cnequity.adapters.eastmoney.em_auth.EastMoneyClient",
            lambda **kwargs: type("Client", (), {"close": lambda self: None})(),
        )
    if job == "daily":
        monkeypatch.setattr("cnequity.steps.common.incremental_trade_dates", lambda *a, **k: [TD])
    first = JobEngine(cfg).run_job(
        job, TD, steps=["margin_trading", "compact"], backfill=job == "backfill"
    )
    assert first["execution_status"] == "completed"
    assert first["coverage_status"] == "partial"
    assert first["publication_status"] == "partial"
    assert first["usable_result"] is True
    assert TD in StateStore(cfg.meta_root).get_missing_dates("margin_trading")
    state = StateStore(cfg.meta_root).get_payload("margin_trading")
    assert state["coverage_status"] == "incomplete"
    assert not state.get("complete_through") or state["complete_through"] < TD.isoformat()
    root = RevisionStore(cfg.meta_root, cfg.curated_root).current_root("margin_trading")
    assert pl.read_parquet(list(root.rglob("*.parquet"))).height == 60

    again = JobEngine(cfg).run_job(
        job, TD, steps=["margin_trading", "compact"], backfill=job == "backfill"
    )
    assert len(calls) == 2
    assert again["status"] == "success", again
    assert TD not in StateStore(cfg.meta_root).get_missing_dates("margin_trading")
    root = RevisionStore(cfg.meta_root, cfg.curated_root).current_root("margin_trading")
    assert pl.read_parquet(list(root.rglob("*.parquet"))).height == 120


def test_the_exchange_is_the_default_owner(tmp_path):
    assert _margin_source(_cfg(tmp_path)) == "exchange"


def test_a_disabled_source_is_refused_rather_than_swapped(tmp_path):
    """Choosing who owns these rows is an operator's call, not a fallback."""
    cfg = _cfg(tmp_path, sources={"exchange": False})
    with pytest.raises(RuntimeError, match="exchange source disabled"):
        _margin_source(cfg)


def test_an_unknown_source_is_refused(tmp_path):
    cfg = _cfg(tmp_path, margin_trading_source="tushare")
    with pytest.raises(RuntimeError, match="unknown source"):
        _margin_source(cfg)


def test_the_vendor_path_stays_selectable(tmp_path):
    assert _margin_source(_cfg(tmp_path, margin_trading_source="eastmoney")) == "eastmoney"


def test_the_backfill_stamps_the_selected_source(tmp_path, monkeypatch):
    """Provenance follows the fetcher; a lake must never mislabel who wrote it."""
    import cnequity.adapters.exchange.margin_trading as adapter
    from cnequity.steps.capital import _backfill_margin_trading

    cfg = _cfg(tmp_path)
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 2))
    monkeypatch.setattr(cfg, "rate_limit", lambda source: None)
    monkeypatch.setattr(
        adapter,
        "fetch_exchange_margin_trading",
        lambda d, **k: adapter.ExchangeMarginResult(
            rows=adapter._finish(
                [
                    {
                        "symbol": f"{600000 + i:06d}.SH" if i % 2 == 0 else f"{i:06d}.SZ",
                        "trade_date": d,
                        "margin_balance": 1.0,
                        "margin_buy": 1.0,
                        "short_balance": None,
                        "short_sell_volume": 0.0,
                    }
                    for i in range(60)
                ]
            ),
            covered=frozenset({"sse", "szse"}),
            failures={},
        ),
    )
    out = _backfill_margin_trading(cfg, date(2026, 7, 1), "run-x")
    assert out["days_fetched"] == 2
    staged = list((cfg.staging_root / "margin_trading").glob("**/*.parquet"))
    assert set(pl.read_parquet(staged[0])["source"]) == {"exchange"}


def test_the_grace_window_follows_the_publisher(tmp_path, monkeypatch):
    """A day still inside the publisher's lag is pending; past it, an error.

    The vendor answers same-day, so two days is right there. The exchanges add
    a business day for SZSE, which a weekend stretches to three — a step that
    errored after two would fail every Monday for no reason.
    """
    from cnequity.steps import capital as cap
    from cnequity.storage.state import StateStore

    cfg = _cfg(tmp_path)
    cfg.staging_root.mkdir(parents=True, exist_ok=True)
    StateStore(cfg.meta_root).set_date("margin_trading", date(2026, 6, 25))
    monkeypatch.setattr(cap, "shanghai_now", lambda: __import__("datetime").datetime(2026, 6, 30))
    monkeypatch.setattr(cap, "_fetch_margin_via_exchange", lambda d, *, config: pl.DataFrame())

    # Friday 2026-06-26 is four days back from Tuesday: inside the exchange
    # grace window, so the day stays pending rather than failing the run.
    assert cap.step_margin_trading(cfg, date(2026, 6, 26), "run-a", {})["rows_written"] == 0

    # The vendor path has no such lag, so the same day is an error there.
    cfg.margin_trading_source = "eastmoney"
    monkeypatch.setattr(cap, "fetch_margin_trading", lambda d, **k: pl.DataFrame())
    with pytest.raises(RuntimeError, match="no rows returned"):
        cap.step_margin_trading(cfg, date(2026, 6, 26), "run-b", {})


def test_complete_session_covers_every_exchange_the_adapter_asks(monkeypatch):
    """Completeness comes from the adapter's own exchange list, not a caller literal."""
    import cnequity.adapters.exchange.margin_trading as adapter

    row = {"symbol": "600000.SH", "trade_date": TD, "margin_balance": 1.0}
    for exchange in adapter.EXCHANGES:
        monkeypatch.setattr(
            adapter, f"fetch_{exchange}_margin_trading", lambda *a, **k: adapter._finish([row])
        )
    result = adapter.fetch_exchange_margin_trading(TD)
    assert result.covered == frozenset(adapter.EXCHANGES)
    assert result.missing == frozenset()
    partial = adapter.ExchangeMarginResult(rows=result.rows, covered=frozenset(), failures={})
    assert partial.missing == frozenset(adapter.EXCHANGES)
