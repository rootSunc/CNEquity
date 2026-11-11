from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.units import turnover_defect_expr
from cnequity.quality.unit_checks import daily_bars_turnover_defect_findings
from cnequity.query import load
from cnequity.run_scope import set_run_flags
from cnequity.steps import bars
from cnequity.steps.finalize import step_compact
from cnequity.storage.revisions import RevisionStore

DAY = date(2001, 1, 3)
FETCHED = datetime(2026, 7, 23, tzinfo=timezone.utc)


def _row(symbol, *, volume, amount, close=18.26, source="ths", high=18.88, low=17.9):
    return {
        "symbol": symbol,
        "trade_date": DAY,
        "open": 18.0,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "amount": amount,
        "source": source,
        "data_version": "v2",
        "fetched_at": FETCHED,
    }


def test_peer_review_corroborates_whole_bars_and_preserves_conflicts():
    stored = pl.DataFrame([_row(s, volume=5000, amount=None) for s in ["A", "B", "C", "D"]])
    fresh = pl.DataFrame(
        [
            _row(s, volume=5099, amount=93000.0, close=18.4, source="baostock")
            for s in ["A", "B", "C", "D"]
        ]
    )
    peer = pl.DataFrame(
        [
            _row("A", volume=5000, amount=93000.0, close=18.4, source="eastmoney"),
            _row("B", volume=5000, amount=93000.0, source="eastmoney"),
            _row("C", volume=5000, amount=130000.0, close=18.4, source="eastmoney"),
            _row("D", volume=5000, amount=95000.0, close=18.4, source="eastmoney"),
        ]
    )
    bao, em = bars._peer_turnover_replacements(stored, fresh, peer)
    assert bao["symbol"].to_list() == ["A"]
    assert bao["volume"].to_list() == [5099]
    assert em["symbol"].to_list() == ["B"]
    assert em["close"].to_list() == [18.26]


def test_opted_in_peer_review_publishes_matching_source_whole_row(lake, monkeypatch):
    import cnequity.adapters.baostock.delisted_bars as delisted
    import cnequity.adapters.eastmoney.bars as eastmoney
    import cnequity.adapters.exchange.turnover_prices as exchange
    import cnequity.adapters.tdx_protocol.client as tdx

    monkeypatch.setattr(
        exchange, "fetch_exchange_turnover_prices", lambda *a, **k: (pl.DataFrame(), [])
    )
    monkeypatch.setattr(tdx, "fetch_daily_bars", lambda *a, **k: pl.DataFrame())

    set_run_flags(lake, turnover_peer_review=True)
    fresh = _row("600217.SH", volume=500099, amount=9300000.0, close=18.4)
    plain = {k: v for k, v in fresh.items() if k not in {"source", "data_version", "fetched_at"}}
    monkeypatch.setattr(delisted, "fetch_delisted_bars", lambda *a, **k: ([plain], []))
    monkeypatch.setattr(
        eastmoney,
        "fetch_daily_bars",
        lambda *a, **k: pl.DataFrame(
            [
                {**plain, "volume": 500000},
            ]
        ),
    )
    result = bars.repair_daily_bar_turnover(lake, DAY, DAY, "peer-repair", None)
    assert result["context_updates"]["audit_findings"][0]["rows_replaced"] == 1
    step_compact(lake, DAY, "peer-repair", {})
    row = load("daily_bars", config=lake).filter(pl.col("symbol") == "600217.SH").row(0, named=True)
    assert row["close"] == 18.4 and row["volume"] == 500099 and row["source"] == "baostock"


def test_exchange_prices_confirm_whole_bao_bar_without_exchange_turnover():
    stored = pl.DataFrame([_row("A", volume=100, amount=None)])
    fresh = pl.DataFrame([_row("A", volume=5099, amount=93000.0, close=18.4)])
    quotes = fresh.with_columns(pl.lit(999999999).alias("volume"), pl.lit(1.0).alias("amount"))
    confirmed = bars._authority_turnover_replacements(stored, fresh, quotes)
    assert confirmed["volume"].to_list() == [5099]
    assert confirmed["amount"].to_list() == [93000.0]
    assert bars._authority_turnover_replacements(
        stored, fresh, quotes.with_columns(pl.lit(18.5).alias("close"))
    ).is_empty()


def test_native_peer_confirms_bao_without_enabling_disabled_http_sources(lake, monkeypatch):
    import cnequity.adapters.baostock.delisted_bars as delisted
    import cnequity.adapters.tdx_protocol.client as tdx

    set_run_flags(lake, turnover_peer_review=True)
    lake.sources = {"eastmoney": False, "exchange": False}
    fresh = _row("600217.SH", volume=500099, amount=9300000.0, close=18.4)
    plain = {k: v for k, v in fresh.items() if k not in {"source", "data_version", "fetched_at"}}
    monkeypatch.setattr(delisted, "fetch_delisted_bars", lambda *a, **k: ([plain], []))
    monkeypatch.setattr(
        tdx, "fetch_daily_bars", lambda *a, **k: pl.DataFrame([{**plain, "volume": 500000}])
    )
    result = bars.repair_daily_bar_turnover(lake, DAY, DAY, "native-review", None)
    assert result["context_updates"]["audit_findings"][0]["rows_tdx_confirmed"] == 1
    step_compact(lake, DAY, "native-review", {})
    row = load("daily_bars", config=lake).filter(pl.col("symbol") == "600217.SH").row(0, named=True)
    assert row["volume"] == 500099 and row["source"] == "baostock"


def test_turnover_defects_are_classified_without_flagging_block_trades():
    frame = pl.DataFrame(
        [
            _row("A", volume=100, amount=None),
            _row("B", volume=100, amount=0.0),
            _row("C", volume=15704, amount=28904000.0),  # volume in 手
            _row("D", volume=100, amount=1950.0),  # 3% above high: block trade, fine
            _row("E", volume=0, amount=0.0),  # suspension
        ],
        schema_overrides={"amount": pl.Float64},
    )
    defects = frame.select(turnover_defect_expr()).to_series().to_list()
    assert defects == ["missing_amount", "zero_amount", "unit_mismatch", None, None]


@pytest.fixture
def lake(tmp_path):
    cfg = Config(data_root=tmp_path)
    path = cfg.curated_root / f"daily_bars/trade_date={DAY}/part-merged.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            _row("600215.SH", volume=15704, amount=28904000.0),  # unit fault
            _row("600216.SH", volume=5000, amount=0.0),  # zero turnover
            _row("600217.SH", volume=5000, amount=0.0),  # baostock prices disagree
            _row("600218.SH", volume=5000, amount=91000.0),  # healthy, untouched
            _row("161816.SZ", volume=5000, amount=0.0),  # a fund: baostock cannot serve
        ],
        schema_overrides={"amount": pl.Float64},
    ).write_parquet(path)
    RevisionStore(cfg.meta_root, cfg.curated_root).commit(
        "daily_bars",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="contract",
    )
    return cfg


def test_repair_replaces_only_rows_baostock_confirms(lake, monkeypatch):
    requested = []

    def fake_fetch(symbols, start, end, *, config=None):
        requested.append(sorted(symbols))
        rows = []
        for symbol in symbols:
            close = 18.99 if symbol == "600217.SH" else 18.26
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": DAY,
                    "open": 18.0,
                    "high": 18.88,
                    "low": 17.9,
                    "close": close,
                    "volume": 1570400,
                    "amount": 28904000.0,
                }
            )
        return rows, []

    import cnequity.adapters.baostock.delisted_bars as delisted

    monkeypatch.setattr(delisted, "fetch_delisted_bars", fake_fetch)
    before = daily_bars_turnover_defect_findings(lake, DAY, full=True)
    assert {f["check"] for f in before} == {"daily_bars_zero_amount", "daily_bars_turnover_unit"}

    result = bars.repair_daily_bar_turnover(lake, DAY, DAY, "repair", None)
    assert requested == [["600215.SH", "600216.SH", "600217.SH"]]
    summary = result["context_updates"]["audit_findings"][0]
    assert summary["rows_replaced"] == 2
    assert summary["rows_disagreeing"] == 1
    # The fund is outside the repair: baostock serves stocks only.
    assert summary["defects"] == {"unit_mismatch": 1, "zero_amount": 2}

    step_compact(lake, DAY, "repair", {})
    stored = {r["symbol"]: r for r in load("daily_bars", config=lake).iter_rows(named=True)}
    assert stored["600215.SH"]["volume"] == 1570400
    assert stored["600215.SH"]["source"] == "baostock"
    assert stored["600216.SH"]["amount"] == pytest.approx(28904000.0)
    assert stored["600217.SH"]["source"] == "ths"  # disagreeing prices: kept
    assert stored["600218.SH"]["amount"] == 91000.0
    assert stored["161816.SZ"]["amount"] == 0.0


def test_access_limit_keeps_fetched_rows_and_the_next_run_skips_them(lake, monkeypatch):
    import cnequity.adapters.baostock.delisted_bars as delisted
    from cnequity.domain.http_policy import SourceCoolingDown

    calls: list[list[str]] = []

    def fake_fetch(symbols, start, end, *, config=None):
        calls.append(list(symbols))
        if symbols == ["600215.SH"]:
            return (
                [
                    {
                        "symbol": "600215.SH",
                        "trade_date": DAY,
                        "open": 18.0,
                        "high": 18.88,
                        "low": 17.9,
                        "close": 18.26,
                        "volume": 1570400,
                        "amount": 28904000.0,
                    }
                ],
                [],
            )
        raise SourceCoolingDown("今日请求已达 50000 次上限")

    monkeypatch.setattr(bars, "_TURNOVER_REPAIR_BATCH", 1)
    monkeypatch.setattr(delisted, "fetch_delisted_bars", fake_fetch)
    result = bars.repair_daily_bar_turnover(lake, DAY, DAY, "repair", None)
    assert calls == [["600215.SH"], ["600216.SH"]]
    assert result["rows_written"] > 0
    assert result["status"] == "warning"
    stopped = result["context_updates"]["audit_findings"][-1]
    assert stopped["check"] == "daily_bars_turnover_repair_stopped"

    step_compact(lake, DAY, "repair", {})
    stored = {r["symbol"]: r for r in load("daily_bars", config=lake).iter_rows(named=True)}
    assert stored["600215.SH"]["source"] == "baostock"
    assert stored["600216.SH"]["amount"] == 0.0

    again: list[list[str]] = []

    def resume(symbols, start, end, *, config=None):
        again.append(sorted(symbols))
        return [], []

    monkeypatch.setattr(delisted, "fetch_delisted_bars", resume)
    bars.repair_daily_bar_turnover(lake, DAY, DAY, "repair-2", None)
    asked = {symbol for chunk in again for symbol in chunk}
    assert "600215.SH" not in asked
    assert "600216.SH" in asked


def test_a_failed_baostock_session_loses_one_batch_not_the_run(lake, monkeypatch):
    import cnequity.adapters.baostock.delisted_bars as delisted

    def dead(symbols, start, end, *, config=None):
        raise RuntimeError("baostock login exceeded 30.0s deadline")

    monkeypatch.setattr(delisted, "fetch_delisted_bars", dead)
    result = bars.repair_daily_bar_turnover(lake, DAY, DAY, "repair", None)
    assert result["status"] == "warning"
    skipped = result["context_updates"]["audit_findings"][1]
    assert skipped["check"] == "daily_bars_turnover_repair_skipped"


def test_empty_successful_fetch_does_not_report_a_successful_repair(lake, monkeypatch):
    import cnequity.adapters.baostock.delisted_bars as delisted

    monkeypatch.setattr(delisted, "fetch_delisted_bars", lambda *args, **kwargs: ([], []))
    result = bars.repair_daily_bar_turnover(lake, DAY, DAY, "repair-empty", None)
    assert result["status"] == "warning"
    assert result["rows_read"] == 3
    assert result["rows_written"] == 0
    skipped = result["context_updates"]["audit_findings"][1]
    assert skipped["rows_unserved"] == 3
    assert skipped["rows_disagreeing"] == 0
    assert skipped["failed_symbols"] == []
