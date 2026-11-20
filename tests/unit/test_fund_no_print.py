import json
from datetime import date
from io import BytesIO

import pandas as pd
import polars as pl
import pytest

from cnequity.adapters.exchange.fund_quotes import (
    SSE_SQL_ID,
    SSE_URL,
    URL,
    no_print_symbols,
    parameters,
    sse_no_print_symbols,
    sse_parameters,
)
from cnequity.config import Config
from cnequity.steps.common import classify_daily_bar_ownership
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.fund_no_print import approved_no_print_keys, reconcile_no_print_coverage
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.repairs.fund_no_print import repair_fund_no_print
from cnequity.storage.state import StateStore

DAY = date(2026, 9, 28)
SYMBOL = "160515.SZ"


def workbook(**changes):
    row = {
        "交易日期": str(DAY),
        "证券代码": "160515",
        "前收": "1.0360",
        "今收": "1.0360",
        "成交金额(元)": "0.000",
    }
    row.update(changes)
    out = BytesIO()
    pd.DataFrame([row]).to_excel(out, index=False)
    return out.getvalue()


@pytest.mark.parametrize("amount", ["0.000", "0", "0.0000"])
def test_explicit_yuan_zero_proves_no_print(amount):
    assert no_print_symbols(workbook(**{"成交金额(元)": amount}), DAY) == {SYMBOL}


@pytest.mark.parametrize(
    "changes",
    [
        {"成交金额(元)": "0.001"},
        {"成交金额(元)": "-"},
        {"成交金额(元)": None},
        {"成交金额(元)": "NaN"},
        {"成交金额(元)": "-1"},
        {"今收": "1.035"},
        {"前收": "0", "今收": "0"},
        {"证券代码": "000001"},
    ],
)
def test_unusable_or_positive_amount_cannot_prove_absence(changes):
    assert not no_print_symbols(workbook(**changes), DAY)


def test_wrong_session_and_duplicate_identity_rejected():
    with pytest.raises(ValueError, match="different session"):
        no_print_symbols(workbook(**{"交易日期": "2026-09-29"}), DAY)
    rows = pd.read_excel(BytesIO(workbook()), dtype=str)
    out = BytesIO()
    pd.concat([rows, rows]).to_excel(out, index=False)
    with pytest.raises(ValueError, match="duplicate"):
        no_print_symbols(out.getvalue(), DAY)


SH_SYMBOL = "501071.SH"


def sse_report(**changes):
    row = {
        "SEC_CODE": "501071",
        "TX_DATE": DAY.strftime("%Y%m%d"),
        "DATE_TYPE": "D",
        "OPEN_PRICE": "0",
        "HIGH_PRICE": "0.0",
        "LOW_PRICE": "0.0",
        "CLOSE_PRICE": "0.935",
        "TRADE_AMT": "0.0",
        "TRADE_VOL": "0.0",
        "CHANGE_PRICE": "0.0",
        "ACCU_TRADE_DAYS": "0",
    }
    row.update(changes)
    return json.dumps({"sqlId": SSE_SQL_ID, "result": [row]}).encode()


def test_sse_explicit_absent_auction_prices_prove_no_print():
    assert sse_no_print_symbols(sse_report(), DAY, SH_SYMBOL) == {SH_SYMBOL}


@pytest.mark.parametrize(
    "changes",
    [
        {"OPEN_PRICE": "0.935"},
        {"HIGH_PRICE": "0.935"},
        {"LOW_PRICE": "0.935"},
        {"TRADE_AMT": "-0.001"},
        {"TRADE_VOL": "-0.0001"},
        {"CLOSE_PRICE": "0"},
        {"TRADE_VOL": None},
        {"CHANGE_PRICE": "NaN"},
    ],
)
def test_sse_rounded_zeros_or_missing_values_do_not_prove_no_print(changes):
    assert not sse_no_print_symbols(sse_report(**changes), DAY, SH_SYMBOL)


def test_sse_market_totals_do_not_supply_missing_auction_prices():
    assert sse_no_print_symbols(sse_report(TRADE_AMT="0.01", TRADE_VOL="0.01"), DAY, SH_SYMBOL) == {
        SH_SYMBOL
    }
    assert not sse_no_print_symbols(
        sse_report(OPEN_PRICE="0.935", TRADE_AMT="0", TRADE_VOL="0"), DAY, SH_SYMBOL
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"SEC_CODE": "501072"},
        {"TX_DATE": "20260929"},
        {"DATE_TYPE": "M"},
    ],
)
def test_sse_rejects_misrouted_identity(changes):
    with pytest.raises(ValueError, match="identity"):
        sse_no_print_symbols(sse_report(**changes), DAY, SH_SYMBOL)


def test_sse_owed_key_application_is_replayable_and_retains_sz_proof(lake, monkeypatch):
    cfg, _ = lake
    monkeypatch.setattr("cnequity.steps.finalize._update_watermarks", lambda *args: None)
    repair_fund_no_print(cfg, apply=True)
    catalog = cfg.curated_root / "instruments" / "part.parquet"
    original = pl.read_parquet(catalog)
    pl.concat([original, original.with_columns(pl.lit(SH_SYMBOL).alias("symbol"))]).write_parquet(
        catalog
    )
    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars", [(SH_SYMBOL, DAY)], run_id="sh-fixture", reason="interior_gap"
    )
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["daily_bars"])
    record = archive.archive(
        "daily_bars",
        sse_report(),
        source="exchange",
        run_id="sh-fixture",
        url=SSE_URL,
        request_params=sse_parameters(SH_SYMBOL, DAY),
        payload_format="json",
    )
    write_json_atomic(
        cfg.meta_root / "source_cache" / "fund_no_print" / f"{DAY}-{SH_SYMBOL}.json",
        record.to_dict(),
    )
    assert repair_fund_no_print(cfg)["proved_keys"] == 1
    assert repair_fund_no_print(cfg, apply=True)["remaining_keys"] == 0
    assert approved_no_print_keys(cfg) == {(SYMBOL, DAY), (SH_SYMBOL, DAY)}
    cfg.meta_root.joinpath(record.payload_path).unlink()
    reconcile_no_print_coverage(cfg)
    assert StateStore(cfg.meta_root).get_outstanding_keys("daily_bars")[0]["symbol"] == SH_SYMBOL


@pytest.fixture
def lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": [SYMBOL], "asset_type": ["etf"], "list_date": [DAY], "delist_date": [None]},
        schema_overrides={"delist_date": pl.Date},
    ).write_parquet(root / "part.parquet")
    cfg.curated_root.joinpath("daily_bars").mkdir()
    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars", [(SYMBOL, DAY)], run_id="fixture", reason="interior_gap"
    )
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["daily_bars"])
    record = archive.archive(
        "daily_bars",
        workbook(),
        source="exchange",
        run_id="fixture",
        url=URL,
        request_params=parameters(DAY),
        payload_format="xlsx",
    )
    write_json_atomic(
        cfg.meta_root / "source_cache" / "fund_no_print" / f"{DAY}.json", record.to_dict()
    )
    return cfg, record


def test_preview_does_not_certify_and_apply_is_replayable(lake, monkeypatch):
    cfg, record = lake
    monkeypatch.setattr("cnequity.steps.finalize._update_watermarks", lambda *args: None)
    assert repair_fund_no_print(cfg)["proved_keys"] == 1
    assert not approved_no_print_keys(cfg)
    assert len(StateStore(cfg.meta_root).get_outstanding_keys("daily_bars")) == 1
    assert repair_fund_no_print(cfg, apply=True)["remaining_keys"] == 0
    assert approved_no_print_keys(cfg) == {(SYMBOL, DAY)}
    assert not cfg.curated_root.joinpath("trading_status").exists()
    assert not list(cfg.curated_root.joinpath("daily_bars").rglob("*.parquet"))
    assert repair_fund_no_print(cfg, apply=True)["remaining_keys"] == 0
    # Missing retained bytes revoke coverage and reopen the exact obligation.
    cfg.meta_root.joinpath(record.payload_path).unlink()
    assert not approved_no_print_keys(cfg)
    assert reconcile_no_print_coverage(cfg)["invalidated_keys"] == 1
    assert len(StateStore(cfg.meta_root).get_outstanding_keys("daily_bars")) == 1


def test_actual_trade_and_catalog_changes_override_no_print(lake, monkeypatch):
    cfg, _ = lake
    monkeypatch.setattr("cnequity.steps.finalize._update_watermarks", lambda *args: None)
    repair_fund_no_print(cfg, apply=True)
    root = cfg.curated_root / "daily_bars" / f"trade_date={DAY}"
    root.mkdir()
    pl.DataFrame(
        {"symbol": [SYMBOL], "trade_date": [DAY], "volume": [1], "amount": [1]}
    ).write_parquet(root / "part.parquet")
    assert not approved_no_print_keys(cfg)
    reconcile_no_print_coverage(cfg)
    assert not StateStore(cfg.meta_root).get_outstanding_keys("daily_bars")
    root.joinpath("part.parquet").unlink()
    catalog = cfg.curated_root / "instruments" / "part.parquet"
    pl.read_parquet(catalog).with_columns(
        pl.lit(DAY.replace(day=29)).alias("list_date")
    ).write_parquet(catalog)
    assert not approved_no_print_keys(cfg)


def test_positive_amount_also_overrides_absence_even_with_bad_zero_volume(lake, monkeypatch):
    cfg, _ = lake
    monkeypatch.setattr("cnequity.steps.finalize._update_watermarks", lambda *args: None)
    repair_fund_no_print(cfg, apply=True)
    root = cfg.curated_root / "daily_bars" / f"trade_date={DAY}"
    root.mkdir()
    pl.DataFrame(
        {"symbol": [SYMBOL], "trade_date": [DAY], "volume": [0], "amount": [10.0]}
    ).write_parquet(root / "part.parquet")
    assert not approved_no_print_keys(cfg)
    reconcile_no_print_coverage(cfg)
    # A stored bar resolves coverage; its bad quantity remains a quality issue.
    assert not StateStore(cfg.meta_root).get_payload("daily_bars")["verified_no_print_keys"]


def test_second_apply_retains_existing_selections(lake, monkeypatch):
    cfg, _ = lake
    monkeypatch.setattr("cnequity.steps.finalize._update_watermarks", lambda *args: None)
    repair_fund_no_print(cfg, apply=True)
    another = "160516.SZ"
    catalog = cfg.curated_root / "instruments" / "part.parquet"
    original = pl.read_parquet(catalog)
    pl.concat([original, original.with_columns(pl.lit(another).alias("symbol"))]).write_parquet(
        catalog
    )
    out = BytesIO()
    rows = pd.read_excel(BytesIO(workbook()), dtype=str)
    pd.concat([rows, rows.assign(证券代码="160516")]).to_excel(out, index=False)
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["daily_bars"])
    record = archive.archive(
        "daily_bars",
        out.getvalue(),
        source="exchange",
        run_id="fixture-second",
        url=URL,
        request_params=parameters(DAY),
        payload_format="xlsx",
    )
    write_json_atomic(
        cfg.meta_root / "source_cache" / "fund_no_print" / f"{DAY}.json", record.to_dict()
    )
    StateStore(cfg.meta_root).record_outstanding_keys(
        "daily_bars", [(another, DAY)], run_id="fixture-second", reason="interior_gap"
    )
    repair_fund_no_print(cfg, apply=True)
    assert approved_no_print_keys(cfg) == {(SYMBOL, DAY), (another, DAY)}


def test_bad_workbook_revokes_proof_and_reopens_debt(lake, monkeypatch):
    cfg, _ = lake
    monkeypatch.setattr("cnequity.steps.finalize._update_watermarks", lambda *args: None)
    repair_fund_no_print(cfg, apply=True)
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["daily_bars"])
    record = archive.archive(
        "daily_bars",
        b"PK\x03\x04not-a-workbook",
        source="exchange",
        run_id="fixture-broken",
        url=URL,
        request_params=parameters(DAY),
        payload_format="xlsx",
    )
    write_json_atomic(
        cfg.meta_root / "repairs" / "fund_no_print" / f"{DAY}.json",
        {
            "schema_version": 1,
            "trade_date": str(DAY),
            "symbols": [SYMBOL],
            "archive": record.to_dict(),
        },
    )
    assert not approved_no_print_keys(cfg)
    assert reconcile_no_print_coverage(cfg)["invalidated_keys"] == 1
    assert len(StateStore(cfg.meta_root).get_outstanding_keys("daily_bars")) == 1


def test_normal_tradability_is_compatible_with_no_print_but_not_stock_exemption():
    status = pl.DataFrame({"symbol": [SYMBOL], "trade_date": [DAY], "is_trading": [True]})
    args = {"trading_status": status, "trading_sessions": [DAY], "no_print_keys": {(SYMBOL, DAY)}}
    fund = classify_daily_bar_ownership([SYMBOL], {SYMBOL: (DAY, None, "etf")}, DAY, DAY, **args)
    assert fund.no_data_reasons[SYMBOL] == "exchange_fund_no_print"
    stock = classify_daily_bar_ownership([SYMBOL], {SYMBOL: (DAY, None, "stock")}, DAY, DAY, **args)
    assert not stock.expected_no_data


def test_a_fund_zero_volume_row_answers_its_session_and_a_stock_s_does_not(tmp_path):
    from cnequity.storage.fund_no_print import quiet_fund_keys

    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [SYMBOL, "600000.SH", "161834.SZ"],
            "asset_type": ["lof", "stock", "lof"],
            "list_date": [DAY, DAY, DAY.replace(day=29)],
            "delist_date": [None, None, None],
        },
        schema_overrides={"delist_date": pl.Date},
    ).write_parquet(root / "part.parquet")
    bars = cfg.curated_root / "daily_bars" / f"trade_date={DAY}"
    bars.mkdir(parents=True)
    pl.DataFrame(
        {
            # A quiet fund, a quiet stock, and a fund before it listed.
            "symbol": [SYMBOL, "600000.SH", "161834.SZ"],
            "trade_date": [DAY] * 3,
            "volume": [0, 0, 0],
            "amount": [0.0, 0.0, 0.0],
        }
    ).write_parquet(bars / "part.parquet")
    assert quiet_fund_keys(cfg) == {(SYMBOL, DAY)}
    # A row that printed is a trade, not a quiet session.
    pl.DataFrame(
        {"symbol": [SYMBOL], "trade_date": [DAY], "volume": [0], "amount": [12.5]}
    ).write_parquet(bars / "part.parquet")
    assert quiet_fund_keys(cfg) == set()
