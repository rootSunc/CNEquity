"""Cross-section proof and per-exchange gap audit for futures/option bars."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.quality.derivative_checks import (
    derivative_findings,
    derivative_tip_scope,
    exchange_session_gaps,
)
from cnequity.storage.parquet import StagingWriter, compact_dataset
from cnequity.storage.revisions import RevisionStore


@pytest.fixture
def cfg(tmp_path) -> Config:
    config = Config(data_root=tmp_path / "data")
    config.futures_enabled = True
    config.futures_exchanges = ["CFE"]
    return config


def _bar(symbol: str, day: date) -> dict:
    return {
        "symbol": symbol,
        "exchange": "CFE",
        "exchange_code": symbol.split(".")[0],
        "product": "IF",
        "trade_date": day,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "settle": 4000.0,
        "pre_settle": 4000.0,
        "volume": 0,
        "amount": 0.0,
        "open_interest": 10,
        "oi_change": 0,
        "source": "futures_exchange",
    }


def _write(cfg: Config, dataset: str, rows: list[dict], granularity="month") -> None:
    frame = with_provenance(
        pl.DataFrame(rows, infer_schema_length=None), source="futures_exchange", data_version="v1"
    )
    frame = validate_dataframe(frame, dataset)
    StagingWriter(cfg.staging_root).write_batch(dataset, "r1", "b0", frame)
    partition = None if dataset.endswith("_contracts") else "trade_date"
    compact_dataset(
        cfg.staging_root,
        cfg.curated_root,
        dataset,
        "r1",
        partition_col=partition,
        granularity=granularity if partition else None,
    )


def test_a_live_contract_missing_from_the_tip_is_incomplete(cfg):
    _write(
        cfg,
        "futures_bars",
        [
            _bar("IF2610.CFE", date(2026, 9, 23)),
            _bar("IF2611.CFE", date(2026, 9, 23)),
            _bar("IF2610.CFE", date(2026, 9, 24)),
        ],
    )
    report = derivative_tip_scope(cfg, "futures_bars")
    assert report["state"] == "incomplete"
    assert report["missing"] == ["IF2611.CFE"]
    assert report["exchanges"] == {"CFE": [1, 2]}


def test_a_contract_past_its_last_trading_day_is_not_owed(cfg):
    _write(
        cfg,
        "futures_bars",
        [
            _bar("IF2609.CFE", date(2026, 9, 23)),
            _bar("IF2610.CFE", date(2026, 9, 23)),
            _bar("IF2610.CFE", date(2026, 9, 24)),
        ],
    )
    contract = {
        "symbol": "IF2609.CFE",
        "exchange": "CFE",
        "exchange_code": "IF2609",
        "product": "IF",
        "product_name": "沪深300股指期货",
        "delivery_month": date(2026, 9, 1),
        "list_date": date(2026, 1, 19),
        "last_trade_date": date(2026, 9, 23),
        "multiplier": 300.0,
        "tick_size": 0.2,
        "quote_unit": "点",
        "dates_basis": "exchange",
        "first_seen_date": date(2026, 1, 19),
        "last_seen_date": date(2026, 9, 23),
        "source": "futures_exchange",
    }
    _write(cfg, "futures_contracts", [contract])
    report = derivative_tip_scope(cfg, "futures_bars")
    assert report["state"] == "unverified"
    assert report["expected"] == 1


def test_a_single_session_cannot_be_proved(cfg):
    _write(cfg, "futures_bars", [_bar("IF2610.CFE", date(2026, 9, 24))])
    assert derivative_tip_scope(cfg, "futures_bars")["state"] == "unverified"


def test_disabled_family_is_not_judged(tmp_path):
    assert derivative_tip_scope(Config(data_root=tmp_path / "d"), "futures_bars") is None


def test_an_exchange_hole_inside_its_span_is_reported(cfg):
    _write(
        cfg,
        "futures_bars",
        [
            _bar("IF2610.CFE", date(2026, 9, 21)),
            _bar("IF2610.CFE", date(2026, 9, 23)),
            _bar("IF2610.CFE", date(2026, 9, 24)),
        ],
    )
    assert exchange_session_gaps(cfg, "futures_bars") == {"CFE": [date(2026, 9, 22)]}
    checks = [f["check"] for f in derivative_findings(cfg)]
    assert "futures_exchange_session_gap" in checks
    assert "derivative_contracts_missing" in checks


def test_a_session_the_exchange_never_published_is_not_a_gap(cfg):
    cfg.futures_exchanges = ["SHF"]
    rows = []
    for day in (date(2007, 6, 1), date(2007, 6, 5)):
        bar = _bar("CU0708.SHF", day)
        bar.update({"exchange": "SHF", "exchange_code": "CU0708", "product": "CU"})
        rows.append(bar)
    _write(cfg, "futures_bars", rows)
    # SHFE's archive has no 2007-06-04 file (404 while both neighbours exist).
    assert exchange_session_gaps(cfg, "futures_bars") == {}


def test_prices_off_the_tick_grid_and_a_wrong_lot_are_reported(cfg):
    traded = []
    for day, price in ((date(2026, 9, 23), 4000.1), (date(2026, 9, 24), 4000.3)):
        row = _bar("IF2610.CFE", day)
        # IF ticks by 0.2, so 4000.1 and 4000.3 are off the grid; 300 × 5 lots
        # at ~4000 is ~6,000,000, so 2,000,000 implies a lot of ~100, not 300.
        row.update(
            open=price,
            high=price,
            low=price,
            close=price,
            settle=price,
            volume=5,
            amount=2_000_000.0,
        )
        traded.append(row)
    _write(cfg, "futures_bars", traded)
    checks = {f["check"] for f in derivative_findings(cfg)}
    assert {"futures_tick_grid", "futures_multiplier_mismatch"} <= checks


def test_delivery_month_without_authoritative_expiry_does_not_excuse_missing_rows(cfg):
    _write(
        cfg,
        "futures_bars",
        [
            # IF2609 is in its delivery month on the tip and has no known end:
            # it may have stopped trading, so it is not owed.
            _bar("IF2609.CFE", date(2026, 9, 23)),
            _bar("IF2610.CFE", date(2026, 9, 23)),
            _bar("IF2610.CFE", date(2026, 9, 24)),
        ],
    )
    report = derivative_tip_scope(cfg, "futures_bars")
    assert report["state"] == "incomplete"
    assert report["expected"] == 2
    assert report["missing"] == ["IF2609.CFE"]


def test_rebuilding_contracts_cannot_turn_a_missing_row_into_proven_expiry(cfg):
    from cnequity.derive.derivative_contracts import build_futures_contracts

    rows = [
        _bar("IF2610.CFE", date(2026, 9, 23)),
        _bar("IF2611.CFE", date(2026, 9, 23)),
        _bar("IF2610.CFE", date(2026, 9, 24)),
    ]
    _write(cfg, "futures_bars", rows)
    _write(cfg, "futures_contracts", build_futures_contracts(pl.DataFrame(rows).lazy()).to_dicts())
    report = derivative_tip_scope(cfg, "futures_bars")
    assert report["state"] == "incomplete"
    assert report["missing"] == ["IF2611.CFE"]


def test_dated_reference_adds_a_new_listing_to_expected_contracts(cfg):
    _write(
        cfg,
        "futures_bars",
        [_bar("IF2610.CFE", date(2026, 9, 23)), _bar("IF2610.CFE", date(2026, 9, 24))],
    )
    path = cfg.meta_root / "derivatives" / "references" / "CFE"
    path.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["IF2611.CFE"],
            "kind": ["future"],
            "list_date": [date(2026, 9, 24)],
            "last_trade_date": [date(2026, 11, 20)],
        }
    ).write_parquet(path / "2026-09-24-future.parquet")
    report = derivative_tip_scope(cfg, "futures_bars")
    assert report["missing"] == ["IF2611.CFE"]


def test_exchange_missing_for_multiple_sessions_is_visible(cfg):
    cfg.futures_exchanges = ["CFE", "GFE"]
    rows = [_bar("IF2610.CFE", date(2026, 9, d)) for d in (21, 22, 23, 24)]
    _write(cfg, "futures_bars", rows)
    report = derivative_tip_scope(cfg, "futures_bars")
    assert report["state"] == "incomplete"
    assert report["missing_exchanges"] == ["GFE"]
    assert exchange_session_gaps(cfg, "futures_bars")["GFE"] == [
        date(2026, 9, d) for d in (21, 22, 23, 24)
    ]


def test_unpublished_mutable_rows_do_not_change_coverage_evidence(cfg):

    rows = [
        _bar("IF2610.CFE", date(2026, 9, 23)),
        _bar("IF2611.CFE", date(2026, 9, 23)),
        _bar("IF2610.CFE", date(2026, 9, 24)),
    ]
    _write(cfg, "futures_bars", rows)
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        "futures_bars",
        run_id="published",
        changed_files=sorted((cfg.curated_root / "futures_bars").rglob("*.parquet")),
        schema_version=1,
        contract_fingerprint="seed",
    )
    _write(cfg, "futures_bars", [_bar("IF2611.CFE", date(2026, 9, 24))])
    assert derivative_tip_scope(cfg, "futures_bars")["missing"] == ["IF2611.CFE"]


def test_explicit_window_reports_empty_exchange(cfg):
    assert exchange_session_gaps(
        cfg, "futures_bars", start=date(2026, 9, 23), end=date(2026, 9, 24)
    ) == {"CFE": [date(2026, 9, 23), date(2026, 9, 24)]}


def test_window_finds_historical_contract_gap_when_tip_has_recovered(cfg):
    from cnequity.quality.derivative_window import derivative_window

    _write(
        cfg,
        "futures_bars",
        [
            _bar("IF2610.CFE", date(2026, 9, 22)),
            _bar("IF2611.CFE", date(2026, 9, 22)),
            _bar("IF2610.CFE", date(2026, 9, 23)),
            _bar("IF2610.CFE", date(2026, 9, 24)),
            _bar("IF2611.CFE", date(2026, 9, 24)),
        ],
    )
    report = derivative_window(cfg, "futures_bars", date(2026, 9, 23), date(2026, 9, 24))
    assert report["state"] == "incomplete"
    assert report["exchange_gaps"] == {}
    assert report["missing_contracts"] == [
        {"date": date(2026, 9, 23), "exchange": "CFE", "symbols": ["IF2611.CFE"]}
    ]
    assert report["observed_contracts"] == 2
    assert report["missing_metadata"] == ["IF2610.CFE", "IF2611.CFE"]


def test_window_without_supported_route_never_claims_complete(cfg):
    from cnequity.quality.derivative_window import derivative_window

    cfg.futures_exchanges = ["DCE"]
    report = derivative_window(cfg, "option_bars", date(2026, 9, 23), date(2026, 9, 24))
    assert report["state"] == "unverified"
    assert any("does not supply" in reason for reason in report["unverified_reasons"])


def test_window_checks_authoritative_contract_absent_for_entire_window(cfg, monkeypatch):
    from cnequity.quality import derivative_window as module

    _write(cfg, "futures_bars", [_bar("IF2610.CFE", date(2026, 9, 24))])
    real_scan = module._scan
    metadata = pl.DataFrame(
        {
            "symbol": ["IF2611.CFE"],
            "exchange": ["CFE"],
            "dates_basis": ["exchange"],
            "list_date": [date(2026, 9, 23)],
            "last_trade_date": [date(2026, 9, 24)],
        }
    ).lazy()
    monkeypatch.setattr(
        module,
        "_scan",
        lambda config, dataset: (
            metadata if dataset == "futures_contracts" else real_scan(config, dataset)
        ),
    )
    report = module.derivative_window(cfg, "futures_bars", date(2026, 9, 23), date(2026, 9, 24))
    assert [row["symbols"] for row in report["missing_contracts"]] == [
        ["IF2611.CFE"],
        ["IF2611.CFE"],
    ]


def _reference(cfg: Config, exchange: str, day: date, symbols: list[str], kind="future") -> None:
    path = cfg.meta_root / "derivatives" / "references" / exchange
    path.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": symbols,
            "kind": [kind] * len(symbols),
            "list_date": [date(2026, 1, 19)] * len(symbols),
            "last_trade_date": [date(2026, 12, 18)] * len(symbols),
        }
    ).write_parquet(path / f"{day}-{kind}.parquet")


def test_the_days_official_list_proves_the_exchange_complete(cfg):
    _write(
        cfg,
        "futures_bars",
        [
            _bar("IF2610.CFE", date(2026, 9, 23)),
            _bar("IF2610.CFE", date(2026, 9, 24)),
            _bar("IF2611.CFE", date(2026, 9, 24)),
        ],
    )
    _reference(cfg, "CFE", date(2026, 9, 24), ["IF2610.CFE", "IF2611.CFE"])

    report = derivative_tip_scope(cfg, "futures_bars")

    assert report["state"] == "complete"
    assert (report["covered"], report["expected"]) == (2, 2)
    assert report["unproven_exchanges"] == []


def test_an_older_or_other_kind_list_does_not_prove_the_day(cfg):
    _write(
        cfg,
        "futures_bars",
        [_bar("IF2610.CFE", date(2026, 9, 23)), _bar("IF2610.CFE", date(2026, 9, 24))],
    )
    _reference(cfg, "CFE", date(2026, 9, 23), ["IF2610.CFE"])  # the day before
    _reference(cfg, "CFE", date(2026, 9, 24), ["IO2610-C-4000.CFE"], kind="option")

    report = derivative_tip_scope(cfg, "futures_bars")

    assert report["state"] == "unverified"
    assert report["unproven_exchanges"] == ["CFE"]


def test_a_vendor_route_is_a_stated_limitation_not_a_failure(cfg):
    cfg.futures_exchanges = ["CFE", "DCE"]
    cfg.futures_dce_route = "sina"
    _write(
        cfg,
        "futures_bars",
        [_bar("IF2610.CFE", date(2026, 9, 23)), _bar("IF2610.CFE", date(2026, 9, 24))],
    )
    _reference(cfg, "CFE", date(2026, 9, 24), ["IF2610.CFE"])

    report = derivative_tip_scope(cfg, "futures_bars")

    assert report["state"] == "complete"
    assert report["unverifiable_exchanges"] == ["DCE"]
