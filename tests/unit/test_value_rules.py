"""Registry value rules: one place decides what a stored value means."""

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import (
    DATASET_SCHEMAS,
    SchemaValidationError,
    derivative_bar_violations,
    validate_dataframe,
)
from cnequity.domain.value_rules import apply_value_rules
from cnequity.storage.repairs.missing_values import repair_missing_values
from cnequity.storage.revisions import RevisionStore

STAMP = datetime(2026, 11, 6, tzinfo=timezone.utc)
DAY = date(2024, 1, 2)


def _bond(**values) -> dict:
    row = {
        "symbol": "110001.SH",
        "trade_date": DAY,
        "open": 101.0,
        "high": 102.0,
        "low": 100.0,
        "close": 101.5,
        "pre_close": 101.0,
        "volume": 10,
        "amount": 1015.0,
        "source": "tushare",
        "data_version": "v1",
        "fetched_at": STAMP,
    }
    return row | values


def test_every_rule_names_a_column_its_dataset_stores():
    """A typo in a rule would silently never apply."""
    for name, spec in DATASETS.items():
        rules = spec.value_rules
        named = {
            *rules.zero_is_missing,
            *rules.no_trade_missing,
            *rules.non_null,
        }
        assert named <= set(DATASET_SCHEMAS[name]), name
        if rules.no_trade_missing:
            assert "volume" in DATASET_SCHEMAS[name], name


def test_every_dataset_with_ohlc_is_checked_as_a_bar():
    """convertible_bond_bars stored 15k impossible rows because no list named it.

    futures_continuous was the last exception, on the grounds that its rows come
    from checked futures_bars; it is now checked when written and audited too.
    """
    unchecked = sorted(
        name
        for name, schema in DATASET_SCHEMAS.items()
        if {"open", "high", "low", "close"} <= set(schema)
        and DATASETS[name].value_rules.bar_kind is None
    )
    assert not unchecked


def test_bar_kind_is_one_of_the_two_rule_sets():
    from cnequity.domain.value_rules import ValueRules

    with pytest.raises(ValueError, match="bar_kind"):
        ValueRules(bar_kind="equity")
    kinds = {name: spec.value_rules.bar_kind for name, spec in DATASETS.items()}
    assert {name for name, kind in kinds.items() if kind == "derivative"} == {
        "futures_bars",
        "option_bars",
        "futures_minute_bars",
        "futures_continuous",
    }


def _continuous(**values) -> dict:
    row = {
        "symbol": "CU",
        "series": "main",
        "trade_date": DAY,
        "contract_symbol": "CU2402.SHF",
        "open": 68000.0,
        "high": 68500.0,
        "low": 67800.0,
        "close": 68200.0,
        "settle": 68100.0,
        "volume": 1200,
        "open_interest": 54000,
        "rolled": False,
        "adj_ratio": 1.0,
        "adj_diff": 0.0,
        "roll_yield": None,
        "rule": "oi_t-1_v2",
        "source": "derived",
        "data_version": "v1",
        "fetched_at": STAMP,
    }
    return row | values


def test_futures_continuous_is_checked_with_the_futures_rules():
    good = pl.DataFrame([_continuous()])
    assert validate_dataframe(good, "futures_continuous").height == 1
    # A close-only session is a real futures row, not a violation.
    close_only = _continuous(open=None, high=None, low=None)
    assert validate_dataframe(pl.DataFrame([close_only]), "futures_continuous").height == 1
    for bad in (
        _continuous(settle=0.0),
        _continuous(high=68100.0),
        _continuous(volume=None),
        _continuous(open_interest=-1),
    ):
        with pytest.raises(SchemaValidationError, match="futures/option bar invariants"):
            validate_dataframe(pl.DataFrame([bad]), "futures_continuous")
    # It stores no pre_settle or amount, so the predicate never names them.
    predicate = derivative_bar_violations("futures_continuous")
    assert good.filter(predicate).height == 0


def test_an_untraded_bond_session_keeps_its_row_without_prices():
    frame = validate_dataframe(
        pl.DataFrame([_bond(open=0.0, high=0.0, low=0.0, close=140.49, volume=0, amount=0.0)]),
        "convertible_bond_bars",
    )
    assert frame.height == 1
    assert frame.select("open", "high", "low", "close").row(0) == (None, None, None, None)
    assert frame["pre_close"].to_list() == [101.0]


def test_a_traded_bond_keeps_its_prices_and_loses_only_zero_placeholders():
    frame = validate_dataframe(
        pl.DataFrame([_bond(), _bond(symbol="124017.SZ", open=0.0, high=0.0, low=0.0)]),
        "convertible_bond_bars",
    )
    assert frame.row(0, named=True)["close"] == 101.5
    assert frame.row(1, named=True)["open"] is None
    assert frame.row(1, named=True)["close"] == 101.5


def test_bond_bars_are_checked_as_market_bars():
    with pytest.raises(SchemaValidationError, match="market-data invariants"):
        validate_dataframe(pl.DataFrame([_bond(close=-1.0)]), "convertible_bond_bars")
    with pytest.raises(SchemaValidationError, match="market-data invariants"):
        validate_dataframe(pl.DataFrame([_bond(high=99.0)]), "convertible_bond_bars")


def test_zero_rules_respect_their_source_scope():
    frame = pl.DataFrame(
        {
            "source": ["eastmoney", "cni", "other"],
            "weight": [0.0, 0.0, 0.0],
        }
    )
    assert apply_value_rules(frame, "index_constituents")["weight"].to_list() == [
        None,
        None,
        0.0,
    ]


def test_a_zero_is_missing_but_a_negative_ratio_is_a_value():
    frame = pl.DataFrame({"pe_ttm": [0.0, -2.0, 3.0, None]})
    assert apply_value_rules(frame, "valuation_metrics")["pe_ttm"].to_list() == [
        None,
        -2.0,
        3.0,
        None,
    ]


def test_lazy_frames_get_the_same_rules():
    lazy = pl.LazyFrame({"price": [0.0, 12.5]})
    assert apply_value_rules(lazy, "popularity_ranks").collect()["price"].to_list() == [
        None,
        12.5,
    ]


def test_a_dataset_without_rules_is_untouched():
    frame = pl.DataFrame({"close": [0.0]})
    assert apply_value_rules(frame, "block_trades") is frame


def _bond_lake(tmp_path) -> tuple[Config, RevisionStore]:
    cfg = Config(data_root=tmp_path / "lake")
    path = cfg.curated_root / "convertible_bond_bars" / "trade_date=2024-01" / "part-merged.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        [_bond(), _bond(symbol="113001.SH", open=0.0, high=0.0, low=0.0, volume=0, amount=0.0)],
        schema=DATASET_SCHEMAS["convertible_bond_bars"],
    ).write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store.commit(
        "convertible_bond_bars",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="test",
    )
    return cfg, store


def test_the_migration_rewrites_stored_placeholders_once(tmp_path):
    cfg, store = _bond_lake(tmp_path)
    preview = repair_missing_values(cfg, "convertible_bond_bars")
    assert preview["rows"] == 1
    assert preview["fields"] == {"open": 1, "high": 1, "low": 1, "close": 1}

    result = repair_missing_values(cfg, "convertible_bond_bars", apply=True)
    assert result["applied"] is True
    stored = pl.read_parquet(list(store.current_root("convertible_bond_bars").rglob("*.parquet")))
    untraded = stored.filter(pl.col("symbol") == "113001.SH")
    assert untraded.select("open", "high", "low", "close").row(0) == (None, None, None, None)
    assert stored.filter(pl.col("symbol") == "110001.SH")["close"].to_list() == [101.5]
    assert repair_missing_values(cfg, "convertible_bond_bars")["rows"] == 0


def test_the_audit_names_stored_placeholders_instead_of_failing(tmp_path):
    from cnequity.quality.dataset_checks import audit_curated_dataset

    cfg, store = _bond_lake(tmp_path)
    findings = audit_curated_dataset(
        "convertible_bond_bars",
        "trade_date",
        store.current_root("convertible_bond_bars"),
        DAY,
        full=True,
    )
    by_check = {f["check"]: f for f in findings}
    assert "schema_contract" not in by_check
    assert by_check["stored_value_placeholders"]["fields"]["open"] == 1


def test_the_dashboard_judges_registry_session_datasets_per_session(tmp_path):
    from cnequity.serve.lake import LakeView

    cfg = Config(data_root=tmp_path / "lake")
    for name in ("margin_trading", "block_trades"):
        out = cfg.curated_root / name / "trade_date=2024-01"
        out.mkdir(parents=True)
        pl.DataFrame({"symbol": ["600000.SH"], "trade_date": [DAY]}).write_parquet(
            out / "part-0.parquet"
        )
    view = LakeView(cfg)
    assert view._session_days(DATASETS["margin_trading"]) == frozenset({DAY})
    assert view._session_days(DATASETS["block_trades"]) is None
