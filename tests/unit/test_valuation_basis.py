from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import SchemaValidationError, validate_dataframe
from cnequity.domain.valuation import (
    FLOAT_MV_TURN_IMPLIED_REPAIRED,
    FLOAT_MV_VWAP_TURN_IMPLIED,
    MV_VENDOR_REPORTED,
    TOTAL_MV_SHARE_STRUCTURE,
    TOTAL_MV_YEAR_END_ESTIMATE,
    reconstruct_total_mv,
)
from cnequity.query import load
from cnequity.storage.repairs.valuation_basis import repair_valuation_basis
from cnequity.storage.revisions import RevisionStore

DAY = date(2024, 3, 1)
FETCHED = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _valuation(**overrides) -> dict:
    row = {
        "symbol": "600519.SH",
        "trade_date": DAY,
        "pe_ttm": 30.0,
        "pb": 9.0,
        "ps_ttm": 12.0,
        "total_mv": 2.0e12,
        "float_mv": 1.9e12,
        "source": "baostock",
        "data_version": "v1",
        "fetched_at": FETCHED,
    }
    row.update(overrides)
    return row


def _write(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


@pytest.fixture(params=["day", "month"])
def lake(tmp_path, request):
    bar_partition = DAY.isoformat() if request.param == "day" else f"{DAY:%Y-%m}"
    cfg = Config(data_root=tmp_path)
    curated = cfg.curated_root
    # Legacy layout: no basis columns at all, as written before 2026-09.
    _write(
        curated / "valuation_metrics" / f"trade_date={DAY}" / "part-merged.parquet",
        [
            _valuation(),  # bar consistent, share history present
            _valuation(symbol="000001.SZ", total_mv=2.0e11, float_mv=1.8e11),  # bar out of range
            _valuation(symbol="300750.SZ", total_mv=8.0e11, float_mv=7.0e11),  # no share history
            _valuation(symbol="601318.SH", pe_ttm=8.5, source="eastmoney"),
        ],
    )
    bars = [
        # VWAP 100 inside 99..103, close 102: float_mv scales by 1.02.
        {
            "symbol": "600519.SH",
            "close": 102.0,
            "low": 99.0,
            "high": 103.0,
            "volume": 1e4,
            "amount": 1e6,
        },
        # VWAP 1000 far outside 9..11: the bar's volume/amount basis is wrong.
        {
            "symbol": "000001.SZ",
            "close": 10.0,
            "low": 9.0,
            "high": 11.0,
            "volume": 1e3,
            "amount": 1e6,
        },
        {
            "symbol": "300750.SZ",
            "close": 200.0,
            "low": 190.0,
            "high": 210.0,
            "volume": 1e4,
            "amount": 2e6,
        },
    ]
    _write(
        # daily_bars is month-partitioned on live lakes and day-partitioned on
        # unmigrated ones; the repair must find its bars in either.
        curated / "daily_bars" / f"trade_date={bar_partition}" / "part-merged.parquet",
        [
            {
                **bar,
                "trade_date": DAY,
                "open": bar["close"],
                "source": "tdx_protocol",
                "data_version": "v2",
                "fetched_at": FETCHED,
            }
            for bar in bars
        ],
    )
    _write(
        curated / "share_structure" / "change_date=2023" / "part-merged.parquet",
        [
            {
                "symbol": "600519.SH",
                "change_date": date(2023, 12, 31),
                "total_shares": 1.0e9,
                "float_shares": 1.0e9,
                "restricted_shares": 0.0,
                "free_float_shares": 1.0e9,
                "change_reason": "年报",
                "announce_date": date(2023, 12, 20),
                "source": "eastmoney",
                "data_version": "v1",
                "fetched_at": FETCHED,
            },
        ],
    )
    store = RevisionStore(cfg.meta_root, cfg.curated_root)
    for dataset in ("valuation_metrics", "daily_bars", "share_structure"):
        store.commit(
            dataset,
            run_id="seed",
            changed_files=sorted((curated / dataset).rglob("*.parquet")),
            schema_version=1,
            contract_fingerprint="contract",
        )
    return cfg


def test_repair_labels_every_value_and_rebuilds_only_what_evidence_supports(lake):
    plan = repair_valuation_basis(lake)
    assert plan["applied"] is False
    assert plan["partitions_changed"] == 1
    assert plan["counts"]["pe_moved_to_dynamic"] == 1
    # Every fixture row gains a basis label, so all of them change; `rows` is what was read.
    assert 0 < plan["rows_changed"] <= plan["rows"]

    report = repair_valuation_basis(lake, apply=True)
    assert report["applied"] is True
    rows = {r["symbol"]: r for r in load("valuation_metrics", config=lake).iter_rows(named=True)}

    moutai = rows["600519.SH"]
    assert moutai["float_mv"] == pytest.approx(1.9e12 * 102.0 / 100.0)
    assert moutai["float_mv_basis"] == FLOAT_MV_TURN_IMPLIED_REPAIRED
    assert moutai["total_mv"] == pytest.approx(102.0 * 1.0e9)
    assert moutai["total_mv_basis"] == TOTAL_MV_SHARE_STRUCTURE
    assert moutai["shares_as_of"] == date(2023, 12, 31)

    inconsistent = rows["000001.SZ"]
    assert inconsistent["float_mv"] == pytest.approx(1.8e11)
    assert inconsistent["float_mv_basis"] == FLOAT_MV_VWAP_TURN_IMPLIED

    unsupported = rows["300750.SZ"]
    assert unsupported["total_mv"] == pytest.approx(8.0e11)
    assert unsupported["total_mv_basis"] == TOTAL_MV_YEAR_END_ESTIMATE

    push2 = rows["601318.SH"]
    assert push2["pe_ttm"] is None
    assert push2["pe_dynamic"] == 8.5
    assert push2["total_mv_basis"] == MV_VENDOR_REPORTED

    # Idempotent: nothing left to change, and the old revision stays readable.
    again = repair_valuation_basis(lake)
    assert again["partitions_changed"] == 0 and again["rows_changed"] == 0
    old = load("valuation_metrics", config=lake, revision=1)
    assert old.filter(pl.col("symbol") == "601318.SH")["pe_ttm"].item() == 8.5


def test_vendor_rows_never_receive_derived_market_caps():
    frame = pl.DataFrame(
        [
            {
                "symbol": "A",
                "trade_date": DAY,
                "close": 10.0,
                "total_mv": None,
                "total_mv_basis": None,
                "shares_as_of": None,
                "source": "eastmoney",
            },
            {
                "symbol": "A",
                "trade_date": DAY,
                "close": 10.0,
                "total_mv": None,
                "total_mv_basis": None,
                "shares_as_of": None,
                "source": "baostock",
            },
        ],
        schema_overrides={
            "total_mv": pl.Float64,
            "total_mv_basis": pl.Utf8,
            "shares_as_of": pl.Date,
        },
    )
    shares = pl.DataFrame(
        {"symbol": ["A"], "change_date": [date(2020, 1, 1)], "total_shares": [5.0]}
    )
    out = reconstruct_total_mv(frame, shares)
    assert out["total_mv"].to_list() == [None, 50.0]


def test_unknown_basis_label_is_rejected():
    frame = pl.DataFrame([_valuation(total_mv_basis="guess")])
    with pytest.raises(SchemaValidationError, match="unknown total_mv_basis"):
        validate_dataframe(frame, "valuation_metrics")


def test_labelled_vwap_values_convert_once_a_consistent_bar_exists():
    from cnequity.domain.valuation import repair_legacy_float_mv

    frame = pl.DataFrame(
        [
            {
                "source": "baostock",
                "float_mv": 100.0,
                "float_mv_basis": FLOAT_MV_VWAP_TURN_IMPLIED,
                "close": 11.0,
                "low": 9.0,
                "high": 12.0,
                "volume": 10.0,
                "amount": 100.0,
            },
        ]
    )
    out = repair_legacy_float_mv(frame)
    assert out["float_mv"].item() == pytest.approx(110.0)
    assert out["float_mv_basis"].item() == FLOAT_MV_TURN_IMPLIED_REPAIRED


def test_share_structure_totals_follow_corrected_share_history():
    frame = pl.DataFrame(
        [
            {
                "symbol": "A",
                "trade_date": DAY,
                "close": 10.0,
                "total_mv": 50.0,
                "total_mv_basis": TOTAL_MV_SHARE_STRUCTURE,
                "shares_as_of": date(2020, 1, 1),
                "source": "baostock",
            }
        ]
    )
    shares = pl.DataFrame(
        {
            "symbol": ["A", "A"],
            "change_date": [date(2020, 1, 1), date(2024, 1, 1)],
            "total_shares": [5.0, 8.0],
        }
    )
    out = reconstruct_total_mv(frame, shares)
    assert out["total_mv"].item() == 80.0
    assert out["shares_as_of"].item() == date(2024, 1, 1)
