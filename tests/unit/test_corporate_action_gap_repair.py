import json
from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest
from tests.catalog_seed import commit_tree, publish_folders

from cnequity.config import Config
from cnequity.query import load
from cnequity.storage.repairs.corporate_action_gaps import repair_corporate_action_gaps
from cnequity.storage.revisions import RevisionStore

FETCHED = datetime(2026, 9, 29, tzinfo=timezone.utc)
SESSIONS = [
    date(2024, 1, 2) + timedelta(days=i)
    for i in range(30)
    if (date(2024, 1, 2) + timedelta(days=i)).weekday() < 5
]
CLOSE = 10.0
HALTED = date(2024, 1, 16)
# symbol -> (true ex-date, factor step it causes)
STEPS = {
    "600001.SH": (date(2024, 1, 15), CLOSE / (CLOSE - 0.1) - 1),  # recorded Fri 01-12
    "600002.SH": (date(2024, 1, 17), CLOSE / (CLOSE - 0.2) - 1),  # halted 01-16, unrecorded
    "600003.SH": (date(2024, 1, 19), 0.1),  # nearby row whose terms do not fit
}


def _action(symbol, ex_date, action_type, *, cash=0.0, bonus=0.0, source="tdx_protocol"):
    return {
        "symbol": symbol,
        "ex_date": ex_date,
        "action_type": action_type,
        "cash_dividend": cash,
        "bonus_ratio": bonus,
        "transfer_ratio": 0.0,
        "allotment_ratio": None,
        "allotment_price": None,
        "source": source,
        "data_version": "v1",
        "fetched_at": FETCHED,
        "payment_date": None,
        "payment_source": None,
        "split_factor": 1.0,
    }


_SCHEMA = {
    "allotment_ratio": pl.Float64,
    "allotment_price": pl.Float64,
    "payment_date": pl.Date,
    "payment_source": pl.Utf8,
}


@pytest.fixture
def lake(tmp_path) -> Config:
    cfg = Config(data_root=tmp_path)
    for day in SESSIONS:
        bars = cfg.curated_root / "daily_bars" / f"trade_date={day}"
        bars.mkdir(parents=True)
        factors = cfg.derived_root / "adj_factors" / f"trade_date={day}"
        factors.mkdir(parents=True)
        pl.DataFrame(
            [
                {
                    "symbol": s,
                    "trade_date": day,
                    "open": CLOSE,
                    "high": CLOSE,
                    "low": CLOSE,
                    "close": CLOSE,
                    "volume": 100.0,
                    "amount": CLOSE * 100,
                }
                for s in STEPS
                if not (s == "600002.SH" and day == HALTED)
            ]
        ).write_parquet(bars / "part-0.parquet")
        pl.DataFrame(
            [
                {
                    "symbol": s,
                    "trade_date": day,
                    "adjust_type": "hfq",
                    "factor": 1.0 + (step if day >= ex else 0.0),
                    "source": "sina",
                    "data_version": "v1",
                    "fetched_at": FETCHED,
                }
                for s, (ex, step) in STEPS.items()
                if not (s == "600002.SH" and day == HALTED)
            ]
        ).write_parquet(factors / "part-0.parquet")
    part = cfg.curated_root / "corporate_actions" / "ex_date=2024"
    part.mkdir(parents=True)
    pl.DataFrame(
        [
            _action("600001.SH", date(2024, 1, 12), "cash_dividend", cash=0.1),
            _action("600003.SH", date(2024, 1, 18), "bonus", bonus=0.5),
            _action("600003.SH", date(2024, 6, 3), "cash_dividend", cash=0.3),
        ],
        schema_overrides=_SCHEMA,
    ).write_parquet(part / "part-merged.parquet")
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root),
        "corporate_actions",
        run_id="seed",
        changed_files=sorted((cfg.curated_root / "corporate_actions").rglob("*.parquet")),
        schema_version=1,
        contract_fingerprint="contract",
    )
    publish_folders(cfg, "daily_bars", "adj_factors")
    evidence = cfg.meta_root / "quality" / "evidence"
    evidence.mkdir(parents=True)
    (evidence / "adj_factor_source_arbitration-20260929T000000Z.json").write_text(
        json.dumps(
            {
                "verdict_rows": [
                    {
                        "symbol": s,
                        "ex_date": ex.isoformat(),
                        "kind": "step_without_action",
                        "verdict": "action_missing",
                        "sina_step": step,
                        "bao_step": step,
                    }
                    for s, (ex, step) in STEPS.items()
                ]
            }
        )
    )
    return cfg


def _baostock(asked):
    def fetch(symbols, start, end):
        asked.append((tuple(symbols), start.year))
        rows = [
            # Baostock dates it inside the halt; it takes effect on resumption.
            _action("600002.SH", date(2024, 1, 16), "cash_dividend", cash=0.2),
            # Another event that year is not a gap and is not added.
            _action("600002.SH", date(2024, 7, 1), "cash_dividend", cash=0.4),
        ]
        frame = pl.DataFrame(rows, schema_overrides=_SCHEMA).drop(
            "source", "data_version", "fetched_at"
        )
        return frame.filter(pl.col("symbol").is_in(symbols)), []

    return fetch


def _no_notices(config, run_id, gaps):
    return {}, []


def test_plan_is_offline_and_counts_both_paths(lake):
    asked: list = []
    plan = repair_corporate_action_gaps(lake, fetch=_baostock(asked))
    assert (plan["missing"], plan["moves"], plan["unpaired"]) == (3, 1, 2)
    assert plan["applied"] is False and asked == []


def test_apply_moves_misdated_rows_and_adds_only_fitting_gaps(lake, settle):
    settle(lake)
    asked: list = []
    report = repair_corporate_action_gaps(
        lake, apply=True, fetch=_baostock(asked), reorg_fetch=_no_notices
    )
    assert report["applied"] is True
    assert report["added_rows"] == 1
    # One request per symbol-year, only for gaps nothing nearby explains.
    assert asked == [(("600002.SH", "600003.SH"), 2024)]

    rows = load("corporate_actions", config=lake).sort("symbol", "ex_date")
    keys = rows.select("symbol", "ex_date", "action_type", "source").rows()
    assert keys == [
        ("600001.SH", date(2024, 1, 15), "cash_dividend", "tdx_protocol"),
        ("600002.SH", date(2024, 1, 16), "cash_dividend", "baostock"),
        # A share-count row that cannot explain a 10% step stays where it is.
        ("600003.SH", date(2024, 1, 18), "bonus", "tdx_protocol"),
        ("600003.SH", date(2024, 6, 3), "cash_dividend", "tdx_protocol"),
    ]
    # The previous generation stays readable by revision.
    old = load("corporate_actions", config=lake, revision=1)
    assert date(2024, 1, 12) in old.get_column("ex_date").to_list()


def test_a_row_dated_inside_a_halt_is_never_moved(lake):
    # A recorded ex-date in a halt is genuine; its step lands on resumption.
    part = lake.curated_root / "corporate_actions" / "ex_date=2024"
    frame = pl.read_parquet(part / "part-merged.parquet")
    halted = frame.head(1).with_columns(
        pl.lit("600002.SH").alias("symbol"),
        pl.lit(HALTED).alias("ex_date"),
        pl.lit(0.2).alias("cash_dividend"),
    )
    pl.concat([frame, halted]).write_parquet(part / "part-merged.parquet")
    commit_tree(
        RevisionStore(lake.meta_root, lake.curated_root),
        "corporate_actions",
        run_id="halt",
        changed_files=[part / "part-merged.parquet"],
        schema_version=1,
        contract_fingerprint="contract",
    )
    plan = repair_corporate_action_gaps(lake, fetch=_baostock([]))
    assert plan["moves"] == 1  # only 600001's traded-day misdate


def test_a_restructuring_conversion_is_added_at_its_reference_price(lake, settle):
    settle(lake)
    asked_notices: list = []

    def notices(config, run_id, gaps):
        asked_notices.extend(gaps)
        # 600003's 10% step: an early estimate (7.0) and the final price (9.09).
        facts = {
            ("600003.SH", date(2024, 1, 19)): [
                {
                    "transfer_ratios": [0.5],
                    "ex_dates": [date(2024, 1, 19)],
                    "reference_prices": [7.0],
                },
                {"transfer_ratios": [], "ex_dates": [], "reference_prices": [9.09]},
            ]
        }
        return {gap: facts.get(gap, []) for gap in gaps}, []

    report = repair_corporate_action_gaps(
        lake, apply=True, fetch=_baostock([]), reorg_fetch=notices
    )
    # Only the gap Baostock's dividends could not explain asks the issuer.
    assert asked_notices == [("600003.SH", date(2024, 1, 19))]
    assert report["reorg_transfers"] == 1
    row = (
        load("corporate_actions", config=lake)
        .filter(pl.col("action_type") == "reorg_transfer")
        .row(0, named=True)
    )
    assert (row["symbol"], row["ex_date"], row["source"]) == (
        "600003.SH",
        date(2024, 1, 19),
        "cninfo",
    )
    assert row["transfer_ratio"] == 0.5 and row["reference_price"] == 9.09


def test_an_early_estimate_one_cent_off_does_not_block_the_final_price(lake, settle):
    settle(lake)
    # 600306.SH 2023 published 8.11 and then 8.14; only the final price
    # reproduces the step within cent rounding.
    step = STEPS["600003.SH"][1]
    final = round(CLOSE / (1 + step), 2)

    def notices(config, run_id, gaps):
        facts = [
            {
                "transfer_ratios": [0.85],
                "ex_dates": [date(2024, 1, 19)],
                "reference_prices": [final - 0.03, final],
            }
        ]
        return {gap: facts if gap[0] == "600003.SH" else [] for gap in gaps}, []

    report = repair_corporate_action_gaps(
        lake, apply=True, fetch=_baostock([]), reorg_fetch=notices
    )
    assert report["reorg_transfers"] == 1
    row = load("corporate_actions", config=lake).filter(pl.col("action_type") == "reorg_transfer")
    assert row["reference_price"].item() == final


def test_the_gate_blocks_moving_a_halt_dated_ex_date(lake, monkeypatch):
    # Revision 322: a correct ex-date inside a halt moved onto the resumption
    # session. Every factor check is unchanged by that move, so the planner is
    # forced into it here and only the publication gate stands in the way.
    from cnequity.storage.repairs import corporate_action_gaps as repair

    part = lake.curated_root / "corporate_actions" / "ex_date=2024"
    frame = pl.read_parquet(part / "part-merged.parquet")
    halted = frame.head(1).with_columns(
        pl.lit("600002.SH").alias("symbol"),
        pl.lit(HALTED).alias("ex_date"),
        pl.lit(0.2).alias("cash_dividend"),
    )
    pl.concat([frame, halted]).write_parquet(part / "part-merged.parquet")
    commit_tree(
        RevisionStore(lake.meta_root, lake.curated_root),
        "corporate_actions",
        run_id="halt",
        changed_files=[part / "part-merged.parquet"],
        schema_version=1,
        contract_fingerprint="contract",
    )
    bad = pl.DataFrame(
        {"symbol": ["600002.SH"], "d0": [HALTED], "step_date": [STEPS["600002.SH"][0]]}
    )
    monkeypatch.setattr(repair, "plan_moves", lambda *args, **kwargs: bad)

    with pytest.raises(RuntimeError, match="repair publication gate blocked"):
        repair_corporate_action_gaps(lake, apply=True, fetch=_baostock([]), reorg_fetch=_no_notices)
    stored = load("corporate_actions", config=lake)
    assert HALTED in stored.filter(pl.col("symbol") == "600002.SH").get_column("ex_date").to_list()
