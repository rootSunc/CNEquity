from datetime import date

import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.derive.fund_factors import fund_share_series

DAYS = [date(2026, 9, day) for day in (1, 2, 3, 4, 7)]


def _bars():
    return pl.DataFrame({"trade_date": DAYS, "close": [10.0, 10.0, 5.0, 5.0, 2.5]})


def _terms(days, *, splits=None, transfers=None, dividends=None):
    n = len(days)
    return pl.DataFrame(
        {
            "ex_date": days,
            "_split": splits or [1.0] * n,
            "_transfer": transfers or [0.0] * n,
            "_bonus": [0.0] * n,
            "_dividend": dividends or [0.0] * n,
            "_allotment": [0.0] * n,
            "_allot_cash": [0.0] * n,
        }
    )


@pytest.mark.parametrize("ratio", [2.0, 0.1])
def test_missing_split_composes_with_existing_vendor_steps(ratio):
    raw = pl.DataFrame({"trade_date": DAYS, "factor": [3.0, 3.0, 3.0, 4.5, 4.5]})
    result, events = fund_share_series(_bars(), raw, _terms([DAYS[2]], splits=[ratio]))
    assert result["factor"].to_list() == pytest.approx([3, 3, 3 * ratio, 4.5 * ratio, 4.5 * ratio])
    assert events[0]["expected_step"] == ratio


def test_existing_split_is_not_doubled_and_cash_does_not_step():
    raw = pl.DataFrame({"trade_date": DAYS, "factor": [1, 1, 2, 2, 2]})
    result, events = fund_share_series(
        _bars(), raw, _terms([DAYS[2], DAYS[3]], splits=[2, 1], dividends=[0, 1])
    )
    assert result["factor"].to_list() == [1, 1, 2, 2, 2]
    assert events == []


@pytest.mark.parametrize("vendor_day", [1, 3])
def test_adjacent_vendor_split_moves_only_with_price_evidence(vendor_day):
    raw = pl.DataFrame(
        {"trade_date": DAYS, "factor": [1.0 if n < vendor_day else 2.0 for n in range(5)]}
    )
    result, events = fund_share_series(_bars(), raw, _terms([DAYS[2]], splits=[2]))
    assert result["factor"].to_list() == [1, 1, 2, 2, 2]
    assert events[0]["relocated_from"] == str(DAYS[vendor_day])
    result, events = fund_share_series(
        _bars().with_columns(pl.lit(10.0).alias("close")), raw, _terms([DAYS[2]], splits=[2])
    )
    assert result["factor"].to_list() == raw["factor"].to_list()
    assert "reason" in events[0]


def test_split_and_transfer_inside_halt_compose_on_resumption():
    bars = _bars().filter(pl.col("trade_date").is_in([DAYS[0], DAYS[-1]]))
    raw = pl.DataFrame({"trade_date": [DAYS[0]], "factor": [1.0]})
    result, events = fund_share_series(
        bars, raw, _terms([DAYS[2], DAYS[3]], splits=[2, 1], transfers=[0, 1])
    )
    assert result["factor"].to_list() == [1, 4]
    assert events[0]["trade_date"] == str(DAYS[-1])


def test_explicit_reference_sets_restructuring_step():
    terms = _terms([DAYS[2]], transfers=[1]).with_columns(pl.lit(4.0).alias("_reference"))
    result, _ = fund_share_series(
        _bars(), pl.DataFrame({"trade_date": [DAYS[0]], "factor": [1.0]}), terms
    )
    assert result["factor"].to_list() == [1, 1, 2.5, 2.5, 2.5]


@pytest.mark.parametrize("factor", [None, 0.0, float("nan")])
def test_invalid_vendor_levels_cannot_create_guessed_factors(factor):
    raw = pl.DataFrame(
        {"trade_date": [DAYS[0]], "factor": [factor]}, schema_overrides={"factor": pl.Float64}
    )
    with pytest.raises(ValueError, match="original vendor"):
        fund_share_series(_bars(), raw, _terms([DAYS[2]], splits=[2]))


def _rows(root):
    """The rows of a published root, whether a directory or a catalog revision."""
    from cnequity.query.parquet_scan import scan_parquet_root

    return scan_parquet_root(root, committed=False).collect()


@pytest.mark.parametrize("halt_after_split", [False, True])
def test_offline_repair_is_idempotent_and_keeps_old_revision(tmp_path, halt_after_split, settle):
    from cnequity.config import Config
    from cnequity.domain.schemas import with_provenance
    from cnequity.query import load
    from cnequity.storage.repairs.fund_factors import repair_fund_factors
    from cnequity.storage.revisions import RevisionStore

    cfg = Config(data_root=tmp_path / "lake")
    frames = {
        "instruments": pl.DataFrame(
            {
                "symbol": ["510300.SH"],
                "asset_type": ["etf"],
                "name": ["Test ETF"],
                "exchange": ["SH"],
                "list_date": [date(2020, 1, 1)],
                "delist_date": [None],
            },
            schema_overrides={"delist_date": pl.Date},
        ),
        "daily_bars": pl.DataFrame(
            {
                "symbol": ["510300.SH"] * 5,
                "trade_date": DAYS,
                **{
                    col: [10.0, 10.0, 5.0, 5.0, 5.0]
                    for col in ("open", "high", "low", "close", "pre_close")
                },
                "volume": [100, 100, 100, 0 if halt_after_split else 100, 100],
                "amount": [1000.0, 1000.0, 500.0, 0.0 if halt_after_split else 500.0, 500.0],
            }
        ),
        "corporate_actions": pl.DataFrame(
            {
                "symbol": ["510300.SH"],
                "ex_date": [DAYS[2]],
                "action_type": ["unit_split"],
                "split_factor": [2.0],
                "cash_dividend": [0.0],
                "bonus_ratio": [0.0],
                "transfer_ratio": [0.0],
            }
        ),
        "adj_factors": pl.DataFrame(
            {
                "symbol": ["510300.SH"] * 5,
                "trade_date": DAYS,
                "adjust_type": ["hfq"] * 5,
                "factor": [1.0] * 5,
            }
        ),
    }
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    for dataset, frame in frames.items():
        root = (cfg.derived_root if dataset == "adj_factors" else cfg.curated_root) / dataset
        root.mkdir(parents=True)
        frame = with_provenance(
            frame, source="sina" if dataset == "adj_factors" else "tdx_protocol", data_version="v1"
        )
        paths = []
        pcol = "ex_date" if dataset == "corporate_actions" else "trade_date"
        groups = frame.partition_by(pcol, as_dict=True) if pcol in frame.columns else {None: frame}
        for key, group in groups.items():
            directory = root / f"{pcol}={key[0]}" if key is not None else root
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / "part.parquet"
            group.write_parquet(path)
            paths.append(path)
        commit_tree(
            store,
            dataset,
            run_id="seed",
            changed_files=paths,
            schema_version=1,
            contract_fingerprint="fixture",
        )
    cache = cfg.meta_root / "adj_factors_cache"
    cache.mkdir()
    pl.DataFrame({"trade_date": [DAYS[0]], "factor": [1.0]}).write_parquet(
        cache / "510300_SH_hfq.parquet"
    )
    settle(cfg)
    original = store.current_root("adj_factors")
    preview = repair_fund_factors(cfg)
    assert preview["rows"] == 3
    assert store.current_root("adj_factors") == original
    from cnequity.derive.fund_factors import apply_fund_share_factors

    tip = frames["adj_factors"].tail(1).with_columns(pl.lit("sina").alias("_vendor"))
    aligned, _ = apply_fund_share_factors(cfg, tip)
    assert aligned["factor"].to_list() == [2.0]
    result = repair_fund_factors(cfg, apply=True)
    assert result["applied"]
    current = load("adj_factors", config=cfg).sort("trade_date")
    assert current["factor"].to_list() == [1, 1, 2, 2, 2]
    assert load("adj_factors", config=cfg, revision=1)["factor"].to_list() == [1] * 5
    assert repair_fund_factors(cfg, apply=True)["rows"] == 0
