"""Factor repair is reproducible, advisory, and scoped to supported events."""

from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.derive.stock_factors import stock_event_series
from cnequity.quality.factor_policy import advisory_factor_findings
from cnequity.storage.repairs.adj_factors import repair_adj_factors
from cnequity.storage.revisions import RevisionStore

DAYS = [date(2024, 1, 2) + timedelta(days=i) for i in range(10)]


def _terms(days=(3, 7)):
    return pl.DataFrame(
        {
            "ex_date": [DAYS[i] for i in days],
            "_dividend": [1.0] * len(days),
            "_bonus": [0.0] * len(days),
            "_transfer": [0.0] * len(days),
            "_allotment": [0.0] * len(days),
            "_allot_cash": [0.0] * len(days),
            "_split": [1.0] * len(days),
        }
    )


def _series():
    bars = pl.DataFrame(
        {
            "trade_date": DAYS,
            "close": [10.0] * 10,
            "pre_close": [9.0 if i == 3 else None for i in range(10)],
        }
    )
    vendor = pl.DataFrame({"trade_date": DAYS, "factor": [2.0] * 10})
    return bars, vendor


def test_one_unproven_event_does_not_prevent_a_supported_repair():
    bars, vendor = _series()
    result, events = stock_event_series(bars, vendor, _terms(), pl.DataFrame())
    assert result["factor"].to_list() == pytest.approx([2.0] * 3 + [2.0 * 10 / 9] * 7)
    assert len([e for e in events if "correction" in e]) == 1
    assert events[1]["reason"] == "independent reference missing or disagrees"
    # Market price remained at 10: actual daily returns do not decide the factor.


def test_rebuilding_same_raw_inputs_is_idempotent():
    bars, vendor = _series()
    a, _ = stock_event_series(bars, vendor, _terms(), pl.DataFrame())
    b, _ = stock_event_series(bars, vendor, _terms(), pl.DataFrame())
    assert a.equals(b)


def test_peer_disagreement_does_not_force_a_correction():
    bars, vendor = _series()
    peer = vendor.with_columns(pl.lit(1.0).alias("factor"))
    result, events = stock_event_series(bars, vendor, _terms((3,)), peer)
    assert result.equals(vendor)
    assert "reason" in events[0]


def test_share_reform_guard_only_flags_disagreeing_steps():
    reform = [date(2006, 3, 1) + timedelta(days=i) for i in range(10)]
    bars = pl.DataFrame({"trade_date": reform, "close": [10.0] * 10, "pre_close": [9.0] * 10})
    # Vendor already steps 10/9 on day 3; day 7 has no step at all.
    vendor = pl.DataFrame({"trade_date": reform, "factor": [1.0] * 3 + [10 / 9] * 7})
    terms = _terms().with_columns(pl.Series("ex_date", [reform[3], reform[7]]))
    result, events = stock_event_series(bars, vendor, terms, pl.DataFrame())
    assert result.equals(vendor)
    assert [(e["trade_date"], e["reason"]) for e in events] == [
        (str(reform[7]), "share reform requires exceptional event evidence")
    ]


def _reform_case(vendor_step):
    reform = [date(2006, 3, 1) + timedelta(days=i) for i in range(10)]
    bars = pl.DataFrame({"trade_date": reform, "close": [10.0] * 10, "pre_close": [None] * 10})
    vendor = pl.DataFrame({"trade_date": reform, "factor": [1.0] * 5 + [vendor_step] * 5})
    # 10送3.75 on top of 10转增15: a tradable holder ends with 2.875 shares.
    terms = _terms((5,)).with_columns(
        pl.Series("ex_date", [reform[5]]), _dividend=0.0, _bonus=1.875
    )
    counts = pl.DataFrame(
        {
            "change_date": [date(2003, 7, 21), reform[4]],
            "float_a_shares": [267172500.0, 768113750.0],
        }
    )
    return bars, vendor, terms, counts


def test_float_share_count_confirms_reform_terms():
    bars, vendor, terms, counts = _reform_case(3.4375)
    result, events = stock_event_series(bars, vendor, terms, pl.DataFrame(), share_counts=counts)
    assert result["factor"].to_list() == pytest.approx([1.0] * 5 + [2.875] * 5)
    assert events[0]["evidence"] == {"share_count_ratio": pytest.approx(2.875)}
    assert events[0]["correction"] == pytest.approx(2.875 / 3.4375)


def test_float_share_count_confirms_reform_vendor_step():
    bars, vendor, terms, counts = _reform_case(2.875)
    terms = terms.with_columns(_bonus=1.5)
    result, events = stock_event_series(bars, vendor, terms, pl.DataFrame(), share_counts=counts)
    assert result.equals(vendor)
    assert "reason" not in events[0] and "confirmed" in events[0]


def test_reform_without_share_counts_stays_a_warning():
    bars, vendor, terms, _ = _reform_case(3.4375)
    result, events = stock_event_series(bars, vendor, terms, pl.DataFrame())
    assert result.equals(vendor)
    assert events[0]["reason"] == "share reform requires exceptional event evidence"


def test_no_leading_factor_one_is_invented():
    bars, vendor = _series()
    result, _ = stock_event_series(bars, vendor.tail(3), _terms(), pl.DataFrame())
    assert result["trade_date"].to_list() == DAYS[-3:]


def test_retained_independent_proof_survives_cache_expiry():
    bars, vendor = _series()
    first, events = stock_event_series(bars, vendor, _terms((3,)), pl.DataFrame())
    bars = bars.with_columns(pl.lit(None, dtype=pl.Float64).alias("pre_close"))
    rebuilt, _ = stock_event_series(
        bars, vendor, _terms((3,)), pl.DataFrame(), proofs={str(DAYS[3]): events[0]}
    )
    assert rebuilt.equals(first)
    # Changed terms cannot borrow the old proof.
    rebuilt, events = stock_event_series(
        bars,
        vendor,
        _terms((3,)).with_columns(pl.lit(2.0).alias("_dividend")),
        pl.DataFrame(),
        proofs={str(DAYS[3]): events[0]},
    )
    assert rebuilt.equals(vendor)
    assert "reason" in events[0]


def _lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake", publication_gate="block", audit_gate="block")
    bars, vendor = _series()
    stamp = datetime(2024, 1, 12, tzinfo=timezone.utc)
    bars = bars.with_columns(
        pl.lit("600001.SH").alias("symbol"),
        pl.col("close").alias("open"),
        pl.col("close").alias("high"),
        pl.col("close").alias("low"),
        pl.lit(100).alias("volume"),
        pl.lit(1000.0).alias("amount"),
        pl.lit("tdx_protocol").alias("source"),
        pl.lit("v2").alias("data_version"),
        pl.lit(stamp).alias("fetched_at"),
    )
    factors = vendor.with_columns(
        pl.lit("600001.SH").alias("symbol"),
        pl.lit("hfq").alias("adjust_type"),
        pl.lit("sina").alias("source"),
        pl.lit("v1").alias("data_version"),
        pl.lit(stamp).alias("fetched_at"),
    )
    actions = pl.DataFrame(
        {
            "symbol": ["600001.SH", "600001.SH"],
            "ex_date": [DAYS[3], DAYS[7]],
            "action_type": ["cash_dividend"] * 2,
            "cash_dividend": [1.0] * 2,
            "bonus_ratio": [0.0] * 2,
            "transfer_ratio": [0.0] * 2,
            "allotment_ratio": [0.0] * 2,
            "allotment_price": [0.0] * 2,
            "split_factor": [1.0] * 2,
            "source": ["tdx_protocol"] * 2,
            "data_version": ["v1"] * 2,
            "fetched_at": [stamp] * 2,
        }
    )
    for dataset, frame, root in (
        ("daily_bars", bars, cfg.curated_root),
        ("corporate_actions", actions, cfg.curated_root),
        ("adj_factors", factors, cfg.derived_root),
    ):
        if dataset == "adj_factors":
            for day in DAYS:
                path = root / dataset / f"trade_date={day}" / "part.parquet"
                path.parent.mkdir(parents=True, exist_ok=True)
                frame.filter(pl.col("trade_date") == day).write_parquet(path)
            continue
        path = root / dataset / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)
    raw = cfg.meta_root / "adj_factors_cache" / "600001_SH_hfq.parquet"
    raw.parent.mkdir(parents=True)
    vendor.write_parquet(raw)
    return cfg


def test_preview_apply_and_second_apply_are_reproducible(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *args: [])
    report = repair_adj_factors(cfg, symbols=["600001.SH"])
    assert report["rows"] == 7 and not report["applied"]
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root, create=False)
    assert store.current_pointer("adj_factors") is None
    applied = repair_adj_factors(cfg, symbols=["600001.SH"], apply=True, plan_id=report["plan_id"])
    assert applied["rows"] == 7 and applied["applied"]
    old_revision = store.latest("adj_factors").revision
    assert store.latest("adj_factors").metadata["factor_event_proofs"]
    assert repair_adj_factors(cfg, symbols=["600001.SH"], apply=True)["rows"] == 0
    assert store.latest("adj_factors").revision == old_revision


def test_stale_plan_cannot_change_the_lake(tmp_path):
    cfg = _lake(tmp_path)
    plan = repair_adj_factors(cfg, symbols=["600001.SH"])
    path = cfg.curated_root / "daily_bars" / "part.parquet"
    pl.read_parquet(path).with_columns(pl.lit(10.0).alias("pre_close")).write_parquet(path)
    with pytest.raises(ValueError, match="refresh the preview"):
        repair_adj_factors(cfg, symbols=["600001.SH"], apply=True, plan_id=plan["plan_id"])
    assert not RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root).current_pointer(
        "adj_factors"
    )


def test_publish_failure_preserves_source_and_generation(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    old = store.ensure_current("adj_factors")

    def fail(*args, **kwargs):
        raise RuntimeError("publication failed")

    monkeypatch.setattr("cnequity.storage.repairs.adj_factors.check_repair_publication", fail)
    with pytest.raises(RuntimeError, match="publication failed"):
        repair_adj_factors(cfg, symbols=["600001.SH"], apply=True)
    assert store.current_root("adj_factors") == old
    assert (
        pl.scan_parquet(str(cfg.derived_root / "adj_factors" / "**/*.parquet"))
        .collect()["factor"]
        .to_list()
        == [2.0] * 10
    )


def test_semantic_findings_are_advisory_but_invalid_rows_still_block():
    items = advisory_factor_findings(
        [
            {"check": "adj_factor_continuity", "severity": "error"},
            {"check": "unstepped_corporate_action_overflow", "severity": "error"},
            {"check": "invalid_values", "dataset": "adj_factors", "severity": "error"},
        ]
    )
    assert [r["severity"] for r in items] == ["warning", "warning", "error"]


def test_same_provider_event_and_factor_are_not_independent():
    bars, vendor = _series()
    bars = bars.with_columns(pl.lit(None, dtype=pl.Float64).alias("pre_close"))
    peer = vendor.with_columns(
        pl.when(pl.col("trade_date") >= DAYS[3]).then(2 * 10 / 9).otherwise(2.0).alias("factor")
    )
    terms = _terms((3,)).with_columns(pl.lit(["baostock"]).alias("_term_sources"))
    result, events = stock_event_series(bars, vendor, terms, peer)
    assert result.equals(vendor)
    assert events[0]["reason"] == "independent reference missing or disagrees"


def test_original_cache_restores_a_missing_key_without_inventing_leading_rows(
    tmp_path, monkeypatch
):
    cfg = _lake(tmp_path)
    from cnequity.storage.repairs.adj_factors import _restore_coverage

    bars = pl.read_parquet(next((cfg.curated_root / "daily_bars").rglob("*.parquet")))
    # Read the fixture's complete bar/factor series rather than relying on partition order.
    bars = pl.scan_parquet(str(cfg.curated_root / "daily_bars" / "**/*.parquet")).collect()
    factors = pl.scan_parquet(str(cfg.derived_root / "adj_factors" / "**/*.parquet")).collect()
    before = factors.filter(pl.col("trade_date") != DAYS[5])
    restored, events = _restore_coverage(cfg, before, bars, pl.DataFrame())
    assert restored.height == factors.height
    assert events[0]["restored_rows"] == 1
    assert restored.filter(pl.col("trade_date") == DAYS[5])["factor"].to_list() == [2.0]
    path = cfg.meta_root / "adj_factors_cache" / "600001_SH_hfq.parquet"
    raw = pl.read_parquet(path).filter(pl.col("trade_date") >= DAYS[3])
    raw.write_parquet(path)
    empty = factors.clear()
    restored, events = _restore_coverage(cfg, empty, bars, pl.DataFrame())
    assert restored["trade_date"].min() == DAYS[3]
    assert events[0]["missing_rows"] == 3


def test_scoped_repair_preserves_corrections_outside_event_window(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *a, **k: [])
    first = repair_adj_factors(cfg, symbols=["600001.SH"], apply=True)
    assert first["applied"]
    second = repair_adj_factors(cfg, symbols=["600001.SH"], start=DAYS[7], end=DAYS[7])
    assert second["rows"] == 0


def test_diagnostic_failure_does_not_undo_or_misreport_publication(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *a, **k: [])

    def fail(*args, **kwargs):
        raise OSError("optional observation unavailable")

    monkeypatch.setattr("cnequity.quality.factor_diagnostics.publish_factor_diagnostics", fail)
    result = repair_adj_factors(cfg, symbols=["600001.SH"], apply=True)
    assert result["applied"]
    assert result["diagnostics"]["assessed"] is False
    assert (
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
        .latest("adj_factors")
        .revision
        == result["revision"]
    )


def test_adjusted_receipt_is_available_with_unresolved_history(tmp_path):
    from cnequity.quality.factor_diagnostics import publish_factor_diagnostics
    from cnequity.query import load_with_receipt

    cfg = _lake(tmp_path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store.ensure_current("adj_factors")
    diagnostics = publish_factor_diagnostics(cfg)
    assert diagnostics["assessed"]
    result = load_with_receipt(
        "daily_bars", config=cfg, symbols=["600001.SH"], adjust="hfq", strict_adj=True
    )
    assert result.frame.height == 10
    assert result.frame["adj_is_exact"].all()
    assert result.receipt["adjustment"]["related_event_count"] == 2
    assert result.receipt["adjustment"]["gate_impact"] == "none"
    assert result.receipt["adjustment"]["factor_sources"] == {"sina": 10}


def test_partial_fund_event_repair_does_not_fabricate_leading_coverage():
    from cnequity.derive.fund_factors import fund_share_series

    bars, vendor = _series()
    terms = _terms((7,)).with_columns(pl.lit(2.0).alias("_split"))
    rebuilt, _ = fund_share_series(bars, vendor.tail(5), terms)
    assert rebuilt["trade_date"].min() == DAYS[5]


def test_refresh_failure_keeps_cached_and_other_supported_repairs_available(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)

    def unavailable(*args, **kwargs):
        raise ConnectionError("independent source unavailable")

    result = repair_adj_factors(cfg, symbols=["600001.SH"], refresh_sources=True, fetch=unavailable)
    assert result["rows"] == 7
    assert result["arbitration"]["switch"] == []


def test_explicit_scope_does_not_replace_historical_cursor(tmp_path, monkeypatch):
    import json

    from cnequity.storage.atomic import write_json_atomic

    cfg = _lake(tmp_path)
    path = cfg.meta_root / "quality" / "factor_repairs" / "cursor.json"
    cursor = {"offset": 25, "wanted": ["historical"], "roots": {}}
    write_json_atomic(path, cursor)
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *a, **k: [])
    repair_adj_factors(cfg, symbols=["600001.SH"], apply=True)
    assert json.loads(path.read_text()) == cursor


def test_old_native_cache_does_not_fill_new_sessions_beyond_the_published_tip(tmp_path):
    from cnequity.storage.repairs.adj_factors import _restore_coverage

    cfg = _lake(tmp_path)
    factors = pl.scan_parquet(str(cfg.derived_root / "adj_factors" / "**/*.parquet")).collect()
    bars = pl.scan_parquet(str(cfg.curated_root / "daily_bars" / "**/*.parquet")).collect()
    before = factors.filter(pl.col("trade_date") < DAYS[-1])
    restored, events = _restore_coverage(cfg, before, bars, pl.DataFrame())
    assert restored.height == before.height
    assert not events


def test_vendor_console_messages_do_not_pollute_json_stdout(tmp_path, capsys):
    cfg = _lake(tmp_path)

    def sdk_prints(symbols, start, end, *, config=None):
        print("login success!")
        _, vendor = _series()
        return vendor.with_columns(pl.lit(symbols[0]).alias("symbol")), []

    repair_adj_factors(cfg, symbols=["600001.SH"], refresh_sources=True, fetch=sdk_prints)
    output = capsys.readouterr()
    assert "login success!" in output.err
    assert "login success!" not in output.out


def test_later_factor_revision_retains_committed_event_proofs(tmp_path, monkeypatch):
    cfg = _lake(tmp_path)
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *a, **k: [])
    repair_adj_factors(cfg, symbols=["600001.SH"], apply=True)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    proofs = store.latest("adj_factors").metadata["factor_event_proofs"]
    path = cfg.derived_root / "adj_factors" / f"trade_date={DAYS[-1]}" / "part-0.parquet"
    files = sorted((cfg.derived_root / "adj_factors" / f"trade_date={DAYS[-1]}").glob("*.parquet"))
    path = files[0]
    pl.read_parquet(path).with_columns(
        (pl.col("fetched_at") + pl.duration(hours=1)).alias("fetched_at")
    ).write_parquet(path)
    revision = store.commit(
        "adj_factors",
        run_id="ordinary-derive",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="test",
    )
    assert revision.metadata["factor_event_proofs"] == proofs
    assert repair_adj_factors(cfg, symbols=["600001.SH"])["rows"] == 0


def test_partial_rebuild_does_not_retract_a_previously_repaired_pending_event(
    tmp_path, monkeypatch
):
    cfg = _lake(tmp_path)
    monkeypatch.setattr("cnequity.quality.publication._errors", lambda *a, **k: [])
    path = cfg.curated_root / "daily_bars" / "part.parquet"
    frame = pl.read_parquet(path)
    frame.with_columns(
        pl.when(pl.col("trade_date") == DAYS[7])
        .then(9.0)
        .otherwise(pl.col("pre_close"))
        .alias("pre_close")
    ).write_parquet(path)
    assert repair_adj_factors(cfg, symbols=["600001.SH"], apply=True)["applied"]
    # New conflicting evidence for the second event must not silently undo it
    # while the first event is rebuilt from the original vendor series.
    frame.with_columns(
        pl.when(pl.col("trade_date") == DAYS[7])
        .then(8.0)
        .otherwise(pl.col("pre_close"))
        .alias("pre_close")
    ).write_parquet(path)
    pending = repair_adj_factors(cfg, symbols=["600001.SH"])
    assert any(e.get("reason") for e in pending["events"])
    assert pending["rows"] == 0


def test_plain_adjusted_load_skips_diagnostics_and_receipt_survives_them(tmp_path, monkeypatch):
    from cnequity.query import load, load_with_receipt

    cfg = _lake(tmp_path)
    RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root).ensure_current("adj_factors")

    def fail(*args, **kwargs):
        raise ValueError("corrupt diagnostics snapshot")

    monkeypatch.setattr("cnequity.quality.factor_diagnostics.factor_read_quality", fail)
    frame = load("daily_bars", config=cfg, symbols=["600001.SH"], adjust="hfq")
    assert frame.height == 10
    result = load_with_receipt("daily_bars", config=cfg, symbols=["600001.SH"], adjust="hfq")
    assert result.frame.height == 10
    assert result.receipt["adjustment"]["diagnostics_assessed"] is False
