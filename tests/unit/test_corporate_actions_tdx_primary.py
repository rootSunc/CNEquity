"""TDX as the daily corporate-actions primary, with EastMoney as backup and peer."""

from __future__ import annotations

from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.steps import events
from cnequity.steps.events import (
    _peer_finding,
    _split_with_vendor,
    detect_ex_symbols,
    step_corporate_actions,
)

DAY = date(2026, 10, 27)


def _actions(rows):
    return pl.DataFrame(
        rows,
        schema={
            "symbol": pl.Utf8,
            "ex_date": pl.Date,
            "action_type": pl.Utf8,
            "cash_dividend": pl.Float64,
            "bonus_ratio": pl.Float64,
            "transfer_ratio": pl.Float64,
        },
        orient="row",
    ).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("allotment_ratio"),
        pl.lit(None, dtype=pl.Float64).alias("allotment_price"),
    )


class FakeQuotes:
    def __init__(self, last_close_raw):
        self.raw = last_close_raw

    def security_quotes(self, pairs):
        return [{"market": m, "code": c, "last_close_raw": self.raw.get(c)} for m, c in pairs]

    def close(self):
        pass


def test_detector_flags_an_adjusted_previous_close(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    monkeypatch.setattr(
        events, "_previous_closes", lambda c, d, s: {"600017.SH": 2.81, "600519.SH": 1349.98}
    )
    # 600017 pays 0.033: the exchange's previous close drops to 2.78.
    client = FakeQuotes({"600017": 278, "600519": 134998})

    found = detect_ex_symbols(cfg, DAY, ["600017.SH", "600519.SH"], client=client)

    assert found == {"600017.SH"}


def test_vendor_split_takes_over_a_matching_combined_distribution():
    tdx = _actions(
        [
            ("603093.SH", DAY, "bonus", 0.0, 0.45, 0.0),
            ("603093.SH", DAY, "cash_dividend", 0.069, 0.0, 0.0),
            ("600001.SH", DAY, "bonus", 0.0, 0.3, 0.0),
        ]
    )
    vendor = _actions(
        [
            ("603093.SH", DAY, "transfer", 0.0, 0.0, 0.45),
            ("600001.SH", DAY, "transfer", 0.0, 0.0, 0.2),  # disagrees: TDX stands
        ]
    )

    kept, taking_over = _split_with_vendor(tdx, vendor)

    assert sorted(kept.select("symbol", "action_type").rows()) == [
        ("600001.SH", "bonus"),
        ("603093.SH", "cash_dividend"),
    ]
    assert taking_over.select("symbol", "action_type").rows() == [("603093.SH", "transfer")]


def test_peer_finding_reports_misses_and_differences():
    tdx = _actions([("600017.SH", DAY, "cash_dividend", 0.033, 0.0, 0.0)])
    vendor = _actions(
        [
            ("600017.SH", DAY, "cash_dividend", 0.033, 0.0, 0.0),
            ("600360.SH", DAY, "cash_dividend", 0.004, 0.0, 0.0),
        ]
    )

    finding = _peer_finding(tdx, vendor, DAY)

    assert finding["eastmoney_only"] == ["600360.SH"]
    assert finding["differ"] == []
    assert finding["severity"] == "warning"


def _cfg(tmp_path):
    return Config(
        data_root=tmp_path / "lake",
        sources={"eastmoney": True, "tdx_protocol": True},
        raw_archive_enabled=False,
    )


def test_daily_step_writes_tdx_rows_and_keeps_vendor_only_events(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    tdx = _actions([("600017.SH", DAY, "cash_dividend", 0.033, 0.0, 0.0)])
    vendor = _actions(
        [
            ("600017.SH", DAY, "cash_dividend", 0.033, 0.0, 0.0),
            ("600360.SH", DAY, "cash_dividend", 0.004, 0.0, 0.0),
        ]
    )
    monkeypatch.setattr("cnequity.domain.market_time.shanghai_today", lambda: DAY)
    monkeypatch.setattr(events, "_tdx_primary_events", lambda c, d, r, rl: (tdx, "primary:x"))
    monkeypatch.setattr(events, "fetch_corporate_actions_eastmoney", lambda d, **k: vendor)
    monkeypatch.setattr(events, "incremental_trade_dates", lambda c, ds, d: [DAY])
    monkeypatch.setattr(events, "_heal_unrecorded_ex_events", lambda *a: 0)
    monkeypatch.setattr(events, "_sweep_unswept_fund_history", lambda *a: (0, None))

    result = step_corporate_actions(cfg, DAY, "run-1", {})

    staged = pl.concat(
        [pl.read_parquet(p) for p in (cfg.staging_root / "corporate_actions").rglob("*.parquet")],
        how="diagonal_relaxed",
    )
    assert sorted(staged.select("symbol", "source").rows()) == [
        ("600017.SH", "tdx_protocol"),
        ("600360.SH", "eastmoney"),
    ]
    checks = [f["check"] for f in result["context_updates"]["audit_findings"]]
    assert "corporate_actions_peer" in checks


def test_daily_step_falls_back_to_eastmoney_when_tdx_fails(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    vendor = _actions([("600017.SH", DAY, "cash_dividend", 0.033, 0.0, 0.0)])

    def offline(*args):
        raise ConnectionError("no TDX host")

    monkeypatch.setattr("cnequity.domain.market_time.shanghai_today", lambda: DAY)
    monkeypatch.setattr(events, "_tdx_primary_events", offline)
    monkeypatch.setattr(events, "fetch_corporate_actions_eastmoney", lambda d, **k: vendor)
    monkeypatch.setattr(events, "incremental_trade_dates", lambda c, ds, d: [DAY])
    monkeypatch.setattr(events, "_heal_unrecorded_ex_events", lambda *a: 0)
    monkeypatch.setattr(events, "_sweep_unswept_fund_history", lambda *a: (0, None))

    result = step_corporate_actions(cfg, DAY, "run-1", {})

    staged = pl.read_parquet(next((cfg.staging_root / "corporate_actions").rglob("*.parquet")))
    assert staged["source"].to_list() == ["eastmoney"]
    checks = [f["check"] for f in result["context_updates"]["audit_findings"]]
    assert "corporate_actions_primary_unavailable" in checks


def test_sweep_does_not_double_a_split_the_lake_already_holds(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    stored = _actions([("688809.SH", date(2026, 10, 23), "transfer", 0.0, 0.0, 0.4)]).with_columns(
        pl.lit("eastmoney").alias("source")
    )
    part = cfg.curated_root / "corporate_actions" / "ex_date=2026"
    part.mkdir(parents=True)
    stored.write_parquet(part / "part-merged.parquet")
    tdx = _actions(
        [
            ("688809.SH", date(2026, 10, 23), "bonus", 0.0, 0.4, 0.0),
            ("688809.SH", date(2026, 10, 23), "cash_dividend", 0.1, 0.0, 0.0),
            ("600000.SH", date(2026, 10, 23), "bonus", 0.0, 0.3, 0.0),
        ]
    )
    written = []
    monkeypatch.setattr(events, "_sweep_universe", lambda c: ["688809.SH", "600000.SH"])
    monkeypatch.setattr(events, "fetch_corporate_actions", lambda *a, **k: tdx)
    monkeypatch.setattr(
        events, "write_fetched", lambda c, r, d, frame, **k: written.append(frame) or {}
    )
    monkeypatch.setattr(cfg, "should_archive_raw", lambda d: False, raising=False)

    events.step_corporate_actions_sweep(cfg, DAY, "run", {})

    rows = pl.concat(written).select("symbol", "action_type").sort("symbol", "action_type")
    # 688809's 0.4 is already EastMoney's 转增; only its cash and 600000's
    # unsplit TDX bonus are written.
    assert rows.rows() == [("600000.SH", "bonus"), ("688809.SH", "cash_dividend")]
