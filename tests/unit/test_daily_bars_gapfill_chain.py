"""The daily_bars multi-day gap-fill chain: which source is asked, in what
order, with what, and what the chain reports back.

Recorded from the hand-written chain before it became a list of stages, and
kept as the contract the stages must honour. Every source is a fake that fills
the keys a scenario gives it by staging real rows, so the chain's own
``missing_keys`` bookkeeping runs unchanged.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from cnequity.config import Config, FailoverDatasetSpec
from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps import bars
from cnequity.storage import StagingWriter
from cnequity.storage.layout import init_data_layout

GOLDEN = Path(__file__).with_name("daily_bars_gapfill_chain.json")
D1, D2 = date(2026, 7, 20), date(2026, 7, 21)
SYMBOLS = ["600001.SH", "600002.SH", "600003.SH"]
A, B, C = SYMBOLS


def _cfg(tmp_path, *, eastmoney: bool = True) -> Config:
    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        batch_size=10,
        tdx_allow_mock=True,
        failover_enabled=True,
        failover_datasets=[
            FailoverDatasetSpec(name="daily_bars", primary="tdx_protocol", backup="eastmoney")
        ],
        sources={"eastmoney": eastmoney, "tdx_protocol": True, "sina": True, "ths": True},
    )
    init_data_layout(cfg)
    return cfg


def _rows(keys: list[tuple[str, date]], source: str) -> pl.DataFrame:
    return with_provenance(
        pl.DataFrame(
            {
                "symbol": [k[0] for k in keys],
                "trade_date": [k[1] for k in keys],
                "open": [10.0] * len(keys),
                "high": [11.0] * len(keys),
                "low": [9.0] * len(keys),
                "close": [10.5] * len(keys),
                "volume": [100] * len(keys),
                "amount": [1000.0] * len(keys),
            },
            schema_overrides={"trade_date": pl.Date},
        ),
        source=source,
        data_version="v1",
    )


def _keys(value) -> list[str]:
    return sorted(f"{symbol}@{day}" for symbol, day in value)


class Chain:
    """Fakes every source; records each call and stages the keys it fills."""

    def __init__(self, cfg, run_id, plan, *, empty=None, complete=()):
        self.cfg, self.run_id, self.plan = cfg, run_id, plan
        self.empty = empty or {}
        self.complete = list(complete)
        self.calls: list[dict] = []

    def _fill(self, stage: str) -> int:
        keys = sorted(self.plan.get(stage, []))
        if keys:
            StagingWriter(self.cfg.staging_root).write_batch(
                "daily_bars", self.run_id, f"fake-{stage}", _rows(keys, stage)
            )
        return len(keys)

    def _result(self, stage: str, written: int, empty_key: str) -> dict:
        return {
            "rows_read": written,
            "rows_written": written,
            "audit_findings": [{"check": f"fake_{stage}"}],
            "source_outcomes": {stage: {"status": "success" if written else "empty"}},
            empty_key: list(self.empty.get(stage, [])),
        }

    def install(self, monkeypatch) -> None:
        def exchange(config, run_id, *, symbols, start, end):
            self.calls.append({"stage": "exchange", "symbols": sorted(symbols)})
            result = self._result("exchange", self._fill("exchange"), "empty_symbols")
            result["complete_symbols"] = self.complete
            return result

        def dayk(config, run_id, *, missing, start, end):
            self.calls.append({"stage": "exchange_dayk", "missing": _keys(missing)})
            return self._result("exchange_dayk", self._fill("exchange_dayk"), "empty_symbols")

        def sina(config, symbols, start, end, run_id, *, batch_prefix):
            self.calls.append({"stage": "sina", "symbols": list(symbols), "batch": batch_prefix})
            result = self._result("sina", self._fill("sina"), "empty_symbol_names")
            return result

        def eastmoney(symbols, start, end, *, config, timeout_sec, diagnostics):
            self.calls.append({"stage": "eastmoney", "symbols": list(symbols)})
            diagnostics["empty_symbols"] = list(self.empty.get("eastmoney", []))
            keys = sorted(self.plan.get("eastmoney", []))
            return _rows(keys, "eastmoney") if keys else pl.DataFrame()

        def ths(config, run_id, *, missing_keys, start, end):
            self.calls.append({"stage": "ths", "missing": _keys(missing_keys)})
            return self._result("ths", self._fill("ths"), "empty_symbols")

        def baostock(config, run_id, *, missing_keys, start, end):
            self.calls.append({"stage": "baostock", "missing": _keys(missing_keys)})
            return self._result("baostock", self._fill("baostock"), "empty_symbols")

        def suspensions(config, run_id, remaining, start, end):
            self.calls.append({"stage": "suspensions", "missing": _keys(remaining)})
            return set(), {"status": "fake"}

        monkeypatch.setattr(bars, "_gapfill_complete_symbols_via_exchange", exchange)
        monkeypatch.setattr(bars, "_gapfill_fund_keys_via_exchange_dayk", dayk)
        monkeypatch.setattr(bars, "fetch_bars_via_sina", sina)
        monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", eastmoney)
        monkeypatch.setattr(bars, "_gapfill_missing_keys_via_ths", ths)
        monkeypatch.setattr(bars, "_gapfill_missing_keys_via_baostock", baostock)
        monkeypatch.setattr(bars, "_learn_suspensions_from_baostock", suspensions)
        monkeypatch.setattr(bars, "list_trading_dates", lambda config, start, end: [D1, D2])


SCENARIOS = {
    # The exchange stages settle everything: the chain stops early.
    "exchange_settles_all": dict(
        plan={"exchange": [(s, d) for s in SYMBOLS for d in (D1, D2)]},
        complete=SYMBOLS,
    ),
    # Each later source fills part of what the one before left.
    "each_stage_fills_part": dict(
        plan={
            "exchange": [(A, D1), (A, D2)],
            "exchange_dayk": [(B, D1)],
            "sina": [(B, D2)],
            "eastmoney": [(C, D1)],
            "ths": [],
            "baostock": [(C, D2)],
        },
        complete=[A],
    ),
    # Partial mode skips Sina; EastMoney is switched off; THS and baostock finish.
    "partial_mode_eastmoney_off": dict(
        plan={"ths": [(A, D1), (A, D2), (B, D1)], "baostock": [(B, D2), (C, D1), (C, D2)]},
        require_complete=False,
        eastmoney=False,
    ),
    # Nothing fills; two sources say C is empty, so only C is certified no-data.
    "nothing_fills_two_empty_sources": dict(
        plan={},
        empty={"sina": [C], "ths": [C]},
    ),
}


def _run(tmp_path, monkeypatch, name: str) -> dict:
    scenario = SCENARIOS[name]
    cfg = _cfg(tmp_path, eastmoney=scenario.get("eastmoney", True))
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    chain = Chain(
        cfg,
        run_id,
        scenario["plan"],
        empty=scenario.get("empty"),
        complete=scenario.get("complete", ()),
    )
    chain.install(monkeypatch)
    result = bars._gapfill_multiday_via_kline(
        cfg,
        run_id,
        symbols=SYMBOLS,
        start=D1,
        end=D2,
        require_complete=scenario.get("require_complete", True),
    )
    return json.loads(json.dumps({"calls": chain.calls, "result": result}, default=str))


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_the_chain_asks_the_same_sources_in_the_same_order(tmp_path, monkeypatch, name):
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert _run(tmp_path, monkeypatch, name) == golden[name]


def test_every_chain_source_is_one_the_registry_declares():
    """A4: the spec's sources and the code's chain agree by test, not by comment."""
    spec = DATASETS["daily_bars"]
    declared = {spec.primary_source, spec.backup_source, *spec.supplementary_sources}
    used = {stage.source for stage in bars.MULTIDAY_GAPFILL_CHAIN}
    assert used <= declared, sorted(used - declared)


if __name__ == "__main__":  # regenerate: python tests/unit/test_daily_bars_gapfill_chain.py
    import tempfile

    out = {}
    for name in sorted(SCENARIOS):
        with tempfile.TemporaryDirectory() as tmp, pytest.MonkeyPatch.context() as mp:
            out[name] = _run(Path(tmp), mp, name)
    GOLDEN.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {GOLDEN}")
