"""option_greeks: inputs joined per row, parity forwards, statuses."""

from __future__ import annotations

import os
from datetime import date

import numpy as np
import polars as pl

from cnequity.config import Config
from cnequity.derive.option_greeks import (
    compute_option_greeks,
    derive_option_greeks,
    stale_sessions,
)
from cnequity.domain.option_pricing import baw, black76
from cnequity.domain.schemas import validate_dataframe, with_provenance

DAY = date(2026, 9, 24)
EXPIRY = date(2026, 12, 18)
T = (EXPIRY - DAY).days / 365


def _options(rows):
    return pl.DataFrame(
        rows,
        schema={
            "symbol": pl.Utf8,
            "trade_date": pl.Date,
            "exchange": pl.Utf8,
            "underlying_symbol": pl.Utf8,
            "option_type": pl.Utf8,
            "strike": pl.Float64,
            "settle": pl.Float64,
        },
        orient="row",
    )


def _contracts(symbols, style):
    return pl.DataFrame(
        {
            "symbol": symbols,
            "expiry_date": [EXPIRY] * len(symbols),
            "exercise_style": [style] * len(symbols),
            "expiry_month": [date(2026, 12, 1)] * len(symbols),
        }
    )


def test_commodity_options_use_the_future_settlement_and_baw():
    call = float(baw(3000.0, 3100.0, T, 0.02, 0.22, True))
    options = _options([["M2612C3100.DCE", DAY, "DCE", "M2612.DCE", "C", 3100.0, round(call, 4)]])
    futures = pl.DataFrame({"symbol": ["M2612.DCE"], "trade_date": [DAY], "settle": [3000.0]})
    frame = compute_option_greeks(
        options, contracts=_contracts(["M2612C3100.DCE"], "american"), futures=futures
    )
    row = frame.row(0, named=True)
    assert row["model"] == "baw" and row["forward_source"] == "future_settle"
    assert row["rate_source"] == "fallback_constant"
    assert abs(row["iv"] - 0.22) < 1e-4
    assert 0 < row["delta"] < 1


def test_index_options_read_the_forward_off_put_call_parity():
    forward = 4450.0
    rows = []
    for strike in (4300.0, 4400.0, 4500.0, 4600.0):
        for side in ("C", "P"):
            value = float(black76(forward, strike, T, 0.02, 0.2, side == "C"))
            rows.append(
                [f"IO2612{side}{int(strike)}.CFE", DAY, "CFE", "000300.SH", side, strike, value]
            )
    frame = compute_option_greeks(
        _options(rows),
        contracts=_contracts([r[0] for r in rows], "european"),
        futures=pl.DataFrame(
            schema={"symbol": pl.Utf8, "trade_date": pl.Date, "settle": pl.Float64}
        ),
    )
    assert set(frame["forward_source"]) == {"parity"}
    assert np.allclose(frame["underlying_price"].to_numpy(), forward, atol=1e-6)
    assert np.allclose(frame["iv"].to_numpy(), 0.2, atol=1e-5)


def test_rows_without_inputs_say_why_and_store_nulls():
    options = _options(
        [
            ["M2612C3100.DCE", DAY, "DCE", "M2612.DCE", "C", 3100.0, 50.0],
            ["M2612C2000.DCE", DAY, "DCE", "M2612.DCE", "C", 2000.0, 1.0],
        ]
    )
    futures = pl.DataFrame({"symbol": ["M2612.DCE"], "trade_date": [DAY], "settle": [3000.0]})
    frame = compute_option_greeks(
        options,
        contracts=_contracts(["M2612C3100.DCE"], "american"),
        futures=futures,
    )
    statuses = dict(zip(frame["symbol"], frame["status"], strict=True))
    # No contract row for the second: no expiry, so no IV.
    assert statuses["M2612C2000.DCE"] == "no_expiry"
    stamped = with_provenance(frame, source="derived", data_version="v1")
    assert validate_dataframe(stamped, "option_greeks").height == 2


def test_the_expiry_session_is_marked_not_solved():
    # CFFEX 2026-09-18: calls settle at intrinsic, out-of-the-money puts at 0.
    rows = [
        ["IO2609C4400.CFE", EXPIRY, "CFE", "000300.SH", "C", 4400.0, 108.38],
        ["IO2609P4400.CFE", EXPIRY, "CFE", "000300.SH", "P", 4400.0, 0.0],
        ["IO2609C4600.CFE", EXPIRY, "CFE", "000300.SH", "C", 4600.0, 0.0],
        ["IO2609P4600.CFE", EXPIRY, "CFE", "000300.SH", "P", 4600.0, 91.62],
    ]
    frame = compute_option_greeks(
        _options(rows),
        contracts=_contracts([r[0] for r in rows], "european"),
        futures=pl.DataFrame(
            schema={"symbol": pl.Utf8, "trade_date": pl.Date, "settle": pl.Float64}
        ),
    )
    assert set(frame["status"]) == {"expiry_day"}
    assert frame["iv"].null_count() == 4
    # The zero puts complete the parity pairs, so the forward is still known.
    assert np.allclose(frame["underlying_price"].to_numpy(), 4508.38, atol=1e-6)


def _lake(tmp_path) -> Config:
    config = Config(data_root=tmp_path / "data")
    config.futures_enabled = True
    days = [date(2026, 9, 23), DAY]
    for day in days:
        part = config.curated_root / "option_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True)
        _options([["M2612C3100.DCE", day, "DCE", "M2612.DCE", "C", 3100.0, 80.0]]).write_parquet(
            part / "part-000.parquet"
        )
    month = config.curated_root / "futures_bars" / "trade_date=2026-09"
    month.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": ["M2612.DCE"] * 2, "trade_date": days, "settle": [3000.0, 3010.0]}
    ).write_parquet(month / "part-000.parquet")
    contracts = config.curated_root / "option_contracts"
    contracts.mkdir(parents=True)
    _contracts(["M2612C3100.DCE"], "american").write_parquet(contracts / "part-000.parquet")
    return config


def _age(path, seconds: float) -> None:
    for file in path.glob("*.parquet"):
        stat = file.stat()
        os.utime(file, (stat.st_atime - seconds, stat.st_mtime - seconds))


def test_sessions_recompute_when_their_bars_are_rewritten(tmp_path):
    config = _lake(tmp_path)
    assert stale_sessions(config) == [date(2026, 9, 23), DAY]
    assert derive_option_greeks(config)["sessions"] == 2
    assert stale_sessions(config) == []
    assert derive_option_greeks(config)["rows"] == 0
    # Touching files without changing rows is not a reason to recompute.
    greeks = config.derived_root / "option_greeks"
    for day in (date(2026, 9, 23), DAY):
        _age(greeks / f"trade_date={day.isoformat()}", 60)
    assert stale_sessions(config) == []
    # A backfill rewrites the month's futures: both sessions read it.
    month = config.curated_root / "futures_bars" / "trade_date=2026-09"
    pl.DataFrame(
        {
            "symbol": ["M2612.DCE"] * 2,
            "trade_date": [date(2026, 9, 23), DAY],
            "settle": [3001.0, 3010.0],
        }
    ).write_parquet(month / "part-000.parquet")
    assert stale_sessions(config) == [date(2026, 9, 23), DAY]


def test_rewriting_an_earlier_window_keeps_the_watermark(tmp_path):
    from cnequity.storage.state import StateStore

    config = _lake(tmp_path)
    derive_option_greeks(config)
    summary = derive_option_greeks(config, start=date(2026, 9, 23), end=date(2026, 9, 23))
    assert summary["sessions"] == 1
    assert StateStore(config.meta_root).get_date("option_greeks") == DAY


def test_contracts_rates_and_model_changes_invalidate_even_with_preserved_mtime(
    tmp_path, monkeypatch
):
    from cnequity.derive import option_greeks as module

    cfg = _lake(tmp_path)
    derive_option_greeks(cfg)
    contract = next((cfg.curated_root / "option_contracts").glob("*.parquet"))
    before = contract.stat()
    pl.read_parquet(contract).with_columns(pl.lit(DAY).alias("expiry_date")).write_parquet(contract)
    os.utime(contract, (before.st_atime, before.st_mtime))
    assert stale_sessions(cfg) == [date(2026, 9, 23), DAY]
    derive_option_greeks(cfg, start=DAY, end=DAY)
    assert stale_sessions(cfg) == [date(2026, 9, 23)]
    derive_option_greeks(cfg)
    rates = cfg.curated_root / "macro_indicators"
    rates.mkdir()
    pl.DataFrame({"indicator_id": ["shibor_3m"], "obs_date": [DAY], "value": [1.5]}).write_parquet(
        rates / "rates.parquet"
    )
    # Only a session on or after the new observation applies it.
    assert stale_sessions(cfg) == [DAY]
    derive_option_greeks(cfg)
    original = module._dependency_identity
    monkeypatch.setattr(
        module, "_dependency_identity", lambda config: {**original(config), "model": "changed"}
    )
    assert stale_sessions(cfg) == [date(2026, 9, 23), DAY]


def test_unknown_style_and_observed_expiry_never_produce_priced_greeks():
    options = _options([["M2612C3100.DCE", DAY, "DCE", "M2612.DCE", "C", 3100.0, 50.0]])
    futures = pl.DataFrame({"symbol": ["M2612.DCE"], "trade_date": [DAY], "settle": [3000.0]})
    contracts = _contracts(["M2612C3100.DCE"], "unknown")
    result = compute_option_greeks(options, contracts=contracts, futures=futures)
    assert result["status"].item() == "no_exercise_style"
    contracts = contracts.with_columns(pl.lit("observed").alias("dates_basis"))
    result = compute_option_greeks(options, contracts=contracts, futures=futures)
    assert result["status"].item() == "no_expiry"


def test_a_newly_listed_contract_leaves_settled_sessions_alone(tmp_path):
    """Whole-table fingerprints made every session stale whenever a contract
    was listed, so the full history was recomputed daily (~50 min)."""
    cfg = _lake(tmp_path)
    derive_option_greeks(cfg)
    contracts = cfg.curated_root / "option_contracts"
    pl.concat(
        [
            pl.read_parquet(next(contracts.glob("*.parquet"))),
            _contracts(["M2701C3200.DCE"], "american"),
        ],
        how="diagonal_relaxed",
    ).write_parquet(contracts / "part-000.parquet")
    assert stale_sessions(cfg) == []


def test_the_derive_does_not_hold_the_lake_lock_while_computing(tmp_path, monkeypatch):
    from cnequity.derive import option_greeks as module
    from cnequity.file_lock import LockUnavailable, lake_mutation_lock

    cfg = _lake(tmp_path)
    seen: list[bool] = []
    original = module.compute_option_greeks

    def probe(*args, **kwargs):
        import threading

        def other_job():
            # Another thread stands in for another process: lock re-entry is
            # tracked per thread, the file lock itself is not.
            try:
                with lake_mutation_lock(cfg.meta_root, blocking=False):
                    seen.append(True)
            except LockUnavailable:
                seen.append(False)

        worker = threading.Thread(target=other_job)
        worker.start()
        worker.join()
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "compute_option_greeks", probe)
    derive_option_greeks(cfg)
    assert seen == [True], "another job's compact can run while Greeks compute"
