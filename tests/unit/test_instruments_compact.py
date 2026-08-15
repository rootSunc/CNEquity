import json
from datetime import date, datetime, timezone

import polars as pl

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.query.universe import tradable_symbols_on_date
from cn_market_lake.steps.finalize import step_compact
from cn_market_lake.storage import StagingWriter
from cn_market_lake.storage.instruments import compact_instruments


def _prov(source: str = "tdx_protocol") -> dict:
    return {
        "source": source,
        "data_version": "v1",
        "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
    }


def _instrument(
    symbol: str, *, list_date: date | None = None, delist_date: date | None = None
) -> dict:
    exchange = symbol.split(".")[1]
    return {
        "symbol": symbol,
        "name": symbol,
        "exchange": exchange,
        "asset_type": "stock",
        "list_date": list_date,
        "delist_date": delist_date,
        "prev_symbol": None,
        **_prov(),
    }


def test_compact_instruments_preserves_missing_symbols_and_marks_delist(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-inst"
    trade_date = date(2024, 6, 28)

    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    existing_rows = [
        _instrument("600519.SH", list_date=date(2001, 8, 27)),
        _instrument("000001.SZ", list_date=date(1991, 4, 3)),
    ]
    for i in range(98):
        existing_rows.append(
            _instrument(f"600{i:03d}.SH", list_date=date(2000, 1, 1)),
        )
    existing_rows.append(_instrument("600000.SH", list_date=date(1999, 11, 10)))
    pl.DataFrame(existing_rows).write_parquet(curated_path)

    writer = StagingWriter(cfg.staging_root)
    incoming_rows = [r for r in existing_rows if r["symbol"] != "600000.SH"]
    writer.write_batch(
        "instruments",
        run_id,
        "batch-0",
        pl.DataFrame(incoming_rows),
    )

    rows, findings = compact_instruments(cfg.staging_root, cfg.curated_root, run_id, trade_date)
    assert rows == 100
    assert findings == []

    merged = pl.read_parquet(curated_path)
    delisted = merged.filter(pl.col("symbol") == "600000.SH")
    assert delisted.height == 1
    assert delisted["delist_date"][0] == trade_date

    active = merged.filter(pl.col("symbol") == "600519.SH")
    assert active["delist_date"][0] is None


def test_compact_instruments_suppresses_delist_when_absent_ratio_exceeds_threshold(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-circuit"
    trade_date = date(2024, 6, 28)

    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    existing_rows = [
        _instrument("600519.SH", list_date=date(2001, 8, 27)),
        _instrument("000001.SZ", list_date=date(1991, 4, 3)),
    ]
    for i in range(8):
        existing_rows.append(_instrument(f"600{i:03d}.SH", list_date=date(2000, 1, 1)))
    pl.DataFrame(existing_rows).write_parquet(curated_path)

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "instruments",
        run_id,
        "batch-0",
        pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]),
    )

    rows, findings = compact_instruments(cfg.staging_root, cfg.curated_root, run_id, trade_date)
    assert rows == 10
    assert len(findings) == 1
    assert findings[0]["check"] == "instruments_delist_suppressed"
    assert findings[0]["severity"] == "error"

    merged = pl.read_parquet(curated_path)
    absent = merged.filter(pl.col("symbol") == "000001.SZ")
    assert absent.height == 1
    assert absent["delist_date"][0] is None


def test_compact_instruments_via_step_respects_manifest_gate(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-step"
    trade_date = date(2024, 6, 28)

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "instruments",
        run_id,
        "batch-0",
        pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]),
    )

    step_compact(cfg, trade_date, run_id, {})
    assert (cfg.curated_root / "instruments" / "part-merged.parquet").exists()


def test_audit_emits_instruments_delist_suppressed_error(tmp_path):
    from cn_market_lake.steps.finalize import step_audit, step_compact

    root = tmp_path / "data"
    cfg = Config(data_root=root)
    run_id = "run-audit-circuit"
    trade_date = date(2024, 6, 28)

    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    existing_rows = [_instrument("600519.SH", list_date=date(2001, 8, 27))]
    for i in range(9):
        existing_rows.append(_instrument(f"600{i:03d}.SH", list_date=date(2000, 1, 1)))
    pl.DataFrame(existing_rows).write_parquet(curated_path)

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "instruments",
        run_id,
        "batch-0",
        pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]),
    )

    compact_result = step_compact(cfg, trade_date, run_id, {})
    context = compact_result.get("context_updates", {})
    step_audit(cfg, trade_date, run_id, context)

    payload = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{run_id}.json").read_text(encoding="utf-8")
    )
    suppressed = [
        f for f in payload["findings"] if f.get("check") == "instruments_delist_suppressed"
    ]
    assert len(suppressed) == 1
    assert suppressed[0]["severity"] == "error"


def test_tradable_universe_excludes_delisted_symbol(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            _instrument("600519.SH", list_date=date(2001, 8, 27)),
            _instrument(
                "600000.SH",
                list_date=date(1999, 11, 10),
                delist_date=date(2024, 6, 27),
            ),
        ]
    ).write_parquet(curated_path)

    out = tradable_symbols_on_date(cfg, date(2024, 6, 28))
    assert out is not None
    assert set(out["symbol"].to_list()) == {"600519.SH"}


def test_tradable_universe_excludes_cdr(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    # exclusion is prefix-based, so even a stale asset_type="stock" row is dropped
    pl.DataFrame(
        [
            _instrument("600519.SH", list_date=date(2001, 8, 27)),
            _instrument("689009.SH", list_date=date(2020, 10, 29)),
        ]
    ).write_parquet(curated_path)

    out = tradable_symbols_on_date(cfg, date(2024, 6, 28))
    assert out is not None
    assert set(out["symbol"].to_list()) == {"600519.SH"}


def test_tradable_universe_excludes_etf(tmp_path):
    root = tmp_path / "data"
    cfg = Config(data_root=root)
    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            _instrument("600519.SH", list_date=date(2001, 8, 27)),
            _instrument("510300.SH", list_date=date(2012, 5, 28)),
            _instrument("159915.SZ", list_date=date(2011, 12, 9)),
        ]
    ).write_parquet(curated_path)

    out = tradable_symbols_on_date(cfg, date(2024, 6, 28))
    assert out is not None
    assert set(out["symbol"].to_list()) == {"600519.SH"}


def test_tdx_instrument_frame_marks_cdr_asset_type():
    from cn_market_lake.adapters.tdx_protocol.client import _filter_instrument_frame

    pdf = pl.DataFrame({"code": ["600519", "689009"], "name": ["Moutai", "Ninebot"]})
    out = _filter_instrument_frame(pdf, "SH")
    types = dict(zip(out["symbol"].to_list(), out["asset_type"].to_list(), strict=True))
    assert types == {"600519.SH": "stock", "689009.SH": "cdr"}


def test_tdx_instrument_frame_marks_etf_asset_type():
    from cn_market_lake.adapters.tdx_protocol.client import _filter_instrument_frame

    sh_out = _filter_instrument_frame(
        pl.DataFrame(
            {"code": ["600519", "510300", "588000"], "name": ["Moutai", "HS300", "STAR50"]}
        ),
        "SH",
    )
    sz_out = _filter_instrument_frame(
        pl.DataFrame({"code": ["000001", "159915"], "name": ["PingAn", "ChiNext"]}),
        "SZ",
    )
    sh_types = dict(zip(sh_out["symbol"].to_list(), sh_out["asset_type"].to_list(), strict=True))
    sz_types = dict(zip(sz_out["symbol"].to_list(), sz_out["asset_type"].to_list(), strict=True))
    assert sh_types == {
        "600519.SH": "stock",
        "510300.SH": "etf",
        "588000.SH": "etf",
    }
    assert sz_types == {"000001.SZ": "stock", "159915.SZ": "etf"}


def test_enrich_instrument_list_dates_fills_nulls(tmp_path, monkeypatch):
    from cn_market_lake.adapters.eastmoney import instruments as em_inst

    cfg = Config(data_root=tmp_path / "data", sources={"eastmoney": True})
    df = pl.DataFrame([_instrument("600519.SH")])

    monkeypatch.setattr(
        em_inst,
        "fetch_list_date_map",
        lambda **kwargs: {"600519.SH": date(2001, 8, 27)},
    )

    enriched = em_inst.enrich_instrument_list_dates(cfg, df)
    assert enriched["list_date"][0] == date(2001, 8, 27)
