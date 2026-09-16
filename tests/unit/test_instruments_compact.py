import json
import shutil
from datetime import date, datetime, timezone

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.query.universe import UniverseCoverageError, tradable_symbols_on_date
from cnequity.steps.common import instrument_metadata, load_symbols
from cnequity.steps.finalize import step_compact
from cnequity.storage import StagingWriter
from cnequity.storage.instruments import compact_instruments


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
    assert findings and findings[0]["check"] == "instruments_delist_pending"

    merged = pl.read_parquet(curated_path)
    delisted = merged.filter(pl.col("symbol") == "600000.SH")
    assert delisted.height == 1
    assert delisted["delist_date"][0] is None

    writer.write_batch(
        "instruments",
        "run-inst-2",
        "batch-0",
        pl.DataFrame(incoming_rows),
    )
    rows, findings = compact_instruments(
        cfg.staging_root, cfg.curated_root, "run-inst-2", date(2024, 7, 1)
    )
    assert rows == 100
    assert findings == []
    merged = pl.read_parquet(curated_path)
    delisted = merged.filter(pl.col("symbol") == "600000.SH")
    assert delisted["delist_date"][0] == date(2024, 7, 1)

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


def test_compact_instruments_ignores_known_delisted_symbols_for_absence_circuit(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    existing_rows = [
        _instrument("600519.SH", list_date=date(2001, 8, 27)),
        _instrument("000001.SZ", list_date=date(1991, 4, 3)),
        _instrument(
            "600001.SH",
            list_date=date(1998, 1, 22),
            delist_date=date(2009, 12, 25),
        ),
        _instrument(
            "000003.SZ",
            list_date=date(1991, 1, 14),
            delist_date=date(2002, 6, 14),
        ),
    ]
    pl.DataFrame(existing_rows).write_parquet(curated_path)

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "instruments",
        "run-known-delisted",
        "batch-0",
        pl.DataFrame(existing_rows[:2]),
    )

    rows, findings = compact_instruments(
        cfg.staging_root,
        cfg.curated_root,
        "run-known-delisted",
        date(2024, 6, 28),
    )

    assert rows == 4
    assert findings == []
    merged = pl.read_parquet(curated_path)
    assert merged.filter(pl.col("symbol") == "600001.SH")["delist_date"].item() == date(
        2009, 12, 25
    )
    absence_state = cfg.meta_root / "instruments_absence_streak.json"
    assert json.loads(absence_state.read_text(encoding="utf-8")) == {}


def test_compact_instruments_removes_padded_subscription_placeholders(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            _instrument("600519.SH"),
            {
                **_instrument("517234.SH", delist_date=date(2026, 8, 18)),
                "name": "认购款\x00\x00",
                "asset_type": "etf",
            },
        ]
    ).write_parquet(curated_path)

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "instruments",
        "run-padded-placeholder",
        "batch-0",
        pl.DataFrame([_instrument("600519.SH")]),
    )

    rows, findings = compact_instruments(
        cfg.staging_root,
        cfg.curated_root,
        "run-padded-placeholder",
        date(2026, 8, 21),
    )

    assert rows == 1
    assert findings == []
    assert pl.read_parquet(curated_path)["symbol"].to_list() == ["600519.SH"]


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


def test_compact_instruments_removes_stale_siblings(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-inst-fragments"
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "instruments",
        run_id,
        "batch-0",
        pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]),
    )

    compact_instruments(cfg.staging_root, cfg.curated_root, run_id, date(2024, 6, 28))
    canonical = cfg.curated_root / "instruments" / "part-merged.parquet"
    canonical.with_name("part-old.parquet").write_bytes(canonical.read_bytes())
    nested = canonical.parent / ".old-fragments"
    nested.mkdir()
    (nested / "part-old.parquet").write_bytes(canonical.read_bytes())

    writer.write_batch(
        "instruments",
        "run-inst-fragments-2",
        "batch-0",
        pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]),
    )
    compact_instruments(
        cfg.staging_root,
        cfg.curated_root,
        "run-inst-fragments-2",
        date(2024, 6, 29),
    )

    assert [p.name for p in canonical.parent.rglob("*.parquet")] == ["part-merged.parquet"]


def test_compact_instruments_does_not_rewrite_mutable_file_for_immutable_base_refresh(tmp_path):
    """An immutable generation is a merge base, not a mutable fragment."""

    cfg = Config(data_root=tmp_path / "data")
    writer = StagingWriter(cfg.staging_root)
    first = _instrument("600519.SH", list_date=date(2001, 8, 27))
    writer.write_batch("instruments", "run-instruments-v1", "batch-0", pl.DataFrame([first]))
    changed: list = []
    compact_instruments(
        cfg.staging_root,
        cfg.curated_root,
        "run-instruments-v1",
        date(2024, 6, 28),
        changed_files=changed,
    )
    mutable = cfg.curated_root / "instruments" / "part-merged.parquet"
    immutable = tmp_path / "immutable"
    immutable.mkdir(parents=True)
    shutil.copy2(mutable, immutable / mutable.name)

    refreshed = dict(first)
    refreshed["fetched_at"] = datetime(2024, 6, 29, tzinfo=timezone.utc)
    writer.write_batch("instruments", "run-instruments-v2", "batch-0", pl.DataFrame([refreshed]))
    before_bytes = mutable.read_bytes()
    before_mtime = mutable.stat().st_mtime_ns
    second_changed: list = []
    compact_instruments(
        cfg.staging_root,
        cfg.curated_root,
        "run-instruments-v2",
        date(2024, 6, 29),
        changed_files=second_changed,
        base_root=immutable,
    )

    assert second_changed == []
    assert mutable.read_bytes() == before_bytes
    assert mutable.stat().st_mtime_ns == before_mtime


def test_compact_instruments_preserves_rows_from_nested_legacy_fragments(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True)
    pl.DataFrame([_instrument("600519.SH")]).write_parquet(root / "part-merged.parquet")
    nested = root / ".old-fragments"
    nested.mkdir()
    pl.DataFrame([_instrument("000001.SZ")]).write_parquet(nested / "part-old.parquet")

    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "instruments",
        "run-legacy-fragments",
        "batch-0",
        pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]),
    )

    compact_instruments(
        cfg.staging_root,
        cfg.curated_root,
        "run-legacy-fragments",
        date(2024, 6, 28),
    )

    merged = pl.read_parquet(root / "part-merged.parquet")
    assert set(merged["symbol"]) == {"600519.SH", "000001.SZ"}


def test_audit_emits_instruments_delist_suppressed_error(tmp_path):
    from cnequity.steps.finalize import step_audit, step_compact

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


def test_tradable_universe_uses_latest_status_fragment(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]).write_parquet(
        instruments / "part-merged.parquet"
    )
    status = cfg.curated_root / "trading_status" / "trade_date=2024-06"
    status.mkdir(parents=True)
    base = {
        "symbol": "600519.SH",
        "trade_date": date(2024, 6, 28),
        "source": "eastmoney",
        "data_version": "v1",
    }
    pl.DataFrame(
        [
            {
                **base,
                "is_trading": False,
                "status": "suspended",
                "fetched_at": "2024-06-28T00:00:00+00:00",
            }
        ]
    ).write_parquet(status / "part-old.parquet")
    nested = status / ".fragments"
    nested.mkdir()
    pl.DataFrame(
        [
            {
                **base,
                "is_trading": True,
                "status": "normal",
                "fetched_at": "2024-06-28T01:00:00+00:00",
            }
        ]
    ).write_parquet(nested / "part-new.parquet")

    out = tradable_symbols_on_date(cfg, date(2024, 6, 28))

    assert out is not None
    assert out["symbol"].to_list() == ["600519.SH"]


def test_tradable_universe_dedupes_nested_instrument_fragments(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True)
    pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]).write_parquet(
        root / "part-merged.parquet"
    )
    nested = root / ".old-fragments"
    nested.mkdir()
    pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]).write_parquet(
        nested / "part-old.parquet"
    )

    out = tradable_symbols_on_date(cfg, date(2024, 6, 28))

    assert out is not None
    assert out["symbol"].to_list() == ["600519.SH"]


def test_step_instrument_reads_preserve_nested_catalog_rows(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True)
    pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]).write_parquet(
        root / "part-merged.parquet"
    )
    nested = root / ".old-fragments"
    nested.mkdir()
    pl.DataFrame([_instrument("000001.SZ", list_date=date(1991, 4, 3))]).write_parquet(
        nested / "part-old.parquet"
    )

    assert set(load_symbols(cfg)) == {"600519.SH", "000001.SZ"}
    assert set(instrument_metadata(cfg)["symbol"]) == {"600519.SH", "000001.SZ"}


def test_step_instrument_fallback_merges_nested_staging_fragments(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.staging_root / "instruments" / "run_id=run-recovered" / ".fragments"
    root.mkdir(parents=True)
    pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]).write_parquet(
        root / "part-0.parquet"
    )
    pl.DataFrame([_instrument("000001.SZ", list_date=date(1991, 4, 3))]).write_parquet(
        root / "part-1.parquet"
    )

    assert set(load_symbols(cfg)) == {"600519.SH", "000001.SZ"}
    assert set(instrument_metadata(cfg)["symbol"]) == {"600519.SH", "000001.SZ"}


def test_symbol_universe_ignores_padded_subscription_placeholders(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True)
    valid = _instrument("600519.SH", list_date=date(2001, 8, 27))
    placeholder = _instrument("561834.SH", list_date=date(2026, 8, 20))
    placeholder["name"] = "认购款\x00\x00"
    pl.DataFrame([valid, placeholder]).write_parquet(root / "part-merged.parquet")

    assert load_symbols(cfg) == ["600519.SH"]
    assert instrument_metadata(cfg)["symbol"].to_list() == ["600519.SH"]


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


def test_strict_tradable_universe_requires_status_coverage(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    pl.DataFrame([_instrument("600519.SH", list_date=date(2001, 8, 27))]).write_parquet(
        curated_path
    )

    with pytest.raises(UniverseCoverageError, match="trading_status coverage"):
        tradable_symbols_on_date(cfg, date(2024, 6, 28), strict=True)


def test_strict_tradable_universe_rejects_partial_status_coverage(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            _instrument("600519.SH", list_date=date(2001, 8, 27)),
            _instrument("600000.SH", list_date=date(1999, 11, 10)),
        ]
    ).write_parquet(curated_path)
    status_path = cfg.curated_root / "trading_status" / "trade_date=2024-06-28" / "part-0.parquet"
    status_path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "is_trading": [True],
            "status": ["normal"],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": [date(2024, 6, 28)],
        }
    ).write_parquet(status_path)

    with pytest.raises(UniverseCoverageError, match=r"missing 1 symbol\(s\)"):
        tradable_symbols_on_date(cfg, date(2024, 6, 28), strict=True)


def test_tdx_instrument_frame_marks_cdr_asset_type():
    from cnequity.adapters.tdx_protocol.client import _filter_instrument_frame

    pdf = pl.DataFrame({"code": ["600519", "689009"], "name": ["Moutai", "Ninebot"]})
    out = _filter_instrument_frame(pdf, "SH")
    types = dict(zip(out["symbol"].to_list(), out["asset_type"].to_list(), strict=True))
    assert types == {"600519.SH": "stock", "689009.SH": "cdr"}


def test_tdx_instrument_frame_rejects_malformed_codes():
    from cnequity.adapters.tdx_protocol.client import _filter_instrument_frame

    out = _filter_instrument_frame(
        pl.DataFrame(
            {
                "code": ["abc", "0000001", "600519"],
                "name": ["bad", "bad", "Moutai"],
            }
        ),
        "SH",
    )
    assert out["symbol"].to_list() == ["600519.SH"]


def test_tdx_instrument_frame_rejects_non_numeric_code_with_valid_prefix():
    # "60abcd" starts with the SH "60" prefix but is not a clean 6-digit code —
    # regression test for a mask that ANDed the digit-validity check with a
    # literal False, silently disabling it while still passing prefix-only cases.
    from cnequity.adapters.tdx_protocol.client import _filter_instrument_frame

    out = _filter_instrument_frame(
        pl.DataFrame({"code": ["60abcd", "600519"], "name": ["bad", "Moutai"]}),
        "SH",
    )
    assert out["symbol"].to_list() == ["600519.SH"]


def test_tdx_instrument_frame_marks_etf_asset_type():
    from cnequity.adapters.tdx_protocol.client import _filter_instrument_frame

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


def test_tdx_instrument_frame_rejects_unlisted_pre_close_sentinel():
    from cnequity.adapters.tdx_protocol.client import _filter_instrument_frame

    sentinel = 5.877471754111438e-39
    out = _filter_instrument_frame(
        pl.DataFrame(
            {
                "code": ["600519", "601123", "510300", "588999"],
                "name": ["Moutai", "IPO applicant", "HS300", "Fund applicant"],
                "pre_close": [1418.0, sentinel, 4.2, sentinel],
            }
        ),
        "SH",
    )

    assert out["symbol"].to_list() == ["600519.SH", "510300.SH"]


def test_enrich_instrument_list_dates_fills_nulls(tmp_path, monkeypatch):
    from cnequity.adapters.eastmoney import instruments as em_inst

    cfg = Config(data_root=tmp_path / "data", sources={"eastmoney": True})
    df = pl.DataFrame([_instrument("600519.SH")])

    monkeypatch.setattr(
        em_inst,
        "fetch_list_date_map",
        lambda **kwargs: {"600519.SH": date(2001, 8, 27)},
    )

    enriched = em_inst.enrich_instrument_list_dates(cfg, df)
    assert enriched["list_date"][0] == date(2001, 8, 27)


def test_fetch_list_date_map_closes_owned_client_on_success(monkeypatch):
    from cnequity.adapters.eastmoney import instruments as em_inst

    class _Client:
        closed = False

        def close(self):
            self.closed = True

    created: list[_Client] = []

    def _factory(**kwargs):
        client = _Client()
        created.append(client)
        return client

    monkeypatch.setattr(em_inst, "EastMoneyClient", _factory)
    monkeypatch.setattr(
        em_inst,
        "fetch_clist_pages",
        lambda client, fields, fs=None: [{"f12": "600519", "f13": 1, "f26": "20010827"}],
    )
    assert em_inst.fetch_list_date_map() == {"600519.SH": date(2001, 8, 27)}
    assert created[0].closed is True


def test_fetch_list_date_map_infers_exchange_when_clist_market_is_missing(monkeypatch):
    from cnequity.adapters.eastmoney import instruments as em_inst

    monkeypatch.setattr(
        em_inst,
        "fetch_clist_pages",
        lambda client, fields, fs=None: [
            {"f12": "600519", "f13": 0, "f26": "20010827"},
            {"f12": "000001", "f13": "0", "f26": "19910403"},
            {"f12": "920001", "f13": "bad", "f26": "20240701"},
        ],
    )
    assert em_inst.fetch_list_date_map(client=object()) == {
        "600519.SH": date(2001, 8, 27),
        "000001.SZ": date(1991, 4, 3),
        "920001.BJ": date(2024, 7, 1),
    }


def test_fetch_list_date_map_skips_nonfinite_list_date(monkeypatch):
    from cnequity.adapters.eastmoney import instruments as em_inst

    monkeypatch.setattr(
        em_inst,
        "fetch_clist_pages",
        lambda client, fields, fs=None: [
            {"f12": "600519", "f13": 1, "f26": "inf"},
            {"f12": "000001", "f13": 0, "f26": "19910403"},
        ],
    )

    assert em_inst.fetch_list_date_map(client=object()) == {
        "000001.SZ": date(1991, 4, 3),
    }


def test_fetch_list_date_map_skips_invalid_eight_digit_date(monkeypatch):
    from cnequity.adapters.eastmoney import instruments as em_inst

    monkeypatch.setattr(
        em_inst,
        "fetch_clist_pages",
        lambda client, fields, fs=None: [
            {"f12": "600519", "f13": 1, "f26": "20241340"},
            {"f12": "000001", "f13": 0, "f26": "19910403"},
        ],
    )

    assert em_inst.fetch_list_date_map(client=object()) == {
        "000001.SZ": date(1991, 4, 3),
    }


def test_fetch_list_date_map_includes_etf_board(monkeypatch):
    from cnequity.adapters.eastmoney import instruments as em_inst

    def _pages(client, fields, fs=None):
        if "MK" in (fs or ""):
            return [{"f12": "588200", "f13": 1, "f26": "20221026"}]
        return [{"f12": "600519", "f13": 1, "f26": "20010827"}]

    monkeypatch.setattr(em_inst, "fetch_clist_pages", _pages)
    assert em_inst.fetch_list_date_map(client=object()) == {
        "600519.SH": date(2001, 8, 27),
        "588200.SH": date(2022, 10, 26),
    }


def test_symbol_from_clist_etf_infers_exchange_from_prefix():
    from cnequity.adapters.eastmoney import common as em_common

    assert em_common.symbol_from_clist_etf("588200", 0) == "588200.SH"
    assert em_common.symbol_from_clist_etf("159915", 0) == "159915.SZ"
    assert em_common.symbol_from_clist_etf("600519", 1) is None


def test_fetch_list_date_map_closes_owned_client_on_error(monkeypatch):
    from cnequity.adapters.eastmoney import instruments as em_inst

    class _Client:
        closed = False

        def close(self):
            self.closed = True

    created: list[_Client] = []

    def _factory(**kwargs):
        client = _Client()
        created.append(client)
        return client

    monkeypatch.setattr(em_inst, "EastMoneyClient", _factory)

    def _boom(client, fields, fs=None):
        raise RuntimeError("clist failed")

    monkeypatch.setattr(em_inst, "fetch_clist_pages", _boom)
    with pytest.raises(RuntimeError, match="clist failed"):
        em_inst.fetch_list_date_map()
    assert created[0].closed is True


def test_tdx_instrument_frame_strips_fixed_width_name_padding():
    # TDX serves names in a fixed-width field padded with NULs. Left in, they
    # reach curated ("C格林\x00\x00\x00") and silently break every equality,
    # join and display on the column — 1,718 stored rows carried them.
    from cnequity.adapters.tdx_protocol.client import _filter_instrument_frame

    out = _filter_instrument_frame(
        pl.DataFrame(
            {
                "code": ["600519", "603448"],
                "name": ["贵州茅台\x00\x00", " C天博\x00\x00\x00 "],
            }
        ),
        "SH",
    )
    names = dict(zip(out["symbol"].to_list(), out["name"].to_list(), strict=True))
    assert names == {"600519.SH": "贵州茅台", "603448.SH": "C天博"}
    assert not any("\x00" in name for name in names.values())


def test_absence_does_not_delist_a_security_that_never_traded(tmp_path):
    """TDX lists a new code before its first session, then drops it.

    Two such absences used to earn a delist_date days before the listing — 36
    rows were stamped that way on 2026-09-10 alone. A security with no bar in
    the lake has not stopped trading; it has not started.
    """
    curated = tmp_path / "curated"
    staging = tmp_path / "staging"
    (curated / "instruments").mkdir(parents=True)

    def row(symbol, list_date):
        return {
            "symbol": symbol,
            "name": symbol[:6],
            "exchange": symbol.split(".")[1],
            "asset_type": "stock",
            "list_date": list_date,
            "delist_date": None,
            "prev_symbol": None,
            "source": "tdx_protocol",
            "data_version": "v1",
            "fetched_at": datetime(2026, 9, 10, tzinfo=timezone.utc),
        }

    # A filler universe so the two absences stay under the partial-fetch circuit
    # breaker, which refuses inference when more than 5% of the lake vanishes.
    filler = [row(f"6011{i:02d}.SH", date(2010, 1, 4)) for i in range(60)]
    # Both absent from the incoming snapshot: 600519.SH has traded, 301688.SZ
    # is a pending listing with no bars.
    traded = row("600519.SH", date(2001, 8, 27))
    pending = row("301688.SZ", None)
    pl.DataFrame([*filler, traded, pending]).write_parquet(
        curated / "instruments" / "part-merged.parquet"
    )

    bars = curated / "daily_bars" / "trade_date=2026-09-08"
    bars.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": ["600519.SH"], "trade_date": [date(2026, 9, 8)], "close": [1500.0]}
    ).write_parquet(bars / "part-merged.parquet")

    writer = StagingWriter(staging)
    for run in ("run-nt-1", "run-nt-2"):
        writer.write_batch("instruments", run, "batch-0", pl.DataFrame(filler))
        compact_instruments(staging, curated, run, date(2026, 9, 10))

    out = pl.read_parquet(curated / "instruments" / "part-merged.parquet")
    marks = dict(zip(out["symbol"].to_list(), out["delist_date"].to_list(), strict=True))
    # The traded name is still inferred delisted after two absences.
    assert marks["600519.SH"] == date(2026, 9, 10)
    # The pending listing is left alone.
    assert marks["301688.SZ"] is None


def test_a_relisted_symbol_drops_the_inferred_delist_date_that_predates_it(tmp_path):
    """TDX lists a new code, drops it, then lists it for real.

    The run of absences earns an inferred delist_date; when the code comes back
    with a real listing date the sticky coalesce kept that inference, leaving
    `delist_date` 13 days *before* `list_date`. Those rows reach
    `delisting_events` and the survivorship check as a delisting that never
    happened.
    """
    cfg = Config(data_root=tmp_path / "data")
    curated_path = cfg.curated_root / "instruments" / "part-merged.parquet"
    curated_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            # Buried by a prior run while it had no listing date of its own.
            _instrument("159025.SZ", list_date=None, delist_date=date(2026, 9, 1)),
            # Same damage, but the vendor has since dropped the code again, so
            # it is preserved rather than incoming. A contradiction does not
            # stop being one because the name is no longer listed.
            _instrument("158032.SZ", list_date=date(2026, 9, 14), delist_date=date(2026, 9, 1)),
            # A genuine retirement: listed first, delisted after.
            _instrument("600001.SH", list_date=date(2000, 1, 1), delist_date=date(2026, 9, 1)),
            _instrument("600519.SH", list_date=date(2001, 8, 27)),
        ]
    ).write_parquet(curated_path)

    StagingWriter(cfg.staging_root).write_batch(
        "instruments",
        "run-relist",
        "batch-0",
        pl.DataFrame(
            [
                # Back in the live snapshot, now with a real listing date.
                _instrument("159025.SZ", list_date=date(2026, 9, 14)),
                _instrument("600519.SH", list_date=date(2001, 8, 27)),
            ]
        ),
    )

    compact_instruments(cfg.staging_root, cfg.curated_root, "run-relist", date(2026, 9, 15))

    merged = pl.read_parquet(curated_path)
    relisted = merged.filter(pl.col("symbol") == "159025.SZ").to_dicts()[0]
    assert relisted["list_date"] == date(2026, 9, 14)
    assert relisted["delist_date"] is None, "a live listing refutes the earlier inference"

    absent = merged.filter(pl.col("symbol") == "158032.SZ").to_dicts()[0]
    assert absent["delist_date"] is None, "an absent name's contradiction is still a contradiction"

    # The retired name keeps its date: this must not become a resurrection path.
    retired = merged.filter(pl.col("symbol") == "600001.SH").to_dicts()[0]
    assert retired["delist_date"] == date(2026, 9, 1)

    inverted = merged.filter(
        pl.col("list_date").is_not_null()
        & pl.col("delist_date").is_not_null()
        & (pl.col("delist_date") < pl.col("list_date"))
    )
    assert inverted.is_empty()
