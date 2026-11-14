import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.adapters.exchange.fund_volume import (
    PARAMS,
    PEER_URL,
    SZ_URL_ROOT,
    URL_ROOT,
    fund_dayk_url,
    fund_peer_params,
    parse_sse_fund_dayk,
)
from cnequity.config import Config
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.query import load
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.repairs.fund_volume import repair_fund_volume
from cnequity.storage.revisions import RevisionStore

DAY = date(2017, 4, 6)
SYMBOL = "511950.SH"
PAYLOAD = {
    "code": "511950",
    "total": 1,
    "begin": 0,
    "end": 1,
    "kline": [[20170406, 99.981, 99.984, 99.981, 99.984, 51, 5099]],
}
SZ_PAYLOAD = {
    **PAYLOAD,
    "code": "160323",
    "kline": [[20170406, 0.933, 0.933, 0.933, 0.933, 9, 8]],
}


def test_publisher_quantity_is_used_without_reverse_calculating_rounding():
    row = parse_sse_fund_dayk(json.dumps(PAYLOAD).encode(), SYMBOL).row(0, named=True)
    assert row["volume"] == 51 and row["amount"] == 5099


def test_shenzhen_odd_lot_uses_published_shares_and_yuan_rounding():
    assert fund_dayk_url("160323.SZ") == SZ_URL_ROOT + "160323"
    assert fund_dayk_url(SYMBOL) == URL_ROOT + "511950"
    row = parse_sse_fund_dayk(json.dumps(SZ_PAYLOAD).encode(), "160323.SZ").row(0, named=True)
    assert row["volume"] == 9 and row["amount"] == 8
    with pytest.raises(ValueError):
        parse_sse_fund_dayk(json.dumps(SZ_PAYLOAD).encode(), "160323.SH")
    with pytest.raises(ValueError):
        fund_dayk_url("000001.SZ")


@pytest.mark.parametrize(
    "change",
    [
        {"code": "511930"},
        {"total": 2},
        {"begin": 1},
        {"kline": [[20170406, 99.981, 99.984, 99.981, 99.984, 0.51, 5099]]},
        {"kline": [[20170406, 99.981, 99.984, 99.981, 99.984, 100, 5099]]},
    ],
)
def test_invalid_identity_quantity_and_turnover_fail(change):
    with pytest.raises(ValueError):
        parse_sse_fund_dayk(json.dumps({**PAYLOAD, **change}).encode(), SYMBOL)


@pytest.mark.parametrize("mismatch", [False, True])
@pytest.mark.parametrize("wrong_market", [False, True])
@pytest.mark.parametrize(
    "symbol,payload,amount,peer,accepted",
    [
        (SYMBOL, PAYLOAD, 5099.091796875, None, True),
        ("160323.SZ", SZ_PAYLOAD, 8.397, None, True),
        ("160323.SZ", SZ_PAYLOAD, 104.022, "agree", True),
        ("160323.SZ", SZ_PAYLOAD, 104.022, "raw_only", True),
        ("160323.SZ", SZ_PAYLOAD, 104.022, "adjusted", False),
        ("160323.SZ", SZ_PAYLOAD, 104.022, "wrong_price", False),
        ("160323.SZ", SZ_PAYLOAD, 104.022, "wrong_date", False),
        ("160323.SZ", SZ_PAYLOAD, 104.022, None, False),
    ],
)
def test_offline_repair_is_retained_idempotent_and_requires_same_prices(
    tmp_path, mismatch, wrong_market, symbol, payload, amount, peer, accepted, settle
):
    cfg = Config(data_root=tmp_path)
    now = datetime(2026, 10, 13, tzinfo=timezone.utc)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    instrument = {
        "symbol": symbol,
        "code": symbol.split(".")[0],
        "exchange": symbol.split(".")[1],
        "name": "fund",
        "asset_type": "etf",
        "list_date": date(2016, 12, 19),
        "source": "exchange",
        "data_version": "v1",
        "fetched_at": now,
    }
    bar = {
        **parse_sse_fund_dayk(json.dumps(payload).encode(), symbol).row(0, named=True),
        "volume": 0,
        "amount": amount,
        "source": "tdx_protocol",
        "data_version": "v2",
        "fetched_at": now.replace(year=2025),
    }
    if mismatch:
        bar["high"] = 100.0
    for dataset, rows in [("instruments", [instrument]), ("daily_bars", [bar])]:
        root = cfg.curated_root / dataset
        if dataset == "daily_bars":
            root /= "trade_date=2017-04-06"
        root.mkdir(parents=True)
        path = root / "part-merged.parquet"
        pl.DataFrame(rows, schema=DATASET_SCHEMAS[dataset]).write_parquet(path)
        store.commit(
            dataset,
            run_id="seed",
            changed_files=[path],
            schema_version=1,
            contract_fingerprint="fixture",
        )
    old = store.current_root("daily_bars")
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["daily_bars"])
    receipt = archive.archive(
        "daily_bars",
        json.dumps(payload).encode(),
        source="exchange",
        run_id="source",
        url=(
            (SZ_URL_ROOT if symbol.endswith(".SH") else URL_ROOT) + symbol.split(".")[0]
            if wrong_market
            else fund_dayk_url(symbol)
        ),
        request_params=PARAMS,
        payload_format="json",
    )
    write_json_atomic(cfg.meta_root / f"source_cache/fund_volume/{symbol}.json", receipt.to_dict())
    if peer is not None:
        day = "2017-04-07" if peer == "wrong_date" else "2017-04-06"
        price = "0.934" if peer == "wrong_price" else "0.933"
        peer_payload = {
            "data": {
                "code": "160323",
                "market": 0,
                "klines": [f"{day},{price},{price},{price},{price},0,8,0,0,0,0"],
            }
        }
        peer_receipt = archive.archive(
            "daily_bars",
            json.dumps(peer_payload).encode(),
            source="eastmoney",
            run_id="peer",
            url=PEER_URL,
            request_params={
                **fund_peer_params(
                    symbol,
                    date(2017, 1, 1) if peer == "raw_only" else DAY,
                    date(2019, 12, 31) if peer == "raw_only" else DAY,
                ),
                "fqt": "1" if peer == "adjusted" else "0",
            },
            payload_format="json",
        )
        if peer != "raw_only":
            write_json_atomic(
                cfg.meta_root / f"source_cache/fund_volume/{symbol}-peer.json",
                peer_receipt.to_dict(),
            )
    settle(cfg)
    report = repair_fund_volume(cfg, apply=True)
    repaired = accepted and not mismatch and not wrong_market
    assert report["applied"] is repaired
    if wrong_market:
        assert "archive request identity differs" in report["failures"][0]["error"]
    row = load("daily_bars", config=cfg).row(0, named=True)
    assert row["volume"] == (payload["kline"][0][5] if repaired else 0)
    if repaired:
        assert row["amount"] == payload["kline"][0][6]
        if peer == "raw_only":
            assert (cfg.meta_root / f"source_cache/fund_volume/{symbol}-peer.json").exists()
            assert len(report["source_receipts"]) == 2
    from cnequity.query.parquet_scan import scan_parquet_root

    assert scan_parquet_root(old, committed=False).collect().height == 1
    assert not repair_fund_volume(cfg, apply=True)["applied"]
