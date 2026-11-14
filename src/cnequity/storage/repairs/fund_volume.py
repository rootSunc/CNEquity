"""Replayable publisher repair for zero-quantity, positive-turnover fund bars."""

import json
from datetime import datetime
from uuid import uuid4

import httpx
import polars as pl

from cnequity.adapters.exchange.etf_profiles import _get_official_response
from cnequity.adapters.exchange.fund_volume import (
    PARAMS,
    PEER_URL,
    fund_dayk_url,
    fund_peer_params,
    parse_fund_peer,
    parse_sse_fund_dayk,
)
from cnequity.domain.canonical import dedupe_lazy_by_primary_key
from cnequity.domain.schemas import data_version_for, with_provenance
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.lake_write import planned_on
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.read_context import ReadContext
from cnequity.storage.repairs.publish import publish_staged
from cnequity.storage.revisions import RevisionStore

DATASET = "daily_bars"
KEYS = ["symbol", "trade_date"]


def _targets(context):
    catalog = (
        scan_parquet_root(context.roots["instruments"], committed=False)
        .filter(pl.col("asset_type").is_in(["etf", "lof"]) & pl.col("list_date").is_not_null())
        .select("symbol", "list_date", "delist_date")
        .collect()
    )
    scan = scan_parquet_root(
        context.roots[DATASET], committed=False, symbols=catalog["symbol"].to_list()
    )
    # Discover only candidate keys first; canonicalize all observations for
    # those keys before deciding whether a defect still exists. Sorting every
    # healthy historical fund bar under the publication lock is unnecessary.
    keys = (
        scan.filter((pl.col("volume") == 0) & (pl.col("amount") > 0))
        .select(KEYS)
        .unique()
        .collect()
    )
    if keys.is_empty():
        return scan.limit(0).collect().join(catalog, on="symbol")
    candidates = scan_parquet_root(
        context.roots[DATASET],
        committed=False,
        symbols=keys["symbol"].unique().to_list(),
        partition_col="trade_date",
        start=keys["trade_date"].min(),
        end=keys["trade_date"].max(),
    ).join(keys.lazy(), on=KEYS, how="semi")
    return (
        dedupe_lazy_by_primary_key(candidates, DATASET)
        .filter((pl.col("volume") == 0) & (pl.col("amount") > 0))
        .collect()
        .join(catalog, on="symbol")
        .filter(
            (pl.col("trade_date") >= pl.col("list_date"))
            & (pl.col("delist_date").is_null() | (pl.col("trade_date") < pl.col("delist_date")))
        )
    )


def _matching(targets, incoming, peer=None):
    if incoming.is_empty():
        return incoming
    shared = targets.join(incoming, on=KEYS, suffix="_official")
    prices = shared.filter(
        pl.all_horizontal(
            (pl.col(c) - pl.col(c + "_official")).abs() <= 0.00001
            for c in ("open", "high", "low", "close")
        )
    )
    keys = prices.filter(
        (pl.col("amount") - pl.col("amount_official")).abs()
        <= pl.max_horizontal(pl.lit(1.0), pl.col("amount").abs() * 2e-7)
    ).select(KEYS)
    if peer is not None and not peer.is_empty():
        peer_keys = _matching(peer, incoming).select(KEYS)
        corroborated = prices.select(KEYS).join(peer_keys, on=KEYS, how="semi")
        keys = pl.concat([keys, corroborated]).unique()
    return incoming.join(keys, on=KEYS, how="semi")


def _peer_identity(receipt, params):
    if not isinstance(receipt, dict) or not isinstance(receipt.get("request_params"), dict):
        return False
    saved = receipt.get("request_params", {})
    return (
        receipt.get("url") == PEER_URL
        and isinstance(receipt.get("captured_at"), str)
        and receipt.get("source") == "eastmoney"
        and receipt.get("dataset") == DATASET
        and all(saved.get(k) == v for k, v in params.items() if k not in {"beg", "end"})
        and isinstance(saved.get("beg"), str)
        and isinstance(saved.get("end"), str)
        and len(saved["beg"]) == len(saved["end"]) == 8
        and saved["beg"].isdigit()
        and saved["end"].isdigit()
        and saved["beg"] <= params["beg"] <= params["end"] <= saved["end"]
    )


def _cached_peer(config, archive, path, params, symbol):
    """Reuse matching raw evidence before spending another request budget."""
    if path.exists():
        receipt = json.loads(path.read_text())
        if not _peer_identity(receipt, params):
            raise ValueError("fund peer archive request identity differs")
        return receipt, parse_fund_peer(archive.read(receipt), symbol)
    root = config.meta_root / "raw" / DATASET / "source=eastmoney"
    compatible = []
    metadata_paths = (
        path
        for directory in sorted(root.glob("captured_date=*"), reverse=True)
        for path in directory.glob("*.json")
    )
    for count, metadata in enumerate(metadata_paths):
        if count >= 512:
            break
        try:
            receipt = json.loads(metadata.read_text())
            if _peer_identity(receipt, params):
                compatible.append(receipt)
        except (OSError, ValueError, AttributeError):
            continue
    for receipt in sorted(compatible, key=lambda r: r.get("captured_at", ""), reverse=True):
        try:
            return receipt, parse_fund_peer(archive.read(receipt), symbol)
        except (OSError, ValueError, RuntimeError):
            continue
    return None


def repair_fund_volume(config, *, apply=False, refresh_sources=False, max_symbols=16):
    if max_symbols < 1:
        raise ValueError("max_symbols must be positive")
    if refresh_sources and not config.sources.get("exchange", True):
        raise ValueError("exchange source disabled")
    context = ReadContext.capture(config, (DATASET, "instruments"), {})
    targets = _targets(context)
    symbols = (
        targets.filter(
            pl.col("symbol").str.ends_with(".SH") | pl.col("symbol").str.ends_with(".SZ")
        )["symbol"]
        .unique()
        .sort()
        .to_list()
    )
    selected = symbols[:max_symbols]
    archive = RawPayloadArchive(config.meta_root, enabled=True, datasets=[DATASET])
    cache = config.meta_root / "source_cache" / "fund_volume"
    run_id = "fund-volume-" + uuid4().hex
    frames, peers, receipts, failures = [], [], [], []
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for symbol in selected:
            path = cache / f"{symbol}.json"
            try:
                url = fund_dayk_url(symbol)
                if refresh_sources:
                    response = _get_official_response(
                        config, client, url, params=PARAMS, referer="https://etf.sse.com.cn/"
                    )
                    parse_sse_fund_dayk(response.content, symbol)
                    receipt = archive.archive(
                        DATASET,
                        response.content,
                        source="exchange",
                        run_id=run_id,
                        url=url,
                        request_params=PARAMS,
                        payload_format="json",
                    ).to_dict()
                    write_json_atomic(path, receipt)
                receipt = json.loads(path.read_text())
                if (
                    receipt.get("url") != url
                    or receipt.get("request_params") != PARAMS
                    or receipt.get("source") != "exchange"
                    or receipt.get("dataset") != DATASET
                ):
                    raise ValueError("SSE dayk archive request identity differs")
                official = parse_sse_fund_dayk(archive.read(receipt), symbol)
                rows = with_provenance(
                    official,
                    source="exchange",
                    data_version=data_version_for(DATASET),
                ).with_columns(
                    pl.lit(datetime.fromisoformat(receipt["captured_at"])).alias("fetched_at")
                )
                frames.append(rows)
                receipts.append(receipt)
                target = targets.filter(pl.col("symbol") == symbol)
                unresolved = target.join(
                    _matching(target, official).select(KEYS), on=KEYS, how="anti"
                )
                if not unresolved.is_empty():
                    try:
                        params = fund_peer_params(
                            symbol, unresolved["trade_date"].min(), unresolved["trade_date"].max()
                        )
                        peer_path = cache / f"{symbol}-peer.json"
                        cached = _cached_peer(config, archive, peer_path, params, symbol)
                        if (
                            cached is None
                            and refresh_sources
                            and config.sources.get("eastmoney", True)
                        ):
                            from cnequity.adapters.eastmoney.em_auth import EastMoneyClient

                            with EastMoneyClient(config=config) as peer_client:
                                response = peer_client.get(PEER_URL, params=params)
                            response.raise_for_status()
                            peer_rows = parse_fund_peer(response.content, symbol)
                            peer_receipt = archive.archive(
                                DATASET,
                                response.content,
                                source="eastmoney",
                                run_id=run_id,
                                url=PEER_URL,
                                request_params=params,
                                payload_format="json",
                            ).to_dict()
                            cached = peer_receipt, peer_rows
                        if cached is None:
                            raise ValueError(
                                "no archived independent fund peer; refresh sources to collect it"
                            )
                        peer_receipt, peer_rows = cached
                        if refresh_sources or apply:
                            write_json_atomic(peer_path, peer_receipt)
                        peers.append(peer_rows)
                        receipts.append(peer_receipt)
                    except (OSError, ValueError, RuntimeError, httpx.HTTPError) as exc:
                        failures.append({"symbol": symbol, "lane": "peer", "error": str(exc)})
            except (OSError, ValueError, RuntimeError, httpx.HTTPError) as exc:
                failures.append({"symbol": symbol, "error": str(exc)})
    incoming = pl.concat(frames) if frames else pl.DataFrame()
    peer = pl.concat(peers) if peers else None
    matching = _matching(targets, incoming, peer)
    report = {
        "applied": False,
        "targets": targets.height,
        "rows": matching.height,
        "rows_peer_confirmed": matching.height - _matching(targets, incoming).height,
        "remaining": targets.height - matching.height,
        "symbols_selected": selected,
        "symbols_pending_batch_limit": len(symbols) - len(selected),
        "failures": failures,
        "source_receipts": receipts,
        "scope": "SSE auction quantities; stored OHLC and stored or independent peer amount must agree",
    }
    if not apply or matching.is_empty():
        return report
    with planned_on(config):
        store = RevisionStore(config.meta_root, config.curated_root, config.derived_root)
        store.ensure_current(DATASET)
        live = ReadContext.capture(config, (DATASET, "instruments"), {})
        live_targets = _targets(live)
        matching = _matching(live_targets, incoming, peer)
        report.update(
            targets=live_targets.height,
            rows=matching.height,
            rows_peer_confirmed=matching.height - _matching(live_targets, incoming).height,
            remaining=live_targets.height - matching.height,
        )
        if matching.is_empty():
            return report
        published = publish_staged(
            config,
            store,
            DATASET,
            matching,
            run_id=run_id,
            base_root=live.roots[DATASET],
            metadata={
                "reason": "official_fund_auction_quantity",
                **report,
            },
            batch="official-0",
            partition_col="trade_date",
        )
        if published is None:
            raise RuntimeError("fund quantity repair produced no revision")
        report.update(applied=True, revision=published["revision"])
    return report
