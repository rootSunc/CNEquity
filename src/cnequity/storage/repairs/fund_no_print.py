"""Bounded publisher evidence repair for outstanding exchange fund sessions."""

import json
from contextlib import nullcontext
from datetime import date
from uuid import uuid4

import httpx

from cnequity.adapters.exchange.etf_profiles import _get_official_response
from cnequity.adapters.exchange.fund_quotes import (
    REFERER,
    SSE_REFERER,
    SSE_URL,
    URL,
    parameters,
    report_no_print_symbols,
    sse_parameters,
)
from cnequity.domain.market_time import shanghai_today
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.instrument_catalog import load_curated_instruments
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive


def _owed_fund_sessions(config) -> list[dict]:
    """The (symbol, trade_date) daily-bar cells the coverage planner still owes."""
    from cnequity.quality.planned_coverage import owed_cells

    return [
        {"symbol": symbol, "trade_date": day.isoformat()}
        for symbol, day in owed_cells(config, "daily_bars", None, shanghai_today())
    ]


def repair_fund_no_print(config, *, apply=False, refresh_sources=False, max_sessions=32):
    if max_sessions < 1:
        raise ValueError("max_sessions must be positive")
    owed = _owed_fund_sessions(config)
    catalog = load_curated_instruments(config)
    bounds = (
        {
            r["symbol"]: (r["list_date"], r["delist_date"])
            for r in catalog.iter_rows(named=True)
            if r.get("asset_type") in {"etf", "lof"} and r.get("list_date") is not None
        }
        if catalog is not None
        else {}
    )
    owed = [
        r
        for r in owed
        if r["symbol"] in bounds
        and bounds[r["symbol"]][0] <= date.fromisoformat(r["trade_date"])
        and (
            bounds[r["symbol"]][1] is None
            or date.fromisoformat(r["trade_date"]) < bounds[r["symbol"]][1]
        )
    ]
    days = sorted(
        {date.fromisoformat(r["trade_date"]) for r in owed if r["symbol"].endswith((".SZ", ".SH"))}
    )
    days = [day for day in days if day < shanghai_today()]
    selected = days[:max_sessions]
    cache = config.meta_root / "source_cache" / "fund_no_print"
    archive = RawPayloadArchive(config.meta_root, enabled=True, datasets=["daily_bars"])
    run_id = "fund-no-print-" + uuid4().hex
    candidates, failures = [], []
    if refresh_sources and not config.sources.get("exchange", True):
        raise ValueError("exchange source disabled")
    requests = []
    for day in selected:
        wanted = {r["symbol"] for r in owed if r["trade_date"] == str(day)}
        sz = {s for s in wanted if s.endswith(".SZ")}
        if sz:
            requests.append((day, str(day), URL, parameters(day), REFERER, "xlsx", sz))
        for symbol in sorted(s for s in wanted if s.endswith(".SH")):
            requests.append(
                (
                    day,
                    f"{day}-{symbol}",
                    SSE_URL,
                    sse_parameters(symbol, day),
                    SSE_REFERER,
                    "json",
                    {symbol},
                )
            )
    with httpx.Client(timeout=45, follow_redirects=True) as client:
        for day, key, url, params, referer, kind, wanted in requests:
            path = cache / f"{key}.json"
            try:
                if refresh_sources:
                    response = _get_official_response(
                        config, client, url, params=params, referer=referer
                    )
                    identity = {
                        "source": "exchange",
                        "dataset": "daily_bars",
                        "url": url,
                        "request_params": params,
                    }
                    served = report_no_print_symbols(response.content, identity, day)
                    record = archive.archive(
                        "daily_bars",
                        response.content,
                        source="exchange",
                        run_id=run_id,
                        request_params=params,
                        url=url,
                        payload_format=kind,
                    ).to_dict()
                    write_json_atomic(path, record, indent=2)
                else:
                    record = json.loads(path.read_text())
                    if record.get("url") != url or record.get("request_params") != params:
                        raise ValueError("cached report identity differs")
                    served = report_no_print_symbols(archive.read(record), record, day)
                candidates.append(
                    {
                        "schema_version": 1,
                        "trade_date": str(day),
                        "report_key": key,
                        "symbols": sorted(wanted & served),
                        "archive": record,
                    }
                )
            except (OSError, ValueError, RawArchiveError, httpx.HTTPError) as exc:
                failures.append({"trade_date": str(day), "report_key": key, "reason": str(exc)})
                if isinstance(exc, httpx.TransportError):
                    break
    report = {
        "applied": False,
        "sessions": len({r["trade_date"] for r in candidates}),
        "reports": len(candidates),
        "proved_keys": sum(len(r["symbols"]) for r in candidates),
        "failures": failures,
        "deferred_sessions": max(0, len(days) - len(selected)),
        "scope": "SZSE zero-yuan turnover or SSE explicit absence of auction prices (not all transactions); no halt or synthetic bar.",
    }
    with lake_mutation_lock(config.meta_root) if apply else nullcontext():
        if apply:
            for proof in candidates:
                if proof["symbols"]:
                    path = (
                        config.meta_root
                        / "repairs"
                        / "fund_no_print"
                        / f"{proof['report_key']}.json"
                    )
                    # Retain earlier selections only when the new report also
                    # proves them. Applying a later debt subset is cumulative.
                    if path.exists():
                        try:
                            previous = json.loads(path.read_text())
                            old_symbols = previous.get("symbols", [])
                            served = report_no_print_symbols(
                                archive.read(proof["archive"]),
                                proof["archive"],
                                date.fromisoformat(proof["trade_date"]),
                            )
                            if isinstance(old_symbols, list):
                                proof["symbols"] = sorted(
                                    set(proof["symbols"]) | (set(old_symbols) & served)
                                )
                        except (OSError, ValueError, TypeError, RawArchiveError):
                            pass
                    write_json_atomic(path, proof, indent=2)
            from cnequity.storage.fund_no_print import approved_no_print_keys

            # The planner excuses what the proofs cover; what it still owes is
            # what is left.
            report.update(
                applied=True,
                proved_keys=len(approved_no_print_keys(config)),
                remaining_keys=len(_owed_fund_sessions(config)),
            )
            write_json_atomic(config.meta_root / "repairs" / f"{run_id}.json", report, indent=2)
    return report
