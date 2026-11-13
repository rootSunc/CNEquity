"""Local evidence for derivative ingestion and derived dependency invalidation.

Receipts describe the downloaded, validated rows, not the entire live market.
They are usable only while their content matches committed rows. Unknown or
rejected sessions are durable debt, including outside the reconciliation tail.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import date
from functools import lru_cache
from pathlib import Path

import polars as pl

from cnequity.storage.atomic import write_json_atomic

VERSION = 1
# Recorded immediately before the 2026 CZCE expiry-row / HTTP 567 repair.
# The only bar parser changed in that repair is CZCE options. Existing row
# identities still have to match before a receipt can be reused.
PRE_CZCE_EXPIRY_REPAIR_PARSER = "021303a3be1773e5c80ee8b03bc8d4f9e813cf1b43251d4f0af26409c14f1590"
# Extending the offline SHFE annual-workbook year allowlist does not alter
# any daily-file parser; daily receipts from immediately before that change
# remain valid if their canonical row identities still match.
PRE_2004_ARCHIVE_PARSER = "6eed06d3a7c36058315c435e3d7d43f08a42b4b356e31f9fb894ba8a36d9663c"
# Last daily parser identity before excluding the offline annual reader from
# the daily-file fingerprint. Its rows still need an exact content match.
PRE_ARCHIVE_DECOUPLING_PARSER = "6defea465c530310b260c6c0d42cb9919a3c3610016a0ba201fc1e1c5b65e9c1"
# Adding INE's separate 2018 futures file changes only the SHF route for
# those sessions. Other previously captured rows remain valid on exact match.
PRE_INE_2018_ROUTE_PARSER = "9dd704704f99b99f9e5539b0ef1103cbb7714d2e83611ce06fb89dd73b8e2e30"
# Historical receipts preceded later reader refactoring. Reuse this hash
# only for the validated dates and routes below, with exact row matching.
PRE_HISTORICAL_READER_REFACTOR_PARSER = (
    "7ecb159fb455912cb82a2a7bc7b14885dbe82c6f1f2443d09f6b506a87442115"
)


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def fingerprint(root: Path) -> str:
    """Content identity, independent of revision paths and preserved mtimes."""
    digest = hashlib.sha256()
    files = [root] if root.is_file() else sorted(root.glob("**/*.parquet"))
    for path in files:
        digest.update(path.name.encode())
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    return digest.hexdigest()


@lru_cache(maxsize=1)
def parser_identity() -> str:
    package = Path(__file__).parents[1]
    files = sorted(
        path
        for path in (package / "adapters" / "futures_exchange").glob("*.py")
        # This reader never parses daily bars. Its independent changes must
        # not invalidate daily receipts and trigger a historical redownload.
        if path.name != "shfe_archive.py"
    )
    files += [
        package / "adapters" / "sina" / "dce_futures.py",
        package / "domain" / "schemas.py",
        package / "domain" / "derivatives.py",
    ]
    return hashlib.sha256("".join(fingerprint(path) for path in files).encode()).hexdigest()


def row_identity(frame: pl.DataFrame) -> str:
    columns = sorted(set(frame.columns) - {"fetched_at", "data_version", "source"})
    content = frame.select(columns).sort("symbol").write_json()
    return hashlib.sha256(content.encode()).hexdigest()


def receipt_path(config, dataset: str, day: date, exchange: str) -> Path:
    return config.meta_root / "derivatives" / dataset / day.isoformat() / f"{exchange}.json"


def record_session(
    config,
    dataset: str,
    day: date,
    exchange: str,
    frame: pl.DataFrame | None,
    *,
    error: str | None = None,
    original_rows: int | None = None,
) -> None:
    receipt = {
        "version": VERSION,
        "parser": parser_identity(),
        "captured_at": time.time(),
        "date": day.isoformat(),
        "exchange": exchange,
        "route": getattr(config, "futures_dce_route", "sina") if exchange == "DCE" else "official",
        "state": "owed" if error or frame is None or frame.is_empty() else "captured",
        "error": error,
    }
    if frame is not None and not frame.is_empty():
        receipt.update(
            rows=frame.height,
            original_rows=original_rows or frame.height,
            rejected_rows=(original_rows or frame.height) - frame.height,
            symbols=sorted(frame["symbol"].unique().to_list()),
            identity=row_identity(frame),
        )
    write_json_atomic(receipt_path(config, dataset, day, exchange), receipt)


def session_matches(config, dataset: str, day: date, exchange: str, frame: pl.DataFrame) -> bool:
    receipt = read_json(receipt_path(config, dataset, day, exchange))
    route = getattr(config, "futures_dce_route", "sina") if exchange == "DCE" else "official"
    needs_ine_2018_route = (
        dataset == "futures_bars"
        and exchange == "SHF"
        and date(2018, 3, 26) <= day <= date(2018, 12, 31)
    )
    return (
        receipt.get("version") == VERSION
        and receipt.get("state") in {"captured", "committed"}
        and (
            receipt.get("parser") == parser_identity()
            or (
                not needs_ine_2018_route
                and (
                    receipt.get("parser") == PRE_INE_2018_ROUTE_PARSER
                    or (
                        receipt.get("parser") == PRE_HISTORICAL_READER_REFACTOR_PARSER
                        and date(2026, 7, 1) <= day <= date(2026, 8, 24)
                        and (dataset, exchange)
                        in {
                            ("futures_bars", "CZC"),
                            ("futures_bars", "CFE"),
                            ("futures_bars", "GFE"),
                            ("futures_bars", "SHF"),
                            ("option_bars", "CFE"),
                            ("option_bars", "GFE"),
                            ("option_bars", "SHF"),
                        }
                    )
                    or receipt.get("parser") == PRE_ARCHIVE_DECOUPLING_PARSER
                    or receipt.get("parser") == PRE_2004_ARCHIVE_PARSER
                    or (
                        receipt.get("parser") == PRE_CZCE_EXPIRY_REPAIR_PARSER
                        and (dataset, exchange) != ("option_bars", "CZC")
                    )
                )
            )
        )
        and receipt.get("route") == route
        and not frame.is_empty()
        and receipt.get("identity") == row_identity(frame)
    )


def _pending(config, dataset: str, through: date):
    root = config.meta_root / "derivatives" / dataset
    return [
        (p, read_json(p))
        for p in root.glob("*/*.json")
        if date.fromisoformat(p.parent.name) <= through
        and read_json(p).get("state") in {"owed", "captured"}
    ]


def _committed_frames(config, dataset: str, dates: list[date]) -> dict:
    from cnequity.domain.canonical import dedupe_lazy_by_primary_key
    from cnequity.storage.published import published

    scan = published(config, dataset).scan()
    if scan is None or not dates:
        return {}
    frame = dedupe_lazy_by_primary_key(
        scan.filter(pl.col("trade_date").is_in(dates)), dataset
    ).collect()
    # The SHF route publishes both SHF and INE, so its existing receipt
    # describes the combined rows. An INE-only historical import can also
    # carry its own receipt without replacing that SHF evidence.
    published = frame.with_columns(pl.col("exchange").replace({"INE": "SHF"}).alias("_publisher"))
    frames = {
        key: part.drop("_publisher")
        for key, part in published.partition_by(["trade_date", "_publisher"], as_dict=True).items()
    }
    frames.update(
        {
            key: part
            for key, part in frame.filter(pl.col("exchange") == "INE")
            .partition_by(["trade_date", "exchange"], as_dict=True)
            .items()
        }
    )
    return frames


def owed_sessions(config, dataset: str, through: date) -> list[date]:
    from cnequity.adapters.futures_exchange.shfe import INE_DIRECT_LAST_SESSION
    from cnequity.domain.derivatives import UNPUBLISHED_FUTURES_SESSIONS

    # A day the exchange never published is not a debt any fetch can pay, and
    # neither is an INE receipt after INE's own route ended: from 2019 its
    # contracts arrive in the SHFE file, whose SHF receipt covers them. One such
    # receipt, left on 2021-07-28 by an older routing, stayed owed forever.
    unpublished = UNPUBLISHED_FUTURES_SESSIONS if dataset == "futures_bars" else {}

    def owes(path: Path) -> bool:
        day = date.fromisoformat(path.parent.name)
        if path.stem == "INE" and day > INE_DIRECT_LAST_SESSION:
            return False
        return day not in unpublished.get(path.stem, ())

    pending = [(p, r) for p, r in _pending(config, dataset, through) if owes(p)]
    dates = [date.fromisoformat(p.parent.name) for p, r in pending if r.get("state") == "captured"]
    frames = _committed_frames(config, dataset, dates)
    return sorted(
        {
            date.fromisoformat(p.parent.name)
            for p, r in pending
            if r.get("state") == "owed"
            or not session_matches(
                config,
                dataset,
                date.fromisoformat(p.parent.name),
                p.stem,
                frames.get((date.fromisoformat(p.parent.name), p.stem), pl.DataFrame()),
            )
        }
    )


def confirm_receipts(config, dataset: str) -> None:
    """Called after revision publication; a download alone cannot settle debt."""
    pending = _pending(config, dataset, date.max)
    dates = [date.fromisoformat(p.parent.name) for p, r in pending if r.get("state") == "captured"]
    frames = _committed_frames(config, dataset, dates)
    for path, receipt in pending:
        day = date.fromisoformat(path.parent.name)
        if session_matches(
            config, dataset, day, path.stem, frames.get((day, path.stem), pl.DataFrame())
        ):
            write_json_atomic(path, {**receipt, "state": "committed"})
