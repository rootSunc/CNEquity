"""Read-only projection of one lake, shaped for the dashboard.

Every value here comes from something already on disk — the registry, the
directory layout, ``meta/stats``, ``meta/quality/health-latest.json``, the
manifest. **Nothing in this module scans curated.** A request that reads parquet
is a request that gets slower as the lake grows, which is the failure mode the
stats tables exist to prevent.

Two things this deliberately does *not* do:

* It does not open ``data/duckdb/cnequity.duckdb``. DuckDB allows many
  readers or one writer, so a held read handle would make
  ``ensure_duckdb_views()`` fail during the nightly run — the dashboard would
  break ingestion. Views are rebuilt in a private in-memory database instead;
  they are generated from the registry and cost milliseconds.
* It does not recompute audit findings. ``lake_health()`` walks the lake; the
  dashboard reads the JSON that ``cne audit --full`` already wrote. A page view
  must not cost what an audit costs.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import (
    DATASETS,
    TIER_LABELS,
    TIERS,
    empty_freshness_label,
    freshness_state,
    history_mode_for,
    is_dataset_enabled,
)
from cnequity.domain.market_time import is_session_final, shanghai_today
from cnequity.domain.partitions import parse_partition
from cnequity.i18n import _, current_language
from cnequity.storage.stats import (
    load_partition_stats,
    load_provenance_stats,
    refresh_stats_if_stale,
    stats_freshness,
)

logger = logging.getLogger(__name__)

# The catalog walks partition directories. Cheap, but the overview page fans out
# to several endpoints at once and they would each redo it.
_CACHE_TTL_SECONDS = 30.0

# Heatmap cell alphabet. One char per (dataset, day) keeps a 40x250 grid a few
# kilobytes instead of ten thousand JSON objects.
CELL_COVERED = "#"
CELL_GAP = "."
CELL_OUTSIDE = " "
CELL_UNPARTITIONED = "-"


@dataclass
class _Cached:
    value: Any
    at: float


# Statuses the runs list may filter on. Anything else is a typo, not a quiet empty page.
RUN_STATUSES = frozenset(
    {
        "running",
        "success",
        "failed",
        "degraded",
        "warning",
        "interrupted",
        "skipped",
        "skipped_non_trading_day",
    }
)
# The runs page "needs attention" count. A later attempt of the same job
# replaces an older one, so only the latest non-skip run of each scope can
# still be open, and only when it is one of these.
_OPEN_ATTENTION_STATUSES = ("failed", "degraded", "warning", "interrupted")
# A skip is not an attempt. It must not hide the failure that came before it.
_NON_ATTEMPT_STATUSES = ("skipped", "skipped_non_trading_day")
# These job names are shared across datasets. The first planned step is the
# scope, so a later backfill of one dataset does not close another.
_STEP_SCOPED_JOBS = ("backfill", "derivatives-rebuild")
# Batch rows that still need an operator. Success (including a recovered
# attempt that kept its error text), superseded, and stale are closed.
_ACTIONABLE_BATCH_STATUSES = ("failed", "warning", "degraded", "interrupted", "blocked")


def _launch_id(metadata_json: str | None) -> str | None:
    if not metadata_json:
        return None
    try:
        launch = json.loads(metadata_json).get("launch") or {}
    except (TypeError, ValueError):
        return None
    ident = launch.get("id")
    return ident if isinstance(ident, str) else None


def _failure_line(message: str) -> str:
    """The reason itself, without the HTTP client's documentation trailer."""
    line = str(message).splitlines()[0].strip()
    trailer = " For more information check:"
    cut = line.find(trailer)
    if cut != -1:
        line = line[:cut].rstrip()
    return line


def _jsonable(value: Any) -> Any:
    """Cell values as JSON, without inventing a type the column does not have."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _gap_meaning(spec) -> str:
    """Whether a missing day on this dataset is a fault or just its shape.

    Two ways a hole is expected rather than wrong, and both would otherwise
    paint most of the grid red:

    * **Not daily.** ``northbound_holdings`` is quarterly, so nearly every
      session inside its span legitimately has no partition.
    * **Snapshot semantics.** A snapshot dataset accumulates one stamped
      reading per run; a day nobody ran has no snapshot and *cannot* be given
      one, because replaying it would forge rows. That is the whole reason
      ``fetch_semantics`` exists — see ``domain/datasets.py``.

    Only a ``by_date`` dataset on a daily cadence can be honestly said to be
    missing a day it should have.
    """
    if spec.fetch_semantics == "snapshot" or spec.max_staleness_days > 1:
        return "cadence"
    return "fault"


def _next_period_start(day: date, granularity: str) -> date:
    """First day of the period after the one holding *day*."""
    if granularity == "year":
        return date(day.year + 1, 1, 1)
    if granularity == "quarter":
        quarter_end_month = 3 * ((day.month - 1) // 3) + 3
        return (
            date(day.year + 1, 1, 1)
            if quarter_end_month == 12
            else date(day.year, quarter_end_month + 1, 1)
        )
    if granularity == "month":
        return date(day.year + 1, 1, 1) if day.month == 12 else date(day.year, day.month + 1, 1)
    return day + timedelta(days=1)


class LakeView:
    """Answers the dashboard's questions about one lake. Thread-safe."""

    def __init__(self, config: Config):
        self.config = config
        self._lock = threading.Lock()
        self._cache: dict[str, _Cached] = {}
        self._inflight: dict[str, threading.Event] = {}
        self._refresh_lock = threading.Lock()
        self._refreshing = False
        self.maintenance_gate = None

    # --- caching -----------------------------------------------------------

    def _cached(self, key: str, build):
        # Cached payloads carry display text, so one per language.
        key = f"{key}@{current_language()}"
        now = time.monotonic()
        with self._lock:
            hit = self._cache.get(key)
            if hit is not None and now - hit.at < _CACHE_TTL_SECONDS:
                return hit.value
            event = self._inflight.get(key)
            if event is None:
                event = threading.Event()
                self._inflight[key] = event
                leader = True
            else:
                leader = False
        if not leader:
            # The overview fires health, tiers, datasets and the heatmap
            # together. They share this catalog; a second walk would just
            # queue behind the same disks.
            event.wait()
            with self._lock:
                hit = self._cache.get(key)
            if hit is not None and time.monotonic() - hit.at < _CACHE_TTL_SECONDS:
                return hit.value
            return self._cached(key, build)
        try:
            value = build()
        except BaseException:
            with self._lock:
                self._inflight.pop(key, None)
            event.set()
            raise
        with self._lock:
            self._cache[key] = _Cached(value, time.monotonic())
            self._inflight.pop(key, None)
        event.set()
        return value

    def invalidate(self) -> None:
        with self._lock:
            self._cache.clear()

    # --- background stats refresh -----------------------------------------

    def refresh_stats_in_background(self, wait: float = 0.0) -> bool:
        """Kick off a rebuild if ingestion has moved the lake.

        Blocks for at most *wait* seconds. A refresh rebuilds only the datasets
        a run wrote, which after the quarter-hourly news wire is a fraction of
        a second; waiting that long lets the page show current numbers instead
        of a "stats are stale" note that is already out of date.

        Threading lives here rather than in ``storage.stats`` so the module
        stays synchronous and testable. One thread at a time: the stats lock
        would already collapse duplicates, but spawning a thread per request to
        immediately lose a lock is waste.
        """
        with self._refresh_lock:
            if self._refreshing:
                return False
            if not stats_freshness(self.config).stale:
                return False
            if self.maintenance_gate is not None and not self.maintenance_gate.enter():
                return False
            self._refreshing = True

        def _run() -> None:
            try:
                result = refresh_stats_if_stale(self.config)
                if result is not None:
                    logger.info(
                        "stats rebuilt in background: %d dataset(s), %d row(s)",
                        len(result.datasets),
                        result.rows,
                    )
                    self.invalidate()
            except Exception:
                logger.exception("background stats refresh failed")
            finally:
                with self._refresh_lock:
                    self._refreshing = False
                if self.maintenance_gate is not None:
                    self.maintenance_gate.leave()

        try:
            thread = threading.Thread(target=_run, name="stats-refresh", daemon=True)
            thread.start()
            if wait > 0:
                thread.join(wait)
        except BaseException:
            with self._refresh_lock:
                self._refreshing = False
            if self.maintenance_gate is not None:
                self.maintenance_gate.leave()
            raise
        return True

    # --- primitives --------------------------------------------------------

    def anchor(self) -> date:
        """Last trading day — the date freshness is judged against."""

        def _build() -> date:
            from cnequity.steps.common import is_trading_day

            today = shanghai_today()
            day = today if is_session_final(today) else today - timedelta(days=1)
            for _day in range(15):
                if is_trading_day(self.config, day):
                    return day
                day -= timedelta(days=1)
            return shanghai_today()

        return self._cached("anchor", _build)

    def _catalog(self) -> pl.DataFrame:
        """``list_datasets()`` joined with the measured rows and bytes."""

        def _build() -> pl.DataFrame:
            from cnequity.query.reader import list_datasets

            catalog = list_datasets(config=self.config)
            stats = load_partition_stats(self.config)
            if stats.is_empty():
                return catalog.with_columns(
                    pl.lit(None, dtype=pl.Int64).alias("row_count"),
                    pl.lit(None, dtype=pl.Int64).alias("bytes"),
                    pl.lit(None, dtype=pl.Int64).alias("partitions"),
                )
            rollup = stats.group_by("dataset").agg(
                pl.col("row_count").sum(),
                pl.col("bytes").sum(),
                pl.len().alias("partitions"),
            )
            return catalog.join(rollup, on="dataset", how="left")

        return self._cached("catalog", _build)

    def _health_findings(self) -> dict:
        """The audit's last written health snapshot, or an empty stand-in."""

        def _build() -> dict:
            path = self.config.meta_root / "quality" / "health-latest.json"
            if not path.exists():
                return {}
            try:
                with open(path, encoding="utf-8") as handle:
                    return json.load(handle)
            except (OSError, json.JSONDecodeError):
                return {}

        return self._cached("health_findings", _build)

    def _freshness_of(self, row: dict, anchor: date) -> str:
        """fresh / STALE / empty / n/a, on the same rules as ``cne status``."""
        if not row["has_data"]:
            # A demo or sample lake leaves most of the registry empty on
            # purpose. `cne status` calls those n/a; reporting them here as
            # empty-and-required painted a first-run lake red.
            if getattr(self.config, "lake_profile", None) in {"demo", "sample"}:
                return "n/a"
            return empty_freshness_label(row["dataset"])
        if not is_dataset_enabled(row["dataset"], self.config):
            return "n/a"
        if not row["watermarked"]:
            return "n/a"
        mark = row["watermark"] or row["coverage_end"]
        return freshness_state(row["dataset"], mark, anchor, self.config)

    def _rows(self) -> list[dict]:
        """One enriched dict per registered dataset."""
        anchor = self.anchor()
        out = []
        for row in self._catalog().iter_rows(named=True):
            spec = DATASETS[row["dataset"]]
            out.append(
                {
                    **row,
                    "tier": spec.tier,
                    "tier_label": _(TIER_LABELS[spec.tier]),
                    "required": spec.required,
                    "intraday": spec.intraday_frequency,
                    # What one row covers. Carried separately from `intraday`
                    # because trade_ticks is intraday without being bars, and
                    # keying the catalog on `intraday` alone showed it as daily.
                    "row_grain": spec.row_grain,
                    "granularity": spec.partition_granularity if spec.partition_col else None,
                    "freshness": self._freshness_of(row, anchor),
                }
            )
        return out

    # --- endpoint payloads -------------------------------------------------

    def health(self) -> dict:
        rows = self._rows()
        findings = self._health_findings()
        historical = findings.get("historical_universe_validity") or {}
        blockers = historical.get("blockers") or []
        source_findings = [
            finding
            for severity in ("error_findings", "warning_findings", "info_findings")
            for finding in (findings.get(severity) or [])
        ]
        freshness = stats_freshness(self.config)
        counts: dict[str, int] = {}
        for row in rows:
            counts[row["freshness"]] = counts.get(row["freshness"], 0) + 1
        return {
            "anchor": self.anchor(),
            "datasets": len(rows),
            "fresh": counts.get("fresh", 0),
            "stale": counts.get("stale", 0),
            "empty": counts.get("empty", 0),
            # Registered, permanently sourceless (see `empty_freshness_label`).
            # Counted apart from `empty` so the four buckets still add up to
            # `datasets` and nothing reads it as work someone forgot to do.
            "no_source": counts.get("no source", 0),
            "not_applicable": counts.get("n/a", 0),
            "stale_datasets": sorted(r["dataset"] for r in rows if r["freshness"] == "stale"),
            # Empty is not automatically a problem: an opt-in dataset nobody
            # enabled and a required one that failed look identical on disk.
            "empty_optional": sorted(
                r["dataset"] for r in rows if r["freshness"] == "empty" and not r["required"]
            ),
            "empty_required": sorted(
                r["dataset"] for r in rows if r["freshness"] == "empty" and r["required"]
            ),
            "rows": sum(r["row_count"] or 0 for r in rows),
            "bytes": sum(r["bytes"] or 0 for r in rows),
            "findings_by_severity": findings.get("findings_by_severity", {}),
            "audit_trade_date": findings.get("trade_date"),
            # Operational health and research readiness are separate contracts:
            # a fresh lake can still be unsafe for a long all-A backtest. Keep
            # the research gate in the read-only API so clients do not have to
            # open the audit artifact themselves (or mistake `healthy=true`
            # for research readiness).
            "historical_universe": historical.get("universe", "all_a"),
            "historical_universe_ready": historical.get("universe_ready"),
            "historical_universe_window": historical.get("window"),
            "historical_all_a_st_evidence": findings.get("historical_all_a_st_evidence"),
            "historical_universe_blockers": [
                str(blocker["message"])
                for blocker in blockers
                if isinstance(blocker, dict) and blocker.get("message")
            ],
            "source_limitations": [
                str(finding["message"])
                for finding in source_findings
                if isinstance(finding, dict)
                and finding.get("source_limited")
                and finding.get("message")
            ],
            "stats_stale": freshness.stale,
            "stats_reason": freshness.reason,
            "stats_generated_at": freshness.generated_at,
        }

    def tiers(self) -> list[dict]:
        rows = self._rows()
        out = []
        for tier in TIERS:
            members = [r for r in rows if r["tier"] == tier]
            if not members:
                continue
            out.append(
                {
                    "tier": tier,
                    "label": _(TIER_LABELS[tier]),
                    "datasets": len(members),
                    "fresh": sum(1 for r in members if r["freshness"] == "fresh"),
                    "stale": sum(1 for r in members if r["freshness"] == "stale"),
                    "empty": sum(1 for r in members if r["freshness"] == "empty"),
                    "rows": sum(r["row_count"] or 0 for r in members),
                    "bytes": sum(r["bytes"] or 0 for r in members),
                    "members": [r["dataset"] for r in members],
                }
            )
        return out

    def datasets(self, *, tier: str | None = None) -> list[dict]:
        rows = self._rows()
        if tier:
            rows = [r for r in rows if r["tier"] == tier]
        return rows

    def provenance(self, dataset: str) -> list[dict]:
        """Source mix for one dataset, newest ``fetched_at`` first."""
        stats = load_provenance_stats(self.config)
        if stats.is_empty():
            return []
        rolled = (
            stats.filter(pl.col("dataset") == dataset)
            .group_by(["source", "data_version"])
            .agg(
                pl.col("row_count").sum(),
                pl.col("fetched_at_min").min(),
                pl.col("fetched_at_max").max(),
            )
            .sort("row_count", descending=True)
        )
        return rolled.to_dicts()

    # --- one dataset -------------------------------------------------------

    def partitions(self, dataset: str) -> list[dict]:
        """Per-partition rows and bytes, oldest first — the size/volume series."""
        stats = load_partition_stats(self.config)
        if stats.is_empty():
            return []
        rows = stats.filter(pl.col("dataset") == dataset)
        if rows.is_empty():
            return []
        return (
            rows.sort("period_start", nulls_last=True)
            .select("partition", "granularity", "period_start", "period_end", "row_count", "bytes")
            .to_dicts()
        )

    def _gaps(self, spec, parts: list[dict]) -> dict:
        """Periods inside the covered span that hold no partition.

        Counted in the dataset's own period, not in days: a year-partitioned
        dataset is not missing 364 days because one directory covers the year,
        and reporting it that way would drown the real gaps.
        """
        dated = [p for p in parts if p["period_start"] is not None]
        if len(dated) < 2:
            return {"missing": [], "total": 0, "unit": spec.partition_granularity}

        present = {p["partition"] for p in dated}
        first, last = dated[0]["period_start"], max(p["period_end"] for p in dated)
        missing: list[str] = []

        if spec.partition_granularity == "day":
            # Only sessions count as missing; a weekend is not a gap.
            from cnequity.query.calendar import _load_trading_calendar_df

            calendar = _load_trading_calendar_df(self.config, start=first, end=last)
            if calendar is None or calendar.is_empty():
                return {"missing": [], "total": 0, "unit": "day"}
            for day in calendar.filter(pl.col("is_trading")).sort("trade_date")["trade_date"]:
                if day.isoformat() not in present:
                    missing.append(day.isoformat())
        else:
            from cnequity.domain.partitions import partition_value

            cursor = first
            while cursor <= last:
                value = partition_value(cursor, spec.partition_granularity)
                if value not in present:
                    missing.append(value)
                cursor = _next_period_start(cursor, spec.partition_granularity)

        return {
            "missing": missing[:60],
            "total": len(missing),
            "unit": spec.partition_granularity,
        }

    def _resample_backlog(self, dataset: str) -> dict | None:
        """Sessions a stored 15m/30m/60m resample has not caught up with."""
        from cnequity.domain.datasets import RESAMPLED_MINUTE_DATASETS

        if dataset not in RESAMPLED_MINUTE_DATASETS:
            return None
        from cnequity.derive.minute_resample import resample_backlog

        return resample_backlog(self.config, dataset)

    def _commands(
        self, spec, freshness: str, gaps: dict | None = None, resample: dict | None = None
    ) -> list[dict]:
        """What to run, and why. ``op`` is set only when the operations page can start it."""
        from cnequity.domain.datasets import RESAMPLED_MINUTE_DATASETS
        from cnequity.serve.ops.catalog import DERIVE_NAMES, backfill_datasets

        name = spec.name
        out: list[dict] = []
        if spec.layer == "derived":
            why = _("recomputed from curated")
            if name in RESAMPLED_MINUTE_DATASETS:
                why = _("not computed by default; resampled from 1m / 5m into the lake")
                if resample and resample["derived"]:
                    why = (
                        _("{pending} sessions of input have not been recomputed yet").format(
                            pending=resample["pending"]
                        )
                        if resample["pending"]
                        else _("consistent with 1m / 5m")
                    )
            command = {"cmd": f"cne derive {name}", "why": why}
            if name in DERIVE_NAMES:
                command["op"] = "derive.run"
                command["params"] = {"name": name}
            out.append(command)
        elif spec.backfill_source or spec.fetch_semantics == "by_date":
            why = (
                _("dedicated history source: {backfill_source}").format(
                    backfill_source=spec.backfill_source
                )
                if spec.backfill_source
                else _("fill gaps by date")
            )
            command = {"cmd": f"cne backfill {name}", "why": why}
            if name in backfill_datasets(self.config):
                params: dict = {"dataset": name}
                missing = (gaps or {}).get("missing") or []
                if (gaps or {}).get("unit") == "day" and missing and gaps["total"] <= len(missing):
                    params["start"] = missing[0]
                    params["end"] = missing[-1]
                command["op"] = "backfill.run"
                command["params"] = params
            out.append(command)
        if freshness == "stale":
            out.append(
                {
                    "cmd": "cne run daily --stale-only",
                    "why": _("re-fetch datasets that still lag"),
                    "op": "daily.stale",
                    "params": {},
                }
            )
        out.append(
            {"cmd": f"cne stats show --dataset {name}", "why": _("rows and size per partition")}
        )
        return out

    # --- quality ------------------------------------------------------------

    def _quality_files(self, kind: str, limit: int) -> list[tuple[str, dict]]:
        """Newest ``meta/quality/<kind>/*.json``, as (run_id, payload).

        Only the per-run files: that directory also holds artefacts written by
        other checks (``authority-<date>.json``) whose shape is entirely
        different, and reading those as run findings would produce nonsense.
        A run id is a UUID, which is what tells them apart.
        """
        root = self.config.meta_root / "quality" / kind
        if not root.is_dir():
            return []
        out: list[tuple[str, dict]] = []
        for path in sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            if len(path.stem) != 36 or path.stem.count("-") != 4:
                continue
            try:
                with open(path, encoding="utf-8") as handle:
                    out.append((path.stem, json.load(handle)))
            except (OSError, json.JSONDecodeError):
                continue
            if len(out) >= limit:
                break
        return out

    def _quarantine(self) -> list[dict]:
        """What has been pulled out of curated, and how big it is.

        Not a wastebasket. Everything here was removed from the lake because
        something was wrong with it, and it is kept as evidence — sizing it is
        how you decide whether the evidence is still worth the disk.
        """
        root = self.config.data_root / "_quarantine"
        if not root.is_dir():
            return []
        out = []
        for entry in sorted(root.iterdir()):
            if not entry.is_dir():
                continue
            files = [f for f in entry.rglob("*") if f.is_file()]
            out.append(
                {
                    "name": entry.name,
                    "files": len(files),
                    "bytes": sum(f.stat().st_size for f in files),
                    "modified": datetime.fromtimestamp(
                        entry.stat().st_mtime, tz=timezone.utc
                    ).isoformat(),
                }
            )
        return out

    def _on_demand(self) -> list[dict]:
        """Per-symbol caches under ``meta/on_demand``.

        On-demand datasets are not in ``DATASETS`` and never reach curated, so
        nothing else on this dashboard can see them. An empty list means nobody
        has queried one yet, which is a normal state rather than a gap.
        """
        root = self.config.meta_root / "on_demand"
        if not root.is_dir():
            return []
        out = []
        for entry in sorted(root.iterdir()):
            if not entry.is_dir():
                continue
            files = [f for f in entry.rglob("*") if f.is_file()]
            newest = max((f.stat().st_mtime for f in files), default=None)
            out.append(
                {
                    "dataset": entry.name,
                    "entries": len(files),
                    "bytes": sum(f.stat().st_size for f in files),
                    "newest": datetime.fromtimestamp(newest, tz=timezone.utc).isoformat()
                    if newest
                    else None,
                }
            )
        return out

    def quality(self, *, limit: int = 30) -> dict:
        findings_runs = []
        for run_id, payload in self._quality_files("findings", limit):
            by_severity: dict[str, int] = {}
            by_check: dict[str, int] = {}
            for finding in payload.get("findings", []):
                sev = finding.get("severity", "info")
                by_severity[sev] = by_severity.get(sev, 0) + 1
                # Older step findings named the check `code`.
                key = finding.get("check") or finding.get("code") or "?"
                by_check[key] = by_check.get(key, 0) + 1
            findings_runs.append(
                {
                    "run_id": run_id,
                    "trade_date": payload.get("trade_date"),
                    "total": len(payload.get("findings", [])),
                    "by_severity": by_severity,
                    "top_checks": sorted(by_check.items(), key=lambda kv: -kv[1])[:5],
                }
            )

        diff_runs = []
        for run_id, payload in self._quality_files("source_diffs", limit):
            by_check: dict[str, int] = {}
            for diff in payload.get("diffs", []):
                key = diff.get("check", "?")
                by_check[key] = by_check.get(key, 0) + 1
            diff_runs.append(
                {
                    "run_id": run_id,
                    "trade_date": payload.get("trade_date"),
                    "diff_count": payload.get("diff_count", len(payload.get("diffs", []))),
                    "by_check": by_check,
                }
            )

        return {
            "findings_runs": findings_runs,
            "diff_runs": diff_runs,
            "quarantine": self._quarantine(),
            "on_demand": self._on_demand(),
        }

    def quality_run(self, run_id: str) -> dict | None:
        """One run's findings and cross-source diffs, in full."""
        findings = dict(self._quality_files("findings", 200)).get(run_id)
        diffs = dict(self._quality_files("source_diffs", 200)).get(run_id)
        if findings is None and diffs is None:
            return None
        return {
            "run_id": run_id,
            "trade_date": (findings or diffs or {}).get("trade_date"),
            "findings": (findings or {}).get("findings", []),
            "diffs": (diffs or {}).get("diffs", []),
        }

    # --- runs ---------------------------------------------------------------

    def _manifest_rows(self, sql: str, params: tuple = ()) -> list[dict]:
        """Query the manifest read-only. Returns [] when there is none yet."""
        import sqlite3

        path = self.config.manifest_path
        if not path.exists():
            return []
        conn = None
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            with conn:
                return [dict(row) for row in conn.execute(sql, params).fetchall()]
        except sqlite3.Error:
            return []
        finally:
            if conn is not None:
                conn.close()

    def _orphaned_runs(self) -> tuple[str, ...]:
        """Runs the manifest still calls ``running`` whose process is gone.

        The engine's own rule (a free run lock past a short grace), so a run
        killed by SIGKILL, an OOM or a closed laptop reads as interrupted here
        straight away rather than as running for ``batch_stale_seconds``.
        Nothing is written: the next run reconciles the manifest itself.
        """
        import sqlite3

        from cnequity.orchestrator.manifest import Manifest

        path = self.config.manifest_path
        if not path.exists():
            return ()
        conn = None
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            orphans, _locked = Manifest.orphaned_running_runs(
                conn,
                stale_after_seconds=float(
                    getattr(self.config, "batch_stale_seconds", 3600) or 3600
                ),
                locks_root=self.config.meta_root,
            )
        except (sqlite3.Error, ValueError, OSError):
            return ()
        finally:
            if conn is not None:
                conn.close()
        return tuple(orphans)

    @staticmethod
    def _status_sql(orphans: tuple[str, ...]) -> tuple[str, tuple[str, ...]]:
        """The run status column as readers should see it, with its parameters."""
        if not orphans:
            return "status", ()
        marks = ",".join("?" * len(orphans))
        return f"(CASE WHEN run_id IN ({marks}) THEN 'interrupted' ELSE status END)", orphans

    def runs(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        statuses: tuple[str, ...] | None = None,
        open_only: bool = False,
    ) -> tuple[int, list[dict], int | None]:
        """One page of runs, newest first, plus the count of matching runs.

        The manifest keeps the full history. The page is only a window: callers
        walk it with ``offset`` instead of pretending the newest slice is all
        there is. ``statuses`` limits both the page and the count; omit it to
        count every run.

        ``open_only`` is the runs-page attention count. It keeps the latest
        attempt of each job that is still failed, degraded, or interrupted.
        An older failure is not counted once a later attempt of that job has
        succeeded, is running, or has replaced it. A skip does not count as
        that later attempt. An init whose missing steps a later run completed
        is closed too. The third value is how many of those open runs are a
        failed ``daily:*`` group, which is what ``cne run retry --failed-groups``
        would pick up; it is ``None`` for an ordinary list.
        """
        if open_only:
            chosen = self._open_attention_runs()
            if statuses:
                allowed = set(statuses)
                chosen = [row for row in chosen if row["status"] in allowed]
            failed_daily = sum(
                1
                for row in chosen
                if row["status"] == "failed" and str(row["job_name"]).startswith("daily:")
            )
            page = chosen[offset : offset + limit]
            rows = self._decorate_runs(self._runs_by_id([row["run_id"] for row in page]))
            return len(chosen), rows, failed_daily
        chosen = tuple(dict.fromkeys(statuses or ()))
        status, status_params = self._status_sql(self._orphaned_runs())
        where = ""
        bound: tuple[str, ...] = ()
        if chosen:
            where = f" WHERE {status} IN ({','.join('?' * len(chosen))})"
            bound = (*status_params, *chosen)
        counted = self._manifest_rows(
            f"SELECT COUNT(*) AS n FROM ingestion_runs{where}",
            bound,
        )
        total = int(counted[0]["n"]) if counted else 0
        runs = self._manifest_rows(
            f"""SELECT run_id, job_name, {status} AS status, started_at, finished_at,
                      rows_read, rows_written, error_message, metadata_json
               FROM ingestion_runs{where} ORDER BY started_at DESC LIMIT ? OFFSET ?""",
            (*status_params, *bound, limit, offset),
        )
        return total, self._decorate_runs(runs), None

    def _open_attention_runs(self) -> list[dict]:
        """Latest still-open attempt of each job, newest first."""
        ignored = ",".join("?" * len(_NON_ATTEMPT_STATUSES))
        scoped = ",".join("?" * len(_STEP_SCOPED_JOBS))
        attention = ",".join("?" * len(_OPEN_ATTENTION_STATUSES))
        status, status_params = self._status_sql(self._orphaned_runs())
        rows = self._manifest_rows(
            f"""
            WITH effective AS (
                SELECT run_id, job_name, {status} AS status, started_at, metadata_json
                FROM ingestion_runs
            ),
            scoped AS (
                SELECT run_id, job_name, status, started_at, metadata_json,
                       CASE
                           WHEN job_name IN ({scoped})
                           THEN COALESCE(json_extract(metadata_json, '$.planned_steps[0]'), '')
                           ELSE ''
                       END AS scope_key
                FROM effective
                WHERE status NOT IN ({ignored})
            ),
            ranked AS (
                SELECT run_id, job_name, status, started_at, metadata_json,
                       ROW_NUMBER() OVER (
                           PARTITION BY job_name, scope_key
                           ORDER BY started_at DESC, run_id DESC
                       ) AS rn
                FROM scoped
            )
            SELECT run_id, job_name, status, started_at, metadata_json
            FROM ranked
            WHERE rn = 1 AND status IN ({attention})
            ORDER BY started_at DESC, run_id DESC
            """,
            (*status_params, *_STEP_SCOPED_JOBS, *_NON_ATTEMPT_STATUSES, *_OPEN_ATTENTION_STATUSES),
        )
        return [
            row for row in rows if row["job_name"] != "init" or self._init_failure_still_open(row)
        ]

    def _init_failure_still_open(self, run: dict) -> bool:
        """Whether this init still owes work a later run has not already done.

        Same coverage question as ``cne status``: a step the init never
        finished, but some later run completed, is not an open failure.
        """
        from cnequity.orchestrator.init_phases import (
            expected_steps,
            init_run_complete,
            step_succeeded,
        )

        try:
            meta = json.loads(run.get("metadata_json") or "{}")
        except (TypeError, ValueError):
            return True
        if not isinstance(meta, dict):
            return True
        phases = [phase for phase in meta.get("phases") or [] if isinstance(phase, str)]
        if not phases:
            return True
        batches = self._manifest_rows(
            "SELECT dataset, status FROM ingestion_batches WHERE run_id = ?",
            (run["run_id"],),
        )
        outcomes = meta.get("step_outcomes") or {}
        if isinstance(outcomes, dict):
            batches = list(batches) + [
                {"dataset": name, "logical_step": True, **outcome}
                for name, outcome in outcomes.items()
                if isinstance(outcome, dict)
            ]
        if init_run_complete(phases, batches):
            return True
        missing = [step for step in expected_steps(phases) if not step_succeeded(batches, step)]
        if not missing:
            return True
        return not all(
            self._dataset_succeeded_after(step, str(run.get("started_at") or ""))
            for step in missing
        )

    def _dataset_succeeded_after(self, dataset: str, started_at: str) -> bool:
        rows = self._manifest_rows(
            """
            SELECT 1 AS ok FROM ingestion_batches b
            JOIN ingestion_runs r ON r.run_id = b.run_id
            WHERE b.dataset = ? AND b.status = 'success' AND r.started_at > ?
            LIMIT 1
            """,
            (dataset, started_at),
        )
        return bool(rows)

    def _runs_by_id(self, run_ids: list[str]) -> list[dict]:
        if not run_ids:
            return []
        status, status_params = self._status_sql(self._orphaned_runs())
        rows = self._manifest_rows(
            f"""SELECT run_id, job_name, {status} AS status, started_at, finished_at,
                      rows_read, rows_written, error_message, metadata_json
               FROM ingestion_runs WHERE run_id IN ({",".join("?" * len(run_ids))})""",
            (*status_params, *run_ids),
        )
        by_id = {row["run_id"]: row for row in rows}
        return [by_id[run_id] for run_id in run_ids if run_id in by_id]

    def _decorate_runs(self, runs: list[dict]) -> list[dict]:
        if not runs:
            return []
        tally = self._manifest_rows(
            """SELECT run_id, status, COUNT(*) AS n FROM ingestion_batches
               WHERE run_id IN ({}) GROUP BY run_id, status""".format(",".join("?" * len(runs))),
            tuple(r["run_id"] for r in runs),
        )
        by_run: dict[str, dict[str, int]] = {}
        for row in tally:
            by_run.setdefault(row["run_id"], {})[row["status"]] = int(row["n"])
        failures = self._run_failures(tuple(run["run_id"] for run in runs))
        for run in runs:
            counts = by_run.get(run["run_id"], {})
            run["batches"] = sum(counts.values())
            run["batch_status"] = counts
            run["datasets"] = []
            run["failures"] = failures.get(run["run_id"], [])
            run["launch_id"] = _launch_id(run.pop("metadata_json", None))
            run["has_staging"] = self._has_staging(run["run_id"])
        return runs

    def _run_failures(self, run_ids: tuple[str, ...]) -> dict[str, list[dict]]:
        """Batch errors for the runs list.

        The run row often says only ``one or more core steps failed``. The
        sentence that names the host, status and page lives on the batch, and
        a degraded run stores it there with no run-level message at all.
        A batch that was retried to success, superseded by that retry, or
        marked stale is no longer something to handle, so it is not counted.
        """
        if not run_ids:
            return {}
        actionable = ",".join("?" * len(_ACTIONABLE_BATCH_STATUSES))
        rows = self._manifest_rows(
            f"""SELECT run_id, dataset, status, error_message
               FROM ingestion_batches
               WHERE run_id IN ({",".join("?" * len(run_ids))})
                 AND status IN ({actionable})
                 AND error_message IS NOT NULL AND TRIM(error_message) != ''
               ORDER BY COALESCE(started_at, '')""",
            (*run_ids, *_ACTIONABLE_BATCH_STATUSES),
        )
        grouped: dict[str, list[dict]] = {}
        seen: dict[str, set[tuple[str, str]]] = {}
        for row in rows:
            text = _failure_line(row["error_message"])
            if not text:
                continue
            key = (str(row["dataset"]), text)
            bag = seen.setdefault(row["run_id"], set())
            if key in bag:
                continue
            bag.add(key)
            grouped.setdefault(row["run_id"], []).append(
                {
                    "dataset": row["dataset"],
                    "status": row["status"],
                    "error_message": text,
                }
            )
        return grouped

    def run_detail(self, run_id: str) -> dict | None:
        """One run and every batch in it, with a stalled flag per batch.

        ``stalled`` is not a manifest column: it is "still ``running`` but
        silent for longer than ``batch_stale_seconds``". The engine uses the
        same threshold to promote such batches on the next run, so surfacing it
        here shows the operator what the engine is about to conclude, before it
        does — a worker that died is otherwise indistinguishable from a slow one.
        """
        orphans = self._orphaned_runs()
        status, status_params = self._status_sql(orphans)
        rows = self._manifest_rows(
            f"""SELECT run_id, job_name, {status} AS status, started_at, finished_at,
                      rows_read, rows_written, error_message, metadata_json
               FROM ingestion_runs WHERE run_id = ?""",
            (*status_params, run_id),
        )
        if not rows:
            return None
        run = rows[0]
        batches = self._manifest_rows(
            """SELECT batch_id, dataset, status, window_start, window_end,
                      rows_read, rows_written, retry_count, started_at,
                      finished_at, heartbeat_at, error_message
               FROM ingestion_batches WHERE run_id = ?
               ORDER BY COALESCE(started_at, '')""",
            (run_id,),
        )
        now = datetime.now(timezone.utc)
        threshold = float(getattr(self.config, "batch_stale_seconds", 3600) or 3600)
        for batch in batches:
            batch["stalled"] = False
            if batch["status"] != "running":
                continue
            mark = batch["heartbeat_at"] or batch["started_at"]
            if not mark:
                continue
            try:
                silent = (now - datetime.fromisoformat(mark)).total_seconds()
            except ValueError:
                continue
            batch["silent_seconds"] = round(silent)
            # A batch of a run whose process is gone will never finish.
            batch["stalled"] = silent >= threshold or run_id in orphans
        run["batches"] = batches
        run["stale_after_seconds"] = threshold
        run["launch_id"] = _launch_id(run.get("metadata_json"))
        run["has_staging"] = self._has_staging(run_id)
        return run

    def _has_staging(self, run_id: str) -> bool:
        staging = self.config.staging_root / run_id
        try:
            return staging.is_dir() and any(staging.iterdir())
        except OSError:
            return False

    def run_fingerprint(self, run_id: str) -> str:
        """Cheap value that changes whenever the run's batches do.

        The stream compares this instead of diffing rows: a poll that finds it
        unchanged sends nothing, which is what keeps an idle subscriber free.
        """
        rows = self._manifest_rows(
            """SELECT COUNT(*) AS n, COALESCE(MAX(finished_at), '') AS f,
                      COALESCE(MAX(heartbeat_at), '') AS h,
                      COALESCE(SUM(rows_written), 0) AS w,
                      COALESCE(SUM(retry_count), 0) AS r,
                      COALESCE(GROUP_CONCAT(status), '') AS s
               FROM ingestion_batches WHERE run_id = ?""",
            (run_id,),
        )
        status, status_params = self._status_sql(self._orphaned_runs())
        state = self._manifest_rows(
            f"SELECT {status} AS status FROM ingestion_runs WHERE run_id = ?",
            (*status_params, run_id),
        )
        head = rows[0] if rows else {}
        return (
            "|".join(str(head.get(k, "")) for k in ("n", "f", "h", "w", "r", "s"))
            + f"|{state[0]['status'] if state else ''}"
        )

    def recent_batches(self, dataset: str, *, limit: int = 15) -> list[dict]:
        """Latest manifest batches for this dataset, newest first.

        stdlib sqlite3 on a read-only URI rather than DuckDB's sqlite_scanner:
        that scanner is an autoloadable extension fetched from the network on
        first use, which on an offline or proxied box turns the page into a
        spinner. The manifest is small and WAL is already on, so a concurrent
        run is not blocked by this read.
        """
        return self._manifest_rows(
            """SELECT run_id, batch_id, status, window_start, window_end, rows_written,
                      retry_count, started_at, finished_at, error_message
               FROM ingestion_batches WHERE dataset = ?
               ORDER BY COALESCE(started_at, '') DESC LIMIT ?""",
            (dataset, limit),
        )

    def dataset_detail(self, dataset: str) -> dict:
        """Everything the detail page shows, in one round trip."""
        from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS
        from cnequity.query.reader import ADJUSTABLE_DATASETS

        spec = DATASETS[dataset]
        row = next(r for r in self._rows() if r["dataset"] == dataset)
        parts = self.partitions(dataset)
        findings = self._health_findings()
        mine = [
            f
            for key in ("error_findings", "warning_findings")
            for f in findings.get(key, [])
            if f.get("dataset") == dataset
        ]

        return {
            **row,
            "layer": spec.layer,
            "partition_col": spec.partition_col,
            "max_staleness_days": spec.max_staleness_days,
            "backfill_chunk_days": spec.backfill_chunk_days,
            "backfill_chunk_symbols": getattr(spec, "backfill_chunk_symbols", None),
            # The source's own floor, not this lake's backlog: earlier windows
            # return nothing rather than less, and no backfill reaches past it.
            "earliest_available": spec.earliest_available(shanghai_today()),
            # Which *kind* of limit produced that floor. Without this the panel
            # cannot tell a fixed date from a rolling count, and a dataset
            # limited by a date (trade_ticks) reads as unlimited because its
            # `history_horizon_days` is null.
            "history_floor_date": spec.history_floor_date,
            # Whether load() can join adj_factors — the data tab only offers the
            # 复权 control where it means something.
            "adjustable": dataset in ADJUSTABLE_DATASETS,
            "primary_key": PRIMARY_KEYS.get(dataset, []),
            "schema": [
                {"column": col, "dtype": str(dtype)}
                for col, dtype in DATASET_SCHEMAS.get(dataset, {}).items()
            ],
            # The per-partition series is not inlined: daily_bars alone is 6,202
            # rows, and the detail payload is loaded on every tab switch while
            # the series is only needed for one chart. `/partitions` serves it.
            "gaps": (gaps := self._gaps(spec, parts)),
            "findings": mine,
            "commands": self._commands(
                spec, row["freshness"], gaps, (resample := self._resample_backlog(dataset))
            ),
            "resample": resample,
            "batches": self.recent_batches(dataset),
        }

    def provenance_series(self, dataset: str, *, max_buckets: int = 400) -> dict:
        """Source mix over time, bucketed to stay chartable.

        The collapsed :meth:`provenance` answers "which sources are in here";
        this answers "when did that change", which is where a routing switch or
        a mis-attributed backfill actually becomes visible.

        daily_bars alone has 11,324 (day, source) points — a megabyte of JSON to
        draw a few hundred pixels. Buckets widen until the series fits, and the
        chosen width is returned rather than applied silently: a caller that
        does not know it is looking at months cannot label the axis honestly.
        """
        stats = load_provenance_stats(self.config)
        partitions = load_partition_stats(self.config)
        empty = {"bucket": "day", "points": []}
        if stats.is_empty() or partitions.is_empty():
            return empty
        periods = partitions.filter(pl.col("dataset") == dataset).select(
            "partition", "period_start"
        )
        rows = stats.filter(pl.col("dataset") == dataset)
        if rows.is_empty() or periods.is_empty():
            return empty

        joined = rows.join(periods, on="partition", how="inner").filter(
            pl.col("period_start").is_not_null()
        )
        if joined.is_empty():
            return empty

        for bucket, expr in (
            ("day", pl.col("period_start")),
            ("month", pl.col("period_start").dt.truncate("1mo")),
            ("year", pl.col("period_start").dt.truncate("1y")),
        ):
            grouped = (
                joined.with_columns(expr.alias("period_start"))
                .group_by(["period_start", "source", "data_version"])
                .agg(pl.col("row_count").sum())
                .sort(["period_start", "source"])
            )
            if grouped.height <= max_buckets or bucket == "year":
                return {"bucket": bucket, "points": grouped.to_dicts()}
        return empty  # pragma: no cover — the year branch always returns

    # --- browsing rows -----------------------------------------------------

    def date_options(self, dataset: str, *, limit: int = 400) -> dict:
        """What the date control may offer, and which control that should be.

        There is no single picker: the registry uses twelve different date
        columns across four shapes, and a calendar widget over ``report_period``
        would invite a query the column cannot answer.

        Only values that exist are offered. A day with no partition is not
        selectable, which is the honest version of the ``snapshot_only`` warning
        — those datasets accumulate one stamped reading per run, and a day
        nobody ran can never be given one.
        """
        spec = DATASETS[dataset]
        parts = self.partitions(dataset)
        if spec.partition_col is None:
            return {
                "kind": "none",
                "column": spec.query_date_col,
                "granularity": None,
                "values": [],
                "total": 0,
                "note": _(
                    "Single-file merge: only the current state, with no notion of fetching by date."
                ),
            }

        values = [p["partition"] for p in reversed(parts) if p["partition"] is not None]
        if spec.partition_col == "report_period":
            kind = "report_period"
        elif spec.partition_granularity == "day" and spec.partition_col == "trade_date":
            kind = "trading_day"
        elif spec.partition_granularity == "day":
            kind = "event_day"
        else:
            kind = "period"

        note = None
        if history_mode_for(spec) == "snapshot_only":
            note = _(
                "snapshot_only: each run lands one snapshot of that day. A day that did not run has no snapshot and cannot be made up; replaying would fabricate rows. The list only has dates that really exist."
            )
        elif spec.partition_granularity != "day":
            note = _(
                "Partitioned by {partition_granularity}: pick a period to get back all its rows."
            ).format(partition_granularity=spec.partition_granularity)

        return {
            "kind": kind,
            "column": spec.query_date_col,
            "granularity": spec.partition_granularity,
            "values": values[:limit],
            "total": len(values),
            "note": note,
        }

    def rows(
        self,
        dataset: str,
        *,
        period: str | None = None,
        symbol: str | None = None,
        as_of: date | None = None,
        pit_mode: str | None = None,
        adjust: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> dict:
        """A page of actual rows, with the provenance columns kept.

        ``source`` / ``data_version`` / ``fetched_at`` are not dropped to make
        room: row-level provenance is the point of this lake, and a viewer that
        hides it teaches you it is not there.

        Reads through ``query.reader.load`` so the dashboard sees exactly what
        ``load()`` gives a researcher — adjustment, PIT collapsing and all —
        rather than a second, subtly different read path.
        """
        from cnequity.query.reader import ReaderError, load

        spec = DATASETS[dataset]
        kwargs: dict[str, Any] = {"config": self.config}

        if period:
            part = parse_partition(period)
            if spec.partition_col == "report_period":
                # A String column: the period *is* the value, and a date range
                # over it would compare text to dates.
                pass
            elif part is None:
                raise ValueError(f"{period!r} is not a period for {dataset}")
            else:
                kwargs["start"], kwargs["end"] = part.start, part.end
        if symbol:
            kwargs["symbols"] = [symbol]
        if as_of:
            kwargs["as_of"] = as_of
            kwargs["pit_mode"] = pit_mode or "best_effort"
        if adjust:
            kwargs["adjust"] = adjust

        try:
            adjustment = None
            if adjust:
                from cnequity.query import load_with_receipt

                result = load_with_receipt(dataset, **kwargs)
                frame = result.frame
                adjustment = result.receipt.get("adjustment")
            else:
                frame = load(dataset, **kwargs)
        except ReaderError as exc:
            raise ValueError(str(exc)) from exc

        if period and spec.partition_col == "report_period":
            frame = frame.filter(pl.col("report_period") == period)

        total = frame.height
        page = frame.slice(offset, limit)
        return {
            "columns": page.columns,
            "rows": [
                [None if v is None else _jsonable(v) for v in row] for row in page.iter_rows()
            ],
            "total": total,
            "offset": offset,
            "limit": limit,
            "adjustment": adjustment,
        }

    def heatmap(self, *, days: int = 90) -> dict:
        """Coverage grid: one row per dataset, one cell per recent trading day.

        Cells answer "does a partition covering this day exist", which for a
        month/year-partitioned dataset is coarser than the day it is drawn on —
        the directory covers the period, and whether one particular session has
        rows in it is not knowable without reading the file. ``granularity``
        rides along on each row so a renderer can say so rather than imply a
        precision the layout does not have.
        """
        from cnequity.query.calendar import _load_trading_calendar_df

        anchor = self.anchor()
        window_start = anchor - timedelta(days=int(days * 1.7) + 10)
        calendar = _load_trading_calendar_df(self.config, start=window_start, end=anchor)
        if calendar is None or calendar.is_empty():
            trading_days: list[date] = []
        else:
            trading_days = (
                calendar.filter(pl.col("is_trading"))
                .sort("trade_date")["trade_date"]
                .to_list()[-days:]
            )

        stats = load_partition_stats(self.config)
        spans: dict[str, list[tuple[date, date]]] = {}
        if not stats.is_empty():
            for row in stats.iter_rows(named=True):
                if row["period_start"] is None or row["period_end"] is None:
                    continue
                spans.setdefault(row["dataset"], []).append(
                    (row["period_start"], row["period_end"])
                )

        rows = []
        for row in self._rows():
            name = row["dataset"]
            intervals = sorted(spans.get(name, []))
            if row["granularity"] is None:
                cells = CELL_UNPARTITIONED * len(trading_days)
            elif not intervals:
                cells = CELL_OUTSIDE * len(trading_days)
            else:
                first, last = intervals[0][0], max(end for _, end in intervals)
                # Binary-search each interval into the sorted day list instead of
                # testing every day against every interval. That inner scan was
                # O(datasets × intervals × days), and a day-partitioned dataset
                # brings one interval per session: daily_bars alone put ~6,200
                # intervals against 250 days. Measured on this lake, the endpoint
                # took 0.1s at days=60 and 24-67s at days=250 — the dashboard's
                # whole first paint waits on it.
                covered_flags = bytearray(len(trading_days))
                for start, end in intervals:
                    lo = bisect_left(trading_days, start)
                    hi = bisect_right(trading_days, end)
                    for i in range(lo, hi):
                        covered_flags[i] = 1
                cells = "".join(
                    CELL_COVERED
                    if covered_flags[i]
                    else (CELL_GAP if first <= day <= last else CELL_OUTSIDE)
                    for i, day in enumerate(trading_days)
                )
            rows.append(
                {
                    "dataset": name,
                    "tier": row["tier"],
                    "granularity": row["granularity"],
                    "freshness": row["freshness"],
                    "cadence_days": DATASETS[name].max_staleness_days,
                    "gap_meaning": _gap_meaning(DATASETS[name]),
                    "cells": cells,
                }
            )

        # Grouped by tier, so a family such as L9 衍生品 reads as one block
        # rather than scattered through an alphabetical list.
        rows.sort(key=lambda r: (TIERS.index(r["tier"]), r["dataset"]))
        return {
            "days": trading_days,
            "legend": {
                CELL_COVERED: "covered",
                CELL_GAP: "gap",
                CELL_OUTSIDE: "outside coverage",
                CELL_UNPARTITIONED: "unpartitioned",
            },
            "rows": rows,
        }
