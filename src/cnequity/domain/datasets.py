"""Single source of truth for dataset metadata (DatasetSpec registry).

Every module that needs per-dataset knowledge — compact partitioning, watermark
policy, fetch semantics, query date columns, DuckDB views, audit — derives it
from ``DATASETS`` below. Schema and primary keys live in
``domain/schemas.py`` (polars dtypes); ``test_dataset_registry.py`` asserts the
two stay in sync.

Adding a dataset = one ``DatasetSpec`` entry here + schema/PK in schemas.py +
a registered step.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

from cnequity.domain.partitions import Granularity, partition_value

# Re-export the storage contract from the registry module for callers that
# already use ``domain.datasets`` as their metadata entry point.
from cnequity.domain.pit import (  # noqa: F401
    PIT_MODES,
    PIT_QUALITIES,
    PIT_STORAGE_COLUMNS,
    PIT_STORAGE_DTYPES,
    PitMode,
    PitQuality,
)
from cnequity.domain.value_rules import NO_RULES, ValueRules
from cnequity.i18n import N_

FetchSemantics = Literal["by_date", "snapshot"]
HistoryMode = Literal["by_date", "snapshot_with_backfill", "snapshot_only"]
Layer = Literal["curated", "derived"]

# These fields deliberately use small, serialisable values.  They are kept on
# ``DatasetSpec`` (rather than in a second registry) so a caller that already
# imports the public registry can discover the complete data contract without
# learning about another configuration file.  The aliases are intentionally
# open-ended at runtime: downstream applications may carry a newer contract
# level while still reading an older cnequity release.  ``domain.contracts``
# performs the strict validation for the values understood by this release.
ContractLevel = str
# ``PitMode`` controls a query; ``PitQuality`` describes the evidence carried
# by a row/dataset.  ``PitGrade`` is retained as a 0.x contract alias below.
PitGrade = str
CompatibilityContract = str | Mapping[str, str]
UnitContract = str | Mapping[str, str]

DEFAULT_SCHEMA_VERSION = 1
DEFAULT_CONTRACT_LEVEL = "stable"
DEFAULT_COMPATIBILITY = "additive"


# Research-use classification, orthogonal to ``Layer`` (which is a storage
# location). L0 is the reference spine everything joins on and L8 the risk
# overlay; the ordering is roughly "how far from the price series". L9 holds
# futures and options, which live beside the equity universe rather than in it
# (ADR-0013), so their contracts, bars and derivations are managed as one family.
Tier = Literal["L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9"]

TIERS: tuple[Tier, ...] = ("L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9")

# How much the project stands behind a dataset, orthogonal to ``Tier`` (what it
# is for). ``core`` is the promise: rebuildable history, a daily path that has
# held up, an independent check. ``extended`` is rebuildable and steady but not
# essential, or single-sourced. ``best_effort`` is either not rebuildable (a
# live snapshot whose history starts the day a lake turns it on) or unsteady,
# and is off unless a config opts in. ``optional`` has its own enable switch.
Support = Literal["core", "extended", "best_effort", "optional"]

SUPPORT_LEVELS: tuple[Support, ...] = ("core", "extended", "best_effort", "optional")

#: Catalog messages: translate where shown, English in JSON.
SUPPORT_LABELS: dict[Support, str] = {
    "core": N_("Core"),
    "extended": N_("Extended"),
    "best_effort": N_("Best effort"),
    "optional": N_("Optional"),
}

#: Catalog messages: translate where shown, English in JSON.
TIER_LABELS: dict[Tier, str] = {
    "L0": N_("Reference"),
    "L1": N_("Prices"),
    "L2": N_("Corporate events"),
    "L3": N_("Fundamentals"),
    "L4": N_("Capital flows"),
    "L5": N_("Industry structure"),
    "L6": N_("Macro"),
    "L7": N_("Sentiment / rotation"),
    "L8": N_("Risk and compliance"),
    "L9": N_("Derivatives"),
}


@dataclass(frozen=True)
class DatasetSpec:
    """Orchestration/query metadata for one dataset.

    support:
        How much the project stands behind it (see ``Support``). Every
        registered dataset declares it; the registry refuses to load without.

    tier:
        L0–L9 research classification (see ``Tier``). Mandatory and without a
        default: an unclassified dataset would silently fall into whichever
        bucket the default named, and this is what groups the catalog docs and
        the lake dashboard. ``layer`` is where the parquet lives; this is what
        the data is *for*, and the two are independent — ``adj_factors`` is
        derived storage but L1 research input.

    partition_col:
        Hive partition directory key under the lake (None = merge-style single
        file, e.g. instruments).
    partition_granularity:
        Period each partition directory covers — ``day``, ``month``, ``quarter``
        or ``year``. ``quarter`` is for the ``report_period`` datasets, whose
        directories are the reporting periods themselves (``2016Q1``) rather
        than a period chosen for file size.
        Pick it from rows per day, not from habit: a Parquet footer costs ~1KB
        whatever it holds, so a dataset with a handful of rows a day spends
        almost all its bytes and all its file opens on metadata. Rough bands
        used here: ≥1000 rows/day → ``day``, 50–1000 → ``month``, <50 → ``year``.
        Only ``day`` values can be hive-parsed (see domain/partitions.py).
    date_col:
        Column used for query date-range filters; defaults to ``partition_col``.
    fetch_semantics:
        ``by_date`` — source returns values for a requested day (gap catch-up
        allowed). ``snapshot`` — live page stamped with trade_date; historical
        replay would forge rows, so only the run day is ever fetched.
    watermark:
        Maintain a date watermark under ``meta/state`` (False for datasets
        partitioned by non-date keys like report_period).
    pit_quality:
        How honestly the dataset answers "what was known on day D" (see the
        field below). ``pit`` and ``pit_grade`` are read from it: a ``strict``
        or ``reconstructed`` dataset is point-in-time — ``load()`` requires
        ``as_of`` and filters on ``announce_date``.
    backfill_source:
        Name of an external historical source that can replay this dataset even
        though daily ``fetch_semantics`` is ``snapshot`` (e.g. valuation_metrics:
        EastMoney live snapshot daily, baostock for history). ``cne backfill``
        is allowed for snapshot datasets only when this is set.
    required:
        When False, an empty curated root is a warning (not an error) and does
        not alone make ``lake_health`` UNHEALTHY. Use for registered datasets
        whose source is not yet wired or is temporarily unavailable.
    empty_severity:
        Override the severity used when the curated root is absent or empty.
        ``None`` derives ``error`` for required datasets and ``warning`` for
        optional datasets. A retired source with no replacement can use
        ``info`` so its expected absence does not look like an ingest failure.
    history_horizon_days:
        Trading days of history the *source* still serves, counted back from
        today. ``None`` (the default) means the source has no such limit and
        history is bounded only by what has been backfilled.

        This is a property of the vendor, not of this lake, and it is the
        difference between "not fetched yet" and "can never be fetched". Asking
        for 2016 intraday does not return less data, it returns none, and no
        backfill source extends it — ``by_date`` alone would promise a decade.

        The vendor caps a **bar count** per symbol, not a date: ~22,800 bars of
        1m and ~23,568 of 5m. This field is that count divided by a full
        session, so it holds for any instrument quoted every session — which is
        every A-share stock, and what these datasets are for. An instrument
        that only has bars on scattered days reaches proportionally further
        back (162107.SZ, a barely-traded LOF, holds 3,216 5m bars spread over
        67 days and so reaches 2012). Treat it as the guarantee for a normal
        stock, not as a hard ceiling for every symbol.

        Use ``history_floor_date`` instead when the source's edge is a *date*
        rather than a count — the two are different mechanisms and only one of
        them moves with today.
    history_floor_date:
        Earliest date the source serves, as a fixed calendar date. Wins over
        ``history_horizon_days`` when both are set.

        The distinction matters because a rolling count and a fixed floor
        diverge every day. TDX serves ``trade_ticks`` back to exactly
        2024-01-02 for *every* symbol, across both exchanges and every
        liquidity band (measured 2026-08-02; 2023-12-28 is empty). Expressing
        that as "624 trading days" would be true on the day it was measured and
        wrong the day after: ``earliest_available`` would walk the floor
        forward and refuse windows the source still happily serves.
    source_retired_date:
        Last date the source ever published, for a feed that has stopped. The
        upper bound to ``history_floor_date``'s lower one.

        Without it a retired feed is indistinguishable from a broken pipeline:
        its watermark freezes, ``is_stale`` calls it stale forever, and
        ``cne verify`` offers a backfill that runs the whole window, writes zero
        rows, and leaves the identical gap behind. Both are the same wrong
        answer — "you are missing data" — to a dataset that holds everything
        that exists.

        Set it to the last session with real values, not the first without.
        ``northbound_flows`` is 2024-08-16: the exchanges stopped publishing
        daily northbound net flow after that, and every row from 2024-08-19 on
        carries a null amount (see adapters/eastmoney/capital.py).
    """

    name: str
    tier: Tier
    layer: Layer = "curated"
    support: Support | None = None
    partition_col: str | None = None
    partition_granularity: Granularity = "day"
    date_col: str | None = None
    fetch_semantics: FetchSemantics = "by_date"
    watermark: bool = True
    # Which upstream the daily path actually reads, and what it falls back to.
    # Kept in the registry rather than in prose because the prose drifted: the
    # published source table had `sector_bars` on EastMoney long after it moved
    # to 同花顺, and `fund_flow` in the wrong schedule group entirely. Anything
    # generated from here — the README data table, the docs — cannot say
    # something the code does not do, and a test asserts the pairing.
    primary_source: str = ""
    backup_source: str | None = None
    # What the backup cannot reach. A backup exists for availability — if the
    # primary is down, 70% of the market beats nothing — but a reader deciding
    # whether to trust a degraded day needs to know *which* 30%, and a gate
    # asking "is this dataset covered" must not read a partial backup as a
    # whole one. Prose in a comment drifts; this is read by the resilience
    # report and asserted against the adapters.
    backup_gaps: tuple[str, ...] = ()
    backfill_source: str | None = None
    # A history source that needs the user's own credentials (Tushare). It
    # makes the dataset replayable only for a config that has them; without
    # one, and wherever no config is at hand, the dataset stays snapshot-only,
    # so a lake without a token is never told to backfill what it cannot.
    keyed_backfill_source: str | None = None
    # The rest of the recovery chain: sources that legitimately write rows to
    # this dataset without being any of the three roles above.
    #
    # Three slots described the design when a dataset had one vendor, one
    # independent peer and one history source. `daily_bars` no longer does: a
    # tip key that TDX misses is chased through the exchange board files, the
    # BSE board, EastMoney's clist and kline, Sina and THS in turn, and each of
    # them stamps its own `source` on the rows it lands. All eight are real
    # dependencies, so the audit was right to report the undeclared ones
    # (`unrouted_source`) and the failure-domain report was right to miss them.
    #
    # Deliberately a separate field rather than a flat ordered list: primary,
    # backup and backfill carry distinct meaning that the gates read — the
    # backup gate asks whether the *backup* is independent of the *primary*,
    # and flattening the roles would make that question unanswerable.
    supplementary_sources: tuple[str, ...] = ()
    # Sources that reach curated only through an explicit, operator-invoked
    # repair (`cne backfill … --*-repair`, `cne ths-official …`). Their terms
    # apply to the rows they land, so the compliance matrix must speak for
    # them; they are not part of any automatic route, so the resilience report
    # must not count them as a fallback. That distinction is why this is its
    # own field rather than another `supplementary_sources` entry.
    repair_sources: tuple[str, ...] = ()
    # ``(capture source, request-scope prefix, row label)``: a step may derive
    # rows from one source's exact wire capture and stamp them with a weaker
    # label so strict PIT excludes them (reconstructed original disclosures).
    # The raw-archive publish check admits exactly these labels, and only for
    # captures whose request scope starts with the prefix.
    reconstructed_sources: tuple[tuple[str, str, str], ...] = ()
    # How far the freshest data may lag the last trading day before it is
    # flagged STALE. For session datasets this is a count of exchange sessions,
    # so a Friday watermark checked on Monday is one day, not three, and a
    # holiday week does not consume the tolerance. 1 is normal T+1 publication.
    # Larger values mark a slower cadence (margin, commodity). A tolerance of
    # 30 or more stays a calendar span — quarterly holdings use 100 calendar
    # days to reach the next quarter-end, and counting those in sessions would
    # hide an extra quarter. Calendar-scoped datasets (news, announcements)
    # also stay on calendar days, because they publish on weekends.
    max_staleness_days: int = 1
    required: bool = True
    empty_severity: Literal["error", "warning", "info"] | None = None
    history_horizon_days: int | None = None
    history_floor_date: date | None = None
    source_retired_date: date | None = None
    # Calendar days of history one backfill sub-run may cover. None = one run
    # for the whole window, which is what every daily-cadence dataset wants.
    #
    # Set it where a full window's staging would not fit in memory: compact
    # reads *every* staging file of a run into one frame. Prefer
    # ``backfill_chunk_symbols`` for tip-paged sources (see below) — date
    # slices re-walk the same tip→start pages on every chunk.
    backfill_chunk_days: int | None = None
    # Symbols per backfill sub-run for tip-paged sources (TDX intraday pages
    # backwards from today). None = do not symbol-chunk.
    #
    # Date-chunking a tip-paged source is catastrophically wasteful: each
    # slice still has to walk tip → slice_start before any in-window row
    # appears, so a 15-slice CSI300 1m seed paid ~8× the wire traffic of one
    # tip→horizon walk. Symbol chunks keep one walk per name and still bound
    # compact memory (200 × 240 × 95 ≈ 4.6M rows of 1m per sub-run).
    backfill_chunk_symbols: int | None = None
    # What `cne backfill` accepts beyond a window and a symbol list. The
    # command looks these up instead of naming datasets; see
    # ``cnequity.orchestrator.backfill_request``.
    #
    # ``--source`` values that replace the configured source for one run.
    backfill_source_overrides: tuple[str, ...] = ()
    # How ``--refetch`` reaches the run, or None when it does not apply:
    # ``scope`` — the step reads it from the backfill window and ignores its
    # receipts; ``checkpoint`` — the date checkpoint is reset once;
    # ``derivatives`` — exchange files are downloaded again.
    backfill_refetch: str | None = None
    # ``--workers`` above 1: dates advance on parallel lanes.
    backfill_parallel_dates: bool = False
    # ``--symbols`` is carried on the backfill window, which the step reads;
    # otherwise it overrides the dataset's configured scope for the run.
    backfill_symbols_in_window: bool = False
    # Bar frequency for intraday datasets ("1m", "5m"), None for everything
    # else. One dataset holds exactly one frequency, so this is also what marks
    # a dataset as intraday — steps, audit checks and the reader all derive the
    # set from here rather than each keeping its own list of names.
    #
    # It is one-per-dataset because ``history_horizon_days`` is one-per-dataset
    # and the two disagree: TDX keeps 95 trading days of 1m but 491 of 5m. So
    # is the watermark, and so is ``coverage_start``. A single dataset holding
    # both frequencies could not answer "how far back does this go" truthfully
    # for either of them.
    intraday_frequency: str | None = None
    # What one row covers, when that is finer than a trading day: "1m", "5m",
    # "tick". Purely descriptive — nothing fetches, checks or reads differently
    # because of it, which is exactly why `trade_ticks` can carry it while
    # deliberately leaving `intraday_frequency` unset.
    #
    # The two exist separately because they answer different questions.
    # `intraday_frequency` means "this dataset holds bars at this frequency",
    # and every consumer of it assumes a `bar_time` column and a bar count per
    # session. Transaction records have neither, so inheriting those code paths
    # would give them checks that pass on the wrong column. But a reader
    # scanning the catalog still needs to see that this is intraday data, and
    # without this field the dashboard showed it a dash — indistinguishable
    # from a daily dataset.
    #
    # Where both are set they must agree; `test_dataset_registry` enforces it.
    row_grain: str | None = None
    # Whether the dataset promises at least one row on every exchange session
    # in its covered span. `fetch_semantics="by_date"` only describes how the
    # source is queried; event and announcement feeds are still sparse.
    coverage_mode: Literal["sparse", "session_dense"] = "sparse"

    # ------------------------------------------------------------------
    # Public data-contract metadata.
    #
    # These fields are appended after the original fields on purpose.  A few
    # integrations construct DatasetSpec positionally, and adding them here
    # keeps every old positional call valid.  The defaults describe the
    # long-standing behaviour of the registry; they do not change how rows
    # are fetched or written.
    # ------------------------------------------------------------------
    schema_version: int = DEFAULT_SCHEMA_VERSION
    contract_level: ContractLevel = DEFAULT_CONTRACT_LEVEL
    # ``None`` means infer from the query/date axis (or ``list_date`` for the
    # merge-style instruments table and ``announce_date`` for PIT tables).
    availability_col: str | None = None
    # ``additive`` is the compatibility policy used by the existing curated
    # schemas: adding a nullable column is safe; removing/changing a column,
    # primary key, unit, PIT, or history meaning is not.
    compatibility: CompatibilityContract = DEFAULT_COMPATIBILITY
    # A scalar is useful for identity/event datasets, while mappings name the
    # units of individual measures (prices, shares, amounts, ratios, ...).
    unit_contract: UnitContract | None = None
    # Canonical PIT evidence quality.  ``reconstructed`` is used for the
    # current historical backfill, whose values are today's restated snapshot
    # paired with an older report/disclosure date.  ``snapshot_only`` denotes
    # a live snapshot with no honest PIT replay.  ``None`` infers
    # ``snapshot_only`` or ``not_applicable`` from ``fetch_semantics``. The
    # ``pit`` flag and the ``pit_grade`` spelling of exported 0.x contracts
    # are derived from it; they used to be declared alongside and had to agree.
    pit_quality: PitQuality | None = None
    # Number of already-covered observations to reconcile on every
    # incremental run. A non-zero value deliberately re-reads the tail
    # instead of assuming a source will never revise a settled row. The
    # default keeps legacy append-only behaviour for sparse/event datasets.
    reconciliation_lookback_days: int = 0
    # A second, shorter window for feeds whose full reconciliation is expensive
    # enough that paying it on every run is the dominant cost of their job.
    # ``announcement_index`` re-reads 30 days of CNINFO every sweep — ~40,000
    # records over ~1,350 pages at 30 rows a page, which the endpoint will not
    # widen — to pick up the few indexed late. Measured against the source: a
    # 3-, 7- and 14-day-old day each returned exactly what the lake already
    # held, while a 21-day-old day returned 21 more rows out of 5,932. So the
    # deep tail does real work and cannot be dropped; it just need not be
    # walked every run. 0 disables tiering.
    shallow_reconciliation_lookback_days: int = 0
    # ``trading_day`` counts exchange sessions through the watermark; the
    # calendar form is useful for feeds whose date axis is not a market
    # session. The name says ``days`` for API compatibility, but the unit is
    # explicit so a weekend cannot accidentally consume a reconciliation slot.
    reconciliation_lookback_mode: Literal["calendar", "trading_day"] = "calendar"
    # Whether a source promises immutable rows once observed.  This is kept
    # separate from the schema ``compatibility`` policy: a schema can be
    # additive while the upstream still revises a settled business key.  A
    # true value permits append-oriented consumers to skip a reconciliation
    # sweep; all datasets whose source can correct/restate rows leave it false.
    append_only: bool = False
    # Default TTL for source-empty evidence when a caller has no more specific
    # deployment setting. Config.negative_evidence_ttl_days overrides it.
    negative_evidence_ttl_days: int = 7
    # Whether one row's date axis is an exchange session or a natural calendar
    # day. Disclosure and news feeds publish on weekends and holidays, so their
    # incremental walk steps day by day and a session-free day that returns
    # zero rows is a fact about the calendar, not a source failure. Session
    # datasets keep the strict trading-day walk and keep failing loud on an
    # empty day, which is what catches a silently broken EOD source.
    session_scope: Literal["session", "calendar"] = "session"
    # Whether each row is an independently usable fact. When True, a batch with
    # a few invalid rows stages the valid ones, keeps the input and the rejects
    # under ``_quarantine`` and leaves the affected dates in the missing-date
    # ledger. Whole-set snapshots (membership, profiles, calendars) keep
    # all-or-nothing staging: a partial write there publishes a truncated set.
    partial_rows: bool = False
    # Whether a row lands on every exchange session in the covered span, so a
    # missing session is a gap whatever the partition period. The dashboard's
    # gaps, date picker and heatmap judge such datasets per session; a month or
    # year directory says nothing about one missing day inside it.
    #
    # Weaker than ``coverage_mode="session_dense"``, which implies it: dense
    # also pins the incremental watermark to the last contiguous session and
    # makes ``cne verify`` report each missing session, and a feed with old
    # holes (``margin_trading`` across the push2 ban) would re-read years of
    # history every run under it.
    session_rows: bool = False
    # How stored values are read: vendor placeholders that mean "no value",
    # the columns that must never be null, and whether rows are market bars.
    # Declared on the dataset's entry; applied once in ``validate_dataframe``.
    value_rules: ValueRules = NO_RULES
    # The config switch an optional capture needs (a key of ``_SWITCHES``), or
    # None for a dataset that is always on. See ``is_dataset_enabled``.
    enable_switch: str | None = None
    # Bumped when stored values change *meaning*, not shape: adding a column is
    # a schema change and leaves it alone; reinterpreting a value already
    # written does not, because old and new rows are then not comparable and a
    # reader has no other way to tell them apart. Stamped on every row as
    # ``data_version`` (see ``domain.schemas.data_version_for``).
    data_version: str = "v1"
    # What ``query.load`` offers on top of a plain read. They used to be names
    # written into the reader.
    #
    # ``adjust=`` joins ``adj_factors``: per-share prices stored unadjusted.
    adjustable: bool = False
    # ``universe=`` / ``profile=`` filter the rows by a research universe.
    universe_filter: bool = False
    # ``adjust="total_return"`` also reinvests fund distributions.
    total_return: bool = False
    # The catalog reports coverage only up to the last session before the first
    # missing one. For derived dense series, where a hole means the derive has
    # not caught up; not for fetched bars, whose history has real closures.
    coverage_end_contiguous: bool = False

    @property
    def pit(self) -> bool:
        """Point-in-time: ``load()`` requires ``as_of`` and filters on ``announce_date``."""
        return self.pit_quality in ("strict", "reconstructed")

    @property
    def pit_grade(self) -> PitGrade:
        """The none/strict/partial spelling of ``pit_quality`` in exported 0.x contracts."""
        return {"strict": "strict", "reconstructed": "partial"}.get(self.pit_quality, "none")

    def __post_init__(self) -> None:
        """Materialise inferred contract fields without changing old APIs.

        Inference lives here rather than in the exporter so code that consumes
        ``DATASETS`` directly sees exactly the same machine-readable metadata
        as code that loads a JSON contract.  Mappings are copied because the
        dataclass is frozen but a plain dict remains a mutable object.
        """

        if self.schema_version < 1:
            raise ValueError("schema_version must be >= 1")
        if self.reconciliation_lookback_days < 0:
            raise ValueError("reconciliation_lookback_days must be >= 0")
        if self.backfill_refetch not in (None, "scope", "checkpoint", "derivatives"):
            raise ValueError(f"unknown backfill_refetch: {self.backfill_refetch!r}")
        if self.shallow_reconciliation_lookback_days < 0:
            raise ValueError("shallow_reconciliation_lookback_days must be >= 0")
        if (
            self.shallow_reconciliation_lookback_days
            and self.shallow_reconciliation_lookback_days >= self.reconciliation_lookback_days
        ):
            raise ValueError(
                "shallow_reconciliation_lookback_days must be shorter than the full window"
            )
        if self.reconciliation_lookback_mode not in {"calendar", "trading_day"}:
            raise ValueError("reconciliation_lookback_mode must be 'calendar' or 'trading_day'")
        if not isinstance(self.append_only, bool):
            raise ValueError("append_only must be a bool")
        if self.append_only and self.reconciliation_lookback_days:
            raise ValueError("append_only datasets cannot have a reconciliation lookback")
        if self.negative_evidence_ttl_days < 0:
            raise ValueError("negative_evidence_ttl_days must be >= 0")
        if self.session_scope not in {"session", "calendar"}:
            raise ValueError("session_scope must be 'session' or 'calendar'")

        if self.pit_quality is None:
            if self.fetch_semantics == "snapshot" and not self.backfill_source:
                quality: PitQuality = "snapshot_only"
            else:
                # Non-PIT by-date data makes no PIT quality claim. This used to
                # default to ``strict`` so the exported contract stayed total,
                # which published the strongest possible claim for every table
                # nobody had considered -- 29 of the 30 ``strict`` datasets,
                # ``daily_bars`` among them. The contract is still total; the
                # value now says what it means.
                quality = "not_applicable"
            object.__setattr__(self, "pit_quality", quality)

        if self.pit_quality not in PIT_QUALITIES:
            raise ValueError(
                f"pit_quality must be one of {PIT_QUALITIES}, got {self.pit_quality!r}"
            )

        if self.availability_col is None:
            if self.pit:
                # All current PIT schemas carry the first-known/disclosure
                # timestamp under this canonical name.
                availability = "announce_date"
            elif self.query_date_col is not None:
                availability = self.query_date_col
            elif self.name == "instruments":
                availability = "list_date"
            else:
                availability = None
            object.__setattr__(self, "availability_col", availability)

        if self.unit_contract is None:
            # Most event/identity tables have no numeric field whose unit needs
            # declaring. ``canonical`` still states that in the contract and
            # gives diff tooling a stable value for when one is added.
            object.__setattr__(self, "unit_contract", "canonical")
        elif isinstance(self.unit_contract, Mapping):
            object.__setattr__(self, "unit_contract", dict(self.unit_contract))

        if isinstance(self.compatibility, Mapping):
            object.__setattr__(self, "compatibility", dict(self.compatibility))

    @property
    def query_date_col(self) -> str | None:
        return self.date_col or self.partition_col

    @property
    def reconciliation_lookback(self) -> int:
        """Compatibility alias for callers that omit the ``_days`` suffix."""
        return self.reconciliation_lookback_days

    @property
    def reconciliation_lookback_basis(self) -> str:
        """Compatibility alias for the explicit lookback unit."""
        return self.reconciliation_lookback_mode

    def earliest_available(self, today: date, *, trading_days_per_year: int = 242) -> date | None:
        """Rough calendar date before which the source serves nothing.

        Two mechanisms, and they must not be confused. A fixed
        ``history_floor_date`` is a date the vendor keeps back to and does not
        move as today does — it is returned as-is. ``history_horizon_days`` is
        a per-symbol retention *count*, expressed in trading days because that
        is how the vendor caps it, and converted with the usual ~242 sessions a
        year. Deliberately approximate and deliberately early: it guards a CLI
        window, and refusing a window the source would in fact have served is
        worse than fetching a few empty days.
        """
        if self.history_floor_date is not None:
            return self.history_floor_date
        if self.history_horizon_days is None:
            return None
        calendar_days = round(self.history_horizon_days * 365 / trading_days_per_year)
        return date.fromordinal(max(1, today.toordinal() - calendar_days))

    def partition_for(self, d: date) -> str:
        """Directory value of the partition holding *d* for this dataset."""
        return partition_value(d, self.partition_granularity)

    def partition_dir_name(self, d: date) -> str:
        """``trade_date=2024-06``: the directory holding *d*, at this dataset's period.

        Writing ``f"trade_date={day}"`` by hand assumes day directories; when a
        dataset moved to monthly files such a path found nothing, silently.
        """
        if self.partition_col is None:
            raise ValueError(f"{self.name} is not partitioned")
        return f"{self.partition_col}={self.partition_for(d)}"


# Coarser bars a user may store with `cne derive <name>`; see
# derive/minute_resample.py. Defined here so the registry, reader and derive
# module share one list.
RESAMPLED_MINUTE_DATASETS: dict[str, str] = {
    "minute_bars_15m": "15m",
    "minute_bars_30m": "30m",
    "minute_bars_60m": "60m",
}

# Derived datasets that `cne derive <name>` rebuilds. Not every derived
# dataset: a repository script builds `delisting_events`, so a remedy that
# named `cne derive` for it would send the user to an unknown target.
DERIVE_COMMAND_DATASETS = frozenset(
    {"adj_factors", "industry_index", "futures_continuous", "option_greeks"}
    | set(RESAMPLED_MINUTE_DATASETS)
)

_BAR = ("open", "high", "low", "close", "volume")

_SPECS = [
    # L0 reference
    # Live sources (TDX/EM) only list what trades today; baostock's stock_basic
    # is what recovers delisted codes, so `cne backfill instruments` is the only
    # path to a survivorship-free universe.
    DatasetSpec(
        "instruments",
        support="core",
        primary_source="tdx_protocol",
        backup_source="baostock",
        # TDX serves SH/SZ only, so the Beijing board is where BJ names enter
        # the catalogue — it is the exchange's own listing and the only source
        # here that carries 证券简称 for it. Sina's code-space sweep names what
        # is left: recovered delistings, and any BJ code the board has dropped.
        supplementary_sources=("bse", "sina"),
        tier="L0",
        partition_col=None,
        watermark=False,
        backfill_source="baostock",
    ),
    # Every name a security has carried, with its dates (Tushare namechange,
    # filled by `cne backfill name_history`, never daily). The names are the
    # exchange's ST labels and, unlike a
    # daily feed, also cover halted sessions; the suspension derive reads
    # trading_status.risk_warning from here. Small enough to re-read whole.
    DatasetSpec(
        "name_history",
        support="extended",
        primary_source="tushare",
        tier="L0",
        partition_col=None,
        date_col="start_date",
        watermark=False,
        required=False,
        empty_severity="info",
    ),
    # Tushare's own order-size money flow (moneyflow, optional token), from
    # 2010. A different measure from fund_flow's EastMoney one, so its own
    # table. Only `cne backfill fund_flow_tushare` fills it.
    DatasetSpec(
        "fund_flow_tushare",
        support="best_effort",
        unit_contract={
            "buy_small_volume": "share",
            "buy_small_amount": "CNY",
            "sell_small_volume": "share",
            "sell_small_amount": "CNY",
            "buy_medium_volume": "share",
            "buy_medium_amount": "CNY",
            "sell_medium_volume": "share",
            "sell_medium_amount": "CNY",
            "buy_large_volume": "share",
            "buy_large_amount": "CNY",
            "sell_large_volume": "share",
            "sell_large_amount": "CNY",
            "buy_xlarge_volume": "share",
            "buy_xlarge_amount": "CNY",
            "sell_xlarge_volume": "share",
            "sell_xlarge_amount": "CNY",
            "net_volume": "share",
            "net_amount": "CNY",
        },
        session_rows=True,
        primary_source="tushare",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        empty_severity="info",
        history_floor_date=date(2010, 1, 4),
    ),
    # Convertible and exchangeable bonds (Tushare cb_basic + cb_issue, optional
    # token): one row per bond, re-read whole; conversion_price is the current one.
    DatasetSpec(
        "convertible_bonds",
        support="best_effort",
        unit_contract={
            "par_value": "CNY",
            "issue_price": "CNY",
            "issue_size": "CNY",
            "remaining_size": "CNY",
            "coupon_rate": "percent",
            "first_conversion_price": "CNY/share",
            "conversion_price": "CNY/share",
        },
        primary_source="tushare",
        tier="L0",
        partition_col=None,
        date_col="value_date",
        watermark=False,
        required=False,
        empty_severity="info",
    ),
    # Daily convertible bond bars (Tushare cb_daily), from 2004; ~300 bonds a day.
    DatasetSpec(
        "convertible_bond_bars",
        support="best_effort",
        # Tushare sends 0 for the open, high and low of a session the bond did not
        # trade (and for every price of one past its redemption): the row records
        # the session, the prices do not exist.
        value_rules=ValueRules(
            zero_is_missing=("open", "high", "low", "close", "pre_close"),
            no_trade_missing=("open", "high", "low", "close"),
            non_null=("volume",),
            bar_kind="market",
        ),
        unit_contract={
            "open": "CNY/100_face",
            "high": "CNY/100_face",
            "low": "CNY/100_face",
            "close": "CNY/100_face",
            "pre_close": "CNY/100_face",
            "volume": "bond",
            "amount": "CNY",
        },
        primary_source="tushare",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        empty_severity="info",
        history_floor_date=date(2004, 1, 2),
    ),
    # Announced major-holder and executive trades (Tushare stk_holdertrade), by
    # announcement date from 2006; tens a day.
    DatasetSpec(
        "holder_trades",
        support="best_effort",
        unit_contract={
            "change_shares": "share",
            "change_ratio": "percent",
            "after_shares": "share",
            "after_ratio": "percent",
            "avg_price": "CNY/share",
        },
        primary_source="tushare",
        tier="L2",
        partition_col="ann_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2006, 1, 1),
    ),
    # Share buyback progress announcements (Tushare repurchase), from 2005.
    DatasetSpec(
        "share_repurchases",
        support="best_effort",
        value_rules=ValueRules(zero_is_missing=("price_high", "price_low")),
        unit_contract={
            "shares": "share",
            "amount": "CNY",
            "price_high": "CNY/share",
            "price_low": "CNY/share",
        },
        primary_source="tushare",
        tier="L2",
        partition_col="ann_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2005, 1, 1),
    ),
    # Weekly share-pledge totals per stock (Tushare pledge_stat), from 2014-03-07.
    DatasetSpec(
        "pledge_stats",
        support="best_effort",
        unit_contract={
            "unrestricted_pledged_shares": "share",
            "restricted_pledged_shares": "share",
            "total_shares": "share",
            "pledge_ratio": "percent",
        },
        primary_source="tushare",
        tier="L8",
        partition_col="end_date",
        required=False,
        empty_severity="info",
        history_floor_date=date(2014, 3, 7),
    ),
    # Tushare's holding-cost distribution estimate per stock (cyq_perf), from 2018.
    DatasetSpec(
        "chip_distribution",
        support="best_effort",
        value_rules=ValueRules(
            zero_is_missing=(
                "history_low",
                "history_high",
                "cost_5pct",
                "cost_15pct",
                "cost_50pct",
                "cost_85pct",
                "cost_95pct",
                "cost_mean",
            )
        ),
        unit_contract={
            "history_low": "CNY/share",
            "history_high": "CNY/share",
            "cost_5pct": "CNY/share",
            "cost_15pct": "CNY/share",
            "cost_50pct": "CNY/share",
            "cost_85pct": "CNY/share",
            "cost_95pct": "CNY/share",
            "cost_mean": "CNY/share",
            "winner_rate": "percent",
        },
        session_rows=True,
        primary_source="tushare",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        empty_severity="info",
        history_floor_date=date(2018, 1, 2),
    ),
    # The ten most traded Connect stocks per channel and session (hsgt_top10),
    # from 2014-11-17; net/buy/sell amounts end 2024-08-16.
    DatasetSpec(
        "northbound_top10",
        support="best_effort",
        unit_contract={
            "close": "CNY/share",
            "change": "percent",
            "amount": "CNY",
            "net_amount": "CNY",
            "buy_amount": "CNY",
            "sell_amount": "CNY",
        },
        primary_source="tushare",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2014, 11, 17),
    ),
    # Stocks at the price limit each session (Tushare limit_list_d), from 2020;
    # up, down, and touched-up-but-opened (up_broken). ~100 a day.
    DatasetSpec(
        "limit_events",
        support="best_effort",
        value_rules=ValueRules(zero_is_missing=("close",)),
        unit_contract={
            "close": "CNY/share",
            "pct_change": "percent",
            "amount": "CNY",
            "amount_at_limit": "CNY",
            "seal_amount": "CNY",
            "float_market_cap": "CNY",
            "total_market_cap": "CNY",
            "turnover_rate": "percent",
        },
        primary_source="tushare",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        empty_severity="info",
        history_floor_date=date(2020, 1, 2),
    ),
    # Futures member volume / long / short rankings per contract and session
    # (Tushare fut_holding), every exchange from 2010; ~5,000 rows a day.
    DatasetSpec(
        "futures_member_positions",
        support="best_effort",
        unit_contract={
            "volume": "contract",
            "volume_change": "contract",
            "long_open_interest": "contract",
            "long_change": "contract",
            "short_open_interest": "contract",
            "short_change": "contract",
        },
        primary_source="tushare",
        tier="L9",
        partition_col="trade_date",
        required=False,
        empty_severity="info",
        history_floor_date=date(2010, 1, 4),
    ),
    # Company earnings forecasts (Tushare forecast) by announcement date, from 2004.
    DatasetSpec(
        "earnings_forecasts",
        support="best_effort",
        unit_contract={
            "change_min_pct": "percent",
            "change_max_pct": "percent",
            "net_profit_min": "CNY",
            "net_profit_max": "CNY",
            "prior_net_profit": "CNY",
        },
        primary_source="tushare",
        tier="L2",
        partition_col="ann_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2004, 1, 1),
    ),
    # Revenue, cost and gross profit per product / region / industry segment
    # (Tushare fina_mainbz_vip), per report period from 2004.
    DatasetSpec(
        "business_segments",
        support="best_effort",
        unit_contract={
            "revenue": "source_native",
            "cost": "source_native",
            "gross_profit": "source_native",
        },
        primary_source="tushare",
        tier="L3",
        partition_col="period_end",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2004, 3, 31),
    ),
    # Institutional investor visits, one row per visiting organisation (Tushare
    # stk_surv), from 2021-08.
    DatasetSpec(
        "investor_visits",
        support="best_effort",
        primary_source="tushare",
        tier="L2",
        partition_col="visit_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2021, 8, 1),
    ),
    # Tushare's hot-money trader roster with seats (hm_list), re-read whole.
    DatasetSpec(
        "hot_money_traders",
        support="best_effort",
        primary_source="tushare",
        tier="L4",
        partition_col=None,
        date_col="trader",
        watermark=False,
        required=False,
        empty_severity="info",
    ),
    # Roster traders' daily dragon-tiger trades (Tushare hm_detail), from 2022-08.
    DatasetSpec(
        "hot_money_trades",
        support="best_effort",
        unit_contract={"buy_amount": "CNY", "sell_amount": "CNY", "net_amount": "CNY"},
        primary_source="tushare",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        empty_severity="info",
        history_floor_date=date(2022, 8, 1),
    ),
    # Convertible bond redemption announcements (Tushare cb_call), per bond.
    DatasetSpec(
        "convertible_bond_calls",
        support="best_effort",
        unit_contract={
            "call_price": "CNY/100_face",
            "call_price_after_tax": "CNY/100_face",
            "call_volume": "bond",
            "call_amount": "CNY",
        },
        primary_source="tushare",
        tier="L2",
        partition_col=None,
        date_col="ann_date",
        watermark=False,
        required=False,
        empty_severity="info",
    ),
    # Periodic convertible bond conversion reports (Tushare cb_share).
    DatasetSpec(
        "convertible_bond_conversions",
        support="best_effort",
        unit_contract={
            "issue_size": "CNY",
            "first_conversion_price": "CNY/share",
            "conversion_price": "CNY/share",
            "converted_amount": "CNY",
            "converted_shares": "share",
            "converted_pct": "percent",
            "total_converted_amount": "CNY",
            "total_converted_shares": "share",
            "total_converted_pct": "percent",
            "remaining_size": "CNY",
            "total_shares": "share",
        },
        primary_source="tushare",
        tier="L2",
        partition_col="end_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2000, 1, 1),
    ),
    # Convertible bond coupon rate per interest year (Tushare cb_rate).
    DatasetSpec(
        "convertible_bond_coupons",
        support="best_effort",
        unit_contract={"coupon_rate": "percent"},
        primary_source="tushare",
        tier="L0",
        partition_col=None,
        date_col="start_date",
        watermark=False,
        required=False,
        empty_severity="info",
    ),
    # THS and EastMoney popularity lists (Tushare ths_hot / dc_hot), from 2023-11.
    DatasetSpec(
        "popularity_ranks",
        support="best_effort",
        # THS gives no price for its bond, ETF, HK, US and board lists.
        value_rules=ValueRules(zero_is_missing=("price",)),
        unit_contract={
            "pct_change": "percent",
            "price": "source_native",
            "heat": "source_native",
        },
        primary_source="tushare",
        tier="L7",
        partition_col="trade_date",
        required=False,
        empty_severity="info",
        history_floor_date=date(2023, 10, 1),
    ),
    # Consecutive limit-up runs (Tushare limit_step), from 2023-11.
    DatasetSpec(
        "limit_ladder",
        support="best_effort",
        primary_source="tushare",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2023, 10, 1),
    ),
    # THS concept boards ranked by limit-up activity (Tushare limit_cpt_list).
    DatasetSpec(
        "strongest_sectors",
        support="best_effort",
        unit_contract={"pct_change": "percent"},
        primary_source="tushare",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(2023, 10, 1),
    ),
    # 开盘啦 limit-up list (Tushare kpl_list, tag 涨停), from 2018-01.
    DatasetSpec(
        "kpl_limit_ups",
        support="best_effort",
        unit_contract={
            "amount": "CNY",
            "turnover_rate": "percent",
            "free_float_cap": "CNY",
            "main_net_amount": "CNY",
            "limit_order_amount": "CNY",
            "max_limit_order_amount": "CNY",
        },
        primary_source="tushare",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        empty_severity="info",
        history_floor_date=date(2017, 10, 1),
    ),
    # THS net flow per stock by order size (Tushare moneyflow_ths), from 2024-12.
    DatasetSpec(
        "fund_flow_ths_tushare",
        support="best_effort",
        value_rules=ValueRules(zero_is_missing=("close",)),
        unit_contract={
            "close": "CNY/share",
            "change_pct": "percent",
            "net_amount": "CNY",
            "net_amount_5d": "CNY",
            "large_net_amount": "CNY",
            "large_net_rate": "percent",
            "medium_net_amount": "CNY",
            "medium_net_rate": "percent",
            "small_net_amount": "CNY",
            "small_net_rate": "percent",
        },
        primary_source="tushare",
        tier="L4",
        partition_col="trade_date",
        required=False,
        empty_severity="info",
        history_floor_date=date(2024, 10, 1),
    ),
    # Investor questions and company answers on SSE/SZSE platforms (Tushare irm_qa_sh/sz).
    DatasetSpec(
        "investor_qa",
        support="best_effort",
        primary_source="tushare",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        empty_severity="info",
        history_floor_date=date(2022, 1, 1),
    ),
    # Announced board members, supervisors and executives (Tushare stk_managers).
    DatasetSpec(
        "executives",
        support="best_effort",
        primary_source="tushare",
        tier="L0",
        partition_col="ann_date",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(1990, 1, 1),
    ),
    # Executive pay and shareholdings per report period (Tushare stk_rewards).
    DatasetSpec(
        "executive_compensation",
        support="best_effort",
        unit_contract={"pay": "CNY", "shares_held": "share"},
        primary_source="tushare",
        tier="L3",
        partition_col="period_end",
        partition_granularity="year",
        required=False,
        empty_severity="info",
        history_floor_date=date(1990, 1, 1),
    ),
    # Exchange price bands per stock and session (Tushare stk_limit, optional
    # token), from 2007. Only `cne backfill limit_prices` fills it.
    DatasetSpec(
        "limit_prices",
        support="best_effort",
        # Both limits arrive as 0 together on sessions Tushare has none for.
        value_rules=ValueRules(zero_is_missing=("up_limit", "down_limit")),
        unit_contract={"up_limit": "CNY/share", "down_limit": "CNY/share"},
        session_rows=True,
        primary_source="tushare",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        empty_severity="info",
        history_floor_date=date(2007, 1, 4),
    ),
    # Official exchange fund directories are current snapshots. A row first
    # observed today cannot be backdated into yesterday's ETF universe.
    DatasetSpec(
        "etf_profiles",
        support="extended",
        value_rules=ValueRules(
            non_null=(
                "exchange",
                "name",
                "eligibility_status",
                "classification_basis",
                "source_url",
            )
        ),
        primary_source="exchange",
        tier="L0",
        partition_col="as_of_date",
        partition_granularity="year",
        fetch_semantics="snapshot",
        watermark=False,
        required=False,
        empty_severity="info",
    ),
    DatasetSpec(
        "trading_calendar",
        support="core",
        value_rules=ValueRules(non_null=("is_trading",)),
        primary_source="tdx_protocol",
        backup_source="exchange",
        tier="L0",
        partition_col="trade_date",
        partition_granularity="year",
    ),
    DatasetSpec(
        "trading_status",
        support="core",
        value_rules=ValueRules(non_null=("is_trading", "status")),
        backfill_symbols_in_window=True,
        # The exchanges' own boards come first: both publish every listed
        # security with its OHLC and its 证券简称, which carries the halt
        # (Shanghai zeroes open/high/low beside a reference close; Shenzhen
        # repeats the close with zero volume) and the ST designation, and the
        # BSE board answers Beijing. Against EastMoney on 2026-09-15 ST agreed
        # on 100.000% and halts on 99.923%, every disagreement a halt the
        # exchange rule then missed. Since the 2026-09-22 push2 ban they have
        # supplied the whole market.
        primary_source="exchange",
        # Suspensions reconstructed from `daily_bars` interior gaps land under
        # `derived_bar_gap`/`derived_delisted`; the BSE board and its
        # announcements are the Beijing route.
        supplementary_sources=("derived", "bse"),
        # EastMoney's current-state board, reached through
        # `tdx_protocol.client.fetch_trading_status` (which only forwards to it;
        # TDX has no ST or halt feed). Asked only for what the boards leave.
        backup_source="eastmoney",
        tier="L0",
        partition_col="trade_date",
        partition_granularity="month",
        # EastMoney's daily ST board is a live current-state snapshot; the
        # dedicated backfill path uses baostock history.  Marking the daily
        # contract as by_date would replay today's labels into missed past
        # sessions when the watermark falls behind.
        fetch_semantics="snapshot",
        backfill_source="baostock",
    ),
    # L1 bars
    DatasetSpec(
        "daily_bars",
        adjustable=True,
        universe_filter=True,
        total_return=True,
        # data_version v2: `volume` is 股 for every source. v1 rows are 手 from
        # tdx_protocol and sina, 股 from ths and baostock — see
        # `cnequity.domain.units` and docs/zh/datasets/schema.md.
        data_version="v2",
        support="core",
        value_rules=ValueRules(non_null=_BAR, bar_kind="market"),
        unit_contract={
            "price": "CNY/share",
            "volume": "share",
            "amount": "CNY",
        },
        backfill_symbols_in_window=True,
        # v2 records month partitions. Columns and the primary key are unchanged.
        schema_version=2,
        partial_rows=True,
        primary_source="tdx_protocol",
        backup_source="eastmoney",
        # Each of these lands rows under its own label through
        # `_finish_daily_bars`' recovery chain.
        supplementary_sources=("exchange", "bse", "sina", "ths", "ths_official", "baostock"),
        tier="L1",
        partition_col="trade_date",
        # Month files: a one-symbol history read opened 11,149 day files of
        # ~44 KiB each (0.87 s); 432 month files answer it in 0.11 s, and a
        # daily compact merges one session into its month in ~30 ms.
        partition_granularity="month",
        reconciliation_lookback_days=5,
        # TDX bills per symbol, not per session — one request returns up to 800
        # bars — so a short reconciliation window costs about the same as one
        # session. The tip comes from each exchange's whole-board publication,
        # which is priced separately and runs on its own cadence. The tail still
        # catches vendor revisions, weekly rather than daily.
        shallow_reconciliation_lookback_days=1,
        reconciliation_lookback_mode="trading_day",
        coverage_mode="session_dense",
    ),
    DatasetSpec(
        "index_bars",
        support="core",
        value_rules=ValueRules(non_null=_BAR, bar_kind="market"),
        unit_contract={
            "price": "index_point",
            "volume": "source_native",
            "amount": "source_native",
        },
        partial_rows=True,
        primary_source="tdx_protocol",
        backup_source="eastmoney",
        # THS serves the board indices whose base the other two disagree on;
        # mixing them inside one series is what a dedicated route avoids.
        supplementary_sources=("ths",),
        tier="L1",
        partition_col="trade_date",
        partition_granularity="year",
        reconciliation_lookback_days=5,
        reconciliation_lookback_mode="trading_day",
        coverage_mode="session_dense",
    ),
    # 1-minute bars. Day partitions: ~240 bars × the configured scope, which is
    # 1.3M rows a day at full market — the top of the ≥1000 rows/day band, and
    # ~30MB a partition. The schema draft once sketched
    # frequency/trade_date/symbol_bucket; a second directory level buys nothing
    # at that size and every partition-aware module here assumes exactly one.
    #
    # Opt-in (required=False): this is not on the default daily waves and a lake
    # that never enabled it must not be judged unhealthy for holding no rows.
    DatasetSpec(
        "minute_bars",
        adjustable=True,
        enable_switch="minute_bars_1m",
        support="optional",
        value_rules=ValueRules(non_null=_BAR, bar_kind="market"),
        unit_contract={
            "price": "CNY/share",
            "volume": "share",
            "amount": "CNY",
        },
        partial_rows=True,
        primary_source="tdx_protocol",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="day",
        fetch_semantics="by_date",
        required=False,
        coverage_mode="session_dense",
        # Measured 2026-08-01 against 120.76.1.198:7709 — 22,800 bars for every
        # symbol probed, across both exchanges and every liquidity band, so it
        # is a server retention window rather than a per-symbol artefact.
        history_horizon_days=95,
        # Tip-paged: chunk by symbol, not by date (see backfill_chunk_symbols).
        backfill_chunk_symbols=200,
        reconciliation_lookback_days=2,
        reconciliation_lookback_mode="trading_day",
        intraday_frequency="1m",
        row_grain="1m",
    ),
    # 5-minute bars — a separate dataset, not a `frequency` value inside
    # minute_bars, because the horizon differs by 5× and a dataset carries one
    # watermark and one coverage_start (see DatasetSpec.intraday_frequency).
    #
    # This is the only intraday frequency with real history: two years against
    # 1m's four and a half months, at a fifth of the volume (~7MB a day at full
    # market). For most research it is the more useful of the two, which is why
    # it is registered rather than left as a resampling exercise.
    #
    # 15m/30m/60m are not fetched. TDX serves them over the same 491-day window,
    # but they resample from 1m/5m already here, so they exist only as the
    # opt-in derived datasets below (`cne derive minute_bars_15m`).
    DatasetSpec(
        "minute_bars_5m",
        adjustable=True,
        enable_switch="minute_bars_5m",
        support="optional",
        value_rules=ValueRules(non_null=_BAR, bar_kind="market"),
        unit_contract={
            "price": "CNY/share",
            "volume": "share",
            "amount": "CNY",
        },
        partial_rows=True,
        primary_source="tdx_protocol",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="day",
        fetch_semantics="by_date",
        required=False,
        coverage_mode="session_dense",
        # Measured 2026-08-01: 23,568 bars = 491 trading days, back to
        # 2024-07-23. 15m/30m/60m share exactly that window (7,856 / 3,928 /
        # 1,964 bars), which is what says it is a time-based retention policy
        # rather than a bar-count cap.
        history_horizon_days=491,
        # Same tip-paged contract as 1m; 200 symbols × 48 bars × 491 days ≈
        # 4.7M rows per sub-run — comparable compact memory to 1m's chunk.
        backfill_chunk_symbols=200,
        reconciliation_lookback_days=2,
        reconciliation_lookback_mode="trading_day",
        intraday_frequency="5m",
        row_grain="5m",
    ),
    # Transaction records (分笔). Not tick data: A-share Level-1 is a 3-second
    # snapshot, so a record aggregates 6–33 real trades (measured) and a
    # session holds ~2,700 on average, ~4,800 at most.
    #
    # Deliberately *not* `intraday_frequency`. That field means "one bar
    # frequency", and every consumer of it — the audit's session-shape checks,
    # the reader's adjustment set, `cne backfill --symbols` — assumes a
    # `bar_time` column and a bar count per session. This dataset has neither,
    # and inheriting those code paths would give it checks that silently pass
    # on the wrong column. It carries its own step group and its own checks.
    DatasetSpec(
        "trade_ticks",
        adjustable=True,
        enable_switch="trade_ticks",
        support="optional",
        value_rules=ValueRules(
            non_null=("price", "volume", "trade_time", "direction"), bar_kind="market"
        ),
        unit_contract={
            "price": "CNY/share",
            "volume": "share",
            "direction": "inferred_tick_rule",
        },
        primary_source="tdx_protocol",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="day",
        fetch_semantics="by_date",
        required=False,
        coverage_mode="session_dense",
        # A *date*, not a rolling count — see DatasetSpec.history_floor_date.
        # Measured 2026-08-02: every symbol probed serves back to exactly
        # 2024-01-02 and no further, which is ~624 trading days and growing.
        # The edge landing on a calendar boundary suggests the retention may be
        # year-granular rather than a fixed date, so re-measure each January
        # with scripts/dev/probe_trade_ticks.py.
        history_floor_date=date(2024, 1, 2),
        # By-date requests, so date chunks are the cheap axis — the exact
        # opposite of the minute bars above, where the wire always walks from
        # today's tip and a date slice re-fetches everything newer than it.
        # 5 days × 200 symbols × ~2,700 rows ≈ 2.7M rows per sub-run.
        backfill_chunk_days=5,
        append_only=True,
        # Intraday, but not a bar frequency — see DatasetSpec.row_grain for why
        # this is not `intraday_frequency`.
        row_grain="tick",
    ),
    # Domestic commodity futures main-continuous (东财主连) + narrow offshore
    # gold (Sina COMEX ``GC0.CMX``); not A-share equity.
    DatasetSpec(
        "commodity_bars",
        support="extended",
        value_rules=ValueRules(non_null=_BAR, bar_kind="market"),
        unit_contract={
            "price": "source_native",
            "volume": "contract",
            "amount": "source_native",
            "open_interest": "contract",
        },
        partial_rows=True,
        primary_source="sina",
        backup_source="eastmoney",
        tier="L9",
        partition_col="trade_date",
        partition_granularity="year",
        fetch_semantics="by_date",
        backfill_source="eastmoney_kline+sina_global",
        required=False,
        max_staleness_days=2,
    ),
    # Futures and options (ADR-0013). Off unless `[futures] enabled`; see
    # `is_dataset_enabled`. The contract tables are rebuilt from what the
    # exchanges' daily files showed plus their reference files, so they carry
    # no watermark of their own.
    DatasetSpec(
        "futures_contracts",
        enable_switch="futures",
        support="optional",
        value_rules=ValueRules(non_null=("exchange", "exchange_code", "product", "dates_basis")),
        # Derivatives (ADR-0013): prices in each product's quote unit, counts on
        # one side of the trade whatever the exchange published, turnover in 元.
        unit_contract={"multiplier": "quote_unit_to_CNY", "tick_size": "source_native"},
        primary_source="futures_exchange",
        # DCE's contracts, while its own endpoints stay behind a challenge.
        supplementary_sources=("sina",),
        tier="L9",
        partition_col=None,
        date_col="list_date",
        watermark=False,
        required=False,
    ),
    DatasetSpec(
        "option_contracts",
        enable_switch="futures_options",
        support="optional",
        value_rules=ValueRules(
            non_null=(
                "exchange",
                "exchange_code",
                "product",
                "underlying_symbol",
                "option_type",
                "strike",
                "dates_basis",
            )
        ),
        unit_contract={
            "strike": "source_native",
            "multiplier": "quote_unit_to_CNY",
            "tick_size": "source_native",
        },
        primary_source="futures_exchange",
        tier="L9",
        partition_col=None,
        date_col="list_date",
        watermark=False,
        required=False,
    ),
    # Per-contract daily bars from the exchanges' own session files. Several
    # exchanges feed one dataset, so a session can arrive partly: the ones that
    # published are written and the reconciliation tail comes back for the
    # rest. `sparse` rather than `session_dense` because a dense watermark
    # would stop at the first historical hole and re-read years every run;
    # completeness is proved per exchange by `cne status` and the audit.
    DatasetSpec(
        "futures_bars",
        enable_switch="futures",
        support="optional",
        value_rules=ValueRules(
            non_null=("exchange", "exchange_code", "product", "volume", "open_interest"),
            bar_kind="derivative",
        ),
        unit_contract={
            "price": "source_native",
            "volume": "contract_single_sided",
            "amount": "CNY",
            "open_interest": "contract_single_sided",
        },
        backfill_refetch="derivatives",
        partial_rows=True,
        primary_source="futures_exchange",
        # DCE, from mid-2018: no turnover, no-trade sessions absent (ADR-0013).
        supplementary_sources=("sina",),
        tier="L9",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
        history_floor_date=date(2002, 1, 7),
        backfill_chunk_days=365,
        reconciliation_lookback_days=3,
        reconciliation_lookback_mode="trading_day",
    ),
    # One-minute bars for a watchlist of contracts. Sina serves only its
    # latest 1023 bars per contract (about two sessions for a night-trading
    # product), so this grows forward from the day it is enabled and must run
    # every session. `row_grain` only: `intraday_frequency` would pull in the
    # A-share TDX minute machinery, whose session windows and
    # trade_date == bar_time.date() do not hold for night sessions.
    DatasetSpec(
        "futures_minute_bars",
        enable_switch="futures_minute",
        support="optional",
        value_rules=ValueRules(bar_kind="derivative"),
        unit_contract={
            "price": "source_native",
            "volume": "contract_single_sided",
            "open_interest": "contract_single_sided",
        },
        primary_source="sina",
        tier="L9",
        partition_col="trade_date",
        partition_granularity="day",
        required=False,
        history_horizon_days=2,
        row_grain="1m",
    ),
    DatasetSpec(
        "option_bars",
        enable_switch="futures_options",
        support="optional",
        value_rules=ValueRules(
            non_null=(
                "exchange",
                "exchange_code",
                "product",
                "underlying_symbol",
                "option_type",
                "strike",
                "volume",
                "open_interest",
            ),
            bar_kind="derivative",
        ),
        unit_contract={
            "price": "source_native",
            "strike": "source_native",
            "volume": "contract_single_sided",
            "amount": "CNY",
            "open_interest": "contract_single_sided",
            "exercise_volume": "contract_single_sided",
            "delta": "unitless",
            "implied_vol": "fraction",
            "series_implied_vol": "fraction",
        },
        backfill_source_overrides=("tushare",),
        backfill_refetch="derivatives",
        partial_rows=True,
        primary_source="futures_exchange",
        tier="L9",
        partition_col="trade_date",
        partition_granularity="day",
        required=False,
        history_floor_date=date(2017, 4, 19),
        backfill_chunk_days=90,
        reconciliation_lookback_days=3,
        reconciliation_lookback_mode="trading_day",
    ),
    # L2 corporate events
    DatasetSpec(
        "corporate_actions",
        support="core",
        unit_contract={
            "cash_dividend": "CNY/share",
            "bonus_ratio": "share/share",
            "transfer_ratio": "share/share",
            "split_factor": "unit/unit",
            "allotment_ratio": "share/share",
            "allotment_price": "CNY/share",
            "reference_price": "CNY/share",
        },
        backfill_symbols_in_window=True,
        schema_version=2,
        # Daily: the exchange's adjusted previous close (TDX quotes) names the
        # session's ex-events and TDX xdxr records them; EastMoney's one
        # market-wide query is the backup, the same-day peer, and the source
        # of the 送/转 split TDX lacks. A weekly TDX sweep catches the rest.
        # Keep this aligned with the failover config because the registry
        # also controls canonical row precedence in query views.
        primary_source="tdx_protocol",
        backup_source="eastmoney",
        # baostock supplies actions for delisted names the live boards cannot
        # answer for; the THS dividend page is the explicit BJ repair route.
        supplementary_sources=("baostock", "ths"),
        # Issuer payment-date repair can publish CNINFO and exchange notice
        # evidence, but neither is an automatic failover for daily actions.
        repair_sources=("cninfo", "exchange"),
        tier="L2",
        partition_col="ex_date",
        partition_granularity="year",
        backfill_source="tdx_protocol",
        reconciliation_lookback_days=30,
    ),
    DatasetSpec(
        "announcement_index",
        support="extended",
        primary_source="cninfo",
        tier="L2",
        partition_col="announce_date",
        pit_quality="strict",
        # Listed companies disclose on Saturdays, and CNINFO indexes those
        # announcements under the calendar date they were published.
        session_scope="calendar",
        reconciliation_lookback_days=30,
        shallow_reconciliation_lookback_days=7,
        # CNINFO's ``hisAnnouncement`` endpoint is page-limited and has been
        # observed to replay an earlier page once a broad request gets deep
        # enough.  Keep each historical request below one calendar month so a
        # source-side pagination defect cannot turn a multi-year backfill into
        # an unbounded walk.  The adapter still recursively halves a busy
        # month/day and fails closed when a single day cannot be split.
        backfill_chunk_days=31,
    ),
    # Current-state timetable (revisions overwrite scheduled_date; not PIT).
    DatasetSpec(
        "earnings_disclosure_schedule",
        support="extended",
        primary_source="eastmoney",
        tier="L2",
        partition_col="report_period",
        partition_granularity="quarter",
        watermark=False,
        reconciliation_lookback_days=120,
    ),
    # L3 fundamentals
    DatasetSpec(
        "financial_statement_items",
        support="core",
        unit_contract={"item_value": "source_native"},
        backfill_source_overrides=("tdx_fin",),
        backfill_refetch="scope",
        backfill_symbols_in_window=True,
        primary_source="eastmoney",
        # 通达信's quarterly packages: the daily route when EastMoney fails, and
        # the source `cne backfill --source tdx_fin` fills missing keys from.
        backup_source="tdx_fin",
        # The keyed 同花顺 API is the arbitration peer and fills what the
        # EastMoney datacenter reports omit.
        supplementary_sources=("ths_official",),
        # `cne repair financial-originals` publishes CNINFO's original-filing
        # figures; never an automatic route.
        repair_sources=("cninfo",),
        tier="L3",
        partition_col="report_period",
        partition_granularity="quarter",
        watermark=False,
        # Historical EastMoney backfill returns today's current/restated
        # value while borrowing the first-disclosure date from LICO.  It is
        # useful for exploratory reads but cannot be advertised as exact PIT.
        pit_quality="reconstructed",
        # Original-date rows rebuilt from a daily capture keep this label.
        reconstructed_sources=(
            ("eastmoney", "daily:", "eastmoney_backfill"),
            # A package's older periods may hold restated values (see
            # adapters/tdx_fin/packages.py); one capture yields both labels.
            ("tdx_fin", "tdx_fin:", "tdx_fin_backfill"),
        ),
        # Daily ingestion is keyed by NOTICE_DATE while storage is keyed by
        # report period.  Re-read the recent disclosure window so late
        # filings/restatements are captured; the adapter's period backfill
        # remains the source of truth for older reports.
        reconciliation_lookback_days=30,
    ),
    # Shareholder structure — the dimensions the long-format statement table
    # cannot hold. `top_holders` is a ranked repeating group of ten, which no
    # amount of `item_code` rows expresses; the other two are wide fixed
    # records that would only be item_codes by accident of shape.
    #
    # All three are PIT for the reason FSI is: a 半年报 shareholder list is
    # dated 06-30 and disclosed in late August, so keying it by period alone
    # would let a July backtest read August's filing.
    DatasetSpec(
        "share_structure",
        # data_version v2: `float_a_shares` is filled.
        data_version="v2",
        support="core",
        unit_contract={
            "total_shares": "share",
            "float_shares": "share",
            "restricted_shares": "share",
            "float_a_shares": "share",
            "free_float_shares": "share",
        },
        backfill_refetch="scope",
        backfill_symbols_in_window=True,
        # v2: `float_a_shares` (流通A股).
        schema_version=2,
        primary_source="eastmoney",
        # TDX xdxr share-change events, read by the weekly corporate-actions
        # sweep: the peer for total and A-share float, and the rows written
        # when EastMoney has gone stale. They carry no disclosure date, so a
        # backup row is dated by the sweep that observed it.
        backup_source="tdx_protocol",
        tier="L3",
        partition_col="change_date",
        partition_granularity="year",
        date_col="change_date",
        # Measured 2026-08 against RPT_F10_EH_EQUITY: 1990 serves 19 rows, and
        # nothing before it. A fixed vendor floor, not a rolling budget.
        history_floor_date=date(1990, 1, 1),
        watermark=False,
        pit_quality="reconstructed",
        reconciliation_lookback_days=30,
    ),
    DatasetSpec(
        "shareholder_counts",
        support="extended",
        unit_contract={
            "holder_count": "holder",
            "avg_float_shares": "share",
            "avg_holding_value": "CNY",
            "holder_count_change_pct": "percent",
        },
        backfill_refetch="scope",
        primary_source="eastmoney",
        tier="L3",
        partition_col="count_date",
        partition_granularity="year",
        date_col="count_date",
        # RPT_F10_EH_HOLDERNUM: 25 rows in 1992, none in 1990/1991.
        history_floor_date=date(1992, 1, 1),
        watermark=False,
        pit_quality="reconstructed",
        reconciliation_lookback_days=30,
    ),
    DatasetSpec(
        "top_holders",
        support="extended",
        unit_contract={"holding_shares": "share", "holding_pct": "fraction"},
        backfill_refetch="scope",
        primary_source="eastmoney",
        tier="L3",
        partition_col="record_date",
        partition_granularity="year",
        date_col="record_date",
        # 2003, and the binding constraint is PIT rather than availability.
        # RPT_F10_EH_HOLDERS reaches back to the 1990s, but it carries no
        # NOTICE_DATE and borrows its disclosure date from
        # RPT_F10_EH_FREEHOLDERS — which starts in 2003 (0 rows in 1999-2002,
        # 13,853 in 2003). Before that the total-scope rows have nothing to
        # borrow from and are dropped as undated, so a backfill reaching
        # further back fetches ~112k rows across four years and writes none.
        history_floor_date=date(2003, 1, 1),
        watermark=False,
        pit_quality="reconstructed",
        reconciliation_lookback_days=240,
    ),
    DatasetSpec(
        "valuation_metrics",
        support="core",
        # A negative ratio is a loss-making company, so only 0 is the placeholder.
        value_rules=ValueRules(
            zero_is_missing=("pe_ttm", "pe_dynamic", "pb", "ps_ttm", "total_mv", "float_mv")
        ),
        unit_contract={
            "pe_ttm": "ratio",
            "pe_dynamic": "ratio",
            "pb": "ratio",
            "ps_ttm": "ratio",
            "total_mv": "CNY",
            "float_mv": "CNY",
        },
        # v2 records month partitions. Columns and the primary key are unchanged.
        schema_version=2,
        session_rows=True,
        partial_rows=True,
        primary_source="eastmoney",
        tier="L3",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        backfill_source="baostock",
        # Snapshot values are allowed to settle/retry on the same session.
    ),
    DatasetSpec(
        "analyst_consensus",
        support="best_effort",
        value_rules=ValueRules(zero_is_missing=("target_price",)),
        unit_contract={
            "eps_forecast": "CNY/share",
            "target_price": "CNY/share",
            "pe_forecast": "ratio",
            "analyst_count": "analyst",
        },
        primary_source="eastmoney",
        tier="L3",
        partition_col="forecast_date",
        fetch_semantics="snapshot",
    ),
    # Dated per-report forecasts (Tushare report_rc, optional token);
    # analyst_consensus above has no history. Reports also land on weekends,
    # so each run reads every calendar day since the previous session.
    DatasetSpec(
        "broker_forecasts",
        support="best_effort",
        value_rules=ValueRules(zero_is_missing=("max_price", "min_price")),
        unit_contract={
            "op_rt": "10k_CNY",
            "op_pr": "10k_CNY",
            "tp": "10k_CNY",
            "np": "10k_CNY",
            "eps": "CNY/share",
            "pe": "ratio",
            "rd": "percent",
            "roe": "percent",
            "ev_ebitda": "ratio",
            "max_price": "CNY/share",
            "min_price": "CNY/share",
        },
        primary_source="tushare",
        tier="L3",
        partition_col="report_date",
        partition_granularity="month",
        fetch_semantics="by_date",
        required=False,
        empty_severity="info",
        history_floor_date=date(2016, 1, 1),
    ),
    # L4 capital flows
    DatasetSpec(
        "fund_flow",
        support="best_effort",
        unit_contract={
            "main_net_inflow": "CNY",
            "super_large_net_inflow": "CNY",
            "large_net_inflow": "CNY",
            "medium_net_inflow": "CNY",
            "small_net_inflow": "CNY",
        },
        backfill_refetch="scope",
        partial_rows=True,
        primary_source="eastmoney",
        # Tushare moneyflow_dc republishes EastMoney's own measure for every
        # session since 2023-09-11; it only fills keys the lake does not have.
        keyed_backfill_source="tushare",
        tier="L4",
        partition_col="trade_date",
        fetch_semantics="snapshot",
    ),
    DatasetSpec(
        "margin_trading",
        support="extended",
        unit_contract={
            "margin_balance": "CNY",
            "margin_buy": "CNY",
            "short_balance": "CNY",
            "short_sell_volume": "share",
        },
        backfill_source_overrides=("exchange", "eastmoney"),
        backfill_parallel_dates=True,
        # v2 records month partitions. Columns and the primary key are unchanged.
        schema_version=2,
        session_rows=True,
        partial_rows=True,
        primary_source="exchange",
        # `[margin_trading].source` picks between the two at runtime: the
        # exchange path is the better data, EastMoney the reachable one from a
        # non-mainland egress. Both are declared because either can own these
        # rows depending on that choice.
        supplementary_sources=("eastmoney",),
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
        max_staleness_days=2,
    ),
    # Per-stock northbound holdings are quarterly since Aug 2024; tolerate the
    # gap to the next quarter-end before flagging stale.
    DatasetSpec(
        "northbound_holdings",
        support="best_effort",
        unit_contract={
            "holding_shares": "share",
            "holding_mv": "CNY",
            "holding_ratio": "fraction",
        },
        primary_source="eastmoney",
        tier="L4",
        partition_col="trade_date",
        max_staleness_days=100,
        reconciliation_lookback_days=5,
    ),
    DatasetSpec(
        "northbound_flows",
        support="best_effort",
        unit_contract={"net_buy": "CNY", "buy_amount": "CNY", "sell_amount": "CNY"},
        primary_source="eastmoney",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="year",
        max_staleness_days=2,
        # The northbound channel opened on 2014-11-17. Earlier dates are not
        # missing lake rows: the source had not published this feed yet.
        history_floor_date=date(2014, 11, 17),
        # The exchanges stopped publishing daily northbound net flow after this
        # session; every row from 2024-08-19 on carries a null amount, and those
        # are dropped rather than zero-filled. The lake holds everything that
        # exists, so this is not staleness and no backfill can change it.
        source_retired_date=date(2024, 8, 16),
        reconciliation_lookback_days=3,
    ),
    DatasetSpec(
        "dragon_tiger",
        support="extended",
        primary_source="eastmoney",
        # Availability, not equivalence. Both exchanges publish their own
        # 龙虎榜 and reach today's session, so a EastMoney outage costs the
        # Beijing names rather than the whole day. Measured 2026-09-16: SZSE 35
        # and SSE 25 records against the lake's SH 29 / SZ 37 / BJ 7.
        backup_source="exchange",
        backup_gaps=("BJ", "SH STAR board", "SH before 2017-01-01"),
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
    ),
    DatasetSpec(
        "block_trades",
        support="extended",
        # EastMoney's report is in 万股 / 万元 and the exchange backup keeps that
        # scale; declared as stored, not as the other bar datasets are.
        unit_contract={
            "price": "CNY/share",
            "volume": "10k_share",
            "amount": "10k_CNY",
            "premium_ratio": "fraction",
        },
        # v2: the declared volume/amount units now say 万股/万元, as stored.
        schema_version=2,
        primary_source="eastmoney",
        # SSE `COMMON_SSE_XXPL_JYXXPL_DZJYXX_L_1` and SZSE `CATALOGID=1265`,
        # both per-transaction and both current. Verified against 2026-09-15:
        # every A-share EastMoney carried for SH/SZ was present, and the four
        # it had beyond them were Beijing, which neither exchange publishes.
        backup_source="exchange",
        backup_gaps=("BJ",),
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
    ),
    DatasetSpec(
        "institutional_holdings",
        support="extended",
        unit_contract={
            "holding_shares": "share",
            "holding_ratio": "percent",
            "holding_mv": "CNY",
        },
        primary_source="eastmoney",
        schema_version=2,
        tier="L4",
        partition_col="report_period",
        partition_granularity="quarter",
        watermark=False,
    ),
    # L5 structure
    DatasetSpec(
        "sector_members",
        support="best_effort",
        primary_source="eastmoney",
        tier="L5",
        partition_col="as_of_date",
        fetch_semantics="snapshot",
    ),
    DatasetSpec(
        "index_constituents",
        support="core",
        value_rules=ValueRules(
            zero_is_missing=("weight",), zero_is_missing_sources=("eastmoney", "cni")
        ),
        unit_contract={"weight": "fraction"},
        # The publishers' own books: CSI files (labelled `exchange`) daily, and
        # CNI (`cni` membership, `cni_weights` books) for the Shenzhen indices.
        # EastMoney fills an index only when its publisher cannot be read.
        primary_source="exchange",
        backup_source="eastmoney",
        supplementary_sources=("cni_weights",),
        tier="L5",
        partition_col="as_of_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        # CNI adjustment history reconstructs 399001/399006 from ~2021-12;
        # CSI publishes only the current book, so CSI history is the retained
        # daily snapshots.
        backfill_source="cni",
        # `cne repair index-snapshots` replays dated exchange index books.
        repair_sources=("exchange",),
    ),
    DatasetSpec(
        "industry_members",
        # data_version v2: industry_code is the level-3 leaf and the level columns are filled.
        data_version="v2",
        support="extended",
        # v2: level columns; industry_code is the level-3 leaf.
        schema_version=2,
        primary_source="eastmoney",
        tier="L5",
        partition_col="as_of_date",
        fetch_semantics="snapshot",
        # Shenwan StockClassifyUse intervals → monthly as_of from 2020.
        backfill_source="sw",
    ),
    # L6 macro
    DatasetSpec(
        "macro_indicators",
        support="extended",
        unit_contract={"value": "source_native"},
        primary_source="eastmoney",
        backup_source="pboc",
        tier="L6",
        partition_col="obs_date",
        partition_granularity="year",
    ),
    DatasetSpec(
        "market_breadth",
        coverage_end_contiguous=True,
        support="extended",
        primary_source="derived",
        tier="L6",
        partition_col="trade_date",
        partition_granularity="year",
        # Derived from daily_bars and emits one metric set for every session
        # with a current and prior bar; a missing interior session is a
        # derivation gap, not the natural sparsity of an event feed.
        coverage_mode="session_dense",
    ),
    # L7 sentiment / rotation
    DatasetSpec(
        "sentiment_scores",
        support="best_effort",
        unit_contract={"sentiment_score": "normalized[-1,1]", "headline_count": "headline"},
        primary_source="derived",
        backup_source="eastmoney",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
    ),
    DatasetSpec(
        "hot_rank",
        support="best_effort",
        primary_source="eastmoney",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
    ),
    DatasetSpec(
        "sector_bars",
        support="best_effort",
        value_rules=ValueRules(non_null=_BAR, bar_kind="market"),
        unit_contract={
            "price": "index_point",
            "volume": "source_native",
            "amount": "source_native",
        },
        backfill_refetch="checkpoint",
        partial_rows=True,
        primary_source="ths",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        # 同花顺 per-year board-kline files (adapters/ths/boards.sweep_board_bars),
        # not EastMoney: the source was migrated to a single 同花顺 base to end the
        # mixed-source basis breaks, and this label had not followed.
        backfill_source="ths",
        # `cne ths-official resource-sectors` writes the licensed board series
        # by hand; nothing schedules it.
        repair_sources=("ths_official",),
    ),
    DatasetSpec(
        "sector_fund_flow",
        support="best_effort",
        unit_contract={
            "main_net_inflow": "CNY",
            "change_pct": "percent",
            "turnover_pct": "percent",
        },
        partial_rows=True,
        primary_source="eastmoney",
        # Tushare moneyflow_ind_dc serves EastMoney's boards by date from
        # 2023-09-12, and the session itself while push2 is paused.
        keyed_backfill_source="tushare",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
    ),
    # 同花顺 money flow, written only when push2 cannot deliver the EastMoney
    # tables above (the `fund_flow` / `sector_fund_flow` steps fall back to it).
    # A different measure, so separate tables; sporadic by design, so an empty
    # or old table is not a finding.
    DatasetSpec(
        "fund_flow_ths",
        support="best_effort",
        unit_contract={
            "inflow": "CNY",
            "outflow": "CNY",
            "net_inflow": "CNY",
            "amount": "CNY",
            "change_pct": "percent",
            "turnover_pct": "percent",
        },
        partial_rows=True,
        primary_source="ths",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        required=False,
        empty_severity="info",
        max_staleness_days=36500,
    ),
    DatasetSpec(
        "sector_fund_flow_ths",
        support="best_effort",
        unit_contract={
            "sector_index": "index_point",
            "change_pct": "percent",
            "inflow": "CNY",
            "outflow": "CNY",
            "net_inflow": "CNY",
            "company_count": "count",
        },
        partial_rows=True,
        primary_source="ths",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        required=False,
        empty_severity="info",
        max_staleness_days=36500,
    ),
    DatasetSpec(
        "news_headlines",
        support="best_effort",
        primary_source="eastmoney",
        tier="L7",
        partition_col="publish_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        # A news wire does not close for the weekend.
        session_scope="calendar",
    ),
    DatasetSpec(
        "flash_news_wire",
        support="best_effort",
        primary_source="eastmoney",
        tier="L7",
        partition_col="publish_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        watermark=False,
        required=False,
        empty_severity="info",
        # A news wire does not close for the weekend.
        session_scope="calendar",
    ),
    # EastMoney retired RPT_ECONOMICCALENDAR (code 9501); 金十数据's calendar
    # replaced it on 2026-10-29.
    DatasetSpec(
        "economic_calendar",
        support="best_effort",
        unit_contract={"value": "source_native"},
        # v2: backfill_source makes history_mode snapshot_with_backfill and
        # pit_quality not_applicable. indicator_code and withdrawn, added in
        # the same version, are nullable.
        schema_version=2,
        primary_source="jin10",
        # The same per-day endpoint answers past days back to 2008, so
        # `cne backfill economic_calendar` walks it month by month.
        backfill_source="jin10",
        tier="L7",
        partition_col="event_date",
        partition_granularity="year",
        fetch_semantics="snapshot",
        # The adapter returns a rolling [today-2, today+14] window. Its
        # partition/date column is the event date, so future scheduled events
        # must not advance a freshness watermark beyond the run day.
        watermark=False,
        required=False,
        # An empty root before the first run is expected, as for the other
        # best-effort snapshots. Past days are filled by backfill, not by
        # the daily rolling window.
        empty_severity="info",
    ),
    # Same Jin10 calendar host: scheduled events (central banks, speeches,
    # meetings) and exchange closures, both rolling windows with future dates.
    DatasetSpec(
        "economic_events",
        support="best_effort",
        primary_source="jin10",
        tier="L7",
        partition_col="event_date",
        partition_granularity="year",
        fetch_semantics="snapshot",
        watermark=False,
        required=False,
        empty_severity="info",
    ),
    DatasetSpec(
        "market_holidays",
        support="best_effort",
        primary_source="jin10",
        tier="L7",
        partition_col="holiday_date",
        partition_granularity="year",
        fetch_semantics="snapshot",
        watermark=False,
        required=False,
        empty_severity="info",
    ),
    # L8 risk
    DatasetSpec(
        "share_unlock_schedule",
        support="extended",
        unit_contract={"unlock_shares": "share", "unlock_ratio": "fraction"},
        primary_source="eastmoney",
        # No backup, and the exchanges are not one. SZSE's `CATALOGID=1902`
        # looked like cover until it was asked for a future month: 28 rows for
        # 2026-08, 3 for the rest of September, 0 for November. It registers
        # unlocks that have *happened*, while this dataset is a forward
        # calendar — 835 of its rows sit in the future. A source that answers a
        # different question is worse than none here, because it would read as
        # cover on the day the schedule is what someone needs.
        tier="L8",
        partition_col="unlock_date",
        partition_granularity="year",
        # The endpoint returns a rolling future window, not historical values
        # for the requested day. A raw max(unlock_date) would therefore push
        # the watermark into the future (the lake once reached 2027-02-04).
        fetch_semantics="snapshot",
        watermark=False,
        backfill_source="eastmoney",
    ),
    DatasetSpec(
        "regulatory_events",
        support="extended",
        primary_source="cninfo",
        tier="L8",
        partition_col="event_date",
        partition_granularity="year",
        # Derived from announcement_index, so it inherits its calendar axis.
        session_scope="calendar",
        reconciliation_lookback_days=30,
        # Derived from announcement_index rather than fetched (see
        # ``derive/regulatory_events``), so the lookback re-reads the same tail
        # its source does and the chunk size only bounds how much one compact
        # has to hold.
        backfill_chunk_days=31,
    ),
    # derived — ``layer`` is where the parquet lives, ``tier`` what the data is
    # for, so these carry the tier of the question they answer, not "derived".
    DatasetSpec(
        "adj_factors",
        support="core",
        # v2 records month partitions. Columns and the primary key are unchanged.
        schema_version=2,
        primary_source="sina",
        # Sina bans by account, not by endpoint, so the one vendor behind every
        # return in this lake could be taken out by an unrelated sweep. Baostock
        # publishes raw and back-adjusted closes whose ratio is this factor —
        # measured against Sina on 600519.SH across its 2026-06-26 ex-date, the
        # implied step agreed to 3e-6, which is the rounding in Baostock's
        # two-decimal adjusted closes.
        backup_source="baostock",
        # BJ stocks and LOF/ETF share actions have no vendor factor; the daily
        # derivation computes them from corporate actions.
        supplementary_sources=("derived",),
        tier="L1",
        layer="derived",
        partition_col="trade_date",
        # Month files, like daily_bars: an hfq history read joined 8,758 day
        # files (~0.9 s); 432 month files answer it in ~0.2 s, and a factor
        # series nearly constant per symbol compresses far better in them.
        partition_granularity="month",
        coverage_mode="session_dense",
    ),
    # Industry returns computed from 申万 membership × hfq bars rather than
    # fetched, so index and constituents cannot disagree. Yearly partitions:
    # ~3 levels × 2 weightings × ~500 industries a day.
    # L5, not L1: the unit of observation is an industry (PK carries
    # industry_code/level/weighting, not symbol), so it belongs beside the
    # membership table that produces it rather than beside the bars.
    DatasetSpec(
        "industry_index",
        coverage_end_contiguous=True,
        support="extended",
        unit_contract={"ret": "fraction", "amount": "source_native"},
        primary_source="derived",
        tier="L5",
        layer="derived",
        partition_col="trade_date",
        partition_granularity="year",
        # Computed from daily bars for every available session; a missing
        # session is a derivation hole rather than sparse event-feed behavior.
        coverage_mode="session_dense",
    ),
    # Main and second-month continuous futures, rolled on T-1 open interest
    # (derive/futures_continuous.py). Rebuilt in full each run from
    # futures_bars, so it has no incremental watermark of its own.
    DatasetSpec(
        "futures_continuous",
        enable_switch="futures",
        support="optional",
        # Rolled from futures_bars and stored at raw contract prices (the
        # adjustment factors are applied at query time), so the same rules hold.
        value_rules=ValueRules(bar_kind="derivative"),
        unit_contract={
            "price": "source_native",
            "volume": "contract_single_sided",
            "open_interest": "contract_single_sided",
            "adj_ratio": "unitless",
            "adj_diff": "source_native",
            "roll_yield": "fraction_per_year",
        },
        primary_source="derived",
        tier="L9",
        layer="derived",
        partition_col="trade_date",
        partition_granularity="month",
        required=False,
    ),
    # The lake's own implied volatility and Greeks per option settlement
    # (derive/option_greeks.py): Black-76 or BAW, one method for every
    # exchange, beside the exchanges' own delta/IV in option_bars.
    DatasetSpec(
        "option_greeks",
        enable_switch="futures_options",
        support="optional",
        unit_contract={
            "underlying_price": "source_native",
            "time_to_expiry": "year",
            "rate": "fraction",
            "iv": "fraction",
            "delta": "unitless",
            "gamma": "per_quote_unit",
            "vega": "price_per_unit_vol",
            "theta": "price_per_year",
            "rho": "price_per_unit_rate",
        },
        primary_source="derived",
        tier="L9",
        layer="derived",
        partition_col="trade_date",
        partition_granularity="day",
        required=False,
    ),
    # 15m / 30m / 60m bars, stored only when a user runs `cne derive` for
    # them (derive/minute_resample.py); nothing schedules them. Each
    # symbol-day comes from 1m where the lake has it and from 5m otherwise.
    # `row_grain` only: `intraday_frequency` would register a TDX fetch step
    # and backfill path for a frequency this lake never fetches. No watermark:
    # nothing advances them, so one would read as stale a session after every
    # derive.
    *(
        DatasetSpec(
            name,
            adjustable=True,
            support="optional",
            value_rules=ValueRules(non_null=_BAR, bar_kind="market"),
            unit_contract={
                "price": "CNY/share",
                "volume": "share",
                "amount": "CNY",
            },
            primary_source="derived",
            tier="L1",
            layer="derived",
            partition_col="trade_date",
            partition_granularity="day",
            watermark=False,
            required=False,
            row_grain=frequency,
        )
        for name, frequency in RESAMPLED_MINUTE_DATASETS.items()
    ),
    # How each recovered delisting's price series ends — see
    # DELISTING_EVENTS_SCHEMA. Merge-style: one row per symbol, a few hundred
    # rows total. date_col (not partition_col) so load(start=/end=) still filters.
    DatasetSpec(
        "delisting_events",
        support="core",
        unit_contract={
            "final_close": "CNY/share",
            "worst_final_return": "fraction",
            "final_window_return": "fraction",
        },
        primary_source="derived",
        # Recovered delisted names carry Sina's label: the derivation reads
        # bars that only Sina could serve for those codes.
        supplementary_sources=("sina",),
        tier="L1",
        layer="derived",
        partition_col=None,
        date_col="last_trade_date",
        watermark=False,
    ),
]
if _unsupported := [
    spec.name for spec in _SPECS if spec.support not in SUPPORT_LEVELS
]:  # pragma: no cover
    raise RuntimeError(f"every registered dataset must declare its support level: {_unsupported}")

DATASETS: dict[str, DatasetSpec] = {spec.name: spec for spec in _SPECS}


def get_dataset(name: str) -> DatasetSpec:
    try:
        return DATASETS[name]
    except KeyError:
        raise KeyError(f"unknown dataset {name!r}") from None


def curated_dataset_names() -> frozenset[str]:
    return frozenset(s.name for s in DATASETS.values() if s.layer == "curated")


def derived_dataset_names() -> frozenset[str]:
    return frozenset(s.name for s in DATASETS.values() if s.layer == "derived")


def datasets_by_tier() -> dict[Tier, list[str]]:
    """``{tier: [dataset, ...]}`` for every tier, in registry order.

    Every tier is present even when empty, so a consumer grouping by tier
    (catalog docs, the lake dashboard) renders a stable set of sections.
    """
    grouped: dict[Tier, list[str]] = {tier: [] for tier in TIERS}
    for spec in _SPECS:
        grouped[spec.tier].append(spec.name)
    return grouped


def datasets_by_support() -> dict[Support, list[str]]:
    """``{support level: [dataset, ...]}`` for every level, in registry order."""
    grouped: dict[Support, list[str]] = {level: [] for level in SUPPORT_LEVELS}
    for spec in _SPECS:
        grouped[spec.support].append(spec.name)
    return grouped


#: Run-level criticality (orchestrator/manifest.py) by support level. Only a
#: core dataset's failure fails a run and the stability gate; the rest degrade.
_CRITICALITY: dict[Support, str] = {
    "core": "core",
    "extended": "research",
    "best_effort": "advisory",
    "optional": "advisory",
}


def dataset_criticality(dataset: str) -> str | None:
    """``core`` / ``research`` / ``advisory`` for a registered dataset, else None."""
    spec = DATASETS.get(dataset)
    if spec is None or spec.support is None:
        return None
    return _CRITICALITY[spec.support]


def core_dataset_names() -> frozenset[str]:
    return frozenset(s.name for s in DATASETS.values() if s.support == "core")


def best_effort_dataset_names() -> frozenset[str]:
    return frozenset(s.name for s in DATASETS.values() if s.support == "best_effort")


def pit_dataset_names() -> frozenset[str]:
    return frozenset(s.name for s in DATASETS.values() if s.pit)


#: The point-in-time datasets: every spec whose ``pit_quality`` is strict or
#: reconstructed (``DatasetSpec.pit``). Storage, quality
#: and the schema checks used to read a second, hand-written copy of this set
#: in ``domain/pit.py``; a dataset added to one and not the other would have
#: been stored as PIT and read as plain, or the reverse.
PIT_DATASET_NAMES = pit_dataset_names()


def intraday_dataset_names() -> frozenset[str]:
    return frozenset(s.name for s in DATASETS.values() if s.intraday_frequency)


def intraday_datasets() -> dict[str, str]:
    """``{frequency: dataset}`` for every registered intraday dataset.

    The one place the mapping lives. Steps register from it, the config
    validates against it, and audit iterates it, so adding a frequency is a
    registry entry rather than an edit in four modules.
    """
    return {s.intraday_frequency: s.name for s in DATASETS.values() if s.intraday_frequency}


def calendar_scope_datasets() -> frozenset[str]:
    """Datasets whose date axis is the natural calendar, not exchange sessions.

    The single source of truth for "this feed publishes on days the market is
    closed": the incremental walk uses it to step calendar days, the empty-day
    guard to tell a quiet Sunday from a broken source, and the events job to
    decide which steps may run outside a trading session.
    """
    return frozenset(s.name for s in DATASETS.values() if s.session_scope == "calendar")


def fetch_semantics(dataset: str) -> FetchSemantics:
    spec = DATASETS.get(dataset)
    return spec.fetch_semantics if spec else "by_date"


def history_mode_for(spec: DatasetSpec, config=None) -> HistoryMode:
    """Whether the dataset can expose an honest historical series.

    Derived only from registry fields (no parallel flags):
    - ``by_date`` — gap-fill / date-walk
    - ``snapshot_with_backfill`` — daily tip snapshot + dedicated history source
    - ``snapshot_only`` — tip-only; no honest historical replay

    A ``keyed_backfill_source`` counts only when *config* has that source's
    credentials.
    """
    if spec.fetch_semantics == "by_date":
        return "by_date"
    if spec.backfill_source:
        return "snapshot_with_backfill"
    if spec.keyed_backfill_source and config is not None:
        if keyed_source_ready(spec.keyed_backfill_source, config):
            return "snapshot_with_backfill"
    return "snapshot_only"


def keyed_source_ready(source: str, config) -> bool:
    """Whether *config* can call a credentialed *source* at all."""
    if source == "tushare":
        # Switched on and a token configured; the Tushare client asks this too.
        if config is None:
            return False
        sources = getattr(config, "sources", None) or {}
        return bool(sources.get("tushare", False) and getattr(config, "tushare_token", None))
    return False


def history_mode(dataset: str) -> HistoryMode:
    spec = DATASETS.get(dataset)
    if spec is None:
        return "by_date"
    return history_mode_for(spec)


def granularity_for_dataset(dataset: str) -> Granularity:
    spec = DATASETS.get(dataset)
    return spec.partition_granularity if spec else "day"


def staleness_counts_trading_sessions(dataset: str) -> bool:
    """Whether *dataset*'s freshness lag is a count of exchange sessions.

    Unknown datasets use the session default. Calendar-scoped feeds and a
    multi-month tolerance stay on calendar days; see ``max_staleness_days``.
    """
    spec = DATASETS.get(dataset)
    if spec is None:
        return True
    if spec.session_scope == "calendar" or spec.max_staleness_days >= 30:
        return False
    return True


def _is_exchange_session(day: date) -> bool:
    from cnequity.domain.holidays_cn import CLOSED_DATES, EXTRA_TRADING_DATES

    iso = day.isoformat()
    if iso in EXTRA_TRADING_DATES:
        return True
    return day.weekday() < 5 and iso not in CLOSED_DATES


def _trading_session_lag(mark: date, anchor: date) -> int:
    """Exchange sessions strictly after *mark* and through *anchor*."""
    if anchor <= mark:
        return 0
    lag = 0
    day = mark
    while day < anchor:
        day += timedelta(days=1)
        if _is_exchange_session(day):
            lag += 1
    return lag


#: ``max_staleness_days`` at or above this marks a fallback that is not expected daily.
_SPORADIC_STALENESS = 3650


def scheduled_steps(config) -> frozenset[str]:
    """Every step the config's daily schedule groups run."""
    groups = getattr(config, "schedule_groups", None) or {}
    return frozenset(step for group in groups.values() for step in group.steps)


def removed_from_schedule(config, dataset: str) -> bool:
    """Whether the config took *dataset*'s step out of its schedule on purpose.

    ``steps_remove`` or ``enabled = false`` removed it and no other daily group
    still runs it. Such a dataset is not owed: its last date is real, nothing
    on this host will move it, and calling it stale every day only teaches the
    reader to ignore the word. Adding the step back makes it due again.
    """
    if config is None or dataset not in (getattr(config, "opted_out_steps", None) or ()):
        return False
    return dataset not in scheduled_steps(config)


def tushare_backfill_dataset(dataset: str) -> bool:
    """A Tushare-primary dataset, filled by ``cne backfill`` and never by a daily run.

    Tushare is a temporary source for improving history, not a route the lake
    updates from: a token is short-lived, so a daily step that depends on one
    fails every day once it lapses. None is on the default daily schedule, and
    none is owed freshness unless a config's own schedule lists its step.
    """
    spec = DATASETS.get(dataset)
    return spec is not None and spec.primary_source == "tushare"


def daily_freshness_owed(config, dataset: str) -> bool:
    """Whether *dataset* is supposed to keep up with the last trading session.

    Tushare backfill datasets are optional and off unless the user turns the
    source on. The open-source default leaves ``[sources.tushare]`` disabled,
    so an empty table or a one-time pull is not a missed daily update and must
    not show up as stale in ``cne serve``. They become due only when that
    source is configured and a daily schedule group actually lists the step.
    ``cnequity.quality.freshness.freshness_owed`` also drops a refused key.
    With no config there is no lake to exempt, so the registry rule stands.
    Every other dataset is owed exactly as before.
    """
    if not tushare_backfill_dataset(dataset) or config is None:
        return True
    if not keyed_source_ready("tushare", config):
        return False
    return dataset in scheduled_steps(config)


def stale_between(dataset: str, mark, anchor, config=None) -> bool:
    """Whether *dataset*'s freshest date (*mark*) lags *anchor* beyond tolerance.

    *mark* and *anchor* are ``datetime.date`` (or None). A dataset with no mark
    is not judged here (callers treat empty separately).

    Session datasets count exchange sessions, so the weekend between Friday and
    Monday is one day of lag. Calendar-scoped datasets and multi-month
    tolerances still count calendar days.

    A retired source is never stale once the lake has caught up to its last
    published session: there is nothing further to fetch, and calling that
    "stale" forever is how a freshness signal stops being read.

    This is the registry's rule alone. Judging a real lake also counts its
    last clean look at the source and the schedule's cadence; that lives in
    ``quality.freshness.is_stale``, which adjusts *mark* and *anchor* and then
    asks this.
    """
    if mark is None or anchor is None:
        return False
    if removed_from_schedule(config, dataset):
        return False
    spec = DATASETS.get(dataset)
    if spec is not None and spec.source_retired_date is not None:
        if mark >= spec.source_retired_date:
            return False
    tolerance = spec.max_staleness_days if spec else 1
    if (
        config is not None
        and tolerance >= _SPORADIC_STALENESS
        and dataset in scheduled_steps(config)
    ):
        # A fallback table is sporadic only while something else is primary. A
        # lake that schedules it every day (push2 unavailable, the THS money flow
        # swapped in with `steps_add`) depends on it each session. It is a
        # snapshot, so a missed session cannot be fetched later by date: the
        # last closed session itself is due, as for a live snapshot.
        return _trading_session_lag(mark, anchor) > 0
    if staleness_counts_trading_sessions(dataset):
        lag = _trading_session_lag(mark, anchor)
    else:
        lag = (anchor - mark).days
    return lag > tolerance


def empty_freshness_label(dataset: str) -> str:
    """Why a dataset holds nothing: absent by design, or simply not ingested.

    A registry entry whose source has been retired with no replacement wired
    reads exactly like one the operator forgot to run — both print `empty`,
    and only one is worth acting on. `economic_calendar` was the standing case
    until 金十数据 replaced EastMoney's retired report. The registry already says so through `empty_severity`, which
    is `info` precisely for the datasets whose absence is not a finding.
    """
    spec = DATASETS.get(dataset)
    if spec is not None and spec.empty_severity == "info":
        return "no source"
    return "empty"


#: The derivatives family, all switched by `[futures] enabled` together.
def _minute_frequency(frequency: str):
    def enabled(config) -> bool:
        return bool(
            getattr(config, "minute_bars_enabled", False)
            and frequency in getattr(config, "minute_bars_frequencies", ())
        )

    return enabled


#: What each ``DatasetSpec.enable_switch`` reads from the config. A stand-in
#: config without a field reads its default, as the CLI and tests always have.
_SWITCHES = {
    "futures": lambda c: bool(getattr(c, "futures_enabled", False)),
    "futures_options": lambda c: bool(
        getattr(c, "futures_enabled", False) and getattr(c, "futures_options", True)
    ),
    "futures_minute": lambda c: bool(
        getattr(c, "futures_enabled", False) and getattr(c, "futures_minute_enabled", False)
    ),
    "trade_ticks": lambda c: bool(getattr(c, "trade_ticks_enabled", False)),
    "minute_bars_1m": _minute_frequency("1m"),
    "minute_bars_5m": _minute_frequency("5m"),
}
if _unknown := sorted(  # pragma: no cover - registry edit
    {s.enable_switch for s in DATASETS.values() if s.enable_switch} - set(_SWITCHES)
):
    raise RuntimeError(f"unknown enable_switch: {_unknown}")


def is_dataset_enabled(dataset: str, config) -> bool:
    """Whether an optional capture is enabled in *config*.

    Optional datasets can outlive the configuration that produced them. A
    disabled tick or minute-bar capture may therefore still have historical
    Parquet on disk, but its old tip is not an ingestion failure and must not
    be reported as stale. Keep this mapping next to the registry so status,
    verify, and the dashboard share the same opt-in semantics.

    A ``best_effort`` dataset is enabled only when the config opts in (see
    ``config/layers.py``). A stand-in config without the field keeps every
    dataset on, as before.
    """
    spec = DATASETS.get(dataset)
    if spec is not None and spec.support == "best_effort":
        enabled = getattr(config, "best_effort_enabled", None)
        if enabled is not None and dataset not in enabled:
            return False
    if spec is not None and spec.enable_switch is not None:
        return _SWITCHES[spec.enable_switch](config)
    return True


# ---------------------------------------------------------------------------
# Derived legacy tables (kept so existing imports stay valid; do not edit these
# directly — edit the DatasetSpec entries above).
# ---------------------------------------------------------------------------

# partition column per curated dataset; None = merge-style (e.g. instruments).
PARTITION_COLS: dict[str, str | None] = {
    s.name: s.partition_col for s in DATASETS.values() if s.layer == "curated"
}

FETCH_SEMANTICS: dict[str, FetchSemantics] = {
    s.name: s.fetch_semantics for s in DATASETS.values() if s.fetch_semantics != "by_date"
}

# Datasets partitioned by non-date keys — skip date-based watermarks.
WATERMARK_SKIP = frozenset(
    s.name
    for s in DATASETS.values()
    if s.layer == "curated" and s.partition_col is not None and not s.watermark
)

# Warn when a partition's row/symbol count falls below this fraction of the prior partition.
ROW_COUNT_MUTATION_MIN_RATIO = 0.5

# Ignore mutation checks when the baseline partition is smaller than this.
ROW_COUNT_MUTATION_MIN_BASELINE_ROWS = 50
