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

from dataclasses import dataclass
from datetime import date
from typing import Literal

from cn_market_lake.domain.partitions import Granularity, partition_value

FetchSemantics = Literal["by_date", "snapshot"]
HistoryMode = Literal["by_date", "snapshot_with_backfill", "snapshot_only"]
Layer = Literal["curated", "derived"]

# Research-use classification, orthogonal to ``Layer`` (which is a storage
# location). L0 is the reference spine everything joins on and L8 the risk
# overlay; the ordering is roughly "how far from the price series".
Tier = Literal["L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8"]

TIERS: tuple[Tier, ...] = ("L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8")

TIER_LABELS: dict[Tier, str] = {
    "L0": "基础参考",
    "L1": "行情",
    "L2": "公司事件",
    "L3": "基本面",
    "L4": "资金面",
    "L5": "结构行业",
    "L6": "宏观",
    "L7": "舆情 / 轮动",
    "L8": "风险合规",
}


@dataclass(frozen=True)
class DatasetSpec:
    """Orchestration/query metadata for one dataset.

    tier:
        L0–L8 research classification (see ``Tier``). Mandatory and without a
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
    pit:
        Point-in-time dataset — ``load()`` requires ``as_of`` and filters on
        ``announce_date``.
    backfill_source:
        Name of an external historical source that can replay this dataset even
        though daily ``fetch_semantics`` is ``snapshot`` (e.g. valuation_metrics:
        EastMoney live snapshot daily, baostock for history). ``cml backfill``
        is allowed for snapshot datasets only when this is set.
    required:
        When False, an empty curated root is a warning (not an error) and does
        not alone make ``lake_health`` UNHEALTHY. Use for registered datasets
        whose source is not yet wired or is temporarily unavailable.
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
        ``cml verify`` offers a backfill that runs the whole window, writes zero
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
    partition_col: str | None = None
    partition_granularity: Granularity = "day"
    date_col: str | None = None
    fetch_semantics: FetchSemantics = "by_date"
    watermark: bool = True
    pit: bool = False
    # Which upstream the daily path actually reads, and what it falls back to.
    # Kept in the registry rather than in prose because the prose drifted: the
    # published source table had `sector_bars` on EastMoney long after it moved
    # to 同花顺, and `fund_flow` in the wrong schedule group entirely. Anything
    # generated from here — the README data table, the docs — cannot say
    # something the code does not do, and a test asserts the pairing.
    primary_source: str = ""
    backup_source: str | None = None
    backfill_source: str | None = None
    # How many days the freshest data may lag the last trading day before it is
    # flagged STALE. 1 tolerates normal T+1 EOD publication; larger values mark
    # sources with a slower cadence (margin T+1, quarterly northbound holdings)
    # so their inherent lag is not mistaken for a stuck pipeline.
    max_staleness_days: int = 1
    required: bool = True
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

    @property
    def query_date_col(self) -> str | None:
        return self.date_col or self.partition_col

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


_SPECS = [
    # L0 reference
    # Live sources (TDX/EM) only list what trades today; baostock's stock_basic
    # is what recovers delisted codes, so `cml backfill instruments` is the only
    # path to a survivorship-free universe.
    DatasetSpec(
        "instruments",
        primary_source="tdx_protocol",
        backup_source="baostock",
        tier="L0",
        partition_col=None,
        watermark=False,
        backfill_source="baostock",
    ),
    DatasetSpec(
        "trading_calendar",
        primary_source="tdx_protocol",
        backup_source="exchange",
        tier="L0",
        partition_col="trade_date",
        partition_granularity="year",
    ),
    DatasetSpec(
        "trading_status",
        primary_source="tdx_protocol",
        backup_source="eastmoney",
        tier="L0",
        partition_col="trade_date",
        partition_granularity="month",
    ),
    # L1 bars
    DatasetSpec(
        "daily_bars",
        primary_source="tdx_protocol",
        backup_source="eastmoney",
        tier="L1",
        partition_col="trade_date",
    ),
    DatasetSpec(
        "index_bars",
        primary_source="tdx_protocol",
        backup_source="eastmoney",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="year",
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
        primary_source="tdx_protocol",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="day",
        fetch_semantics="by_date",
        required=False,
        # Measured 2026-08-01 against 120.76.1.198:7709 — 22,800 bars for every
        # symbol probed, across both exchanges and every liquidity band, so it
        # is a server retention window rather than a per-symbol artefact.
        history_horizon_days=95,
        # Tip-paged: chunk by symbol, not by date (see backfill_chunk_symbols).
        backfill_chunk_symbols=200,
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
    # 15m/30m/60m are deliberately absent. TDX serves them over the same 491-day
    # window, but they aggregate exactly from 5m (48 bars divide by 3, 6 and 12
    # onto identical closing-minute boundaries), so storing them would be three
    # more datasets holding a `group_by_dynamic` away from data already here.
    DatasetSpec(
        "minute_bars_5m",
        primary_source="tdx_protocol",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="day",
        fetch_semantics="by_date",
        required=False,
        # Measured 2026-08-01: 23,568 bars = 491 trading days, back to
        # 2024-07-23. 15m/30m/60m share exactly that window (7,856 / 3,928 /
        # 1,964 bars), which is what says it is a time-based retention policy
        # rather than a bar-count cap.
        history_horizon_days=491,
        # Same tip-paged contract as 1m; 200 symbols × 48 bars × 491 days ≈
        # 4.7M rows per sub-run — comparable compact memory to 1m's chunk.
        backfill_chunk_symbols=200,
        intraday_frequency="5m",
        row_grain="5m",
    ),
    # Transaction records (分笔). Not tick data: A-share Level-1 is a 3-second
    # snapshot, so a record aggregates 6–33 real trades (measured) and a
    # session holds ~2,700 on average, ~4,800 at most.
    #
    # Deliberately *not* `intraday_frequency`. That field means "one bar
    # frequency", and every consumer of it — the audit's session-shape checks,
    # the reader's adjustment set, `cml backfill --symbols` — assumes a
    # `bar_time` column and a bar count per session. This dataset has neither,
    # and inheriting those code paths would give it checks that silently pass
    # on the wrong column. It carries its own step group and its own checks.
    DatasetSpec(
        "trade_ticks",
        primary_source="tdx_protocol",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="day",
        fetch_semantics="by_date",
        required=False,
        # A *date*, not a rolling count — see DatasetSpec.history_floor_date.
        # Measured 2026-08-02: every symbol probed serves back to exactly
        # 2024-01-02 and no further, which is ~624 trading days and growing.
        # The edge landing on a calendar boundary suggests the retention may be
        # year-granular rather than a fixed date, so re-measure each January
        # with scripts/probe_trade_ticks.py.
        history_floor_date=date(2024, 1, 2),
        # By-date requests, so date chunks are the cheap axis — the exact
        # opposite of the minute bars above, where the wire always walks from
        # today's tip and a date slice re-fetches everything newer than it.
        # 5 days × 200 symbols × ~2,700 rows ≈ 2.7M rows per sub-run.
        backfill_chunk_days=5,
        # Intraday, but not a bar frequency — see DatasetSpec.row_grain for why
        # this is not `intraday_frequency`.
        row_grain="tick",
    ),
    # Domestic commodity futures main-continuous (东财主连) + narrow offshore
    # gold (Sina COMEX ``GC0.CMX``); not A-share equity.
    DatasetSpec(
        "commodity_bars",
        primary_source="sina",
        backup_source="eastmoney",
        tier="L1",
        partition_col="trade_date",
        partition_granularity="year",
        fetch_semantics="by_date",
        backfill_source="eastmoney_kline+sina_global",
        required=False,
        max_staleness_days=2,
    ),
    # L2 corporate events
    DatasetSpec(
        "corporate_actions",
        primary_source="tdx_protocol",
        backup_source="eastmoney",
        tier="L2",
        partition_col="ex_date",
        partition_granularity="year",
    ),
    DatasetSpec(
        "announcement_index",
        primary_source="cninfo",
        tier="L2",
        partition_col="announce_date",
        pit=True,
    ),
    # Current-state timetable (revisions overwrite scheduled_date; not PIT).
    DatasetSpec(
        "earnings_disclosure_schedule",
        primary_source="eastmoney",
        tier="L2",
        partition_col="report_period",
        partition_granularity="quarter",
        watermark=False,
    ),
    # L3 fundamentals
    DatasetSpec(
        "financial_statement_items",
        primary_source="eastmoney",
        tier="L3",
        partition_col="report_period",
        partition_granularity="quarter",
        watermark=False,
        pit=True,
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
        primary_source="eastmoney",
        tier="L3",
        partition_col="change_date",
        partition_granularity="year",
        date_col="change_date",
        # Measured 2026-08 against RPT_F10_EH_EQUITY: 1990 serves 19 rows, and
        # nothing before it. A fixed vendor floor, not a rolling budget.
        history_floor_date=date(1990, 1, 1),
        watermark=False,
        pit=True,
    ),
    DatasetSpec(
        "shareholder_counts",
        primary_source="eastmoney",
        tier="L3",
        partition_col="count_date",
        partition_granularity="year",
        date_col="count_date",
        # RPT_F10_EH_HOLDERNUM: 25 rows in 1992, none in 1990/1991.
        history_floor_date=date(1992, 1, 1),
        watermark=False,
        pit=True,
    ),
    DatasetSpec(
        "top_holders",
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
        pit=True,
    ),
    DatasetSpec(
        "valuation_metrics",
        primary_source="eastmoney",
        tier="L3",
        partition_col="trade_date",
        fetch_semantics="snapshot",
        backfill_source="baostock",
    ),
    DatasetSpec(
        "analyst_consensus",
        primary_source="eastmoney",
        tier="L3",
        partition_col="forecast_date",
        fetch_semantics="snapshot",
    ),
    # L4 capital flows
    DatasetSpec(
        "fund_flow",
        primary_source="eastmoney",
        tier="L4",
        partition_col="trade_date",
        fetch_semantics="snapshot",
    ),
    DatasetSpec(
        "margin_trading",
        primary_source="eastmoney",
        tier="L4",
        partition_col="trade_date",
        max_staleness_days=2,
    ),
    # Per-stock northbound holdings are quarterly since Aug 2024; tolerate the
    # gap to the next quarter-end before flagging stale.
    DatasetSpec(
        "northbound_holdings",
        primary_source="eastmoney",
        tier="L4",
        partition_col="trade_date",
        max_staleness_days=100,
    ),
    DatasetSpec(
        "northbound_flows",
        primary_source="eastmoney",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="year",
        max_staleness_days=2,
        # The exchanges stopped publishing daily northbound net flow after this
        # session; every row from 2024-08-19 on carries a null amount, and those
        # are dropped rather than zero-filled. The lake holds everything that
        # exists, so this is not staleness and no backfill can change it.
        source_retired_date=date(2024, 8, 16),
    ),
    DatasetSpec(
        "dragon_tiger",
        primary_source="eastmoney",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
    ),
    DatasetSpec(
        "block_trades",
        primary_source="eastmoney",
        tier="L4",
        partition_col="trade_date",
        partition_granularity="month",
    ),
    DatasetSpec(
        "institutional_holdings",
        primary_source="eastmoney",
        tier="L4",
        partition_col="report_period",
        partition_granularity="quarter",
        watermark=False,
    ),
    # L5 structure
    DatasetSpec(
        "sector_members",
        primary_source="eastmoney",
        tier="L5",
        partition_col="as_of_date",
        fetch_semantics="snapshot",
    ),
    DatasetSpec(
        "index_constituents",
        primary_source="eastmoney",
        tier="L5",
        partition_col="as_of_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        # CNI adjustment history reconstructs 399001/399006 from ~2021-12;
        # CSI indices still accumulate via daily EM snapshots only.
        backfill_source="cni",
    ),
    DatasetSpec(
        "industry_members",
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
        primary_source="eastmoney",
        backup_source="pboc",
        tier="L6",
        partition_col="obs_date",
        partition_granularity="year",
    ),
    DatasetSpec(
        "market_breadth",
        primary_source="derived",
        tier="L6",
        partition_col="trade_date",
        partition_granularity="year",
    ),
    # L7 sentiment / rotation
    DatasetSpec(
        "sentiment_scores",
        primary_source="derived",
        backup_source="eastmoney",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
    ),
    DatasetSpec(
        "hot_rank",
        primary_source="eastmoney",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
    ),
    DatasetSpec(
        "sector_bars",
        primary_source="ths",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
        # 同花顺 per-year board-kline files (adapters/ths/boards.sweep_board_bars),
        # not EastMoney: the source was migrated to a single 同花顺 base to end the
        # mixed-source basis breaks, and this label had not followed.
        backfill_source="ths",
    ),
    DatasetSpec(
        "sector_fund_flow",
        primary_source="eastmoney",
        tier="L7",
        partition_col="trade_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
    ),
    DatasetSpec(
        "news_headlines",
        primary_source="eastmoney",
        tier="L7",
        partition_col="publish_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
    ),
    DatasetSpec(
        "flash_news_wire",
        primary_source="eastmoney",
        tier="L7",
        partition_col="publish_date",
        partition_granularity="month",
        fetch_semantics="snapshot",
    ),
    # EM datacenter report RPT_ECONOMICCALENDAR was retired (code 9501); keep the
    # schema/registry for a replacement source, but do not fail lake health.
    DatasetSpec(
        "economic_calendar",
        primary_source="eastmoney",
        tier="L7",
        partition_col="event_date",
        partition_granularity="year",
        fetch_semantics="snapshot",
        required=False,
    ),
    # L8 risk
    DatasetSpec(
        "share_unlock_schedule",
        primary_source="eastmoney",
        tier="L8",
        partition_col="unlock_date",
        partition_granularity="year",
    ),
    DatasetSpec(
        "regulatory_events",
        primary_source="cninfo",
        tier="L8",
        partition_col="event_date",
        partition_granularity="year",
    ),
    # derived — ``layer`` is where the parquet lives, ``tier`` what the data is
    # for, so these carry the tier of the question they answer, not "derived".
    DatasetSpec(
        "adj_factors", primary_source="sina", tier="L1", layer="derived", partition_col="trade_date"
    ),
    # Industry returns computed from 申万 membership × hfq bars rather than
    # fetched, so index and constituents cannot disagree. Yearly partitions:
    # ~3 levels × 2 weightings × ~500 industries a day.
    # L5, not L1: the unit of observation is an industry (PK carries
    # industry_code/level/weighting, not symbol), so it belongs beside the
    # membership table that produces it rather than beside the bars.
    DatasetSpec(
        "industry_index",
        primary_source="derived",
        tier="L5",
        layer="derived",
        partition_col="trade_date",
        partition_granularity="year",
    ),
    # How each recovered delisting's price series ends — see
    # DELISTING_EVENTS_SCHEMA. Merge-style: one row per symbol, a few hundred
    # rows total. date_col (not partition_col) so load(start=/end=) still filters.
    DatasetSpec(
        "delisting_events",
        primary_source="derived",
        tier="L1",
        layer="derived",
        partition_col=None,
        date_col="last_trade_date",
        watermark=False,
    ),
]

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


def pit_dataset_names() -> frozenset[str]:
    return frozenset(s.name for s in DATASETS.values() if s.pit)


def intraday_dataset_names() -> frozenset[str]:
    return frozenset(s.name for s in DATASETS.values() if s.intraday_frequency)


def intraday_datasets() -> dict[str, str]:
    """``{frequency: dataset}`` for every registered intraday dataset.

    The one place the mapping lives. Steps register from it, the config
    validates against it, and audit iterates it, so adding a frequency is a
    registry entry rather than an edit in four modules.
    """
    return {s.intraday_frequency: s.name for s in DATASETS.values() if s.intraday_frequency}


def fetch_semantics(dataset: str) -> FetchSemantics:
    spec = DATASETS.get(dataset)
    return spec.fetch_semantics if spec else "by_date"


def history_mode_for(spec: DatasetSpec) -> HistoryMode:
    """Whether the dataset can expose an honest historical series.

    Derived only from registry fields (no parallel flags):
    - ``by_date`` — gap-fill / date-walk
    - ``snapshot_with_backfill`` — daily tip snapshot + dedicated history source
    - ``snapshot_only`` — tip-only; no honest historical replay
    """
    if spec.fetch_semantics == "by_date":
        return "by_date"
    if spec.backfill_source:
        return "snapshot_with_backfill"
    return "snapshot_only"


def history_mode(dataset: str) -> HistoryMode:
    spec = DATASETS.get(dataset)
    if spec is None:
        return "by_date"
    return history_mode_for(spec)


def granularity_for_dataset(dataset: str) -> Granularity:
    spec = DATASETS.get(dataset)
    return spec.partition_granularity if spec else "day"


def is_stale(dataset: str, mark, anchor) -> bool:
    """Whether *dataset*'s freshest date (*mark*) lags *anchor* beyond tolerance.

    *mark* and *anchor* are ``datetime.date`` (or None). A dataset with no mark
    is not judged here (callers treat empty separately).

    A retired source is never stale once the lake has caught up to its last
    published session: there is nothing further to fetch, and calling that
    "stale" forever is how a freshness signal stops being read.
    """
    if mark is None or anchor is None:
        return False
    spec = DATASETS.get(dataset)
    if spec is not None and spec.source_retired_date is not None:
        if mark >= spec.source_retired_date:
            return False
    tolerance = spec.max_staleness_days if spec else 1
    return (anchor - mark).days > tolerance


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
