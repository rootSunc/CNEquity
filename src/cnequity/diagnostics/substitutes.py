"""Which reachable source can stand in for one that is not.

A probe report says what is up. That is one question short of the one an
operator has when a run fails: *this* source is down — is there another that
can answer for the datasets it feeds, and is it actually independent of the
one that failed?

Two layers, because a vendor is not an endpoint:

* **Which sources a dataset may use** comes from the dataset registry — the
  same declarations `cne sources resilience` reports on, and the ones the
  failover chain is written against. An earlier version of this module read
  the probe table's `powers` instead, and inherited every place that
  hand-maintained list had drifted: `commodity_bars` read as stranded because
  the only endpoint still claiming it was the EastMoney history host that Sina
  had replaced, and `corporate_actions` read as stranded while its declared
  TDX backup sat there healthy.
* **Whether a source can answer right now** comes from the probes, grouped by
  the source they belong to. One probe per host, deliberately: EastMoney was
  dropping every `push2his` connection while `push2` kept serving, so "is
  EastMoney up" has no answer — only "which of its endpoints is". A source
  with at least one reachable endpoint can still carry work; one whose
  endpoints are all down cannot. Where a probe declares the datasets it
  `powers`, that narrows the verdict to the endpoints that matter for the
  dataset in hand; where it does not, every endpoint of the source counts,
  which can call a source degraded when only an unrelated endpoint of it is
  down. That direction is the safe one: it never invents a substitute and
  never hides a real outage.

This reads a report; it never probes. Reachability measured an hour ago from
one vantage is not a promise about the next request, which is why the failover
chain must still try and fall through on its own rather than consult this.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from cnequity.diagnostics.source_health import PROBES, HealthReport, ProbeStatus
from cnequity.diagnostics.source_resilience import (
    SOURCE_FAILURE_DOMAINS,
    source_components,
    source_failure_domains,
)
from cnequity.domain.datasets import DATASETS

#: A probe in this state answered, so its source can carry a dataset.
USABLE = frozenset({ProbeStatus.OK.value})
#: Explicitly disabled in config is not the same as unreachable — the operator
#: turned it off, and saying "down" about it would send them debugging a
#: network that is fine.
IGNORED = frozenset({ProbeStatus.SKIPPED.value})
#: Declared as a source but not a thing a probe can reach.
NOT_FETCHED = frozenset({"derived"})


@dataclass
class SourceVerdict:
    source: str
    role: str
    endpoints_ok: list[str] = field(default_factory=list)
    endpoints_down: list[str] = field(default_factory=list)
    endpoints_skipped: list[str] = field(default_factory=list)
    failure_domains: tuple[str, ...] = ()
    latency_ms: int | None = None
    #: Whether a probe of this source names this dataset. When none does, the
    #: verdict covers the whole vendor rather than the endpoint that matters.
    scoped: bool = True

    @property
    def probed(self) -> bool:
        return bool(self.endpoints_ok or self.endpoints_down)

    @property
    def usable(self) -> bool:
        return bool(self.endpoints_ok)

    @property
    def impaired(self) -> bool:
        """Answering for this dataset, but not from every endpoint it needs.

        Only claimed for a scoped verdict. Unscoped, a down endpoint may have
        nothing to do with this dataset — most EastMoney datasets read one host
        and are indifferent to the other two — and reporting that as impairment
        would bury the real cases in a list of every dataset the vendor has.
        """
        return self.scoped and bool(self.endpoints_ok) and bool(self.endpoints_down)


@dataclass
class DatasetSubstitution:
    dataset: str
    tier: str
    failing: list[SourceVerdict] = field(default_factory=list)
    healthy: list[SourceVerdict] = field(default_factory=list)
    unprobed: list[str] = field(default_factory=list)

    @property
    def stranded(self) -> bool:
        """A declared source is down and nothing declared is reachable."""
        return bool(self.failing) and not self.healthy

    @property
    def cover_is_independent(self) -> bool:
        """Whether anything still standing is outside the failing domain.

        Every source that survives the registry's own de-duplication owns its
        failure domain, so today this is true whenever there is any cover at
        all. It is computed rather than assumed because the interesting case —
        two declared sources that turn out to share a vendor — is a registry
        edit away, and it should read as cover that is not really cover.
        """
        if not self.healthy:
            return False
        down = {domain for verdict in self.failing for domain in verdict.failure_domains}
        return any(set(v.failure_domains) - down for v in self.healthy)


def _canonical(component: str) -> str:
    """Resolve a declared label to the vendor whose probes answer for it.

    Registry labels name routes as well as vendors — `commodity_bars` declares
    its backfill as `eastmoney_kline+sina_global`, which is two routes over two
    vendors that already have probes. Collapsing on the failure domain keeps
    the report from listing a route as an unprobed source of its own.
    """
    domain = SOURCE_FAILURE_DOMAINS.get(component)
    if domain is None:
        return component
    for probe in PROBES:
        if SOURCE_FAILURE_DOMAINS.get(probe.config_key) == domain:
            return probe.config_key
    return component


def _declared_sources(dataset: str) -> list[tuple[str, str]]:
    """(source, role) for every source the registry says may serve *dataset*."""
    spec = DATASETS[dataset]
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    labels = [
        ("primary", spec.primary_source),
        ("backup", spec.backup_source),
        ("backfill", spec.backfill_source),
    ]
    labels.extend(("supplementary", label) for label in (spec.supplementary_sources or ()))
    for role, label in labels:
        for component in source_components(label):
            if component in NOT_FETCHED:
                continue
            source = _canonical(component)
            if source in seen:
                continue
            seen.add(source)
            out.append((source, role))
    return out


def _verdict(source: str, role: str, dataset: str, results: dict) -> SourceVerdict:
    """Health of one source, narrowed to the endpoints that serve *dataset*."""
    domain = SOURCE_FAILURE_DOMAINS.get(source, source)
    endpoints = [
        probe
        for probe in PROBES
        if SOURCE_FAILURE_DOMAINS.get(probe.config_key, probe.config_key) == domain
    ]
    # A probe that names its datasets is the precise answer; if none of this
    # source's probes name this one, fall back to all of them (see module docs).
    scoped = [probe for probe in endpoints if dataset in probe.powers]
    relevant = scoped or endpoints

    verdict = SourceVerdict(
        source=source,
        role=role,
        failure_domains=source_failure_domains(source),
        scoped=bool(scoped),
    )
    for probe in relevant:
        result = results.get(probe.key)
        if result is None:
            continue
        if result.status in IGNORED:
            verdict.endpoints_skipped.append(probe.key)
        elif result.status in USABLE:
            verdict.endpoints_ok.append(probe.key)
            if result.latency_ms is not None and (
                verdict.latency_ms is None or result.latency_ms < verdict.latency_ms
            ):
                verdict.latency_ms = result.latency_ms
        else:
            verdict.endpoints_down.append(probe.key)
    return verdict


def substitution_report(report: HealthReport) -> list[DatasetSubstitution]:
    """Per dataset with something failing: what is down, and what can stand in.

    Datasets with nothing failing are omitted — the question only arises when
    something is down, and a list of everything that is fine buries it.
    """
    results = {result.key: result for result in report.results}
    out: list[DatasetSubstitution] = []

    for dataset in sorted(DATASETS):
        entry = DatasetSubstitution(dataset=dataset, tier=DATASETS[dataset].tier)
        for source, role in _declared_sources(dataset):
            verdict = _verdict(source, role, dataset, results)
            if not verdict.probed:
                entry.unprobed.append(source)
            elif verdict.usable:
                entry.healthy.append(verdict)
            else:
                entry.failing.append(verdict)
        # A source can be up and still have lost the endpoint this dataset
        # needs — EastMoney served clist throughout the outage that took its
        # history host down, which is exactly the case that broke a run. Report
        # the dataset when any relevant endpoint is unreachable, not only when
        # a whole source is.
        if not entry.failing and not any(v.impaired for v in entry.healthy):
            continue
        # Independent of what failed first, then fastest: the order to try.
        down = {domain for verdict in entry.failing for domain in verdict.failure_domains}
        entry.healthy.sort(
            key=lambda v: (
                not (set(v.failure_domains) - down),
                v.latency_ms if v.latency_ms is not None else 10**9,
            )
        )
        out.append(entry)

    # Stranded first — those are the ones that will fail a run.
    out.sort(key=lambda e: (not e.stranded, e.cover_is_independent, e.dataset))
    return out


def _independent(verdict: SourceVerdict, down: set[str]) -> bool:
    return bool(set(verdict.failure_domains) - down)


def render_substitutions(entries: list[DatasetSubstitution]) -> list[str]:
    """Human-readable lines; a reassuring one when nothing is failing."""
    if not entries:
        return ["每个被探测的端点都可用，没有需要替换的源。"]
    lines: list[str] = []
    for entry in entries:
        if entry.stranded:
            verdict_text = "无可用替代"
        elif not entry.cover_is_independent:
            verdict_text = "有替代，但与故障源同属一个风控面"
        elif not entry.failing:
            verdict_text = "源仍可用，但部分端点不可达"
        else:
            verdict_text = "有独立替代"
        lines.append(f"{entry.dataset}  [{entry.tier}]  —  {verdict_text}")
        down = {domain for verdict in entry.failing for domain in verdict.failure_domains}
        for verdict in entry.failing:
            lines.append(
                f"    失败：{verdict.source} ({verdict.role})  "
                f"端点 {', '.join(verdict.endpoints_down)}"
            )
        for verdict in entry.healthy:
            latency = f"{verdict.latency_ms}ms" if verdict.latency_ms is not None else "—"
            mark = "独立" if _independent(verdict, down) else "同域"
            impaired = "（部分端点不可达）" if verdict.impaired else ""
            lines.append(
                f"    可用：{verdict.source:<16}{latency:>8}  {mark}  {verdict.role}{impaired}"
            )
        if entry.unprobed:
            lines.append(f"    未探测：{', '.join(sorted(entry.unprobed))}")
    return lines


def to_dict(entries: list[DatasetSubstitution]) -> dict:
    def _source(verdict: SourceVerdict, down: set[str]) -> dict:
        return {
            "source": verdict.source,
            "role": verdict.role,
            "latency_ms": verdict.latency_ms,
            "failure_domains": list(verdict.failure_domains),
            "independent": _independent(verdict, down),
            "endpoints_ok": verdict.endpoints_ok,
            "endpoints_down": verdict.endpoints_down,
            "endpoints_skipped": verdict.endpoints_skipped,
        }

    payload = []
    for entry in entries:
        down = {domain for verdict in entry.failing for domain in verdict.failure_domains}
        payload.append(
            {
                "dataset": entry.dataset,
                "tier": entry.tier,
                "stranded": entry.stranded,
                "cover_is_independent": entry.cover_is_independent,
                "failing": [_source(v, down) for v in entry.failing],
                "healthy": [_source(v, down) for v in entry.healthy],
                "unprobed": sorted(entry.unprobed),
            }
        )
    return {"format": "cnequity.source-substitutes", "version": 2, "datasets": payload}
