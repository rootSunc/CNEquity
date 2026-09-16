"""What can stand in for a source that is down.

The measured case these encode: every EastMoney `push2his` host dropped the
connection from one vantage while `push2` kept serving, so "EastMoney is up"
and "daily_bars has a working history vendor" were different questions.
"""

from __future__ import annotations

import pytest

from cnequity.diagnostics import substitutes as sub
from cnequity.diagnostics.source_health import HealthReport, ProbeResult


def _result(key: str, status: str, *, ms: int | None = 100):
    probe = next(p for p in sub.PROBES if p.key == key)
    return ProbeResult(
        key=key,
        label=probe.label,
        host=probe.host,
        powers=list(probe.powers),
        status=status,
        latency_ms=ms,
        detail="",
        blast_radius=probe.blast_radius,
    )


def _report(*results: ProbeResult) -> HealthReport:
    return HealthReport(
        vantage="test",
        generated_at="2026-09-16T07:00:00+00:00",
        version="1",
        results=list(results),
    )


def _entry(entries, dataset):
    return next((e for e in entries if e.dataset == dataset), None)


def test_a_healthy_lake_has_nothing_to_substitute():
    report = _report(*(_result(p.key, "ok") for p in sub.PROBES))
    assert sub.substitution_report(report) == []
    assert "没有需要替换" in sub.render_substitutions([])[0]


def test_the_measured_outage_reads_as_one_endpoint_not_one_vendor():
    """push2his down, push2 serving: EastMoney is impaired for daily_bars, and
    five independent vendors are still available for it."""
    report = _report(
        *(_result(p.key, "down" if p.key == "eastmoney_push2his" else "ok") for p in sub.PROBES)
    )
    entry = _entry(sub.substitution_report(report), "daily_bars")
    assert entry is not None
    assert entry.stranded is False
    em = next(v for v in entry.healthy if v.source == "eastmoney")
    assert em.impaired is True
    assert em.endpoints_down == ["eastmoney_push2his"]
    others = {v.source for v in entry.healthy} - {"eastmoney"}
    assert {"tdx_protocol", "sina", "ths", "baostock", "exchange"} <= others


def test_commodity_bars_is_not_stranded_by_the_host_sina_replaced():
    """The probe table used to be the only dataset→source map, and it still
    named EastMoney's history host as the one thing serving commodity_bars."""
    report = _report(
        *(_result(p.key, "down" if p.key == "eastmoney_push2his" else "ok") for p in sub.PROBES)
    )
    entry = _entry(sub.substitution_report(report), "commodity_bars")
    assert entry.stranded is False
    assert [v.source for v in entry.healthy] == ["sina"]
    assert [v.source for v in entry.failing] == ["eastmoney"]


def test_a_declared_backup_counts_even_when_no_probe_names_the_dataset():
    """corporate_actions read as stranded while its declared TDX backup — which
    the xdxr sweep uses every init — sat there healthy."""
    report = _report(
        *(_result(p.key, "down" if p.key == "eastmoney_datacenter" else "ok") for p in sub.PROBES)
    )
    entry = _entry(sub.substitution_report(report), "corporate_actions")
    assert entry.stranded is False
    assert "tdx_protocol" in {v.source for v in entry.healthy}


def test_a_single_source_dataset_is_stranded_when_that_source_is_down():
    report = _report(
        *(_result(p.key, "down" if p.key == "eastmoney_datacenter" else "ok") for p in sub.PROBES)
    )
    entry = _entry(sub.substitution_report(report), "block_trades")
    assert entry.stranded is True
    assert entry.healthy == []
    assert sub.to_dict([entry])["datasets"][0]["stranded"] is True


def test_an_unscoped_source_does_not_claim_impairment():
    """Most EastMoney datasets read one host and are indifferent to the other
    two; calling all of them impaired would bury the real cases."""
    report = _report(
        *(_result(p.key, "down" if p.key == "eastmoney_push2his" else "ok") for p in sub.PROBES)
    )
    entries = sub.substitution_report(report)
    # analyst_consensus is EastMoney-only and no probe names it, so the down
    # history host must not make it look impaired.
    assert _entry(entries, "analyst_consensus") is None


def test_a_source_turned_off_in_config_is_not_reported_as_down(monkeypatch):
    """Otherwise the report sends someone to debug a network that is fine."""
    report = _report(
        *(_result(p.key, "skipped" if p.key.startswith("ths") else "ok") for p in sub.PROBES)
    )
    entries = sub.substitution_report(report)
    assert _entry(entries, "daily_bars") is None


@pytest.mark.parametrize("status", ["down", "blocked", "empty"])
def test_any_non_ok_endpoint_counts_as_failing(status):
    """A blocked or empty endpoint cannot carry the dataset either."""
    report = _report(
        *(_result(p.key, status if p.key == "eastmoney_datacenter" else "ok") for p in sub.PROBES)
    )
    entry = _entry(sub.substitution_report(report), "block_trades")
    assert entry is not None and entry.stranded is True


def test_cover_inside_the_failing_domain_is_not_cover():
    """A sibling host is not a second opinion. Every source in today's registry
    owns its failure domain, so this is checked directly on the rule."""
    entry = sub.DatasetSubstitution(dataset="x", tier="L1")
    entry.failing.append(
        sub.SourceVerdict(source="eastmoney", role="primary", failure_domains=("eastmoney",))
    )
    entry.healthy.append(
        sub.SourceVerdict(
            source="eastmoney_mirror",
            role="backup",
            endpoints_ok=["mirror"],
            failure_domains=("eastmoney",),
        )
    )
    assert entry.stranded is False
    assert entry.cover_is_independent is False
    assert "同属一个风控面" in "\n".join(sub.render_substitutions([entry]))

    entry.healthy.append(
        sub.SourceVerdict(
            source="sina", role="supplementary", endpoints_ok=["sina"], failure_domains=("sina",)
        )
    )
    assert entry.cover_is_independent is True
