"""Source-health probes and the page they publish.

No network: the probe registry's own callables are the network, and these
exercise the machinery around them — classification, isolation, serialisation,
and the rendering rules that keep the page hard to misread.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import httpx
import pytest

from cnequity.config import Config
from cnequity.diagnostics import source_health as sh
from cnequity.diagnostics.health_page import render_page
from cnequity.i18n import gettext


@pytest.fixture
def config(tmp_path):
    return Config(data_root=tmp_path / "data")


def _probe(**kwargs) -> sh.SourceProbe:
    base = {
        "key": "demo",
        "label": "示例源",
        "host": "example.test",
        "powers": ("daily_bars",),
        "run": lambda cfg: "1 行",
    }
    base.update(kwargs)
    return sh.SourceProbe(**base)


# --- classification --------------------------------------------------------


def test_blocked_is_not_down(config):
    """'Refused you' and 'is not there' send a reader to different fixes."""
    result = sh.run_probe(
        _probe(run=lambda cfg: (_ for _ in ()).throw(sh.ProbeBlocked("HTTP 403"))), config
    )
    assert result.status == sh.ProbeStatus.BLOCKED.value


def test_empty_is_its_own_status(config):
    """A source answering 200 with no rows is what silently truncates a backfill."""
    result = sh.run_probe(
        _probe(run=lambda cfg: (_ for _ in ()).throw(sh.ProbeEmpty("total=0"))), config
    )
    assert result.status == sh.ProbeStatus.EMPTY.value
    assert result.detail == "total=0"


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (403, sh.ProbeStatus.BLOCKED),
        (429, sh.ProbeStatus.BLOCKED),
        (451, sh.ProbeStatus.BLOCKED),
        (502, sh.ProbeStatus.DOWN),
        (404, sh.ProbeStatus.DOWN),
    ],
)
def test_http_status_codes_split_blocked_from_down(config, code, expected):
    def _raise(cfg):
        request = httpx.Request("GET", "https://example.test")
        raise httpx.HTTPStatusError(
            "boom", request=request, response=httpx.Response(code, request=request)
        )

    assert sh.run_probe(_probe(run=_raise), config).status == expected.value


def test_unexpected_exception_is_reported_not_raised(config):
    """A failure is the measurement. One dead source must not end the sweep."""
    result = sh.run_probe(_probe(run=lambda cfg: 1 / 0), config)
    assert result.status == sh.ProbeStatus.DOWN.value
    assert "ZeroDivisionError" in result.detail


def test_disabled_source_is_skipped_not_probed(config):
    called = []
    config.sources = {"demo": False}
    result = sh.run_probe(
        _probe(config_key="demo", run=lambda cfg: called.append(1) or "x"), config
    )
    assert result.status == sh.ProbeStatus.SKIPPED.value
    assert called == []


def test_latency_is_recorded_even_on_failure(config):
    result = sh.run_probe(_probe(run=lambda cfg: 1 / 0), config)
    assert result.latency_ms is not None and result.latency_ms >= 0


# --- the probe window ------------------------------------------------------


def test_probe_date_is_never_a_weekend():
    """A probe that goes red every Monday morning teaches people to ignore it."""
    assert sh._recent_weekday().weekday() < 5


def test_probe_date_is_not_today():
    """Several of these hold nothing for the current session until after close."""
    assert sh._recent_weekday() < date.today()


# --- registry integrity ----------------------------------------------------


def test_probe_keys_are_unique():
    keys = [p.key for p in sh.PROBES]
    assert len(keys) == len(set(keys))


def test_every_probe_names_the_datasets_it_powers():
    """The page's whole value is 'this being down costs you X'."""
    assert all(p.powers for p in sh.PROBES)


def test_probes_grouped_by_blast_radius_are_contiguous():
    """The page emits a group heading when the radius changes, so a radius that
    appears twice non-adjacently would print two headings for one WAF."""
    seen: list[str] = []
    for probe in sh.PROBES:
        if not seen or seen[-1] != probe.blast_radius:
            assert probe.blast_radius not in seen, f"{probe.blast_radius} is split"
            seen.append(probe.blast_radius)


def test_only_filter_selects_a_subset(config, monkeypatch):
    monkeypatch.setattr(sh, "PROBES", (_probe(key="a"), _probe(key="b")))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {p.key: p for p in sh.PROBES})
    report = sh.run_probes(config, vantage="test", only=["a", "a"])
    assert [r.key for r in report.results] == ["a"]


def test_empty_only_probes_nothing(config, monkeypatch):
    """`--only ""` must not mean "sweep every source" — that is the opposite of
    what anyone typing it wants, and it would fire fourteen live requests."""
    monkeypatch.setattr(sh, "PROBES", (_probe(key="a"),))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {p.key: p for p in sh.PROBES})
    assert sh.run_probes(config, vantage="test", only=[]).results == []


def test_manual_only_source_is_not_probed_by_default(config, monkeypatch):
    calls = []
    risky = _probe(key="risky", manual_only=True, run=lambda _cfg: calls.append(1) or "ok")
    monkeypatch.setattr(sh, "PROBES", (risky,))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {risky.key: risky})
    report = sh.run_probes(config, vantage="test")
    assert report.results[0].status == "skipped"
    assert "--only" in report.results[0].detail
    assert calls == []
    assert sh.run_probes(config, vantage="test", only=["risky"]).results[0].status == "ok"
    assert calls == [1]


def test_futures_probe_reads_only_futures_and_at_most_two_sessions(config):
    from cnequity.adapters.futures_exchange.common import FuturesDayUnavailable

    calls = []

    class Reader:
        def fetch(self, day, *, kind, config):
            calls.append((day, kind))
            raise FuturesDayUnavailable("closed")

    with pytest.raises(sh.ProbeEmpty, match="last two weekdays"):
        sh._probe_futures_exchange("SHF", reader=Reader())(config)
    assert len(calls) == 2
    assert all(kind == "futures" for _, kind in calls)


def test_bse_probe_only_requests_landing_and_first_page(config, monkeypatch):
    import httpx

    calls = []
    original_client = httpx.Client

    def respond(request):
        calls.append((request.method, request.url.path))
        if request.method == "GET":
            return httpx.Response(200, text="landing")
        return httpx.Response(200, json=[{"content": [{"hqzqdm": "920571"}], "totalElements": 580}])

    def client_with_mock(*args, **kwargs):
        return original_client(*args, transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(httpx, "Client", client_with_mock)
    detail = sh._probe_bse(config)
    assert "remaining pages not checked" in detail
    assert len(calls) == 2


def test_recent_validated_ingest_avoids_active_probe(config, monkeypatch):
    monkeypatch.setenv("CNE_SOURCE_VANTAGE", "test")
    sh.record_validated_ingest(config, dataset="daily_bars", source="bse", rows=3, run_id="run-1")
    probe = _probe(key="bse", run=lambda _cfg: pytest.fail("probe must not send"))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {"bse": probe})
    report = sh.run_probes(config, vantage="test", only=["bse"], stale_only=True)
    assert report.results[0].status == "ok"
    assert report.results[0].sample_kind == "passive"
    assert report.results[0].latency_ms is None


def test_passive_success_does_not_mask_active_failure_in_slo():
    from cnequity.diagnostics.source_slo import evaluate_source_slo

    now = datetime.now(timezone.utc)
    base = dict(
        key="bse", label="BSE", host="bse", powers=["daily_bars"], latency_ms=None, detail="x"
    )
    report = sh.HealthReport(
        vantage="cn",
        generated_at=now.isoformat(),
        version="test",
        results=[
            sh.ProbeResult(**base, status="blocked", sample_kind="active"),
            sh.ProbeResult(**base, status="ok", sample_kind="passive"),
        ],
    )
    row = next(
        item
        for item in evaluate_source_slo([report], now=now, minimum_observations=1).results
        if item.key == "bse"
    )
    assert row.evidence_kind == "active"
    assert row.active_observations == row.passive_observations == 1
    assert row.availability == 0.0


# --- serialisation ---------------------------------------------------------


def test_report_round_trips_through_json(config):
    report = sh.HealthReport(
        vantage="cn",
        generated_at="2026-08-02T09:20:00+00:00",
        version="0.4.0",
        results=[sh.run_probe(_probe(), config)],
    )
    restored = sh.HealthReport.from_dict(json.loads(json.dumps(report.to_dict())))
    assert restored.vantage == "cn"
    assert restored.results[0].key == "demo"
    assert restored.results[0].status == sh.ProbeStatus.OK.value


def test_generated_at_carries_an_offset(config, monkeypatch):
    """The page is read from several timezones; a bare local stamp is unreadable
    in all but one of them."""
    monkeypatch.setattr(sh, "PROBES", ())
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {})
    report = sh.run_probes(config, vantage="test")
    assert report.generated_at.endswith("+00:00")


# --- the page --------------------------------------------------------------


def _report(vantage: str, statuses: dict[str, str]) -> sh.HealthReport:
    results = [
        sh.ProbeResult(
            key=p.key,
            label=p.label,
            host=p.host,
            powers=list(p.powers),
            status=statuses.get(p.key, sh.ProbeStatus.OK.value),
            latency_ms=120,
            detail="ok",
            note=p.note,
            blast_radius=p.blast_radius,
        )
        for p in sh.PROBES
    ]
    return sh.HealthReport(
        vantage=vantage, generated_at="2026-08-02T09:20:00+00:00", version="0.4.0", results=results
    )


@pytest.mark.usefixtures("chinese_output")
def test_page_keeps_vantages_side_by_side():
    """Merging them would invent a fact neither probe established."""
    page = render_page(
        [
            _report("cn", {}),
            _report("overseas", {"eastmoney_push2his": sh.ProbeStatus.BLOCKED.value}),
        ]
    )
    assert "大陆出口" in page and "海外出口" in page
    assert page.count("<th>") == 3  # two vantages plus the datasets column


@pytest.mark.usefixtures("chinese_output")
def test_page_rows_come_from_the_registry_not_the_report():
    """A source a vantage never probed shows as an explicit blank rather than
    vanishing from that column."""
    partial = sh.HealthReport(vantage="overseas", generated_at="x", version="0", results=[])
    page = render_page([_report("cn", {}), partial])
    for probe in sh.PROBES:
        assert gettext(probe.label) in page
    assert "未探测" in page


@pytest.mark.usefixtures("chinese_output")
def test_page_marks_shared_waf_membership_on_the_rows():
    """A heading alone is positional: the rows below the group would read as
    part of it."""
    page = render_page([_report("cn", {})])
    assert "共用风控面" in page
    assert 'class="grouped"' in page


@pytest.mark.usefixtures("chinese_output")
def test_page_states_the_caveats_that_stop_a_misreading():
    page = render_page([_report("cn", {})])
    assert "一次探测不是 SLA" in page
    assert "HTTP 200 不等于可用" in page
    assert "不等于「挂了」" in page


def test_page_is_self_contained():
    """Published to Pages and read by people whose network is the thing in
    question — an external asset is one more host that can break the page."""
    page = render_page([_report("cn", {})])
    assert "<style>" in page
    for marker in ("<script", "cdn.", "googleapis", "unpkg"):
        assert marker not in page


def test_page_escapes_source_detail():
    report = _report("cn", {})
    report.results[0].detail = '<img src=x onerror="alert(1)">'
    page = render_page([report])
    assert "<img src=x" not in page
    assert "&lt;img" in page


def test_page_needs_at_least_one_report():
    with pytest.raises(ValueError):
        render_page([])


# --- the local viewer ------------------------------------------------------


def _serve_client(config):
    from fastapi.testclient import TestClient

    from cnequity.serve.app import create_app

    return TestClient(create_app(config))


def _store(config, vantage: str, payload) -> None:
    root = config.meta_root / "source_health"
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{vantage}.json").write_text(
        payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


@pytest.mark.usefixtures("chinese_output")
def test_dashboard_renders_the_stored_report(config):
    _store(config, "cn", _report("cn", {}).to_dict())
    body = _serve_client(config).get("/source-health").text
    assert "大陆出口" in body
    assert gettext(sh.PROBES[0].label) in body


@pytest.mark.usefixtures("chinese_output")
def test_dashboard_shows_every_vantage_side_by_side(config):
    _store(config, "cn", _report("cn", {}).to_dict())
    _store(config, "overseas", _report("overseas", {}).to_dict())
    body = _serve_client(config).get("/source-health").text
    assert "大陆出口" in body and "海外出口" in body


def test_dashboard_never_probes(config, monkeypatch):
    """A GET that reaches out to a dozen third parties is what the read-only
    stance exists to prevent; the CLI owns probing."""
    monkeypatch.setattr(
        sh, "run_probes", lambda *a, **k: pytest.fail("the dashboard probed a source")
    )
    _store(config, "cn", _report("cn", {}).to_dict())
    assert _serve_client(config).get("/source-health").status_code == 200


def test_dashboard_without_a_report_says_how_to_make_one(config):
    resp = _serve_client(config).get("/source-health")
    assert resp.status_code == 404
    assert "cne sources" in resp.text


@pytest.mark.usefixtures("chinese_output")
def test_a_corrupt_report_is_skipped_not_fatal(config):
    """Half-written or hand-edited files must not take the page down."""
    _store(config, "cn", _report("cn", {}).to_dict())
    _store(config, "broken", "{not json")
    body = _serve_client(config).get("/source-health").text
    assert "大陆出口" in body


@pytest.mark.usefixtures("chinese_output")
def test_dashboard_does_not_render_the_incident_ledger_as_a_vantage(config):
    """``incidents.json`` sits beside the reports. Missing vantage used to become
    an empty ``unknown`` column of 未探测."""
    _store(config, "cn", _report("cn", {}).to_dict())
    ledger = config.meta_root / "source_health" / "incidents.json"
    ledger.write_text(
        json.dumps(
            {
                "format": "cnequity.source-incidents",
                "version": 1,
                "threshold": 3,
                "open_incidents": [],
                "open_count": 0,
            }
        ),
        encoding="utf-8",
    )
    body = _serve_client(config).get("/source-health").text
    assert "大陆出口" in body
    assert "<b>unknown</b>" not in body


def test_incident_ledger_alone_is_not_a_report(config):
    root = config.meta_root / "source_health"
    root.mkdir(parents=True, exist_ok=True)
    (root / "incidents.json").write_text("{}", encoding="utf-8")
    resp = _serve_client(config).get("/source-health")
    assert resp.status_code == 404
    assert "cne sources" in resp.text


def test_probe_writes_into_the_lake_by_default(config, monkeypatch, tmp_path):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config.bootstrap import path_for_toml

    monkeypatch.setattr(sh, "PROBES", (_probe(key="a"),))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {p.key: p for p in sh.PROBES})
    cfg_path = tmp_path / "c.toml"
    cfg_path.write_text(f'[data]\nroot = "{path_for_toml(config.data_root)}"\n')

    result = CliRunner().invoke(
        cli, ["sources", "probe", "--config", str(cfg_path), "--vantage", "cn"]
    )
    assert result.exit_code == 0, result.output
    written = config.meta_root / "source_health" / "cn.json"
    assert written.exists()
    assert json.loads(written.read_text("utf-8"))["vantage"] == "cn"


def test_probe_exits_zero_when_a_source_is_down(config, monkeypatch, tmp_path):
    """A red source is the command's output, not its failure — a non-zero exit
    would make a scheduled run fail on exactly the days it matters."""
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config.bootstrap import path_for_toml

    monkeypatch.setattr(sh, "PROBES", (_probe(run=lambda cfg: 1 / 0),))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {p.key: p for p in sh.PROBES})
    cfg_path = tmp_path / "c.toml"
    cfg_path.write_text(f'[data]\nroot = "{path_for_toml(config.data_root)}"\n')

    result = CliRunner().invoke(cli, ["sources", "probe", "--config", str(cfg_path)])
    assert result.exit_code == 0
    assert "down" in result.output


def test_a_sole_probe_names_the_core_datasets_its_source_serves():
    """With one probe per source there is no ambiguity about which endpoint serves a dataset."""
    from collections import defaultdict

    from cnequity.diagnostics.source_health import PROBES
    from cnequity.domain.datasets import DATASETS

    probes = defaultdict(list)
    for probe in PROBES:
        probes[probe.config_key].append(probe)
    unlisted = sorted(
        (spec.primary_source, name)
        for name, spec in DATASETS.items()
        if spec.support == "core"
        and len(probes.get(spec.primary_source, ())) == 1
        and name not in probes[spec.primary_source][0].powers
    )
    assert not unlisted
