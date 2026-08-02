"""Source-health probes and the page they publish.

No network: the probe registry's own callables are the network, and these
exercise the machinery around them — classification, isolation, serialisation,
and the rendering rules that keep the page hard to misread.
"""

from __future__ import annotations

import json
from datetime import date

import httpx
import pytest

from ashare_lake.config import Config
from ashare_lake.diagnostics import source_health as sh
from ashare_lake.diagnostics.health_page import render_page


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
    report = sh.run_probes(config, vantage="test", only=["a", "nope"])
    assert [r.key for r in report.results] == ["a"]


def test_empty_only_probes_nothing(config, monkeypatch):
    """`--only ""` must not mean "sweep every source" — that is the opposite of
    what anyone typing it wants, and it would fire fourteen live requests."""
    monkeypatch.setattr(sh, "PROBES", (_probe(key="a"),))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {p.key: p for p in sh.PROBES})
    assert sh.run_probes(config, vantage="test", only=[]).results == []


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


def test_page_rows_come_from_the_registry_not_the_report():
    """A source a vantage never probed shows as an explicit blank rather than
    vanishing from that column."""
    partial = sh.HealthReport(vantage="overseas", generated_at="x", version="0", results=[])
    page = render_page([_report("cn", {}), partial])
    for probe in sh.PROBES:
        assert probe.label in page
    assert "未探测" in page


def test_page_marks_shared_waf_membership_on_the_rows():
    """A heading alone is positional: the rows below the group would read as
    part of it."""
    page = render_page([_report("cn", {})])
    assert "共用风控面" in page
    assert 'class="grouped"' in page


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

    from ashare_lake.serve.app import create_app

    return TestClient(create_app(config))


def _store(config, vantage: str, payload) -> None:
    root = config.meta_root / "source_health"
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{vantage}.json").write_text(
        payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


def test_dashboard_renders_the_stored_report(config):
    _store(config, "cn", _report("cn", {}).to_dict())
    body = _serve_client(config).get("/source-health").text
    assert "大陆出口" in body
    assert sh.PROBES[0].label in body


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
    assert "asl sources" in resp.text


def test_a_corrupt_report_is_skipped_not_fatal(config):
    """Half-written or hand-edited files must not take the page down."""
    _store(config, "cn", _report("cn", {}).to_dict())
    _store(config, "broken", "{not json")
    body = _serve_client(config).get("/source-health").text
    assert "大陆出口" in body


def test_probe_writes_into_the_lake_by_default(config, monkeypatch, tmp_path):
    from click.testing import CliRunner

    from ashare_lake.cli.main import cli
    from ashare_lake.config.bootstrap import path_for_toml

    monkeypatch.setattr(sh, "PROBES", (_probe(key="a"),))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {p.key: p for p in sh.PROBES})
    cfg_path = tmp_path / "c.toml"
    cfg_path.write_text(f'[data]\nroot = "{path_for_toml(config.data_root)}"\n')

    result = CliRunner().invoke(cli, ["sources", "--config", str(cfg_path), "--vantage", "cn"])
    assert result.exit_code == 0, result.output
    written = config.meta_root / "source_health" / "cn.json"
    assert written.exists()
    assert json.loads(written.read_text("utf-8"))["vantage"] == "cn"


def test_probe_exits_zero_when_a_source_is_down(config, monkeypatch, tmp_path):
    """A red source is the command's output, not its failure — a non-zero exit
    would make a scheduled run fail on exactly the days it matters."""
    from click.testing import CliRunner

    from ashare_lake.cli.main import cli
    from ashare_lake.config.bootstrap import path_for_toml

    monkeypatch.setattr(sh, "PROBES", (_probe(run=lambda cfg: 1 / 0),))
    monkeypatch.setattr(sh, "PROBES_BY_KEY", {p.key: p for p in sh.PROBES})
    cfg_path = tmp_path / "c.toml"
    cfg_path.write_text(f'[data]\nroot = "{path_for_toml(config.data_root)}"\n')

    result = CliRunner().invoke(cli, ["sources", "--config", str(cfg_path)])
    assert result.exit_code == 0
    assert "down" in result.output
