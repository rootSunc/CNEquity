from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from cn_market_lake.cli.main import cli
from cn_market_lake.diagnostics.packages import (
    REQUIRED_PACKAGES,
    PackageStatus,
    RequiredPackage,
    probe_packages,
)
from cn_market_lake.diagnostics.render import render_text, to_dict
from cn_market_lake.diagnostics.report import Severity, build_report


def _config(tmp_path, *, data_root=None):
    return SimpleNamespace(data_root=data_root if data_root is not None else tmp_path)


# --- package probes ----------------------------------------------------------


def test_probe_returns_one_status_per_required_package():
    statuses = probe_packages()
    assert [s.package.module for s in statuses] == [p.module for p in REQUIRED_PACKAGES]


def test_every_required_package_declares_a_purpose():
    for pkg in REQUIRED_PACKAGES:
        assert pkg.purpose.strip(), f"{pkg.module} has no stated purpose"


def test_required_packages_are_all_declared_dependencies():
    """The probe list must not drift from what the project actually installs."""
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    pyproject = tomllib.loads(
        (Path(__file__).resolve().parents[2] / "pyproject.toml").read_bytes().decode("utf-8")
    )
    declared = {
        req.split(">")[0].split("=")[0].split("[")[0].strip().replace("-", "_").lower()
        for req in pyproject["project"]["dependencies"]
    }
    for pkg in REQUIRED_PACKAGES:
        assert pkg.module.replace("-", "_").lower() in declared, (
            f"{pkg.module} is probed but not a declared dependency"
        )


def test_missing_package_is_an_error(tmp_path, monkeypatch):
    """A hard dependency that will not import means a damaged install."""
    monkeypatch.setattr(
        "cn_market_lake.diagnostics.report.probe_packages",
        lambda: [
            PackageStatus(RequiredPackage("baostock", "历史回填"), importable=False),
            PackageStatus(RequiredPackage("pandas", "XLS"), importable=True),
        ],
    )
    report = build_report(config=_config(tmp_path))
    finding = next(f for f in report.findings if "必需依赖无法导入" in f.title)
    assert finding.severity is Severity.ERROR
    assert "baostock" in finding.detail
    assert "pandas" not in finding.detail
    assert not report.ok


def test_all_packages_present_produces_no_finding(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "cn_market_lake.diagnostics.report.probe_packages",
        lambda: [PackageStatus(RequiredPackage("baostock", "历史回填"), importable=True)],
    )
    report = build_report(config=_config(tmp_path))
    assert not any("必需依赖" in f.title for f in report.findings)


# --- data.root ---------------------------------------------------------------


def test_relative_data_root_is_an_error(tmp_path):
    report = build_report(config=_config(tmp_path, data_root=Path("./data/cn-market-lake")))
    finding = next(f for f in report.findings if "相对路径" in f.title)
    assert finding.severity is Severity.ERROR
    assert not report.ok


def test_missing_data_root_only_warns(tmp_path):
    report = build_report(config=_config(tmp_path, data_root=tmp_path / "absent"))
    finding = next(f for f in report.findings if "尚不存在" in f.title)
    assert finding.severity is Severity.WARN
    assert report.ok


def test_unwritable_data_root_is_an_error(tmp_path, monkeypatch):
    # Real chmod(0o500) does not deny writes on Windows ACLs, so probe via the
    # helper the doctor actually uses.
    root = tmp_path / "lake"
    root.mkdir()
    monkeypatch.setattr(
        "cn_market_lake.diagnostics.report._data_root_writable",
        lambda _path: False,
    )
    report = build_report(config=_config(tmp_path, data_root=root))
    finding = next(f for f in report.findings if "不可写" in f.title)
    assert finding.severity is Severity.ERROR
    assert "chmod" in finding.fix or "修改" in finding.fix


# --- no-config mode ----------------------------------------------------------


def test_report_without_config_still_probes_packages():
    report = build_report(config=None)
    assert any("未加载配置" in f.title for f in report.findings)
    assert report.packages


# --- rendering ---------------------------------------------------------------


def test_render_text_covers_every_finding(tmp_path):
    report = build_report(config=_config(tmp_path))
    text = "\n".join(render_text(report))
    for finding in report.findings:
        assert finding.title in text


def test_to_dict_is_json_serializable(tmp_path):
    payload = to_dict(build_report(config=_config(tmp_path)))
    json.dumps(payload)
    assert {"environment", "packages", "findings", "ok"} <= payload.keys()


# --- CLI ---------------------------------------------------------------------


def test_doctor_cli_runs_without_config(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--config", str(tmp_path / "nope.toml")])
    assert "依赖" in result.output


def test_doctor_cli_json_output(tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--config", str(tmp_path / "nope.toml"), "--json"])
    payload = json.loads(result.output)
    assert "packages" in payload


@pytest.mark.parametrize("flag", [[], ["--json"]])
@pytest.mark.parametrize(("severity", "expected_exit"), [(Severity.ERROR, 1), (Severity.WARN, 0)])
def test_doctor_exit_code_follows_report_errors(
    tmp_path, monkeypatch, flag, severity, expected_exit
):
    """Only ERROR findings fail the command; warnings must stay exit 0."""
    from cn_market_lake.diagnostics.report import Finding, Report

    monkeypatch.setattr(
        "cn_market_lake.diagnostics.report.build_report",
        lambda config=None, config_path=None: Report(
            environment={"cn-market-lake": "test"},
            packages=[],
            findings=[Finding(severity=severity, title="synthetic")],
        ),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--config", str(tmp_path / "nope.toml"), *flag])
    assert result.exit_code == expected_exit
