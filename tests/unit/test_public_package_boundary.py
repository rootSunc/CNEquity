"""Local notes and validation reports must not enter release archives."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "check_public_package", ROOT / "scripts" / "dev" / "check_public_package.py"
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


@pytest.mark.parametrize("prefix", ["", "cnequity-0.12.0/"])
@pytest.mark.parametrize("directory", ["private", "evidence"])
def test_release_members_reject_local_materials(prefix, directory):
    local = f"{prefix}{directory}/run-report.json"
    required = [f"{prefix}{name}" for name in checker.REQUIRED_FILES]
    assert checker.check_members([*required, local]) == [local]


@pytest.mark.parametrize("name", ["cnequity.toml", "cnequity.demo.toml", "cnequity.sample.toml"])
def test_release_members_reject_local_configs(name):
    path = f"configs/{name}"
    assert checker.check_members([*checker.REQUIRED_FILES, path]) == [path]


def test_release_members_allow_public_contracts_and_tests():
    members = [
        *checker.REQUIRED_FILES,
        "contracts/v0.12.0.json",
        "tests/fixtures/example.json",
        "docs/zh/getting-started/quickstart.md",
    ]
    assert checker.check_members(members) == []


@pytest.mark.parametrize("missing", checker.REQUIRED_FILES)
def test_release_members_require_runtime_assets(missing):
    members = [name for name in checker.REQUIRED_FILES if name != missing]
    assert checker.check_members(members) == [f"missing {missing}"]
