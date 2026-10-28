"""Tests for the stable dataset contract and its compatibility diff."""

from __future__ import annotations

import copy
import json
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.domain.contracts import (
    build_contract,
    contract_fingerprint,
    dataset_contract,
    diff_contracts,
    export_contract,
    validate_contract,
)
from cnequity.domain.datasets import DATASETS


def test_every_registered_dataset_has_machine_readable_contract_fields():
    contract = build_contract()
    assert set(contract["datasets"]) == set(DATASETS)
    assert validate_contract() == []
    for name, record in contract["datasets"].items():
        assert record["schema_version"] >= 1
        assert record["contract_level"]
        assert record["pit_grade"] in {"none", "strict", "partial"}
        assert "availability_col" in record
        assert "compatibility" in record
        assert "unit_contract" in record
        assert record["schema"] == record["columns"]
        assert record["primary_key"] == record["primary_keys"]
        assert dataset_contract(name) == record


def test_contract_fingerprint_is_stable_and_dataset_scoped():
    first = build_contract()
    second = json.loads(json.dumps(first, ensure_ascii=False))
    assert contract_fingerprint(first) == contract_fingerprint(second)
    assert contract_fingerprint("daily_bars") == contract_fingerprint(
        dataset_contract("daily_bars")
    )


def test_contract_diff_finds_shape_pk_units_pit_and_history_breaks():
    old = build_contract()
    new = copy.deepcopy(old)
    row = new["datasets"]["daily_bars"]
    del row["schema"]["amount"]
    del row["columns"]["amount"]
    row["schema"]["volume"] = "float64"
    row["columns"]["volume"] = "float64"
    row["primary_key"] = ["trade_date", "symbol"]
    row["primary_keys"] = ["trade_date", "symbol"]
    row["unit_contract"] = {"volume": "lot"}
    row["pit"] = True
    row["pit_grade"] = "strict"
    row["availability_col"] = "trade_date"
    row["metadata"]["pit"] = True
    row["metadata"]["pit_grade"] = "strict"
    row["metadata"]["availability_col"] = "trade_date"
    row["fetch_semantics"] = "snapshot"
    row["metadata"]["fetch_semantics"] = "snapshot"

    diff = diff_contracts(old, new)
    kinds = {item["kind"] for item in diff["breaking"]}
    assert {"column_removed", "column_type_changed", "primary_key_changed"} <= kinds
    assert "unit_contract_changed" in kinds
    assert "pit_semantics_changed" in kinds
    assert "history_semantics_changed" in kinds
    assert "schema_version_not_bumped" in kinds


def test_unit_for_a_new_column_is_compatible_but_changing_an_old_unit_is_not():
    old = build_contract()
    new = copy.deepcopy(old)
    row = new["datasets"]["valuation_metrics"]
    row["schema"]["pe_forward"] = "float64"
    row["columns"]["pe_forward"] = "float64"
    row["unit_contract"] = {**row["unit_contract"], "pe_forward": "ratio"}
    diff = diff_contracts(old, new)
    assert any(item["kind"] == "unit_contract_extended" for item in diff["compatible"])
    assert not diff["breaking"]

    row["unit_contract"]["total_mv"] = "10k_CNY"
    diff = diff_contracts(old, new)
    assert any(item["kind"] == "unit_contract_changed" for item in diff["breaking"])


def test_new_column_is_compatible_but_removed_dataset_is_breaking():
    old = build_contract()
    new = copy.deepcopy(old)
    new["datasets"]["daily_bars"]["schema"]["new_nullable"] = "string"
    new["datasets"]["daily_bars"]["columns"]["new_nullable"] = "string"
    del new["datasets"]["hot_rank"]
    diff = diff_contracts(old, new)
    assert any(item["kind"] == "column_added" for item in diff["compatible"])
    assert not any(item["kind"] == "schema_version_not_bumped" for item in diff["breaking"])
    assert any(item["kind"] == "dataset_removed" for item in diff["breaking"])


def test_non_nullable_added_column_requires_schema_version_bump():
    old = build_contract()
    new = copy.deepcopy(old)
    row = new["datasets"]["daily_bars"]
    row["schema"]["required_new"] = "string"
    row["columns"]["required_new"] = "string"
    row["nullable_columns"] = []

    diff = diff_contracts(old, new)

    assert any(item["kind"] == "column_added" for item in diff["breaking"])
    assert any(item["kind"] == "schema_version_not_bumped" for item in diff["breaking"])


def test_export_and_cli_contract_commands(tmp_path):
    output = tmp_path / "contract.json"
    document = export_contract(output)
    assert json.loads(output.read_text(encoding="utf-8"))["fingerprint"] == document["fingerprint"]

    runner = CliRunner()
    shown = runner.invoke(cli, ["contract", "show", "daily_bars"])
    assert shown.exit_code == 0, shown.output
    assert json.loads(shown.output)["name"] == "daily_bars"

    # `contract export` folded into `contract show --out`; the written document
    # must still be exactly what `contract validate` and `contract diff` read.
    exported = runner.invoke(cli, ["contract", "show", "--out", str(output)])
    assert exported.exit_code == 0, exported.output
    checked = runner.invoke(cli, ["contract", "validate", str(output)])
    assert checked.exit_code == 0, checked.output
    assert json.loads(output.read_text(encoding="utf-8"))["fingerprint"] == document["fingerprint"]


def test_show_out_writes_a_single_dataset_record(tmp_path):
    """The single-dataset write path is not the registry path: it serialises the
    record itself, so a caller cannot get a whole-registry document by accident."""
    output = tmp_path / "daily_bars.json"

    result = CliRunner().invoke(cli, ["contract", "show", "daily_bars", "--out", str(output)])

    assert result.exit_code == 0, result.output
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["name"] == "daily_bars"
    assert "fingerprint" not in written


def test_current_release_contract_is_versioned_and_packaged():
    """The release cannot drift from the registry or omit its contract artifact."""

    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    version = project["project"]["version"]
    contract_path = root / "contracts" / f"v{version}.json"

    assert contract_path.is_file(), f"missing release contract: {contract_path}"
    document = json.loads(contract_path.read_text(encoding="utf-8"))
    assert validate_contract(contract_path, against_registry=True) == []
    assert document["fingerprint"] == contract_fingerprint(document)

    data_files = project["tool"]["setuptools"]["data-files"]
    assert f"contracts/v{version}.json" in data_files["share/cnequity/contracts"]


def test_data_contract_fingerprint_ignores_source_order_but_not_columns():
    from cnequity.domain.contracts import (
        OPERATIONAL_CONTRACT_FIELDS,
        contract_fingerprint,
        data_contract_fingerprint,
        dataset_contract,
    )

    record = dataset_contract("trading_status")
    swapped = dict(
        record, primary_source=record["backup_source"], backup_source=record["primary_source"]
    )
    strip = lambda r: {k: v for k, v in r.items() if k not in OPERATIONAL_CONTRACT_FIELDS}  # noqa: E731
    assert contract_fingerprint(swapped) != contract_fingerprint(record)
    assert contract_fingerprint(strip(swapped)) == data_contract_fingerprint("trading_status")
    renamed = dict(strip(record), columns=[*record["columns"], "extra"])
    assert contract_fingerprint(renamed) != data_contract_fingerprint("trading_status")
