"""Stable, machine-readable contracts for the registered datasets.

The runtime registry is the source of truth for ingestion and query behaviour.
This module turns that registry, the Polars schemas, and primary keys into a
small JSON document that can be checked into a downstream project or attached
to a revision receipt.  There is deliberately no I/O in the core builders;
the CLI is a thin wrapper around these functions.

The public functions are intentionally boring and dependency-light:

``dataset_contract(name)``
    Return one deterministic dataset contract as a dictionary.
``build_contract()`` / ``export_contract()``
    Return the complete registry contract.  ``export_contract(path=...)`` can
    additionally write it to a JSON file.
``contract_fingerprint(value)``
    Return a SHA-256 fingerprint for a dataset name, a dataset contract, or a
    complete contract.
``validate_contract(value)``
    Return a list of validation errors (empty means valid).
``diff_contracts(old, new)``
    Report compatible and breaking changes between two contract documents.

All JSON keys are sorted when serialised for hashing.  Dates and Polars dtypes
are normalised before they reach the document, so the result does not depend
on ``repr`` ordering or Python's hash randomisation.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from typing import Any

from cnequity.domain.datasets import DATASETS, DatasetSpec, history_mode_for
from cnequity.domain.pit import (
    PIT_MODES,
    PIT_QUALITIES,
    PIT_STORAGE_COLUMNS,
    PIT_STORAGE_DTYPES,
)
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS

CONTRACT_FORMAT = "cnequity.dataset-contract"
CONTRACT_VERSION = 1

_PIT_GRADES = frozenset({"none", "strict", "partial"})
_PIT_QUALITIES = frozenset(PIT_QUALITIES)
_PIT_MODES = frozenset(PIT_MODES)
_HISTORY_MODES = frozenset({"by_date", "snapshot_with_backfill", "snapshot_only"})
_COMPATIBILITY_POLICIES = frozenset({"additive", "append_only", "versioned", "deprecated"})


class ContractValidationError(ValueError):
    """Raised by :func:`assert_valid_contract` for an invalid contract."""

    def __init__(self, errors: Sequence[str]):
        self.errors = tuple(str(error) for error in errors)
        super().__init__("invalid dataset contract: " + "; ".join(self.errors))


def _json_value(value: Any) -> Any:
    """Convert a registry value to deterministic JSON-compatible data."""

    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (tuple, list)):
        # Lists carry order (most importantly, primary-key order).  Do not
        # sort them: changing the key order can change a storage/index plan
        # even when the same column names are present.
        return [_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        # Sets are not used by the current registry, but sorting here makes the
        # helper safe for a caller-provided compatibility declaration too.
        values = [_json_value(item) for item in value]
        return sorted(values, key=lambda item: json.dumps(item, sort_keys=True, default=str))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    # Polars dtypes and future scalar metadata have a stable textual form;
    # avoid leaking an object repr containing a memory address into a hash.
    return str(value)


def _dtype_name(dtype: Any) -> str:
    """Return the canonical textual representation of a Polars dtype."""

    # ``Utf8`` was renamed to ``String`` in Polars while retaining an alias.
    # Treat both spellings as the same public contract token so upgrading
    # Polars does not make every dataset look like a breaking change.
    token = str(dtype)
    if token in {"Utf8", "String"}:
        return "string"
    if token in {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}:
        return token.lower()
    if token in {"Float32", "Float64"}:
        return token.lower()
    if token == "Date":
        return "date"
    if token == "Boolean":
        return "bool"
    if token.startswith("Datetime"):
        # Datetime[μs, UTC] is not guaranteed to use an ASCII micro sign in
        # every Polars release; normalize only the display spelling.
        return token.replace("μ", "u").replace("µ", "u")
    return token


def _schema_json(schema: Mapping[str, Any]) -> dict[str, str]:
    return {str(column): _dtype_name(dtype) for column, dtype in schema.items()}


def _metadata(spec: DatasetSpec) -> dict[str, Any]:
    """Serialize all behaviour-bearing DatasetSpec fields."""

    return {
        "tier": spec.tier,
        "layer": spec.layer,
        "partition_col": spec.partition_col,
        "partition_granularity": spec.partition_granularity,
        "date_col": spec.date_col,
        "query_date_col": spec.query_date_col,
        "availability_col": spec.availability_col,
        "fetch_semantics": spec.fetch_semantics,
        "history_mode": history_mode_for(spec),
        "watermark": spec.watermark,
        "pit": spec.pit,
        "pit_grade": spec.pit_grade,
        "pit_quality": spec.pit_quality,
        "pit_modes": sorted(_PIT_MODES),
        "pit_storage_columns": list(PIT_STORAGE_COLUMNS) if spec.pit else [],
        "pit_storage_dtypes": (
            {column: _dtype_name(dtype) for column, dtype in PIT_STORAGE_DTYPES.items()}
            if spec.pit
            else {}
        ),
        "primary_source": spec.primary_source,
        "backup_source": spec.backup_source,
        "backfill_source": spec.backfill_source,
        "max_staleness_days": spec.max_staleness_days,
        "required": spec.required,
        "empty_severity": spec.empty_severity,
        "history_horizon_days": spec.history_horizon_days,
        "history_floor_date": spec.history_floor_date,
        "source_retired_date": spec.source_retired_date,
        "backfill_chunk_days": spec.backfill_chunk_days,
        "backfill_chunk_symbols": spec.backfill_chunk_symbols,
        "intraday_frequency": spec.intraday_frequency,
        "row_grain": spec.row_grain,
        "coverage_mode": spec.coverage_mode,
        "schema_version": spec.schema_version,
        "contract_level": spec.contract_level,
        "compatibility": spec.compatibility,
        "unit_contract": spec.unit_contract,
        "reconciliation_lookback_days": spec.reconciliation_lookback_days,
        "reconciliation_lookback_mode": spec.reconciliation_lookback_mode,
        "append_only": spec.append_only,
        "negative_evidence_ttl_days": spec.negative_evidence_ttl_days,
    }


def _record(
    name: str,
    *,
    datasets: Mapping[str, DatasetSpec],
    schemas: Mapping[str, Mapping[str, Any]],
    primary_keys: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    try:
        spec = datasets[name]
    except KeyError:
        raise KeyError(f"unknown dataset {name!r}") from None
    try:
        schema = schemas[name]
    except KeyError:
        raise KeyError(f"dataset {name!r} has no schema") from None
    try:
        primary_key = primary_keys[name]
    except KeyError:
        raise KeyError(f"dataset {name!r} has no primary key") from None

    columns = _schema_json(schema)
    pk = [str(column) for column in primary_key]
    metadata = _json_value(_metadata(spec))

    # Keep the high-signal fields at the record root for simple consumers and
    # include the full metadata bundle for clients that want to inspect the
    # orchestration/history policy without knowing DatasetSpec internals.
    record = {
        "name": name,
        "schema_version": spec.schema_version,
        "contract_level": spec.contract_level,
        "pit_grade": spec.pit_grade,
        "pit_quality": spec.pit_quality,
        "pit_modes": sorted(_PIT_MODES),
        "pit_storage_columns": list(PIT_STORAGE_COLUMNS) if spec.pit else [],
        "pit_storage_dtypes": (
            {column: _dtype_name(dtype) for column, dtype in PIT_STORAGE_DTYPES.items()}
            if spec.pit
            else {}
        ),
        "availability_col": spec.availability_col,
        "compatibility": _json_value(spec.compatibility),
        "unit_contract": _json_value(spec.unit_contract),
        "schema": columns,
        # ``columns`` is a friendly alias retained in the public JSON.  Keep a
        # detached copy so a caller experimenting with a proposed schema can
        # mutate one alias without accidentally mutating the other.
        "columns": dict(columns),
        "primary_key": pk,
        "primary_keys": pk,
        "metadata": metadata,
    }
    # Expose the behaviour fields at the root as well.  This makes a contract
    # useful to shell tools and preserves a straightforward migration path for
    # early callers that did not want to walk ``metadata``.
    record.update(metadata)
    return record


def dataset_contract(
    name: str,
    *,
    datasets: Mapping[str, DatasetSpec] | None = None,
    schemas: Mapping[str, Mapping[str, Any]] | None = None,
    primary_keys: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Return the deterministic contract for one registered dataset.

    Optional mappings are primarily useful for tests and tooling that compares
    a proposed registry in memory.  Normal callers should use the defaults,
    which are the live ``DATASETS``, ``DATASET_SCHEMAS`` and ``PRIMARY_KEYS``.
    """

    return _record(
        name,
        datasets=DATASETS if datasets is None else datasets,
        schemas=DATASET_SCHEMAS if schemas is None else schemas,
        primary_keys=PRIMARY_KEYS if primary_keys is None else primary_keys,
    )


def build_contract(
    *,
    datasets: Mapping[str, DatasetSpec] | None = None,
    schemas: Mapping[str, Mapping[str, Any]] | None = None,
    primary_keys: Mapping[str, Sequence[str]] | None = None,
    include_fingerprint: bool = True,
) -> dict[str, Any]:
    """Build the complete stable contract document."""

    registry = DATASETS if datasets is None else datasets
    schema_registry = DATASET_SCHEMAS if schemas is None else schemas
    key_registry = PRIMARY_KEYS if primary_keys is None else primary_keys
    names = sorted(set(registry) | set(schema_registry) | set(key_registry))
    records = {
        name: _record(
            name,
            datasets=registry,
            schemas=schema_registry,
            primary_keys=key_registry,
        )
        for name in names
        if name in registry and name in schema_registry and name in key_registry
    }
    document: dict[str, Any] = {
        "format": CONTRACT_FORMAT,
        "contract_version": CONTRACT_VERSION,
        "version": CONTRACT_VERSION,
        "pit_contract": {
            "modes": sorted(_PIT_MODES),
            "qualities": sorted(_PIT_QUALITIES),
            "storage_columns": list(PIT_STORAGE_COLUMNS),
            "storage_dtypes": {
                column: _dtype_name(dtype) for column, dtype in PIT_STORAGE_DTYPES.items()
            },
        },
        "datasets": records,
    }
    if include_fingerprint:
        document["fingerprint"] = contract_fingerprint(document)
    return document


def _without_fingerprint(value: Mapping[str, Any]) -> dict[str, Any]:
    clean = deepcopy(dict(value))
    clean.pop("fingerprint", None)
    return clean


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def contract_fingerprint(value: Any = None) -> str:
    """Return a stable SHA-256 fingerprint.

    ``value`` may be a dataset name, one record, a complete contract document,
    or ``None`` for the current complete registry.  A complete document's
    self-reported ``fingerprint`` is ignored before hashing, which makes a
    parsed/exported document hash identically to a freshly built one.
    """

    if value is None:
        value = build_contract(include_fingerprint=False)
    elif isinstance(value, str) and value in DATASETS:
        value = dataset_contract(value)
    elif isinstance(value, Mapping):
        value = _without_fingerprint(value)
    else:
        raise TypeError("contract fingerprint expects a dataset name or mapping")
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def contract_json(value: Any = None, *, indent: int | None = 2) -> str:
    """Serialize a contract or dataset record with stable key ordering."""

    payload = build_contract() if value is None else value
    return json.dumps(
        _json_value(payload),
        ensure_ascii=False,
        sort_keys=True,
        indent=indent,
    )


def export_contract(
    path: str | Path | None = None,
    *,
    indent: int | None = 2,
    datasets: Mapping[str, DatasetSpec] | None = None,
    schemas: Mapping[str, Mapping[str, Any]] | None = None,
    primary_keys: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Build the complete contract and optionally write it to *path*.

    The return value is always the dictionary, including when ``path`` is
    supplied, which keeps programmatic and CLI use consistent.
    """

    document = build_contract(
        datasets=datasets,
        schemas=schemas,
        primary_keys=primary_keys,
    )
    if path is not None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(contract_json(document, indent=indent) + "\n", encoding="utf-8")
    return document


def load_contract(value: Any) -> dict[str, Any]:
    """Load a mapping, JSON string, or JSON file into a contract document."""

    if value is None:
        return build_contract()
    if isinstance(value, Mapping):
        return deepcopy(dict(value))
    if isinstance(value, Path):
        return json.loads(value.read_text(encoding="utf-8"))
    if isinstance(value, str):
        candidate = Path(value)
        if candidate.exists():
            return json.loads(candidate.read_text(encoding="utf-8"))
        return json.loads(value)
    raise TypeError("contract must be a mapping, JSON string, or path")


def _record_schema(record: Mapping[str, Any]) -> Mapping[str, Any] | None:
    schema = record.get("schema")
    if not isinstance(schema, Mapping):
        schema = record.get("columns")
    return schema if isinstance(schema, Mapping) else None


def _record_pk(record: Mapping[str, Any]) -> list[Any] | None:
    pk = record.get("primary_key")
    if not isinstance(pk, Sequence) or isinstance(pk, (str, bytes)):
        pk = record.get("primary_keys")
    if not isinstance(pk, Sequence) or isinstance(pk, (str, bytes)):
        return None
    return list(pk)


def _column_is_nullable(record: Mapping[str, Any], column: str) -> bool:
    """Return whether a proposed schema column can be absent/null.

    The registry's compact JSON format historically represented a column as
    its dtype only, and all additions under the ``additive`` policy therefore
    carry the existing schema convention: a new column is nullable unless a
    portable contract explicitly says otherwise.  Accept the optional
    ``nullable_columns`` / ``required_columns`` declarations and descriptor
    values as well, so a downstream producer can make a non-nullable addition
    explicit without changing the public format.
    """

    nullable = _field(record, "nullable_columns")
    if isinstance(nullable, Mapping) and column in nullable:
        return bool(nullable[column])
    if isinstance(nullable, Sequence) and not isinstance(nullable, (str, bytes)):
        return column in nullable

    required = _field(record, "required_columns")
    if isinstance(required, Sequence) and not isinstance(required, (str, bytes)):
        return column not in required

    schema = _record_schema(record) or {}
    descriptor = schema.get(column)
    if isinstance(descriptor, Mapping):
        if "nullable" in descriptor:
            return bool(descriptor["nullable"])
        if "required" in descriptor:
            return not bool(descriptor["required"])
    return True


def _compatible_column_addition(new: Mapping[str, Any], column: str) -> bool:
    """Whether a newly introduced column is backward-compatible."""

    # Missing compatibility metadata is how pre-contract documents represent
    # the long-standing additive default.  An explicitly different policy is
    # not allowed to waive a schema-version bump.
    policy = _field(new, "compatibility")
    return policy in (None, "additive") and _column_is_nullable(new, column)


def _field(record: Mapping[str, Any], name: str, default: Any = None) -> Any:
    if name in record:
        return record[name]
    metadata = record.get("metadata")
    if isinstance(metadata, Mapping):
        return metadata.get(name, default)
    return default


def _validate_record(name: str, record: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(record, Mapping):
        return [f"datasets.{name}: record must be an object"]

    schema = _record_schema(record)
    if schema is None:
        errors.append(f"datasets.{name}.schema: missing object")
    elif any(not isinstance(column, str) or not column for column in schema):
        errors.append(f"datasets.{name}.schema: column names must be non-empty strings")
    explicit_schema = record.get("schema")
    explicit_columns = record.get("columns")
    if isinstance(explicit_schema, Mapping) and isinstance(explicit_columns, Mapping):
        if dict(explicit_schema) != dict(explicit_columns):
            errors.append(f"datasets.{name}.schema: schema and columns aliases disagree")

    pk = _record_pk(record)
    if pk is None:
        errors.append(f"datasets.{name}.primary_key: missing array")
    elif not pk or any(not isinstance(column, str) or not column for column in pk):
        errors.append(f"datasets.{name}.primary_key: must contain non-empty strings")
    elif schema is not None:
        missing = [column for column in pk if column not in schema]
        if missing:
            errors.append(f"datasets.{name}.primary_key: columns missing from schema: {missing}")
    explicit_pk = record.get("primary_key")
    explicit_pks = record.get("primary_keys")
    if (
        isinstance(explicit_pk, Sequence)
        and not isinstance(explicit_pk, (str, bytes))
        and isinstance(explicit_pks, Sequence)
        and not isinstance(explicit_pks, (str, bytes))
        and list(explicit_pk) != list(explicit_pks)
    ):
        errors.append(f"datasets.{name}.primary_key: aliases disagree")

    schema_version = _field(record, "schema_version")
    if schema_version is not None and (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version < 1
    ):
        errors.append(f"datasets.{name}.schema_version: must be a positive integer")

    contract_level = _field(record, "contract_level")
    if contract_level is not None and (not isinstance(contract_level, str) or not contract_level):
        errors.append(f"datasets.{name}.contract_level: must be a non-empty string")

    pit = _field(record, "pit")
    pit_grade = _field(record, "pit_grade")
    pit_quality = _field(record, "pit_quality")
    if pit is not None and not isinstance(pit, bool):
        errors.append(f"datasets.{name}.pit: must be boolean")
    if pit_grade is not None:
        if not isinstance(pit_grade, str) or pit_grade not in _PIT_GRADES:
            errors.append(f"datasets.{name}.pit_grade: unsupported value {pit_grade!r}")
        elif pit is True and pit_grade == "none":
            errors.append(f"datasets.{name}.pit_grade: PIT dataset cannot use 'none'")
        elif pit is False and pit_grade == "strict":
            errors.append(f"datasets.{name}.pit_grade: non-PIT dataset cannot use 'strict'")

    if pit_quality is not None:
        if not isinstance(pit_quality, str) or pit_quality not in _PIT_QUALITIES:
            errors.append(f"datasets.{name}.pit_quality: unsupported value {pit_quality!r}")
        elif pit is True and pit_quality == "snapshot_only":
            errors.append(f"datasets.{name}.pit_quality: PIT dataset cannot be snapshot_only")
        elif pit is False and pit_quality == "reconstructed":
            errors.append(f"datasets.{name}.pit_quality: non-PIT dataset cannot be reconstructed")
    if pit_quality == "reconstructed" and pit_grade == "strict":
        errors.append(f"datasets.{name}.pit_grade: reconstructed quality requires 'partial'")
    if pit_quality == "strict" and pit is True and pit_grade == "partial":
        errors.append(f"datasets.{name}.pit_grade: strict quality cannot use 'partial'")

    pit_modes = _field(record, "pit_modes")
    if pit_modes is not None:
        if (
            not isinstance(pit_modes, Sequence)
            or isinstance(pit_modes, (str, bytes))
            or not all(isinstance(mode, str) for mode in pit_modes)
            or set(pit_modes) != _PIT_MODES
        ):
            errors.append(f"datasets.{name}.pit_modes: must contain exactly strict and best_effort")

    pit_storage = _field(record, "pit_storage_columns")
    if pit_storage is not None and (
        not isinstance(pit_storage, Sequence)
        or isinstance(pit_storage, (str, bytes))
        or any(column not in PIT_STORAGE_COLUMNS for column in pit_storage)
    ):
        errors.append(
            f"datasets.{name}.pit_storage_columns: must be a subset of the PIT storage contract"
        )

    availability = _field(record, "availability_col")
    if availability is not None and schema is not None and availability not in schema:
        errors.append(f"datasets.{name}.availability_col: {availability!r} is not in schema")

    compatibility = _field(record, "compatibility")
    if compatibility is not None:
        if isinstance(compatibility, str):
            if compatibility not in _COMPATIBILITY_POLICIES:
                errors.append(
                    f"datasets.{name}.compatibility: unsupported policy {compatibility!r}"
                )
        elif isinstance(compatibility, Mapping):
            if any(
                not isinstance(key, str) or not key or not isinstance(unit, str) or not unit
                for key, unit in compatibility.items()
            ):
                errors.append(
                    f"datasets.{name}.compatibility: object keys and values must be non-empty strings"
                )
        else:
            errors.append(f"datasets.{name}.compatibility: must be a string or object")

    unit_contract = _field(record, "unit_contract")
    if unit_contract is not None:
        if isinstance(unit_contract, str):
            if not unit_contract:
                errors.append(f"datasets.{name}.unit_contract: must not be empty")
        elif isinstance(unit_contract, Mapping):
            if any(
                not isinstance(key, str) or not key or not isinstance(unit, str) or not unit
                for key, unit in unit_contract.items()
            ):
                errors.append(
                    f"datasets.{name}.unit_contract: object keys and values must be non-empty strings"
                )
        else:
            errors.append(f"datasets.{name}.unit_contract: must be a string or object")

    history_mode = _field(record, "history_mode")
    if history_mode is not None and history_mode not in _HISTORY_MODES:
        errors.append(f"datasets.{name}.history_mode: unsupported value {history_mode!r}")

    fetch = _field(record, "fetch_semantics")
    if fetch is not None and fetch not in {"by_date", "snapshot"}:
        errors.append(f"datasets.{name}.fetch_semantics: unsupported value {fetch!r}")

    lookback = _field(record, "reconciliation_lookback_days")
    if lookback is not None and (
        isinstance(lookback, bool) or not isinstance(lookback, int) or lookback < 0
    ):
        errors.append(
            f"datasets.{name}.reconciliation_lookback_days: must be a non-negative integer"
        )
    lookback_mode = _field(record, "reconciliation_lookback_mode")
    if lookback_mode is not None and lookback_mode not in {"calendar", "trading_day"}:
        errors.append(
            f"datasets.{name}.reconciliation_lookback_mode: unsupported value {lookback_mode!r}"
        )
    append_only = _field(record, "append_only")
    if append_only is not None and not isinstance(append_only, bool):
        errors.append(f"datasets.{name}.append_only: must be a boolean")
    negative_ttl = _field(record, "negative_evidence_ttl_days")
    if negative_ttl is not None and (
        isinstance(negative_ttl, bool) or not isinstance(negative_ttl, int) or negative_ttl < 0
    ):
        errors.append(f"datasets.{name}.negative_evidence_ttl_days: must be a non-negative integer")

    return errors


def validate_contract(
    value: Any = None,
    *,
    against_registry: bool | None = None,
) -> list[str]:
    """Return validation errors for a contract document.

    An omitted value validates the live registry, including schema/PK coverage.
    A supplied document is validated as a portable historical contract and is
    not required to contain today's complete 42-dataset registry.  Pass
    ``against_registry=True`` to require exact current registry membership and
    definitions.
    """

    errors: list[str] = []
    omitted = value is None
    try:
        document = load_contract(value)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return [f"contract: cannot load document: {exc}"]

    if not isinstance(document, Mapping):
        return ["contract: top-level value must be an object"]
    datasets = document.get("datasets")
    # ``cne contract show --dataset --out`` intentionally emits a compact record;
    # accept that record anywhere a one-dataset contract is expected.
    if datasets is None and isinstance(document.get("name"), str):
        datasets = {document["name"]: document}
    if not isinstance(datasets, Mapping):
        return ["contract.datasets: missing object"]

    version = document.get("contract_version", document.get("version"))
    if version is not None and (
        isinstance(version, bool) or not isinstance(version, int) or version < 1
    ):
        errors.append("contract_version: must be a positive integer")
    if document.get("format") is not None and document["format"] != CONTRACT_FORMAT:
        errors.append(f"format: unsupported value {document['format']!r}")

    reported_fingerprint = document.get("fingerprint")
    if reported_fingerprint is not None:
        if not isinstance(reported_fingerprint, str) or not reported_fingerprint:
            errors.append("fingerprint: must be a non-empty string")
        elif reported_fingerprint != contract_fingerprint(document):
            errors.append("fingerprint: does not match contract contents")

    for name in sorted(datasets, key=str):
        if not isinstance(name, str) or not name:
            errors.append("datasets: names must be non-empty strings")
            continue
        errors.extend(_validate_record(name, datasets[name]))

    if against_registry is None:
        against_registry = omitted
    if against_registry:
        current_names = set(DATASETS)
        contract_names = set(datasets)
        for name in sorted(current_names - contract_names):
            errors.append(f"datasets: missing registered dataset {name!r}")
        for name in sorted(contract_names - current_names):
            errors.append(f"datasets: unknown registered dataset {name!r}")
        for name in sorted(current_names & contract_names):
            expected = dataset_contract(name)
            actual = datasets[name]
            if not isinstance(actual, Mapping):
                continue
            expected_schema = expected["schema"]
            actual_schema = _record_schema(actual)
            if actual_schema is not None and dict(actual_schema) != expected_schema:
                errors.append(f"datasets.{name}.schema: does not match DATASET_SCHEMAS")
            expected_pk = expected["primary_key"]
            actual_pk = _record_pk(actual)
            if actual_pk is not None and actual_pk != expected_pk:
                errors.append(f"datasets.{name}.primary_key: does not match PRIMARY_KEYS")
            for field in (
                "schema_version",
                "contract_level",
                "pit_grade",
                "pit_quality",
                "pit_modes",
                "pit_storage_columns",
                "pit_storage_dtypes",
                "availability_col",
                "compatibility",
                "unit_contract",
                "reconciliation_lookback_days",
                "reconciliation_lookback_mode",
                "append_only",
                "negative_evidence_ttl_days",
            ):
                actual_value = _field(actual, field)
                if actual_value is not None and actual_value != expected[field]:
                    errors.append(f"datasets.{name}.{field}: does not match DATASETS")

    return errors


def is_contract_valid(value: Any = None, *, against_registry: bool | None = None) -> bool:
    """Boolean convenience wrapper around :func:`validate_contract`."""

    return not validate_contract(value, against_registry=against_registry)


def assert_valid_contract(
    value: Any = None,
    *,
    against_registry: bool | None = None,
) -> dict[str, Any]:
    """Validate and return *value*, raising :class:`ContractValidationError`."""

    errors = validate_contract(value, against_registry=against_registry)
    if errors:
        raise ContractValidationError(errors)
    return load_contract(value)


class ContractDiff(dict[str, Any]):
    """Dictionary result with convenient attribute access for callers."""

    # This class is intentionally not used as a dataclass record: inheriting
    # dict preserves JSON serialisability and compatibility with callers that
    # already expect ``diff["breaking"]``.
    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


_HISTORY_FIELDS = (
    "fetch_semantics",
    "history_mode",
    "backfill_source",
    "history_horizon_days",
    "history_floor_date",
    "source_retired_date",
)
_PIT_FIELDS = ("pit", "pit_grade", "pit_quality", "availability_col")
_STORAGE_FIELDS = ("partition_col", "partition_granularity", "date_col", "query_date_col")


def _change(
    dataset: str,
    kind: str,
    path: str,
    old: Any,
    new: Any,
    *,
    breaking: bool,
) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "kind": kind,
        "path": path,
        "old": _json_value(old),
        "new": _json_value(new),
        "severity": "breaking" if breaking else "compatible",
    }


def _diff_record(name: str, old: Mapping[str, Any], new: Mapping[str, Any]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    old_schema = dict(_record_schema(old) or {})
    new_schema = dict(_record_schema(new) or {})
    for column in sorted(set(old_schema) - set(new_schema)):
        changes.append(
            _change(
                name,
                "column_removed",
                f"datasets.{name}.schema.{column}",
                old_schema[column],
                None,
                breaking=True,
            )
        )
    for column in sorted(set(new_schema) - set(old_schema)):
        compatible = _compatible_column_addition(new, column)
        changes.append(
            _change(
                name,
                "column_added",
                f"datasets.{name}.schema.{column}",
                None,
                new_schema[column],
                breaking=not compatible,
            )
        )
    for column in sorted(set(old_schema) & set(new_schema)):
        if _dtype_name(old_schema[column]) != _dtype_name(new_schema[column]):
            changes.append(
                _change(
                    name,
                    "column_type_changed",
                    f"datasets.{name}.schema.{column}",
                    old_schema[column],
                    new_schema[column],
                    breaking=True,
                )
            )

    old_pk = _record_pk(old)
    new_pk = _record_pk(new)
    if old_pk != new_pk:
        changes.append(
            _change(
                name,
                "primary_key_changed",
                f"datasets.{name}.primary_key",
                old_pk,
                new_pk,
                breaking=True,
            )
        )

    # Newer records place fields at the root and in metadata; _field lets the
    # diff consume both forms, including contracts emitted by older releases.
    for field in _PIT_FIELDS:
        old_value = _field(old, field)
        new_value = _field(new, field)
        if old_value != new_value and (old_value is not None or new_value is not None):
            changes.append(
                _change(
                    name,
                    "pit_semantics_changed" if field == "pit" else f"{field}_changed",
                    f"datasets.{name}.{field}",
                    old_value,
                    new_value,
                    breaking=True,
                )
            )

    old_unit = _field(old, "unit_contract")
    new_unit = _field(new, "unit_contract")
    if old_unit != new_unit and (old_unit is not None or new_unit is not None):
        changes.append(
            _change(
                name,
                "unit_contract_changed",
                f"datasets.{name}.unit_contract",
                old_unit,
                new_unit,
                breaking=True,
            )
        )

    history_changed = []
    for field in _HISTORY_FIELDS:
        old_value = _field(old, field)
        new_value = _field(new, field)
        if old_value != new_value and (old_value is not None or new_value is not None):
            history_changed.append((field, old_value, new_value))
    if history_changed:
        changes.append(
            _change(
                name,
                "history_semantics_changed",
                f"datasets.{name}.history",
                {field: old for field, old, _ in history_changed},
                {field: new for field, _, new in history_changed},
                breaking=True,
            )
        )

    storage_changed = []
    for field in _STORAGE_FIELDS:
        old_value = _field(old, field)
        new_value = _field(new, field)
        if old_value != new_value and (old_value is not None or new_value is not None):
            storage_changed.append((field, old_value, new_value))
    if storage_changed:
        changes.append(
            _change(
                name,
                "storage_semantics_changed",
                f"datasets.{name}.storage",
                {field: old for field, old, _ in storage_changed},
                {field: new for field, _, new in storage_changed},
                breaking=True,
            )
        )

    old_version = _field(old, "schema_version")
    new_version = _field(new, "schema_version")
    if isinstance(old_version, int) and isinstance(new_version, int) and new_version < old_version:
        changes.append(
            _change(
                name,
                "schema_version_decreased",
                f"datasets.{name}.schema_version",
                old_version,
                new_version,
                breaking=True,
            )
        )
    added_columns = set(new_schema) - set(old_schema)
    removed_columns = set(old_schema) - set(new_schema)
    changed_columns = {
        column
        for column in set(old_schema) & set(new_schema)
        if _dtype_name(old_schema[column]) != _dtype_name(new_schema[column])
    }
    schema_requires_bump = bool(removed_columns or changed_columns) or any(
        not _compatible_column_addition(new, column) for column in added_columns
    )
    if (
        schema_requires_bump
        and isinstance(old_version, int)
        and isinstance(new_version, int)
        and new_version <= old_version
    ):
        changes.append(
            _change(
                name,
                "schema_version_not_bumped",
                f"datasets.{name}.schema_version",
                old_version,
                new_version,
                breaking=True,
            )
        )

    old_level = _field(old, "contract_level")
    new_level = _field(new, "contract_level")
    if old_level != new_level and (old_level is not None or new_level is not None):
        changes.append(
            _change(
                name,
                "contract_level_changed",
                f"datasets.{name}.contract_level",
                old_level,
                new_level,
                breaking=False,
            )
        )

    return changes


def diff_contracts(old: Any, new: Any = None) -> ContractDiff:
    """Compare two contracts and classify changes by compatibility.

    When *new* is omitted, *old* is compared with the current registry.  Added
    datasets and nullable/additive columns are compatible; dropped datasets,
    removed/type-changed columns, primary-key changes, unit changes, PIT
    changes, and history-meaning changes are breaking.
    """

    old_document = load_contract(old)
    new_document = load_contract(new)
    old_datasets = old_document.get("datasets", {})
    new_datasets = new_document.get("datasets", {})
    if not old_datasets and isinstance(old_document.get("name"), str):
        old_datasets = {old_document["name"]: old_document}
    if not new_datasets and isinstance(new_document.get("name"), str):
        new_datasets = {new_document["name"]: new_document}
    if not isinstance(old_datasets, Mapping) or not isinstance(new_datasets, Mapping):
        raise ContractValidationError(["both contracts must contain a datasets object"])

    changes: list[dict[str, Any]] = []
    for name in sorted(set(old_datasets) - set(new_datasets), key=str):
        changes.append(
            _change(
                name, "dataset_removed", f"datasets.{name}", old_datasets[name], None, breaking=True
            )
        )
    for name in sorted(set(new_datasets) - set(old_datasets), key=str):
        changes.append(
            _change(
                name, "dataset_added", f"datasets.{name}", None, new_datasets[name], breaking=False
            )
        )
    for name in sorted(set(old_datasets) & set(new_datasets), key=str):
        old_record = old_datasets[name]
        new_record = new_datasets[name]
        if isinstance(old_record, Mapping) and isinstance(new_record, Mapping):
            changes.extend(_diff_record(name, old_record, new_record))
        elif old_record != new_record:
            changes.append(
                _change(
                    name,
                    "dataset_record_changed",
                    f"datasets.{name}",
                    old_record,
                    new_record,
                    breaking=True,
                )
            )

    changes.sort(key=lambda item: (str(item["dataset"]), str(item["path"]), str(item["kind"])))
    breaking = [item for item in changes if item["severity"] == "breaking"]
    compatible = [item for item in changes if item["severity"] == "compatible"]
    result = ContractDiff(
        {
            "format": "cnequity.contract-diff",
            "old_fingerprint": contract_fingerprint(old_document),
            "new_fingerprint": contract_fingerprint(new_document),
            "changes": changes,
            "breaking": breaking,
            "compatible": compatible,
            "breaking_changes": breaking,
            "compatible_changes": compatible,
            "is_breaking": bool(breaking),
            "has_breaking_changes": bool(breaking),
            "breaking_count": len(breaking),
            "compatible_count": len(compatible),
            "added_datasets": [
                item["dataset"] for item in compatible if item["kind"] == "dataset_added"
            ],
            "removed_datasets": [
                item["dataset"] for item in breaking if item["kind"] == "dataset_removed"
            ],
        }
    )
    return result


def format_contract_diff(diff: Mapping[str, Any]) -> str:
    """Render a compact human-readable diff summary."""

    lines = [
        f"breaking: {diff.get('breaking_count', len(diff.get('breaking', [])))}",
        f"compatible: {diff.get('compatible_count', len(diff.get('compatible', [])))}",
    ]
    for change in diff.get("changes", []):
        marker = "BREAKING" if change.get("severity") == "breaking" else "compatible"
        lines.append(
            f"{marker}: {change.get('dataset')} {change.get('kind')} "
            f"({change.get('old')!r} -> {change.get('new')!r})"
        )
    return "\n".join(lines)


__all__ = [
    "CONTRACT_FORMAT",
    "CONTRACT_VERSION",
    "ContractDiff",
    "ContractValidationError",
    "assert_valid_contract",
    "build_contract",
    "contract_fingerprint",
    "contract_json",
    "dataset_contract",
    "diff_contracts",
    "export_contract",
    "format_contract_diff",
    "is_contract_valid",
    "load_contract",
    "validate_contract",
]
