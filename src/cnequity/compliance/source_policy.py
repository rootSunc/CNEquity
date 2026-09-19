"""Load and conservatively evaluate upstream source-use policies.

The registry in :mod:`cnequity.domain.datasets` names the source labels that
can reach the lake.  ``sources/SOURCES.yml`` is the human-maintained companion
registry for those labels.  This module deliberately does not make legal
determinations: an unreviewed permission is represented by the literal string
``"unknown"`` and never satisfies an allow check.

The loader accepts a path so downstream deployments can provide a reviewed
policy file.  With no path it locates the repository's ``sources/SOURCES.yml``
and validates that every source referenced by the dataset registry is present.
No CLI integration is done here; callers can use the APIs directly while the
CLI wiring remains a separate change.
"""

from __future__ import annotations

import sysconfig
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from cnequity.domain.datasets import DATASETS

UNKNOWN = "unknown"

REQUIRED_FIELDS: frozenset[str] = frozenset(
    {
        "owner",
        "access_type",
        "tos_url",
        "tos_reviewed_at",
        "authentication",
        "personal_use",
        "commercial_use",
        "cache_allowed",
        "redistribution",
        "rate_limit",
        "retained_payloads",
        "legal_status",
        "notes",
    }
)

PERMISSION_FIELDS: tuple[str, ...] = (
    "personal_use",
    "commercial_use",
    "cache_allowed",
    "redistribution",
)

_PERMISSION_ALLOW_VALUES = frozenset({"allowed", "allow", "yes", "true"})
_PERMISSION_DENY_VALUES = frozenset({"not_allowed", "not-allowed", "denied", "deny", "no", "false"})


class SourcePolicyError(ValueError):
    """Base error for malformed or incomplete source policy metadata."""


class SourcePolicyValidationError(SourcePolicyError):
    """Raised when a policy file cannot satisfy the registry contract."""

    def __init__(self, errors: Iterable[str]):
        self.errors = tuple(str(error) for error in errors)
        super().__init__("; ".join(self.errors) or "source policy validation failed")


def default_source_policy_path() -> Path:
    """Return the repository policy path, resolved independently of cwd."""

    # source_policy.py -> compliance -> cnequity -> src -> repository root.
    repository_path = Path(__file__).resolve().parents[3] / "sources" / "SOURCES.yml"
    if repository_path.exists():
        return repository_path

    # Wheels install the reviewed registry as a platform data file so the CLI
    # remains usable outside a source checkout.
    installed = Path(sysconfig.get_path("data")) / "share" / "cnequity" / "SOURCES.yml"
    if installed.exists():
        return installed

    # This fallback is useful for editable-like deployments that place the
    # policy next to the process rather than next to the installed package.
    return Path.cwd() / "sources" / "SOURCES.yml"


DEFAULT_SOURCE_POLICY_PATH = default_source_policy_path()


def _normalise_string(value: object) -> str:
    if isinstance(value, str):
        return value.strip().casefold()
    return str(value).strip().casefold()


def is_explicitly_allowed(value: object) -> bool:
    """Return ``True`` only for an explicit allow value.

    In particular, ``unknown``, missing values, and arbitrary truthy strings
    are not permission.  This is the central guard used by usage assessment.
    """

    if value is True:
        return True
    if isinstance(value, str):
        return _normalise_string(value) in _PERMISSION_ALLOW_VALUES
    return False


def _is_explicitly_denied(value: object) -> bool:
    if value is False:
        return True
    return isinstance(value, str) and _normalise_string(value) in _PERMISSION_DENY_VALUES


def _is_unknown(value: object) -> bool:
    return isinstance(value, str) and _normalise_string(value) == UNKNOWN


@dataclass(frozen=True)
class SourcePolicy(Mapping[str, Any]):
    """One source's machine-readable policy entry.

    Permission fields intentionally accept ``bool`` or a status string.  The
    checked-in matrix uses ``unknown`` until a human has reviewed an upstream
    term, which preserves uncertainty instead of turning it into a claim.
    Additional YAML fields are retained in ``extra`` so a later schema can add
    review notes without making this loader discard information.
    """

    name: str
    owner: Any
    access_type: Any
    tos_url: Any
    tos_reviewed_at: Any
    authentication: Any
    personal_use: Any
    commercial_use: Any
    cache_allowed: Any
    redistribution: Any
    rate_limit: Any
    retained_payloads: Any
    legal_status: Any
    notes: Any
    derived: bool = False
    extra: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)

    def __getitem__(self, key: str) -> Any:
        if key == "name":
            return self.name
        if key == "derived":
            return self.derived
        if key == "extra":
            return self.extra
        if key in REQUIRED_FIELDS:
            return getattr(self, key)
        try:
            return self.extra[key]
        except KeyError:
            raise KeyError(key) from None

    def __iter__(self) -> Iterator[str]:
        yield from ("name", *sorted(REQUIRED_FIELDS), "derived")
        yield from self.extra

    def __len__(self) -> int:
        return 15 + len(self.extra)

    def as_dict(self) -> dict[str, Any]:
        """Return a detached mapping suitable for JSON/reporting."""

        result = {field_name: getattr(self, field_name) for field_name in REQUIRED_FIELDS}
        result["name"] = self.name
        result["derived"] = self.derived
        result.update(self.extra)
        return result


def _coerce_policy(name: str, raw: SourcePolicy | Mapping[str, Any]) -> SourcePolicy:
    if isinstance(raw, SourcePolicy):
        if raw.name == name:
            return raw
        return SourcePolicy(
            name=name,
            **{field_name: raw[field_name] for field_name in REQUIRED_FIELDS},
            derived=raw.derived,
            extra=dict(raw.extra),
        )
    if not isinstance(raw, Mapping):
        raise SourcePolicyError(f"source {name!r}: policy must be a mapping")

    values = {field_name: raw.get(field_name) for field_name in REQUIRED_FIELDS}
    known = REQUIRED_FIELDS | {"derived", "source_kind", "name"}
    extra = {str(key): value for key, value in raw.items() if key not in known}
    derived_raw = raw.get("derived", raw.get("source_kind") == "derived")
    if isinstance(derived_raw, str):
        derived = _normalise_string(derived_raw) in {"true", "yes", "derived"}
    else:
        derived = bool(derived_raw)
    return SourcePolicy(name=name, **values, derived=derived, extra=extra)


def _extract_source_mapping(raw: Mapping[str, Any]) -> Mapping[str, Any]:
    """Accept the canonical ``sources:`` shape and a flat legacy shape."""

    nested = raw.get("sources")
    if nested is not None:
        if not isinstance(nested, Mapping):
            raise SourcePolicyError("top-level 'sources' must be a mapping")
        return nested

    metadata = {"version", "schema_version", "generated_from", "last_reviewed_at", "notes"}
    return {key: value for key, value in raw.items() if key not in metadata}


def _policy_mapping(
    policies: Mapping[str, SourcePolicy | Mapping[str, Any]],
) -> dict[str, SourcePolicy]:
    result: dict[str, SourcePolicy] = {}
    for name, raw in policies.items():
        source_name = str(name).strip()
        if not source_name:
            raise SourcePolicyError("source policy has an empty source name")
        result[source_name] = _coerce_policy(source_name, raw)
    return result


def required_sources(datasets: Mapping[str, Any] | Iterable[Any] | None = None) -> frozenset[str]:
    """Return source labels referenced by a dataset registry.

    ``datasets`` may be the normal ``DATASETS`` mapping, an iterable of
    DatasetSpec-like objects, or ``None`` for the built-in registry.
    """

    registry: Iterable[Any]
    if datasets is None:
        registry = DATASETS.values()
    elif isinstance(datasets, Mapping):
        registry = datasets.values()
    else:
        registry = datasets

    sources: set[str] = set()
    for spec in registry:
        for field_name in ("primary_source", "backup_source", "backfill_source"):
            value = getattr(spec, field_name, None)
            if value:
                sources.add(str(value))
    return frozenset(sources)


def validate_source_policies(
    policies: Mapping[str, SourcePolicy | Mapping[str, Any]] | Path | str,
    *,
    datasets: Mapping[str, Any] | Iterable[Any] | None = None,
) -> list[str]:
    """Return validation errors for *policies*.

    The return shape follows the project's other validators: an empty list is
    success.  ``load_source_policies`` raises ``SourcePolicyValidationError``
    when this list is non-empty, while callers validating an in-memory draft
    can inspect all errors without exception handling.
    """

    if isinstance(policies, (str, Path)):
        path = Path(policies)
        try:
            with path.open("rb") as stream:
                raw = yaml.safe_load(stream)
        except (OSError, yaml.YAMLError) as exc:
            return [f"cannot read source policy file {path}: {exc}"]
        if not isinstance(raw, Mapping):
            return ["source policy document must be a mapping"]
        try:
            policies = _extract_source_mapping(raw)
        except SourcePolicyError as exc:
            return [str(exc)]

    if not isinstance(policies, Mapping):
        return ["source policies must be a mapping"]

    errors: list[str] = []
    try:
        policy_map = _policy_mapping(policies)
    except SourcePolicyError as exc:
        return [str(exc)]

    raw_by_name = {str(name).strip(): raw for name, raw in policies.items()}
    for name, policy in policy_map.items():
        raw = raw_by_name[name]
        present = (
            set(REQUIRED_FIELDS)
            if isinstance(raw, SourcePolicy)
            else {str(field_name) for field_name in raw}
            if isinstance(raw, Mapping)
            else set()
        )
        missing = sorted(REQUIRED_FIELDS - present)
        errors.extend(f"source {name!r}: missing required field {field!r}" for field in missing)
        for field_name in REQUIRED_FIELDS:
            value = policy[field_name]
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(f"source {name!r}: {field_name} must be explicit (use 'unknown')")

        for field_name in REQUIRED_FIELDS:
            value = policy[field_name]
            if isinstance(value, str) and _normalise_string(value) in {
                "unk",
                "n/a",
                "na",
                "not_known",
                "not reviewed",
            }:
                errors.append(
                    f"source {name!r}: {field_name} must use the literal 'unknown' when uncertain"
                )

        if name == "derived" and not policy.derived:
            errors.append("source 'derived': set derived: true (derived provenance is explicit)")

    referenced = required_sources(datasets)
    errors.extend(
        f"missing policy for registered source {name!r}"
        for name in sorted(referenced - set(policy_map))
    )
    return errors


def load_source_policies(
    path: str | Path | None = None,
    *,
    validate: bool = True,
    datasets: Mapping[str, Any] | Iterable[Any] | None = None,
) -> dict[str, SourcePolicy]:
    """Load ``SOURCES.yml`` as ``{source_name: SourcePolicy}``.

    Validation is enabled by default and checks coverage against the built-in
    dataset registry.  Pass ``validate=False`` only when inspecting a partial
    draft; callers should validate before using it for a decision.
    """

    policy_path = Path(path) if path is not None else default_source_policy_path()
    try:
        with policy_path.open("rb") as stream:
            raw = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise SourcePolicyError(f"source policy file not found: {policy_path}") from exc
    except OSError as exc:
        raise SourcePolicyError(f"cannot read source policy file {policy_path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise SourcePolicyError(f"invalid YAML in source policy file {policy_path}: {exc}") from exc

    if not isinstance(raw, Mapping):
        raise SourcePolicyError("source policy document must be a mapping")
    source_mapping = _extract_source_mapping(raw)
    try:
        policies = _policy_mapping(source_mapping)
    except SourcePolicyError:
        raise
    if validate:
        errors = validate_source_policies(policies, datasets=datasets)
        if errors:
            raise SourcePolicyValidationError(errors)
    return policies


def policy_for_source(
    source: str,
    policies: Mapping[str, SourcePolicy] | None = None,
) -> SourcePolicy:
    """Return one source policy, raising a useful ``KeyError`` if absent."""

    policy_map = policies if policies is not None else load_source_policies()
    try:
        return policy_map[source]
    except KeyError:
        raise KeyError(f"no source policy registered for {source!r}") from None


@dataclass(frozen=True)
class DatasetPolicy(Mapping[str, SourcePolicy | None]):
    """Policies grouped by a dataset's routing roles.

    The three named roles are single-valued and stay that way: the gates read
    them, and the backup gate's question — is the backup independent of the
    primary — stops being answerable if the roles are flattened. The two
    tuples carry the rest of what actually wrote rows: an automatic recovery
    chain (`supplementary`) and the operator-invoked repairs (`repair`). Terms
    apply to a row whatever route landed it, so both belong in ``all``.
    """

    dataset: str
    primary: SourcePolicy
    backup: SourcePolicy | None = None
    backfill: SourcePolicy | None = None
    supplementary: tuple[SourcePolicy, ...] = ()
    repair: tuple[SourcePolicy, ...] = ()

    def __getitem__(self, key: str) -> SourcePolicy | None:
        aliases = {
            "primary_source": "primary",
            "backup_source": "backup",
            "backfill_source": "backfill",
        }
        role = aliases.get(key, key)
        if role in {"primary", "backup", "backfill"}:
            return getattr(self, role)
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        yield from ("primary", "backup", "backfill")

    def __len__(self) -> int:
        return 3

    @property
    def all(self) -> tuple[SourcePolicy, ...]:
        """Distinct policies: the three roles, then supplementary, then repair."""

        result: list[SourcePolicy] = []
        seen: set[str] = set()
        for policy in (
            self.primary,
            self.backup,
            self.backfill,
            *self.supplementary,
            *self.repair,
        ):
            if policy is not None and policy.name not in seen:
                result.append(policy)
                seen.add(policy.name)
        return tuple(result)

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "primary": self.primary.as_dict(),
            "backup": self.backup.as_dict() if self.backup else None,
            "backfill": self.backfill.as_dict() if self.backfill else None,
            "supplementary": [policy.as_dict() for policy in self.supplementary],
            "repair": [policy.as_dict() for policy in self.repair],
        }


def policies_for_dataset(
    dataset: str,
    policies: Mapping[str, SourcePolicy] | None = None,
    *,
    datasets: Mapping[str, Any] | None = None,
) -> DatasetPolicy:
    """Resolve source policies for one registered dataset."""

    registry = datasets if datasets is not None else DATASETS
    try:
        spec = registry[dataset]
    except KeyError:
        raise KeyError(f"unknown dataset {dataset!r}") from None
    policy_map = policies if policies is not None else load_source_policies(datasets=registry)
    try:
        primary = policy_for_source(spec.primary_source, policy_map)
        backup = policy_for_source(spec.backup_source, policy_map) if spec.backup_source else None
        backfill = (
            policy_for_source(spec.backfill_source, policy_map) if spec.backfill_source else None
        )
        supplementary = tuple(
            policy_for_source(name, policy_map)
            for name in (getattr(spec, "supplementary_sources", ()) or ())
        )
        repair = tuple(
            policy_for_source(name, policy_map)
            for name in (getattr(spec, "repair_sources", ()) or ())
        )
    except KeyError as exc:
        raise SourcePolicyError(f"dataset {dataset!r}: {exc}") from exc
    return DatasetPolicy(
        dataset=dataset,
        primary=primary,
        backup=backup,
        backfill=backfill,
        supplementary=supplementary,
        repair=repair,
    )


def aggregate_dataset_policies(
    policies: Mapping[str, SourcePolicy] | None = None,
    *,
    datasets: Mapping[str, Any] | None = None,
) -> dict[str, DatasetPolicy]:
    """Return role-grouped source policies for every registered dataset."""

    registry = datasets if datasets is not None else DATASETS
    policy_map = policies if policies is not None else load_source_policies(datasets=registry)
    return {name: policies_for_dataset(name, policy_map, datasets=registry) for name in registry}


@dataclass(frozen=True)
class UsageAssessment(Mapping[str, Any]):
    """Conservative result of checking an intended use against a policy."""

    source: str
    requested: tuple[str, ...]
    allowed: bool
    risk: str
    decision: str
    reasons: tuple[str, ...] = ()
    blocked_fields: tuple[str, ...] = ()

    def __getitem__(self, key: str) -> Any:
        if key in {
            "source",
            "requested",
            "allowed",
            "risk",
            "decision",
            "reasons",
            "blocked_fields",
        }:
            return getattr(self, key)
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        yield from (
            "source",
            "requested",
            "allowed",
            "risk",
            "decision",
            "reasons",
            "blocked_fields",
        )

    def __len__(self) -> int:
        return 7

    def as_dict(self) -> dict[str, Any]:
        return {key: self[key] for key in self}


def _resolve_policy(
    source_or_policy: str | SourcePolicy,
    policies: Mapping[str, SourcePolicy] | None,
) -> SourcePolicy:
    if isinstance(source_or_policy, SourcePolicy):
        return source_or_policy
    return policy_for_source(source_or_policy, policies)


def assess_usage(
    source_or_policy: str | SourcePolicy,
    *,
    personal_use: bool = False,
    commercial_use: bool = False,
    cache: bool = False,
    redistribution: bool = False,
    retain_payloads: bool = False,
    policies: Mapping[str, SourcePolicy] | None = None,
) -> UsageAssessment:
    """Assess an intended source use without granting unknown permissions.

    ``True`` flags identify the operations the caller intends.  Every
    requested operation must have an explicit allow value in the matrix.  A
    policy with unknown terms therefore returns ``allowed=False`` and a
    review/blocking reason rather than silently passing a truthiness check.
    ``retain_payloads`` is checked against ``cache_allowed`` because the
    matrix's cache field is the conservative retention control.
    """

    policy = _resolve_policy(source_or_policy, policies)
    requested: list[str] = []
    requested_fields: list[tuple[str, str]] = []
    if personal_use:
        requested.append("personal_use")
        requested_fields.append(("personal_use", "personal use"))
    if commercial_use:
        requested.append("commercial_use")
        requested_fields.append(("commercial_use", "commercial use"))
    if cache or retain_payloads:
        requested.append("cache_allowed")
        requested_fields.append(("cache_allowed", "payload caching/retention"))
    if redistribution:
        requested.append("redistribution")
        requested_fields.append(("redistribution", "redistribution"))

    reasons: list[str] = []
    blocked_fields: list[str] = []
    for field_name, label in requested_fields:
        value = getattr(policy, field_name)
        if is_explicitly_allowed(value):
            continue
        blocked_fields.append(field_name)
        if _is_explicitly_denied(value):
            reasons.append(f"{label} is explicitly not allowed by the source policy")
        elif _is_unknown(value):
            reasons.append(f"{label} is unknown; obtain upstream permission before use")
        else:
            reasons.append(f"{label} has no explicit allow value in the source policy")

    if blocked_fields:
        risk = "high"
        decision = "blocked"
        allowed = False
    elif requested_fields:
        risk = "low"
        decision = "allowed"
        allowed = True
    else:
        unknown = [field for field in PERMISSION_FIELDS if _is_unknown(getattr(policy, field))]
        if unknown:
            reasons.append(
                "no intended use was selected and permission fields remain unknown: "
                + ", ".join(unknown)
            )
            risk = "high"
            decision = "review_required"
            allowed = False
        else:
            risk = "medium"
            decision = "review_required"
            allowed = False

    return UsageAssessment(
        source=policy.name,
        requested=tuple(requested),
        allowed=allowed,
        risk=risk,
        decision=decision,
        reasons=tuple(reasons),
        blocked_fields=tuple(blocked_fields),
    )


def usage_profile(
    source_or_policy: str | SourcePolicy,
    profile: str | None = None,
    *,
    personal: bool = False,
    commercial: bool = False,
    cache: bool = False,
    redistribution: bool = False,
    personal_use: bool | None = None,
    commercial_use: bool | None = None,
    retain_payloads: bool = False,
    policies: Mapping[str, SourcePolicy] | None = None,
) -> UsageAssessment:
    """Convenience wrapper for common named usage profiles.

    ``profile`` may be ``personal``, ``commercial``, ``cache`` or
    ``redistribution``.  Explicit keyword flags are also accepted; their
    ``*_use`` spellings mirror the YAML fields.
    """

    if profile:
        profile_name = profile.strip().casefold().replace("-", "_")
        if profile_name in {"personal", "personal_use"}:
            personal = True
        elif profile_name in {"commercial", "commercial_use"}:
            commercial = True
        elif profile_name in {"cache", "cache_allowed", "retention", "retain_payloads"}:
            cache = True
        elif profile_name in {"redistribution", "redistribute"}:
            redistribution = True
        else:
            raise ValueError(f"unknown usage profile {profile!r}")
    if personal_use is not None:
        personal = personal_use
    if commercial_use is not None:
        commercial = commercial_use
    return assess_usage(
        source_or_policy,
        personal_use=personal,
        commercial_use=commercial,
        cache=cache,
        redistribution=redistribution,
        retain_payloads=retain_payloads,
        policies=policies,
    )
