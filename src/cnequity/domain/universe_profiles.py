"""Versioned, machine-readable research universe profiles.

The query layer historically accepted a small ``universe=`` string.  That is
useful for interactive exploration, but it is not enough to reproduce a
research result: a name does not say whether Beijing, CDRs, ST rows, or
historical delistings were in scope, nor which evidence was required.  This
module is the public registry for those decisions.

Profiles are deliberately data-only.  They do not import a source adapter and
their canonical representation is stable across Python versions.  A profile
hash therefore identifies the *scope contract*; a concrete symbol-set hash is
available when a caller has materialised the symbols for a date/window.
"""

from __future__ import annotations

import hashlib
import json
import warnings
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


class UniverseProfileError(ValueError):
    """Raised when a profile name or profile payload is invalid."""


def _canonical_json(payload: Any) -> str:
    """Encode JSON in the one representation used for profile identities."""

    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _normalise_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    values = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    return tuple(sorted(values))


@dataclass(frozen=True, slots=True)
class UniverseProfile:
    """A versioned and auditable universe contract.

    ``legacy_universe`` is the old Reader implementation name used to apply
    the common symbol/list-date/status mechanics.  It is intentionally an
    implementation bridge, not the identity of a profile.  Consumers should
    persist ``name``, ``version`` and ``scope_hash``.
    """

    name: str
    version: str
    title: str
    description: str
    exchanges: tuple[str, ...]
    boards: tuple[str, ...]
    include_cdr: bool
    include_etf: bool
    exclude_st: bool
    exclude_suspended: bool
    delisting_policy: str
    pit_evidence_policy: str
    evidence_requirements: tuple[str, ...]
    legacy_universe: str
    strict_research: bool
    research_eligible: bool
    experimental: bool = False

    def __post_init__(self) -> None:
        if not self.name or not self.version:
            raise UniverseProfileError("profile name and version must not be empty")
        if self.legacy_universe not in {"all_a", "all_a_sh_sz"}:
            raise UniverseProfileError(
                f"unsupported legacy universe bridge {self.legacy_universe!r}"
            )
        exchanges = tuple(str(value).upper() for value in self.exchanges)
        if len(set(exchanges)) != len(exchanges):
            raise UniverseProfileError("profile exchanges must be unique")
        object.__setattr__(self, "exchanges", exchanges)
        object.__setattr__(self, "boards", tuple(str(value) for value in self.boards))
        object.__setattr__(self, "evidence_requirements", tuple(self.evidence_requirements))

    @property
    def identity(self) -> str:
        """Stable ``name@version`` label suitable for manifests and logs."""

        return f"{self.name}@{self.version}"

    def rules_dict(self) -> dict[str, Any]:
        """Return only semantic fields (excluding derived hash fields)."""

        return {
            "title": self.title,
            "description": self.description,
            "exchanges": list(self.exchanges),
            "boards": list(self.boards),
            "include_cdr": self.include_cdr,
            "include_etf": self.include_etf,
            "exclude_st": self.exclude_st,
            "exclude_suspended": self.exclude_suspended,
            "delisting_policy": self.delisting_policy,
            "pit_evidence_policy": self.pit_evidence_policy,
            "evidence_requirements": list(self.evidence_requirements),
            "strict_research": self.strict_research,
            "research_eligible": self.research_eligible,
            "experimental": self.experimental,
        }

    @property
    def scope_hash(self) -> str:
        """SHA-256 of this profile's canonical, versioned scope contract."""

        return _hash_payload(
            {
                "name": self.name,
                "version": self.version,
                "rules": self.rules_dict(),
            }
        )

    @property
    def short_scope_hash(self) -> str:
        """Short display form; manifests should persist the full hash."""

        return self.scope_hash[:16]

    def symbol_scope_hash(self, symbols: Iterable[str]) -> str:
        """Hash this profile together with a concrete, order-independent scope."""

        return _hash_payload(
            {
                "profile": {
                    "name": self.name,
                    "version": self.version,
                    "scope_hash": self.scope_hash,
                },
                "symbols": list(_normalise_symbols(symbols)),
            }
        )

    def to_dict(self) -> dict[str, Any]:
        """Machine-readable registry record."""

        payload = {
            "name": self.name,
            "version": self.version,
            "identity": self.identity,
            **self.rules_dict(),
            "legacy_universe": self.legacy_universe,
            "scope_hash": self.scope_hash,
        }
        return payload


# Keep compatibility records in the registry so a manifest can describe an
# old query exactly.  They are not advertised as research-approved profiles.
_PROFILES: dict[str, UniverseProfile] = {
    "cn_a_sh_sz_research_v1": UniverseProfile(
        name="cn_a_sh_sz_research_v1",
        version="1",
        title="China A shares — Shanghai/Shenzhen research",
        description=(
            "Strict reproducible research scope for common SH/SZ A shares; "
            "Beijing, CDR and ETF/LOF instruments are outside scope."
        ),
        exchanges=("SH", "SZ"),
        boards=("main", "star", "chinext"),
        include_cdr=False,
        include_etf=False,
        exclude_st=True,
        exclude_suspended=True,
        delisting_policy="point_in_time_instruments_and_historical_bars",
        pit_evidence_policy="required",
        evidence_requirements=(
            "instruments.list_date_and_delist_date",
            "trading_status.is_trading_and_status_per_symbol_date",
            "historical_st_evidence.complete_versioned_receipt",
            "historical_delisting_coverage.complete_window_receipt",
            "pit_publication_and_observation_dates_for_pit_datasets",
        ),
        legacy_universe="all_a_sh_sz",
        strict_research=True,
        research_eligible=True,
    ),
    "cn_a_all_experimental_v1": UniverseProfile(
        name="cn_a_all_experimental_v1",
        version="1",
        title="China A shares — all exchanges experimental",
        description=(
            "Experimental all-exchange A-share scope, including Beijing common "
            "stocks; it is not a production approval and still fails closed "
            "when strict evidence is requested."
        ),
        exchanges=("SH", "SZ", "BJ"),
        boards=("main", "star", "chinext", "beijing"),
        include_cdr=False,
        include_etf=False,
        exclude_st=True,
        exclude_suspended=True,
        delisting_policy="point_in_time_instruments_and_historical_bars",
        pit_evidence_policy="required_for_research",
        evidence_requirements=(
            "instruments.list_date_and_delist_date",
            "trading_status.is_trading_and_status_per_symbol_date",
            "historical_st_evidence.complete_versioned_receipt_for_all_exchanges",
            "historical_delisting_coverage.complete_window_receipt",
            "pit_publication_and_observation_dates_for_pit_datasets",
        ),
        legacy_universe="all_a",
        strict_research=True,
        research_eligible=False,
        experimental=True,
    ),
    "legacy_all_a": UniverseProfile(
        name="legacy_all_a",
        version="0",
        title="Legacy all_a compatibility scope",
        description=(
            "Compatibility identity for the pre-profile all_a Reader argument; "
            "kept permissive so upgrading does not silently change query results."
        ),
        exchanges=("SH", "SZ", "BJ"),
        boards=("main", "star", "chinext", "beijing"),
        include_cdr=False,
        include_etf=False,
        exclude_st=True,
        exclude_suspended=True,
        delisting_policy="point_in_time_instruments_when_available",
        pit_evidence_policy="best_effort",
        evidence_requirements=(),
        legacy_universe="all_a",
        strict_research=False,
        # Existing downstream research workspaces are governed by the historical all_a
        # contract.  Keep that readiness behavior while the Reader emits a
        # deprecation warning and manifests expose this compatibility identity.
        research_eligible=True,
    ),
    "legacy_all_a_sh_sz": UniverseProfile(
        name="legacy_all_a_sh_sz",
        version="0",
        title="Legacy all_a_sh_sz compatibility scope",
        description="Compatibility identity for the explicit SH/SZ-only universe argument.",
        exchanges=("SH", "SZ"),
        boards=("main", "star", "chinext"),
        include_cdr=False,
        include_etf=False,
        exclude_st=True,
        exclude_suspended=True,
        delisting_policy="point_in_time_instruments_when_available",
        pit_evidence_policy="best_effort",
        evidence_requirements=(),
        legacy_universe="all_a_sh_sz",
        strict_research=False,
        research_eligible=False,
    ),
}

PROFILE_REGISTRY: Mapping[str, UniverseProfile] = MappingProxyType(_PROFILES)
OFFICIAL_PROFILE_NAMES = (
    "cn_a_sh_sz_research_v1",
    "cn_a_all_experimental_v1",
)
COMPATIBILITY_PROFILE_NAMES = ("legacy_all_a", "legacy_all_a_sh_sz")
DEFAULT_RESEARCH_PROFILE = OFFICIAL_PROFILE_NAMES[0]


def resolve_universe_profile(profile: str | UniverseProfile) -> UniverseProfile:
    """Resolve and validate a profile object or registry name."""

    if isinstance(profile, UniverseProfile):
        registered = _PROFILES.get(profile.name)
        if registered is None or registered != profile:
            raise UniverseProfileError(
                f"profile {profile.name!r} is not the immutable registered profile"
            )
        return profile
    name = str(profile).strip()
    if name not in _PROFILES:
        known = ", ".join(sorted(_PROFILES))
        raise UniverseProfileError(f"unknown universe profile {name!r}; known: {known}")
    return _PROFILES[name]


def profile_for_legacy_universe(universe: str) -> UniverseProfile:
    """Return the compatibility profile corresponding to an old universe name."""

    mapping = {"all_a": "legacy_all_a", "all_a_sh_sz": "legacy_all_a_sh_sz"}
    try:
        return resolve_universe_profile(mapping[universe])
    except KeyError as exc:
        raise UniverseProfileError(f"unsupported legacy universe {universe!r}") from exc


def list_universe_profiles(*, include_compatibility: bool = True) -> list[dict[str, Any]]:
    """List registry records in stable name order."""

    names = list(OFFICIAL_PROFILE_NAMES)
    if include_compatibility:
        names.extend(COMPATIBILITY_PROFILE_NAMES)
    return [_PROFILES[name].to_dict() for name in names]


def show_universe_profile(profile: str | UniverseProfile) -> dict[str, Any]:
    """Return one machine-readable profile record."""

    return resolve_universe_profile(profile).to_dict()


def profile_scope_hash(
    profile: str | UniverseProfile = DEFAULT_RESEARCH_PROFILE,
    symbols: Iterable[str] | None = None,
) -> str:
    """Return a stable profile hash, optionally bound to concrete symbols.

    Passing a symbol iterable as the first argument is accepted as a small
    convenience and binds it to the default research profile.
    """

    if not isinstance(profile, (str, UniverseProfile)):
        if symbols is not None:
            raise UniverseProfileError("symbols must be supplied only once")
        symbols = profile
        profile = DEFAULT_RESEARCH_PROFILE
    resolved = resolve_universe_profile(profile)
    return resolved.scope_hash if symbols is None else resolved.symbol_scope_hash(symbols)


def scope_hash(
    profile: str | UniverseProfile = DEFAULT_RESEARCH_PROFILE,
    symbols: Iterable[str] | None = None,
) -> str:
    """Short public alias for :func:`profile_scope_hash`."""

    return profile_scope_hash(profile, symbols)


def symbol_scope_hash(symbols: Iterable[str]) -> str:
    """Order-independent SHA-256 for a concrete symbol set alone."""

    return _hash_payload({"symbols": list(_normalise_symbols(symbols))})


def _profile_json(profile: str | UniverseProfile | None = None, *, indent: int = 2) -> str:
    """Serialize one profile (or the complete registry) as stable JSON."""

    payload: Any = list_universe_profiles() if profile is None else show_universe_profile(profile)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=indent)


#: Second names for the functions above, called by nothing; removed in 0.25.0.
#: ``cnequity.domain`` and ``cnequity.query`` forward them through here.
DEPRECATED_ALIASES: dict[str, tuple[Any, str]] = {
    "get_universe_profile": (resolve_universe_profile, "resolve_universe_profile"),
    "list_profiles": (list_universe_profiles, "list_universe_profiles"),
    "show_profile": (show_universe_profile, "show_universe_profile"),
    "profile_json": (
        _profile_json,
        "json.dumps(show_universe_profile(name)) or json.dumps(list_universe_profiles())",
    ),
}


def deprecated_alias(name: str) -> Any:
    """The function behind a deprecated alias, after warning its caller."""
    value, replacement = DEPRECATED_ALIASES[name]
    warnings.warn(
        f"{name} is deprecated and will be removed in 0.25.0; use {replacement}",
        DeprecationWarning,
        stacklevel=3,
    )
    return value


def __getattr__(name: str) -> Any:
    if name in DEPRECATED_ALIASES:
        return deprecated_alias(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "COMPATIBILITY_PROFILE_NAMES",
    "DEFAULT_RESEARCH_PROFILE",
    "OFFICIAL_PROFILE_NAMES",
    "PROFILE_REGISTRY",
    "UniverseProfile",
    "UniverseProfileError",
    "list_universe_profiles",
    "profile_for_legacy_universe",
    "profile_scope_hash",
    "resolve_universe_profile",
    "scope_hash",
    "show_universe_profile",
    "symbol_scope_hash",
]
