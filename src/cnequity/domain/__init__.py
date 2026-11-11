from cnequity.domain.contracts import (
    build_contract,
    contract_fingerprint,
    data_contract_fingerprint,
    dataset_contract,
    diff_contracts,
    export_contract,
    validate_contract,
)
from cnequity.domain.pit import (
    PIT_MODES,
    PIT_QUALITIES,
    PIT_STORAGE_COLUMNS,
    PIT_STORAGE_DTYPES,
    PitMode,
    PitQuality,
)
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS
from cnequity.domain.symbols import (
    format_symbol,
    is_all_a_symbol,
    is_cdr_symbol,
    is_etf_symbol,
    parse_symbol,
)
from cnequity.domain.universe_profiles import (
    COMPATIBILITY_PROFILE_NAMES,
    DEFAULT_RESEARCH_PROFILE,
    OFFICIAL_PROFILE_NAMES,
    PROFILE_REGISTRY,
    UniverseProfile,
    UniverseProfileError,
    list_universe_profiles,
    profile_for_legacy_universe,
    profile_scope_hash,
    resolve_universe_profile,
    scope_hash,
    show_universe_profile,
    symbol_scope_hash,
)

__all__ = [
    "DATASET_SCHEMAS",
    "PRIMARY_KEYS",
    "PIT_STORAGE_COLUMNS",
    "PIT_STORAGE_DTYPES",
    "PIT_MODES",
    "PIT_QUALITIES",
    "PitMode",
    "PitQuality",
    "build_contract",
    "contract_fingerprint",
    "data_contract_fingerprint",
    "dataset_contract",
    "diff_contracts",
    "export_contract",
    "format_symbol",
    "is_all_a_symbol",
    "is_cdr_symbol",
    "is_etf_symbol",
    "parse_symbol",
    "validate_contract",
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


def __getattr__(name: str):
    # Deprecated second names of the profile functions, removed in 0.25.0.
    from cnequity.domain import universe_profiles

    if name in universe_profiles.DEPRECATED_ALIASES:
        return universe_profiles.deprecated_alias(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
