from cnequity.domain.pit import PitMode, PitQuality
from cnequity.domain.universe_profiles import (
    UniverseProfile,
    UniverseProfileError,
    list_universe_profiles,
    profile_scope_hash,
    resolve_universe_profile,
    scope_hash,
    show_universe_profile,
)
from cnequity.query.profile import ProfileError, load_profile
from cnequity.query.reader import (
    dataset_schema,
    list_datasets,
    load,
    resolve_config,
    scan,
)
from cnequity.query.receipt import ReadReceiptError, ReadResult, load_with_receipt
from cnequity.query.resample import resample_minute_history, resample_trade_bars
from cnequity.query.state import (
    DatasetState,
    RevisionChange,
    dataset_attempt,
    dataset_revisions,
    dataset_state,
)

__all__ = [
    "DatasetState",
    "dataset_schema",
    "dataset_state",
    "dataset_revisions",
    "RevisionChange",
    "dataset_attempt",
    "list_datasets",
    "load",
    "load_profile",
    "load_with_receipt",
    "ProfileError",
    "ReadReceiptError",
    "ReadResult",
    "resolve_config",
    "resample_minute_history",
    "resample_trade_bars",
    "scan",
    "PitMode",
    "PitQuality",
    "UniverseProfile",
    "UniverseProfileError",
    "list_universe_profiles",
    "profile_scope_hash",
    "resolve_universe_profile",
    "scope_hash",
    "show_universe_profile",
]


def __getattr__(name: str):
    # Deprecated second names of the profile functions, removed in 0.25.0.
    from cnequity.domain import universe_profiles

    if name in universe_profiles.DEPRECATED_ALIASES:
        return universe_profiles.deprecated_alias(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
