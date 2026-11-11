# Universe profiles

CNEquity exposes versioned universe contracts through the public
`cnequity.domain.universe_profiles` registry. A profile is a machine-readable
scope definition; it is not a live source adapter or a cached symbol list.
Persist all three values in a research artifact:

```text
name       = cn_a_sh_sz_research_v1
version    = 1
scope_hash = <full SHA-256 returned by the registry>
```

The official profiles are:

| Profile | Exchange/board scope | CDR/ETF | ST/suspension | Evidence posture |
| --- | --- | --- | --- | --- |
| `cn_a_sh_sz_research_v1` | SH/SZ main, STAR and ChiNext | excluded | excluded | strict: instruments, per-date `trading_status`, versioned historical ST and delisting receipts; PIT datasets require publication/observation evidence |
| `cn_a_all_experimental_v1` | SH/SZ plus BJ boards | excluded | excluded | strict when selected, but marked experimental and not research-approved until all-exchange evidence exists |

Both official profiles apply list/delist dates point-in-time. A missing
`instruments`, a missing per-symbol/per-date `trading_status` row, or a missing
complete historical ST receipt raises `UniverseCoverageError` for a strict
profile. This prevents a missing observation from being interpreted as a
normal, tradable row. PIT fundamentals remain subject to the Reader's explicit
`pit_mode` and `as_of` contract.

The old `universe="all_a"` argument remains available for compatibility and
keeps its permissive legacy semantics, but emits `DeprecationWarning` and will
be removed in 0.25.0; `profile="legacy_all_a"` returns the same rows without
the warning. It does not silently become either official profile. Use an explicit profile in new
research code:

```python
from cnequity.query import load

bars = load(
    "daily_bars",
    start="2020-01-01",
    end="2024-12-31",
    profile="cn_a_sh_sz_research_v1",
)
```

The registry and hashes are available without reading Parquet:

```python
from cnequity.query import list_universe_profiles, profile_scope_hash

profiles = list_universe_profiles(include_compatibility=False)
contract_hash = profile_scope_hash("cn_a_sh_sz_research_v1")
concrete_hash = profile_scope_hash(
    "cn_a_sh_sz_research_v1", ["600000.SH", "000001.SZ"]
)
```

Equivalent machine-readable CLI commands are `cne profile list` and
`cne profile show cn_a_sh_sz_research_v1` (add `--symbol` to bind a concrete
symbol set and print its `concrete_scope_hash`).
