# Design principles

## Data quality first

Make sources, units, date semantics, and coverage boundaries explicit. Unknown and missing data should be shown explicitly; empty tables, default values, or the current state must not pass for verified historical facts.

## Users own their data

Data is stored locally in open formats and can be read with common analysis tools. Collection scope, optional sources, and scheduling are configured by the user, and the project does not promise that upstream sources will be available forever.

## Research semantics can be inspected

Raw prices and adjustment factors are stored separately. Historical universes, PIT, and price adjustment each have their own evidence requirements. Examples and offline samples are for checking usage; they do not mean research coverage has passed.

## Corrections are traceable

Data corrections are published as new versions. Long-term reproduction requires saving data dependencies, query parameters, and the software version; saving only the maximum date or the watermark is not enough to pin research inputs.

## Usable and recoverable

Prioritize clear entry points for installation, running, diagnosis, and recovery. Compatibility changes are communicated to users through the changelog and migration notes.

For the shared Error / Warning / Caveat boundaries and Quality Tiers (`A`/`B`/`C`) across CNEquity and USEquity, see the [Shared Data Quality Contract](data-quality-contract.md). For current features and limitations, see [Product direction](overview.md), the [data catalog](../datasets/catalog.md), and the [upgrade notes](../getting-started/installation.md#upgrades-and-compatibility).
