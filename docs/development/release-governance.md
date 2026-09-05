# Release and data-contract governance

CNEquity ships two related public interfaces: the Python/CLI package and the
datasets stored in a user's lake. A release is ready only when both interfaces
have an explicit compatibility decision.

## Version policy

- Package versions follow Semantic Versioning. During `0.x`, a minor release
  may contain a planned breaking change; the changelog must identify it.
- Every dataset has an independent `schema_version`, contract fingerprint and
  compatibility policy. Package version alone is never a data-revision id.
- Additive nullable columns are compatible under the current `additive`
  policy. Removing a column, changing its type/unit/primary key, weakening PIT
  semantics, or changing history meaning requires a schema-version increase
  and a migration note.
- A deprecated Python/CLI spelling remains available for at least one minor
  release unless retaining it would make data incorrect or unsafe. Deprecation
  warnings must name the replacement and the planned removal boundary.

## Required pull-request evidence

Changes to a dataset or source must include all applicable items:

1. machine-readable contract diff and consumer-contract test;
2. offline adapter fixture or parser boundary test;
3. primary/backup source and blast-radius update;
4. terms/redistribution review in `sources/SOURCES.yml`;
5. migration, rollback and PIT-quality notes;
6. unit tests on Linux, Windows and macOS; formatting and lint checks;
7. dependency audit and CycloneDX SBOM artifact.

Unknown source permissions, unknown historical availability and unknown PIT
timestamps fail closed. A source being technically reachable is not evidence
that redistribution or commercial use is allowed.

## Package release gate

Before tagging:

- `cne contract validate` succeeds and the diff against the last release is
  reviewed;
- all offline tests and wheel smoke tests pass;
- snapshot create/verify/restore has been exercised into an empty target;
- the source legal-policy report, dependency audit and SBOM are attached to the
  release workflow run.

Release artifacts are built once in GitHub Actions and published with trusted
publishing. Do not rebuild a wheel locally for the same tag. Package publication
does not depend on the state of any particular user's lake.

## Production-readiness evidence

A package release and a production-readiness claim are deliberately separate.
Operators who claim that a deployed lake is production-ready should retain a
current report with the required consecutive trading days, a passing 30-day
source SLO, and no hidden core failures. `release-evidence/` and
`scripts/validate_release_evidence.py` provide a strict, optional format for
that claim; those reports are not required to tag or publish the package.

The clean CI runner must not run stability or source-SLO checks against an
empty fixture lake and describe them as production evidence. Unknown source
permissions also continue to prohibit unsupported redistribution or commercial
use, but do not prevent publication of CNEquity's Apache-2.0 source code.

## Incident ownership

The default CODEOWNER triages contract breaks, source incidents and security
reports. Source regressions use the structured source-regression issue form;
security vulnerabilities use the private process in `SECURITY.md`. A repeated
source-health failure produces a deterministic incident payload so reruns
update one incident rather than creating duplicates.
