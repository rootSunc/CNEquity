# CNEquity

A local China-market data lake for quantitative research. Collect prices, financial statements, corporate events, and flows according to your configuration, then publish them to local Parquet. Inspect coverage and research semantics, and resume unfinished collection.

```text
Initialize / update / backfill → validate and stage → publish usable data → Python / SQL / MCP
                                        └─ record gaps for checks and retries
```

- [Quickstart](getting-started/quickstart.md) · Query the offline sample, then choose a live collection scope.
- [Design](architecture/overview.md) · How the catalog, publishing, coverage planning, quality gates, and reproducibility fit together.
- [Data and coverage](datasets/catalog.md) · Find datasets, sources, and historical limits.
- [Query and research](datasets/query-guide.md) · Read prices, adjustments, historical universes, and PIT financials.
- Reference · [Python API](reference/python-api.md), [MCP](reference/mcp.md), [configuration](getting-started/configuration.md), [command inventory](reference/cli-surface.md).
- [Daily updates](operations/runbook.md) · Update, verify, backfill, and snapshot; [troubleshooting](operations/troubleshooting.md) by symptom.
- [Install and upgrade](getting-started/installation.md) · Review migrations and rollback for the target version.
- [License, sources, and citation](legal-and-data-sources.md) · Check upstream data-use boundaries.

See [GitHub Releases](https://github.com/rootSunc/CNEquity/releases) for release history. Run `cne --version` to identify your installed version.
