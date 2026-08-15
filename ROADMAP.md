# Roadmap

cn-market-lake is a data layer, not a trading strategy or a hosted quote service. The roadmap is
ordered around one promise: make reproducible A-share history easy to build, inspect, and consume.

## Now · 0.6

- Make the first-run path prove the research value, not only that a quote request works. The optional
  `cml demo --research` path now derives a real hfq series and prints a raw-vs-adjusted return.
- Keep the README, PyPI description, CLI help, and getting-started docs generated from the same
  defaults and history semantics.
- Publish source-health and coverage evidence that can be checked without trusting a marketing claim.
- Add small, runnable recipes for survivorship-safe universes, point-in-time fundamentals, and
  adjustment handling. Adjustment and PIT recipes are now published under the docs site.
- Keep the docs site buildable in pull requests and deploy the searchable site from `main`.
- Keep source availability and releases observable: weekly health artifacts, tag/version checks,
  and a PyPI Trusted Publishing path are now checked into `.github/workflows/`.

## Next · 0.7

- Provide stable integration recipes for DuckDB, Polars, and common quant research workflows. The
  first DuckDB / Polars / MCP recipe is now available; expand it with tested downstream examples.
- Publish the read-only MCP server metadata through the official MCP Registry once the package
  metadata and installation contract are ready.
- Improve contributor onboarding with focused good-first issues and a small set of source-adapter
  contracts that can be tested offline.

## Later · 1.0 criteria

1. The canonical schemas and provenance columns have a documented compatibility policy.
2. A fresh install can complete the demo and diagnose source/network failures clearly.
3. Daily runs are resumable, auditable, and safe to retry after a partial source failure.
4. The supported Python and operating-system matrix is tested in CI.
5. Legal and source-retention limits are documented for every published dataset.

## Explicit non-goals

- A backtesting engine, portfolio optimizer, or trading-signal marketplace.
- Redistributing upstream market data from this repository.
- Hiding source limits behind synthetic rows or silent fallbacks.

Feature requests and source additions are welcome when they preserve the data-layer boundary. Before
opening an issue, see [CONTRIBUTING.md](CONTRIBUTING.md), the [dataset catalog](docs/datasets/catalog.md),
and the [legal notes](docs/legal-and-data-sources.md).
