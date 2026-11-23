# Install and upgrade

Use Python 3.11 or newer on macOS, Linux, or Windows. The package installs DuckDB 2.0 or newer as a dependency. Real collection requires access to the configured upstream sources. The [offline sample](quickstart.md#offline-sample) verifies installation and querying. On a machine with little memory or disk, read [Machines with little memory or disk](configuration.md#small-machines) first.

```bash
python -m pip install cnequity
cne --version
```

To develop the software, install from the [source repository](https://github.com/rootSunc/CNEquity) with `python -m pip install -e .`. Published packages include the dashboard bundle.

## Choose the upgrade instructions {#upgrades-and-compatibility}

Check the installed version with `cne --version`. Read the target version's [release notes](https://github.com/rootSunc/CNEquity/releases) and [dataset migrations](https://github.com/rootSunc/CNEquity/tree/main/contracts/migrations). The software, dataset schemas, and published data revisions are versioned independently.

| Target | Lake upgrade entry point |
|---|---|
| 0.24.0 | See the [0.24.0 release notes](https://github.com/rootSunc/CNEquity/releases/tag/v0.24.0). This release added no required routine data migrations. Use `cne migrate` to preview any migrations the lake still owes from older releases. |
| 0.25.0 | See [Upgrade to 0.25.0](upgrade-0.25.md) and use `cne upgrade`. An existing lake may refuse write commands until it is upgraded. New production lakes use the lake catalog from the start. The 0.25.x series is the last that upgrades lakes built by 0.24.0 or earlier. |
| Other versions | Use the matching release notes. When skipping releases, check each intervening version for compatibility changes. |

## Upgrade to 0.25.0 {#cne-upgrade}

0.25.0 changes how data is stored; an existing lake needs one run of `cne upgrade --apply`. The 0.25.x series is the last that upgrades lakes built by 0.24.0 or earlier. Preparation, steps, what the migrations do and what changes afterwards are in [Upgrade to 0.25.0](upgrade-0.25.md).
