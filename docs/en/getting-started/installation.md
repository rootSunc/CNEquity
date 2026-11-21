# Install and upgrade

Use Python 3.11 or newer on macOS, Linux, or Windows. The package installs DuckDB 2.0 or newer as a dependency. Real collection requires access to the configured upstream sources. The [offline sample](quickstart.md#offline-sample) verifies installation and querying.

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
| 0.25.0 | Use `cne upgrade` as described below. An existing lake may refuse write commands until it is upgraded. New production lakes use the lake catalog from the start. |
| Other versions | Use the matching release notes. When skipping releases, check each intervening version for compatibility changes. |

## Upgrade to 0.25.0 {#cne-upgrade}

1. Stop old schedulers, writers, and readers using this lake. Keep independently restorable copies of the data and config from before the upgrade. Move tools that scan `curated/` or `derived/` to a [query or export interface](../datasets/query-guide.md#parquet) for the new version.
2. Install the target version. For an older, fully expanded config, review `cne config upgrade --dry-run`, then run `cne config upgrade` if needed. Configs with `extends = "builtin"` read the defaults from the installed software.
3. Run `cne upgrade` to preview the migrations and check disk space and extension requirements. The preview can download a missing DuckDB extension.
4. After reviewing the plan and backup, run `cne upgrade --apply`. If interrupted, resolve the reported problem and rerun the same command.
5. Run `cne status --datasets` and `cne check`, verify key results with your existing queries, then resume scheduling and services.

!!! warning "Old layout cleanup"
    The 0.25.0 upgrade removes old layout files superseded by the lake catalog. The default upgrade backup contains lake records. Downgrading the software or restoring metadata cannot reconstruct removed data files. Use the pre-upgrade data copy to roll back.

To preview a single migration, use `cne migrate --only <migration ID>`. Adding `--apply` runs the selected migration even if it has already been recorded.
