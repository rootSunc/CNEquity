# Installation and upgrades

Most users can install from PyPI; clone the source only to modify code, use the repository's operations scripts or build the docs.

## System requirements

| Item | Requirement |
|---|---|
| Python | 3.10 or later; for versions covered by CI, see [Upgrades and feedback](#upgrades-and-compatibility) |
| OS | macOS, Linux, Windows; native x86-64 Windows paths and file locks are covered by CI, 32-bit and ARM64 Windows are not verified |
| Network | Real ingestion needs access to TDX and the relevant HTTP sources; after installation, sample can be used to verify offline |
| Disk | Grows with security scope, history depth, frequency, raw archive and retained versions; leave extra room for staging / snapshots |

## Install from PyPI

Running in a dedicated Python environment is recommended:

```bash
python -m pip install cnequity
cne --version
cne doctor
```

A single install includes all runtime Python dependencies; **no extras are needed**. Optional sources still need their config switch, applicable credentials or an external runtime; having the dependencies installed does not mean a source is reachable or that you have access to it.

Next, go straight to the [Quickstart](quickstart.md) and run `cne init`.

### Windows paths

Use `cne` under PowerShell / cmd as well. For example, to put the production lake on drive D:

```powershell
cne config create --data-root D:/cnequity
cne init
```

The generator handles TOML path escaping. PowerShell 5.1 does not support `&&`, so run the commands one line at a time.

## Install from source

The repository's main branch may be ahead of the stable PyPI release; to use features on main that are not yet released, install from source and record the commit.

```bash
git clone https://github.com/rootSunc/CNEquity.git
cd CNEquity
python -m venv .venv
```

Activate the environment (pick the one for your OS):

```bash
# macOS / Linux
source .venv/bin/activate
```

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

Then install:

```bash
python -m pip install --upgrade pip
pip install -e . --group dev
```

`--group` requires pip 25.1+. You can also use `uv sync` to install the repository's locked environment and then run commands with `uv run cne ...`.

The hexin-v token path for THS public pages also needs the Deno executable; without it, that path fails with an explicit error. It is not a prerequisite for the basic TDX demo; for details, see `adapters/ths/hexin.py`.

Node/npm is needed only to modify the dashboard source; the published package already ships the static assets, see the [frontend notes](https://github.com/rootSunc/CNEquity/blob/main/frontend/README.md). For the documentation tooling, additionally run `pip install -r docs/requirements.txt`.

## Generate the production config

`cne init` generates the default config automatically when it does not exist, so in most cases this separate step is unnecessary. If you want to change settings such as the data directory before initializing, run first:

```bash
cne config create
```

It writes the per-user `~/.config/cnequity/config.toml` by default (or `configs/cnequity.toml` when the current directory already has one) and converts `data.root` to an absolute path; the file holds only `extends = "builtin"` and the data directory, and every other setting comes from the built-in defaults (a conservative `workers=1` on macOS / Windows). You can also add `--data-root /abs/path/to/lake` when creating it for the first time.

Copying the template directly skips path resolution and platform adjustments, so prefer the generator command. A config that extends the defaults picks up new schedule steps on upgrade; for an older full config, add them with `cne config upgrade`, or convert it once with `cne config upgrade --minimize`. Personal configs, credentials and data directories should not be committed to the repository.

## Upgrade an existing installation

```bash
pip install --upgrade cnequity
cne config upgrade
cne migrate            # preview owed data migrations; then cne migrate --apply
```

A config that extends the built-in defaults (`extends = "builtin"`) needs no change, and this command reports nothing to do. For an older full config, `cne config upgrade` adds the schedule steps and schedule groups introduced by the new version, backs up the original file first, and validates after writing; `--minimize` instead rewrites it to extend the defaults. First read the target version's [changelog](../changelog.md) and [Upgrades and feedback](#upgrades-and-compatibility) to confirm whether a data migration is needed. Do not use `config create --force` in place of a config upgrade.

The old `tdx` / `macro` / `nlp` and other extras have been removed. Unit or schema migrations for past versions should be run according to the corresponding release notes; scripts meant for an old lake must not be reapplied to a new lake. The current `httpx` constraint is `>=0.25`; the actual installed version is resolved by the environment and cannot be inferred as a fixed version from the install command.

## Dependencies and diagnostics

| Dependency | Purpose |
|---|---|
| Polars / PyArrow / DuckDB | Data processing, Parquet, SQL |
| httpx / curl_cffi | HTTP source access |
| Baostock | Valuation, historical ST, delisted-bar backfill |
| pandas / openpyxl / xlrd | XLS/XLSX source parsing |
| NumPy / pypdf / SnowNLP | Options calculations, issuer announcement text, optional sentiment scoring |
| FastAPI / Uvicorn / Click | Dashboard server and CLI |

The TDX wire-protocol client is bundled with the package; there is no need to install the TDX desktop software. `pyproject.toml` is the authority for dependency constraints. `cne doctor` is an offline diagnostic; for real source connectivity, use a source-scoped probe, see [Source health](../operations/source-health.md).

Next: [Quickstart](quickstart.md) → [Initialization](initialization.md) → [Configuration reference](configuration.md).

## Upgrades and compatibility {#upgrades-and-compatibility}
Before upgrading, record `cne --version`, read the [changelog](../changelog.md), and back up your config and any data you need to keep long term. Installation methods are described above.

### Software and data versions

Software version, dataset schema version and data revision mean different things. A software upgrade does not mean historical data has been migrated; a data correction does not necessarily change the schema.

0.x minor versions may contain documented compatibility changes. Changes to fields, units, primary keys or historical semantics are governed by the migration notes for the corresponding version; do not keep concatenating data just because the column names are the same.

### Upgrade steps

1. Check the new version's scope, known limitations and migration requirements.
2. After updating the software, run `cne config upgrade`: it adds the schedule steps and schedule groups introduced by the new version to your local config, backs up the original file automatically, and leaves credentials, paths and other settings untouched. Add `--dry-run` to preview the changes first; use `cne config diff` for the full diff.
3. Run `cne migrate` to preview owed data migrations, then `cne migrate --apply`, and check the status and quality reports.
4. Verify the definitions of key data with your existing queries, then re-enable scheduled tasks.

Migration notes are in the repository's [contracts/migrations](https://github.com/rootSunc/CNEquity/tree/main/contracts/migrations); for common tools, see [Operations scripts](../operations/scripts.md). Rolling back the software does not necessarily roll back migrated data; when restoring, check config, software and data versions together.

### Storage cleanup

`cne run clean` only previews, covering staging, source snapshots, logs and historical versions; even without `--dry-run` it does not mark or delete anything. The existing scheduled scripts no longer free space. The actual-deletion fields and `bytes_freed` are empty or 0; candidate sizes are in `logical_bytes_selected`. `--force` only widens the preview scope; an explicit `--reconcile-runs` still modifies run state.

After upgrading, `cne backfill` and `cne run daily` keep the newest 5 generations per dataset ([`[storage.retention].keep_generations`](configuration.md#storage-retention)). Older generation directories are removed when nothing retains them, without the 7-day observation period or the web confirmation. To keep an older generation, `cne storage hold` it first, or set the value to `0`. `cne run clean` still only previews.

Physical deletion lives at `#/storage` in `cne serve`. The page first lists the historical versions that can be deleted, summarized by dataset. You can select historical versions whose observation period has not yet expired, as well as versions among the latest 5 generations that are not the current pointer. The current version, versions still referenced, manually retained versions, experiments still in use, and items missing a receipt or archive cannot be selected. The confirmation form requires checking that the action is irreversible and that external tasks have been stopped; then click permanent delete. Afterwards it lists the deleted versions, their logical size, and free disk space before and after deletion; logical size is not the space actually freed.

You can also first use “检查待标记项目” (Check items to mark) and, after confirming, start an observation period of at least 7 days. Marking does not free space; after 7 days it only produces an expiry notice and never deletes automatically. The CLI's `storage apply --phase purge` and experiment purge are disabled, and `--maintenance-window` cannot bypass the web confirmation. The operator must first stop external queries, other services and ingestion scheduling. The dashboard pauses its own reads but does not stop external processes for you. The page also lists staging runs. Terminal runs with recorded successful compaction and no incomplete batches can be selected for deletion. Confirmation rechecks holds, run locks, status and file fingerprints and processes only the reviewed list. Interrupted deletions can be reviewed again and resumed. Failed or incomplete batches, runs without compaction and orphan staging remain protected; age alone does not permit eviction. Source snapshots and logs remain report-only, with no web deletion.

After upgrading, restart serve to load the new routes and safeguards. For detailed constraints, see [version lifecycle commands](../reference/cli.md#cne-storage).

Archived original directories also have their own observation period. A full snapshot carries the retention basis and external dependency declarations, and rebinds references after restore; incremental packages involving lifecycle registration are rejected for now, so use a full snapshot instead. Neither archives nor snapshots automatically lift existing protections.

### Reporting issues

In [Issues](https://github.com/rootSunc/CNEquity/issues), provide the version, a redacted config excerpt, a minimal reproduction and the error message. Do not upload credentials, your personal lake, full run logs or private experiment notes. For security issues, see [SECURITY.md](https://github.com/rootSunc/CNEquity/blob/main/SECURITY.md).
