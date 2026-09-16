# ADR 0010: Committed generations are retained by count, not forever

- Status: Accepted
- Date: 2026-09-13
- Relates to: [ADR-0002](0002-parquet-lake-over-database.md), [ADR-0003](0003-canonical-curated-with-source-snapshots.md)

## Context

`RevisionStore` gives the lake a real commit: `commit()` copies the whole
mutable dataset into an immutable generation under `meta/revisions/data/<dataset>/<revision_id>/`,
writes the receipt, then publishes `current.json` as a single inode. Readers
resolve the pointer once and stay on one generation for a whole lazy plan, and
`load(revision=…)` can pin an older one. That is genuine snapshot isolation and
time travel, and it is more than most "Parquet in folders" projects have.

It had no retention policy, and the copy is O(dataset), not O(change).
`_copy_generation` walks every file and `shutil.copy2`s it; `changed_files`
decides only *whether* to commit, never *what* to copy. Nothing ever deleted a
generation — there was no `gc`, no retention setting, and no CLI to list or
drop one.

Measured on a two-year lake:

| | |
|---|---|
| `curated/` | 14 GB |
| `meta/revisions/data/` | **16 GB** — larger than the data |
| generations retained | 307 |
| `adj_factors` alone | 46 generations, 9.5 GB |

The store grows by the dataset's full size every time a dataset changes, for
ever. `adj_factors` recomputes broadly, so it commits often and dominates.

This had already caused one incident. `scripts/backup_meta.sh` tarred
`meta/revisions` into a daily archive kept for 14 days, turning ~1 GB/day of
generation growth into ~14 GB/day of rotating tarballs; the backup directory
reached 82 GB and a single archive took 2h19m to write. That script now
excludes `revisions/data`, but the underlying growth remained unbounded.

## Decision

Retain a bounded number of generations per dataset, and prune the rest:

- `cne run clean --keep-revision-generations N` (default 5) drops the stored bytes
  of all but the newest N generations per dataset.
- **Receipts are never pruned.** They are a few KB and carry the lineage — run
  id, code version, config fingerprint, and the per-file sha256 of the
  generation. Pruning removes the ability to *read* an old revision, not the
  record that it existed or the evidence of what it contained.
- The generation `current.json` resolves to is never pruned, whatever N says.
- The pre-revision baseline (`legacy`) is ordered as generation zero so `N`
  covers it like any other, instead of being dropped unconditionally for
  having no receipt.

Pinning a pruned revision already had defined behaviour: `current_root` raises
`revision … is not retained for <dataset>`. Retention makes that path normal
rather than theoretical.

## Consequences

**Positive.** Growth is bounded by a number the operator sets. On the measured
lake, `--keep-revision-generations 5` reclaims 11.2 GB of 16 GB while leaving
every dataset readable and every receipt intact.

**Negative, and the real cost.** Time travel becomes shallow. `load(revision=…)`
for anything older than N stops working, and the default of 5 is a guess, not a
measurement — nobody has data on how far back a pinned read is actually used.
An operator who pins revisions in published research must raise N or stop
pruning (`--keep-revision-generations 0`), and nothing currently warns them
that a revision they cite is about to age out.

**Neutral.** The commit path is unchanged: a commit still copies the whole
dataset. This bounds the store; it does not make writing cheaper.

## Alternatives considered

**Hard-link unchanged files into the generation.** The obvious fix: a
generation would cost the size of what changed rather than the whole dataset,
because every writer under `curated/` replaces its target inode
(`write_parquet_atomic` writes a temp file and `os.replace`s it) so a link is
indistinguishable from a copy. Implemented and reverted, for two reasons found
by trying it.

First, it makes correctness depend on an invariant nothing enforces — *no
code ever writes a curated file in place*. One `df.write_parquet(path)` on an
existing path truncates the shared inode and silently rewrites history in
every generation linked to it. This is not hypothetical: `_append_diff` in
`steps/bars.py` did exactly that until it was fixed, and the existing COW test
helper does it in three lines, which is how the regression surfaced — a read
pinned to revision 1 returned revision 2's value.

Enforcing the invariant means making the shared inode read-only (`0444`), so an
in-place write fails loudly while `os.replace` still succeeds. That is the
standard hard-linked-snapshot design, but it puts the lake's integrity on
POSIX permission semantics, and Windows is a supported platform where
`os.replace` over a read-only destination is exactly the `PermissionError`
`storage/atomic.py` already retries around.

Second, it collides with snapshot materialization. `SnapshotStore` rejects
`st_nlink > 1` on files it archives, because two manifest entries sharing one
inode would alias after extraction. Hard-linked generations make `nlink > 1`
the normal state of every curated file, so that check would have to be
narrowed to the materialized artifact — correct in principle (snapshot
creation always `copy2`s, so a source link cannot alias anything), but it
trades a blunt, obviously-safe check for a subtle one in the code that
produces portable backups.

Retention gets most of the benefit (11.2 GB of 16 GB) for none of that.
Hard-linking remains available later, behind the read-only-inode enforcement
and a narrowed snapshot check, if bounded retention proves insufficient.

**Adopt a table format (Iceberg / Delta / DuckLake).** These solve generation
management as a built-in, with expiry and compaction. Rejected: the capability
gap they close is multi-writer coordination and catalog-level evolution, which
a single-host, single-user lake does not have, and `RevisionStore` already
provides atomic commit, reader isolation and time travel. The migration cost is
the whole storage layer; the problem here was a missing `rm`.

**Retain by age rather than count.** Simpler to reason about in a runbook
("30 days"), but the datasets commit at wildly different rates — `adj_factors`
46 generations against `daily_bars` 4 over the same period — so an age policy
keeps 46 generations of the thing that is large and frequent, and none of the
thing that is rarely touched. Count is the dimension that actually bounds the
store. Age remains a reasonable addition later, as a floor rather than a
replacement.
