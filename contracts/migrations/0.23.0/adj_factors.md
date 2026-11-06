# adj_factors month partitions

## Change

Schema version 2 keeps the same columns and primary key. `partition_granularity`
changes from `day` to `month`. Files live under `derived/adj_factors/` in
directories named `trade_date=YYYY-MM` instead of `trade_date=YYYY-MM-DD`.
Row values are unchanged.

## Migration

Run `cne migrate` to preview, then `cne migrate --apply`. The migration
`adj-factors-month-partitions` rewrites history one month at a time and
publishes one new revision. While month files and old day directories both
exist, reads still work and the weekly audit reports
`mixed_partition_granularity`.

Programs that read the Parquet files directly must turn Hive partitioning off
and filter on the `trade_date` column. A `YYYY-MM` directory name is not a date.

## Rollback

The migration keeps the previous revision. Software before 0.23 looks for day
directories and will not see rows that exist only under month directories.
Install the previous release and read the revision it published. Do not delete
the new revision if anything still needs it.
