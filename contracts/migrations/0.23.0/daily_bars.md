# daily_bars month partitions

## Change

Schema version 2 keeps the same columns and primary key. `partition_granularity`
changes from `day` to `month`. Directories are named `trade_date=YYYY-MM`
instead of `trade_date=YYYY-MM-DD`. A month directory may hold more than one
Parquet file. Row values are unchanged.

## Migration

Run `cne migrate` to preview, then `cne migrate --apply`. The migration
`daily-bars-month-partitions` rewrites history one month at a time and publishes
one new revision. Month files written before the migration sit beside the old
day directories until it finishes. Reads still work. The weekly audit reports
`mixed_partition_granularity` while both layouts are present.

Programs that read the Parquet files directly must not infer Hive partitions
for this dataset: a `YYYY-MM` directory is not a date. Filter on the
`trade_date` column inside the files.

## Rollback

The migration keeps the previous revision. Software before 0.23 looks for day
directories and will not see rows that exist only under month directories.
Install the previous release and read the revision it published. Do not delete
the new revision if anything still needs it.
