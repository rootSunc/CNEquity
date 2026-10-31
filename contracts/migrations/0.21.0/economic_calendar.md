# Economic calendar history and Jin10 columns

## Change

Schema version 2 records two contract changes that are not a row rewrite.

The dataset is no longer tip-only. EastMoney's calendar report is retired, and
Jin10 can answer any day back to 2008, so `history_mode` is
`snapshot_with_backfill` and `pit_quality` is `not_applicable` rather than
`snapshot_only`. Two nullable columns arrive with the same version:
`indicator_code` (Jin10's series id) and `withdrawn` (true once Jin10 drops a
rescheduled event's old id). The primary key stays `event_id`.

## Migration

No revision rewrite. Rows already in the lake keep their values; the new
columns read as null until that day is fetched again. To fill missing days
from 2008, run `cne backfill economic_calendar --start YYYY-MM-DD --end
YYYY-MM-DD`. A backfill writes only days the lake does not already have.

## Rollback

Software before 0.21 does not know `indicator_code` or `withdrawn` and treats
the calendar as a snapshot that cannot be backfilled. Leave the published
revision in place; do not delete rows to go back. A reader on the old software
should select the columns it knows and ignore the two new ones.
