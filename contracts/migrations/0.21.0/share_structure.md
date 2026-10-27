# A-share float in share_structure

## Change

Schema version 2 adds one nullable column, `float_a_shares` (流通A股): listed,
tradable A shares only. Rows written this way carry `data_version=v2`. The
primary key is unchanged.

`float_shares` is EastMoney's unrestricted share count, which for an A+H company
includes the H shares: 600188.SH reports 7.66 bn there against 5.96 bn listed A
shares. An A-share float market cap or turnover needs the A-only count.

TDX share-change events, read by the weekly corporate-actions sweep, become the
dataset's peer (`share_structure_peer`) and its backup while EastMoney is stale.
A backup row has no disclosure date from its source, so its `announce_date` is
the date of the sweep that observed it, and its `float_shares` is filled only
where that count is known to equal the A-share float.

## Migration

None required. Rows already in the lake read `float_a_shares` as null. To fill
it for a window, re-fetch it from EastMoney:
`cne backfill share_structure --start YYYY-MM-DD --end YYYY-MM-DD`.
