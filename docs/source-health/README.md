# Operator-supplied vantages

Drop probe reports here and the published page grows a column for each.

```bash
asl sources probe --vantage cn --out docs/source-health/cn.json
```

The scheduled workflow (`.github/workflows/source-health.yml`) runs on GitHub's
hosted runners, which are outside the mainland. It therefore labels its own
report `overseas` and copies whatever it finds here beside it, unmerged —
several of these sources refuse non-mainland egress, so publishing the runner's
view as *the* status would report an outage that mainland users do not have.

A file here is a snapshot, not a feed: it shows the moment it was taken, so
refresh it on the same cadence you run the daily pipeline. Nothing breaks if it
goes stale or is absent — the page just shows one column, with the timestamp it
actually has.

Filename becomes nothing; the `vantage` field inside the JSON is what labels the
column. Keep one file per vantage.

See [source health](../operations/source-health.md).
