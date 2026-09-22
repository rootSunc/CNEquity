## Problem: bootstrap `cne init` deadlocks on first-build lakes with dead funds

On a first-build lake (empty `curated/`), a full-universe `cne init` fails at
`phase2c_daily_bars_backfill` and every `--resume` repeats the failure:

```
daily_bars 2023-09-17..2026-09-17: 94112 interior symbol×session key(s)
remain absent; refusing to checkpoint
```

Quantified composition of the 94,112 missing keys (real run, quick profile,
2026-09-17/18):

- **~65,300** — ~90 delisted/liquidated ETFs and LOFs that neither TDX nor
  EastMoney can serve. Their instruments rows carry `list_date=None`, so at
  bootstrap they route to `generic` (the `_etf_placeholder_bar_universe`
  guard needs positive-volume curated evidence, which a first build does not
  have), and each still stages exactly one zero-volume pre-open placeholder
  on the tip day — so the gate sees them as "in-window" and counts ~726
  sessions each as interior gaps.
- **~28,600** — ~300 funds whose source history starts mid-window (source
  retention cut). Real tail rows exist; the unreachable prefix keys remain.

The designed two-source escape hatch cannot fire for either class, because
`_gapfill_multiday_via_kline` requires *no staged rows at all* and *every
session missing*:

- the dead funds' own tip placeholder disqualifies them (while
  `load_bar_universe` documents that a zero-volume placeholder "is not
  evidence that the symbol ever traded" — the two rules disagree), and
- truncated funds fail the all-sessions-missing rule.

Since the negative-evidence write point sits behind the gate's raise, no
evidence is ever recorded and `--resume` loops forever. Workaround used
before this fix: seed `source_empty` negative evidence out-of-band, with an
EastMoney probe per candidate to keep stock keys (which *are* recoverable)
out of the certification.

## Fix

Three changes, preserving the two-independent-sources-agreement principle:

1. **Placeholders no longer disqualify certification.** The rule becomes "no
   positive-volume staged row in the window", matching `load_bar_universe`'s
   documented placeholder semantics.
2. **Segment-level certification for partially-staged symbols**
   (`_certify_missing_segments`). For each run of missing sessions bounded by
   the symbol's real staged rows: stage whatever EastMoney kline and Sina
   return for missing keys, and only when both agree the segment is empty,
   certify those keys and persist bounded negative evidence per segment
   (`daily_bars_segment_no_data` finding; `expected_no_data_keys` added to
   the gapfill result).
3. **The interior-gap gate consults live negative evidence**
   (`_staged_daily_bar_missing_keys`), symmetric with its existing
   trading-status exclusion. A key certified earlier in the same invocation
   no longer raises the gate after the fact.

## Tests

Three new unit tests in `tests/unit/test_daily_bars_clist_gapfill.py`
(written failing first):

- `test_placeholder_only_symbol_certified_after_two_source_agreement`
- `test_truncated_symbol_missing_prefix_certified_segment_level`
- `test_missing_key_gate_skips_negative_evidence_keys`

Full unit suite: 2888 passed. (7 pre-existing symlink tests fail on a
Windows runner without symlink privilege — `WinError 1314` — unrelated to
this change.)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
