## 1. Adapter: category-bucketed pagination in announcements.py

- [x] 1.1 Add module constant `_CNINFO_CATEGORIES: tuple[str, ...]` with the 26 `category_*_szsh` codes (akshare `__get_category_dict` values, in stable order) in `announcements.py`, and verify it matches the akshare list via a quick assertion/test.
- [x] 1.2 Add optional `findings: list[dict] | None = None` param to `fetch_announcement_index`, changing the outer walk from `for column in ("szse","sse")` to `for bucket in _CNINFO_CATEGORIES` (payload `category=bucket`, `column` left at `szse`), and verify existing transport/symbol/dedupe/date tests still pass.
- [x] 1.3 Convert the repeated-page guardian in `fetch_announcement_index` from `raise` to "stop this bucket + append `cninfo_truncation_at_100_pages` finding" (bucket + page fields), and verify a truncating fake client yields partial rows plus a finding instead of raising.
- [x] 1.4 Preserve all inner pagination semantics unchanged in `fetch_announcement_index` (empty-page-before-totalpages raise, `totalpages` authoritative stop, None+no-metadata full-page continue) and verify the existing parametrized malformed-metadata / overrun / stale-hasmore tests pass. **Deviation from plan:** the old `totalpages=0`-with-rows raise was removed — measured live, CNINFO legitimately reports `totalpages=0` with rows for small category buckets (e.g. 年报 totalAnnouncement=2), so this is a small bucket, not a corrupted source; a dedicated `test_fetch_announcement_index_accepts_totalpages_zero_with_rows` locks the new behavior.

## 2. Adapter: regulatory.py shares the same contract

- [x] 2.1 Add optional `findings` param to `fetch_regulatory_events`, switch its outer walk to the same `_CNINFO_CATEGORIES` buckets, and verify regulatory filter/identity/dedupe tests still pass.
- [x] 2.2 Apply the same repeated-page → stop-bucket + `cninfo_truncation_at_100_pages` finding behavior in `fetch_regulatory_events`, and verify a truncating fake client for regulatory returns partial rows plus a finding.

## 3. Step wiring: surface truncation findings

- [x] 3.1 In `step_announcement_index` (events.py:591-616), collect adapter findings via a closure passed to `fetch_announcement_index` and merge them into `context_updates.audit_findings` with `status="warning"` when non-empty, and verify a truncated fetch propagates the finding into the step result.
- [x] 3.2 Do the same for `step_regulatory_events` (macro_risk.py:357-383, which calls `fetch_regulatory_events`), merging the `cninfo_truncation_at_100_pages` findings into its audit findings.

## 4. Unit tests

- [x] 4.1 Update `_FakeClient` in `tests/unit/test_cninfo_announcements.py` to key pages by `category` instead of `column` and gate request counts by bucket count (26 "szse" buckets + adjacency), keeping the existing call-count assertions meaningful, and verify the whole module passes.
- [x] 4.2 Add tests: cross-bucket dedupe (`announcement_id` keep-last across two buckets), empty bucket skipped, repeated page produces partial rows + `cninfo_truncation_at_100_pages` finding (replacing `test_fetch_announcement_index_rejects_a_repeated_page`), transport failure still raises, `totalpages=0`-with-rows accepted, and regulatory truncation finding; verify each new test passes.
- [x] 4.3 Add a test asserting `_CNINFO_CATEGORIES` has 26 entries and each is a `category_*_szsh` code, and verify it passes.

## 5. Verification

- [x] 5.1 Re-run the previously failing run `cne retry --run-id ad1e00b4-9aed-4331-bea6-b7c7b50de9d1` and confirm announcement_index completes past page 100 (8-28 no longer aborts), writing rows instead of 0, and that a live replay of `fetch_announcement_index(2026-08-28)` returns on the order of thousands of rows within the 100-page cap.
- [x] 5.2 Run `ruff` and `pytest tests/unit` from the CNEquity repo root and confirm all green.
- [x] 5.3 Update CHANGELOG with the category-bucketed pagination + truncation tolerance change.