## Purpose

Defines the pagination contract for CNINFO announcement index and regulatory event fetching, protecting against the server's hard 100-page cap by bucketing on announcement category and weakening a repeated page into an audited truncation instead of a wholesale failure.

## ADDED Requirements

### Requirement: Announcement fetch is paginated by announcement category

The system SHALL fetch a day's CNINFO announcements by walking the announcement-category buckets (`category_*_szsh` codes) rather than a single unfiltered/column walk. Within each bucket, pages are walked by the server-reported `totalpages`, and rows across all buckets SHALL be merged, deduplicated by `announcement_id` keeping the last occurrence.

- Every category bucket reports fewer than the server's max fetchable pages (`totalpages <= 100` on normal days); a bucket that reports more SHALL be treated as server-side truncation per the truncation requirement below.
- The merged result MUST contain each distinct `announcement_id` at most once, and empty buckets MUST NOT abort the fetch.

#### Scenario: Multi-bucket merge
- **WHEN** a day's announcements span multiple category buckets and one announcement id appears in more than one bucket
- **THEN** the merged frame contains that announcement id exactly once (last-seen wins)

#### Scenario: Empty bucket
- **WHEN** a category bucket returns zero rows for the requested day
- **THEN** the bucket contributes nothing and the overall fetch still succeeds with the non-empty buckets' rows

### Requirement: Server-side 100-page truncation is tolerated and audited

The system SHALL recognize the CNINFO server behavior where a requested page past the first 100 pages re-serves an earlier page (identical page signature). When a repeated page signature is detected within a category bucket, the system SHALL stop collecting that bucket instead of raising a hard failure.

- The stopped bucket's partial rows SHALL be emitted; the truncation SHALL be recorded as an `audit_finding` (check name `cninfo_truncation_at_100_pages`) with the bucket name and the page where truncation occurred.
- A genuine transport/HTTP failure (non-repeated page) MUST still raise — retries exhausted after `post_with_retry` remain a hard error.
- The repeated-page stop condition applies per bucket, so one truncated bucket degrades only that bucket, not the whole day.

#### Scenario: Repeated page past page 100
- **WHEN** a category bucket returns a page whose signature matches a previously seen page within that bucket
- **THEN** the fetch stops that bucket, still returns the rows collected so far, and the run records a truncation audit finding instead of failing the step

#### Scenario: Transport failure still raises
- **WHEN** `post_with_retry` exhausts its retries on a non-repeated page (e.g. CNINFO 504s or connection drop)
- **THEN** the fetch raises a pagination failure for that bucket (loud failure preserved)

### Requirement: Regulatory event fetch shares the pagination contract

The CNINFO regulatory-events fetch SHALL apply the same category-bucketed pagination and same repeated-page tolerance as the announcement fetch, merging by `event_id` and reporting any bucket truncation via the same `cninfo_truncation_at_100_pages` check.

#### Scenario: Regulatory bucket truncation
- **WHEN** a regulatory category bucket returns a repeated page
- **THEN** the regulatory fetch stops that bucket, keeps earlier rows, and records the truncation in audit findings