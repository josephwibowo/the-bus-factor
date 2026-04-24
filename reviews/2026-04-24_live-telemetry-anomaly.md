---
title: Live Telemetry Anomaly Notes
date: 2026-04-24
author: Codex
review_target: "live BigQuery frontend source-health and leaderboard output"
status: investigation-needed
---

# Live Telemetry Anomaly Notes

## Trigger

After an end-to-end BigQuery run, the frontend showed:

- `github_commits`: `PARTIAL`, `success_ratio=1.000`, `attempted=152`, `succeeded=152`, `exceptions=152`
- `github_issues`: `PARTIAL`, `success_ratio=1.000`, `attempted=152`, `succeeded=152`, `exceptions=152`
- `github_contributors`: `PARTIAL`, `success_ratio=0.000`, `attempted=152`, `succeeded=0`, `exceptions=152`
- Coverage summary around `npm tracked=100 eligible=95 flagged=0` and `pypi tracked=100 eligible=94 flagged=0`
- Leaderboard rows with plausible npm packages but `confidence=low`

## Assessment

The package-level findings shown in the screenshot look plausible for the sampled npm packages. I verified several current npm publish timelines:

- `tslib` latest publish is `2.8.1` on `2024-10-31`, so `0 releases in the last 365 days vs 4 in the prior 365 days` for snapshot `2026-04-20` is plausible.
- `readable-stream` latest publish is `4.7.0` on `2025-01-07`, so `0 releases in the last 365 days vs 2 in the prior 365 days` is plausible.
- `inherits`, `safe-buffer`, and `graceful-fs` latest publish dates also align with the large day-count findings.

I do not see evidence that this telemetry issue is inflating fragility scores. The scoring path is conservative:

- `int.fragility_inputs` maps missing GitHub commit/contributor/issue values to `0` contribution, not high fragility.
- `mart.package_scores` rolls source health into confidence in live mode.
- `flagged` requires `confidence IN ('medium', 'high')`, so low-confidence rows cannot be flagged.

The likely impact is underconfidence and suppressed flags, not false-positive flags.

## Likely Root Cause

The source-health note is internally misleading because the raw GitHub assets emit placeholder rows when per-repo fetches fail.

In `pipeline/assets/raw/raw_github_commits.py` and `pipeline/assets/raw/raw_github_issues.py`:

- `_ingest()` catches per-repo exceptions.
- On exception, it appends an empty placeholder row for that repo.
- `_live()` passes `succeeded=len(rows)` into `live.mark_degraded_if_low_success()`.

That means `succeeded` can equal `attempted` even when every worker raised an exception. The note can therefore say:

```text
success_ratio=1.000, attempted=152, succeeded=152, exceptions=152
```

For contributors, the asset uses usable signal count instead:

```python
usable_signals = _usable_contributor_signal_count(rows)
```

So the same all-exception failure mode reports `success_ratio=0.000`, which is more honest.

## Why The Frontend Looks Odd

The source-health table is showing run telemetry, while the leaderboard is showing scores after missing-data safeguards. So these can be true at the same time:

- GitHub side-source telemetry is degraded.
- Registry/release-derived findings still look plausible.
- Confidence is low.
- `flagged=0`.

This is not a contradiction, but the telemetry note makes it harder to diagnose because "success" is counting emitted rows, not usable fetches.

## Recommended Fix

Track three separate concepts in raw GitHub ingestion:

- `attempted`: number of repo URLs requested.
- `emitted_rows`: number of rows returned to the raw table, including placeholders.
- `succeeded`: number of repo fetches with usable, non-placeholder source data.

Then call `mark_degraded_if_low_success()` with the usable success count, not `len(rows)`.

Suggested asset-specific definitions:

- `github_commits`: success if the fetch returned without exception and produced a real row; optionally require at least one of `last_commit_date`, `commits_last_365d > 0`, or `top_contributor_share_365d IS NOT NULL`.
- `github_issues`: success if the fetch returned without exception; do not require issues to exist because zero eligible issues is a valid observation.
- `github_contributors`: current `usable_signals` behavior is closer, but keep a separate `emitted_rows` count in logs/notes.

Also update the degradation note to avoid overloading `succeeded`:

```text
partial ingestion for github_commits: usable_success_ratio=0.000 (min=0.900),
attempted=152, usable=0, emitted_rows=152, exceptions=152
```

## Follow-Up Checks

Run these checks against the run artifacts or warehouse:

```sql
SELECT source_name, status, stale, failure_count, row_count, note
FROM mart.source_health
ORDER BY source_name;
```

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(last_commit_date) AS rows_with_last_commit,
  SUM(CASE WHEN commits_last_365d > 0 THEN 1 ELSE 0 END) AS rows_with_recent_commits
FROM raw.github_commits;
```

```sql
SELECT
  COUNT(*) AS rows,
  COUNT(top_contributor_share_all_time) AS usable_contributor_rows,
  SUM(CASE WHEN contributors_all_time > 0 THEN 1 ELSE 0 END) AS rows_with_contributors
FROM raw.github_contributors;
```

```sql
SELECT
  confidence,
  COUNT(*) AS packages,
  SUM(CASE WHEN flagged THEN 1 ELSE 0 END) AS flagged
FROM mart.package_scores
GROUP BY confidence
ORDER BY confidence;
```

## Suspected Operational Cause To Confirm

Because commits/issues/contributors all showed `exceptions=152`, this may be a common GitHub request failure rather than package-specific bad data. Check logs for:

- 403 / rate-limit / auth failures.
- 429 or retry exhaustion.
- DNS/network failures.
- Cache entries created under an unauthenticated scope and reused unexpectedly.
- GitHub token env mismatch between the raw Bruin run and the BigQuery smoke/export step.

## Publish Guidance

Do not publish a public leaderboard from a run where critical GitHub repo-derived sources show all-exception or near-all-exception telemetry. The current scoring behavior appears conservative, but the run should be labeled incomplete until source-health reports usable coverage.
