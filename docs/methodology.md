# Methodology

> The version displayed on the public site is always the `methodology_version` stamped on the current snapshot. This file is the human-readable narrative; the source of truth for weights and thresholds is [`../pipeline/config/scoring.yml`](../pipeline/config/scoring.yml).

## TL;DR

1. For each tracked package we compute an **importance score** (dependency reach + downloads + security exposure) and a **fragility score** (release recency, commit recency, release cadence decay, issue responsiveness, contributor concentration, Scorecard health).
2. `risk_score = round(importance × fragility / 100)`, both normalized to 0–100 **within ecosystem** (npm and PyPI ranked independently).
3. A package is **flagged** only when it clears *all* of: `risk_score ≥ 30`, `High` or `Critical` tier, medium or high confidence, ≥2 independent fragility signals each contributing ≥40, and top-quartile ecosystem importance.
4. Weekly cadence. Monday 00:00 UTC → Monday 00:00 UTC reporting window.
5. False positives cost more than false negatives — thresholds are deliberately conservative.

## v0.3.0 accuracy rules

- Missing dependency reach remains unknown. It is excluded from the importance denominator instead of being treated as zero reach.
- Live source-health failures block publication or reduce confidence; stale inputs are not allowed to silently suppress fragility.
- Rolling windows are anchored to the stamped `snapshot_week`, not the wall-clock time when a retry happens.
- Issue responsiveness measures first maintainer-like response (`OWNER`, `MEMBER`, or `COLLABORATOR`), not the first community or bot comment.

## What flagged does NOT mean

- It does **not** mean the package is abandoned.
- It does **not** imply anything about individual maintainers.
- It does **not** assert the package is unsafe to use.
- Flagged means: "public-data signals suggest this important package has measurably thinner maintenance margin than its ecosystem peers this week, and at least two independent pieces of evidence support that."

## Exclusions

Packages are excluded from ranking (but disclosed in coverage tables) when:

- `too_new` — less than 12 months old.
- `archived_deprecated` — intentionally ended.
- `unmappable` — no confident repository mapping (mapping-confidence <40 points).
- `stub_types` — `@types/*`, `*-stubs`, `types-*`.

## Sources

See [`sources.md`](sources.md). Commercial SCA products (Snyk, Sonatype, Endor, Socket) are **not** used as input data. OpenSSF Scorecard is a complementary signal, not a substitute.

## Versioning

Changing *any* weight or threshold in `scoring.yml` requires a `methodology_version` bump. Cross-week comparisons across different `methodology_version` values are labeled non-comparable on the site.
