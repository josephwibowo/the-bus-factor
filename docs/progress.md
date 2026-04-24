# The Bus Factor — Progress Tracker

Single-source view of where the project stands across all three build cycles. Every plan file in [`.cursor/plans/`](../.cursor/plans/) should point back here for status.

Authoritative spec: [`docs/init.md`](init.md).
Baseline rules: [`AGENTS.md`](../AGENTS.md), [`CLAUDE.md`](../CLAUDE.md).

## Status banner

| Cycle | Scope | Status |
| --- | --- | --- |
| 1 | Fixture pipeline + full Astro site, zero API keys | **Complete** |
| 2 | Live BigQuery-backed ingestion for npm, PyPI, deps.dev, GitHub, OSV, Scorecard | **Complete** |
| 3 | Scoring tune, Bruin proof, AI gallery, launch hardening | **Phase A complete** |

## Deadline

**2026-06-01 12:00 UTC** — Bruin Data Engineering Project Competition submission. Slack post + LinkedIn post + GitHub repo + hosted demo + submission metadata must all be live before this boundary.

## Cut-first priorities (from [`AGENTS.md`](../AGENTS.md))

Under scope pressure, cut **package-count breadth first** (e.g. ship at 250 instead of 500). Never cut:

- Quality checks
- Fixture reproducibility
- README clarity
- Competitive positioning
- AI-analyst screenshots
- Launch materials

## Cycle 1 — Fixture pipeline (complete)

Plan: [`cycle1_fixture_pipeline_plan_c7a41f9b`](../.cursor/plans/cycle1_fixture_pipeline_plan_c7a41f9b.plan.md).

| Item | Status |
| --- | --- |
| Fixture seeds + `known_states.md` (flag / not-flag / excluded per ecosystem) | Done |
| Seed assets loading CSVs into DuckDB | Done |
| Python raw assets with fixture branches (10 sources) | Done |
| `pipeline/lib/` (config, scoring mirror, schemas, snapshot clock) | Done |
| DuckDB staging SQL with built-in checks | Done |
| Intermediate SQL (repo mapping, importance, fragility, eligibility) | Done |
| Eight required marts with Bruin column descriptions | Done |
| Custom SQL checks (no-dup keys, no flagged+low, fragility-evidence count, score bounds, known-state agreement) | Done |
| `export_public_bundle.py` → `public-data/*.json` + JSON Schemas | Done |
| `generate_weekly_card.py` → `reports/cards/*.png` | Done |
| Full Astro site (`index`, `package/[slug]`, `weekly`, `analysis`, `methodology`, `positioning`) | Done |
| Placeholder analysis gallery screenshots | Done |
| Pytest coverage (config, snapshot, schemas, scoring, public-data round-trip) | Done |
| Tightened CI (`ci.yml`) | Done |
| README with Bruin proof table, SCA comparison, architecture diagram | Done |

## Cycle 2 — Live ingestion + BigQuery (complete)

Plan: [`cycle2_live_ingestion_plan_74ddacb9`](../.cursor/plans/cycle2_live_ingestion_plan_74ddacb9.plan.md).

GCP target: project `bus-factor-494119` (under josephwibowo@gmail.com). Local dev uses ADC. CI will use a service account JSON in GitHub Secrets (`GCP_SERVICE_ACCOUNT_JSON`, `GCP_PROJECT_ID`).

| # | Item | Acceptance |
| --- | --- | --- |
| 1 | `docs/progress.md` tracker (this file) | Linked from README Status |
| 2 | `.bruin.yml.example` ADC + SA wiring | `bruin validate` green; docs/sources.md documents both paths |
| 3 | `pipeline/lib/http.py` (httpx + tenacity + disk cache) | `tests/test_http_cache.py` green; Retry-After honored; tokens injected |
| 4 | `pipeline/lib/universe.py` (top-N npm + PyPI with 95%-overlap gate) | `tests/test_universe.py` green; cached under `.cache/` |
| 5 | `pipeline/lib/sources.py` + `raw.source_health` + `stg.source_health` asset | `mart.source_health` lists all 10 sources from buffered rows |
| 6 | Live branch: `raw_npm_registry`, `raw_pypi_registry`, `raw_osv` | 50-package smoke returns ≥45 rows each |
| 7 | Live branch: `raw_github_repos`, `commits`, `releases`, `issues`, `contributors` | 50-repo smoke returns ≥45 rows each; 202 polling works for contributors |
| 8 | Live branch: `raw_deps_dev`, `raw_scorecard` (BQ public datasets) | Queries respect `maximum_bytes_billed` |
| 9 | `*.bq.sql` siblings + partition/cluster frontmatter | BigQuery smoke renders and executes every sibling into `bf_smoke_*` datasets |
| 10 | `tests/test_sql_parity.py` | Every `*.sql` has a matching `*.bq.sql`; column lists equal |
| 11 | Live-smoke CI job (manual dispatch, limit=5) | Runs real-source Bruin smoke, then `scripts/run_bigquery_smoke.py`; doesn't run on PRs |
| 12 | `.github/workflows/weekly.yml` hardening (concurrency, `BQ_MAX_BYTES_BILLED`, stale_fallback) | Dispatch succeeds; stale_fallback path tested via forced failure |
| 13 | First real 500/500 live snapshot committed | Done — `public-data/metadata.json` shows `source_mode: live`; 782 packages scored (2026-W17) |
| 14 | `docs/sources.md` + README "Run live sources" walkthrough | Matches verified command output |

Validation note: a 5 npm + 5 PyPI `local_live_bq` smoke passed on
2026-04-22 with all 51 assets, 303 quality checks, fresh `public-data/`,
and regenerated weekly cards. A follow-up BigQuery SQL smoke passed on
2026-04-22 against project `bus-factor-494119`, populating
`bf_smoke_raw`, `bf_smoke_stg`, `bf_smoke_int`, and `bf_smoke_mart`;
`bf_smoke_mart.source_health` had 10 healthy sources and mart custom
checks were zero-failure. This validates Cycle 2 at smoke scale, but it
is **not** the first real 500/500 snapshot and does not populate a GCS
bucket.

## Cycle 3 — Polish + Bruin proof + AI gallery + launch

### Phase A — complete (2026-04-22)

Plan: [`cycle3_phase_a_604a6ad8`](../.cursor/plans/cycle3_phase_a_604a6ad8.plan.md).

| Item | Status |
| --- | --- |
| Refresh npm universe from deps.dev BigQuery (real `dependent_count`) | Done |
| Refresh PyPI universe (HTTP) | Done |
| Full 500/500 `local_live_bq` run — 782 packages scored | Done |
| BigQuery SQL smoke against `bus-factor-494119` (`bf_live_*` datasets) | Done |
| Tone / false-positive review of live leaderboard | Done |
| Scoring weights tuned; `methodology_version` bumped to v0.2.0 | Done |
| `mart_package_scores` SQL updated to v0.2.0 thresholds (risk_min: 30, severity tiers) | Done |
| `known_states.csv` / `known_states.md` realigned to v0.2.0 (4 packages flipped to flagged) | Done |
| Live `public-data/` bundle committed | Done |

### Phase B — complete for the repo deliverable (2026-04-23)

Completed Cycle 3 work:

- Reviewed and kept the useful `bruin ai enhance` metadata/check updates on the mart layer
- Regenerated the BigQuery sidecar tree and fixed parity drift so the BQ SQL checks pass again
- Added a checked-in lineage proof asset for the README
- Added a checked-in custom-check failure proof asset from an intentional local mismatch
- Replaced the eight analysis placeholders with grounded local analysis captures under `analysis/screenshots/` and `web/public/screenshots/`
- Updated `pipeline/fixtures/analysis_gallery.csv` and the exported `public-data/analysis.json` metadata to match the real captures
- Finalized `launch/slack-post.md`, `launch/linkedin-post.md`, and `launch/submission-checklist.md`
- Rebuilt the weekly card and refreshed the hosted-analysis copy so the site and repo tell the same story

### Phase C — production accuracy hardening (2026-04-23)

- Weekly production now rebuilds `public-data/` from BigQuery mart outputs after the DuckDB raw-ingestion stage
- `methodology_version` bumped to v0.3.0 for nullable dependency reach, source-health confidence gates, snapshot-anchored extraction windows, and maintainer-like issue responsiveness
- BigQuery custom checks and export validation now block publishing unhealthy live source snapshots

Remaining blocker before a fully live launch baseline:

- A fresh full-breadth live export is still pending. Both the original 500/500 pass and a reduced-breadth retry were blocked by GitHub contributor-stats retries on `raw.github_contributors`, so the committed `public-data/` bundle remains fixture-backed for now.

## Reference

- Spec: [`docs/init.md`](init.md) — authoritative, conflicts resolved here
- Methodology: [`docs/methodology.md`](methodology.md)
- Sources: [`docs/sources.md`](sources.md)
- Scoring weights: [`pipeline/config/scoring.yml`](../pipeline/config/scoring.yml) — bump `methodology_version` on any change
- Competitive positioning: [`launch/market-positioning.md`](../launch/market-positioning.md)
