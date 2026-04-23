---
title: "Pre-release repository audit (full codebase)"
date: "2026-04-23"
author: "Codex"
review_target: "main (repository-wide pre-release audit)"
changed_paths:
  - pipeline/assets/intermediate/int_snapshot.sql
  - pipeline/assets_bq/intermediate/int_snapshot.bq.sql
  - pipeline/config/scoring.yml
  - pipeline/assets/marts/mart_package_scores.sql
  - pipeline/assets/intermediate/int_fragility_inputs.sql
  - pipeline/assets/marts/export_public_bundle.py
  - docs/init.md
  - docs/methodology.md
  - web/src/pages/methodology.astro
  - pipeline/assets/raw/raw_github_repos.py
  - pipeline/assets/raw/raw_github_commits.py
  - pipeline/assets/raw/raw_github_contributors.py
  - pipeline/assets/raw/raw_scorecard.py
spectacles:
  used: false
  resolve_command: "spectacles resolve --paths pipeline/assets/raw/raw_github_contributors.py public-data/metadata.json --format md (failed: no .spectacles directory in repo)"
  validate_command: "spectacles validate-paths --paths <changed-files> --strict (blocked until Spectacles project is initialized)"
  domains: []
summary:
  recommendation: "request-changes"
  risk: "high"
---

## Executive summary
- Automated quality gates are mostly healthy after a fresh fixture run: `ruff`, `mypy`, `pytest`, `bruin validate`, fixture `bruin run`, and `web` lint/typecheck/build all pass.
- Two contract-level mismatches remain release-critical: methodology version stamping is stale (`v0.1.0` in snapshots) and public/docs copy still states the old `risk >= 75` threshold while scoring logic uses `risk >= 30`.
- Scoring constants are hardcoded in SQL instead of being sourced from `pipeline/config/scoring.yml`, which creates high drift risk and already contributed to versioning inconsistency.
- Multiple live raw assets suppress partial ingestion failures and still report source status `ok`, which can mask degraded data quality and overstate confidence.

## Spectacles alignment
- Spectacles CLI is installed, but this repository is not initialized as a Spectacles project (`No .spectacles directory found`), so MVC bundle resolution/validation could not be completed.
- Drift risks relative to repo constraints are clear even without Spectacles:
  - Root/pipeline AGENTS require config-driven scoring and methodology-version discipline.
  - Current SQL/docs/web outputs diverge from config and from each other on flagged criteria.
- Required follow-up once Spectacles is available:
  - `spectacles resolve --paths <affected files> --format md`
  - `spectacles validate-paths --paths <affected files> --strict`
  - `spectacles impact --paths <affected files>` (cross-cutting scoring/docs/export changes)

## Findings (triage-ready)
1. **Severity**: BLOCKER  
   **Category**: correctness  
   **Where**: `pipeline/assets/intermediate/int_snapshot.sql:53`, `pipeline/assets_bq/intermediate/int_snapshot.bq.sql:53`, `pipeline/config/scoring.yml:7`  
   **Evidence**: Snapshot stamping hardcodes `methodology_version` to `'v0.1.0'` while scoring config is already `v0.2.0`. This propagates stale version metadata to exported artefacts and weakens comparability/audit guarantees.  
   **Recommendation**: Derive snapshot methodology version from scoring config (single source), not string literals in SQL. Use a Jinja variable wired from config generation or a small seed/config table joined by `int.snapshot`.  
   **Test impact**: Add a test that runs fixture export and asserts `metadata.methodology_version == load_scoring_config().methodology_version`.  
   **Spectacles link**: N/A (Spectacles project not initialized).

2. **Severity**: BLOCKER  
   **Category**: docs  
   **Where**: `docs/init.md:758-773`, `docs/methodology.md:9`, `web/src/pages/methodology.astro:72`, `pipeline/assets/marts/export_public_bundle.py:413`  
   **Evidence**: Public and spec-facing surfaces still describe old thresholds (`risk >= 75`, old severity bands), while runtime scoring SQL/config uses `risk >= 30` and `High` starting at 30. This creates externally visible methodology contradictions for judges/users.  
   **Recommendation**: Pick one authoritative ruleset and align all surfaces immediately. If v0.2.0 is intended, update `docs/init.md`, `docs/methodology.md`, web methodology page, and zero-flagged fallback text in export. If not intended, revert SQL/config to v1 thresholds.  
   **Test impact**: Add a consistency test that validates exported narrative strings and methodology docs against `scoring.yml` thresholds.  
   **Spectacles link**: N/A (Spectacles project not initialized).

3. **Severity**: MAJOR  
   **Category**: maintainability  
   **Where**: `pipeline/assets/intermediate/int_fragility_inputs.sql:173-214`, `pipeline/assets/marts/mart_package_scores.sql:225-307` (+ BigQuery siblings)  
   **Evidence**: Weights/thresholds are hardcoded throughout SQL (`0.60/0.25/0.15`, signal threshold 40, risk floor 30, etc.) even though repo policy says these must come from `pipeline/config/scoring.yml`. This is high-risk drift territory and already coexists with version mismatch.  
   **Recommendation**: Make SQL constants config-driven (Jinja-injected vars or generated constants CTE) and remove duplicated literals in DuckDB/BQ SQL files.  
   **Test impact**: Add contract tests that compare SQL-applied thresholds/weights to loaded scoring config values.  
   **Spectacles link**: N/A (Spectacles project not initialized).

4. **Severity**: MAJOR  
   **Category**: observability  
   **Where**: `pipeline/assets/raw/raw_github_repos.py:106-123`, `pipeline/assets/raw/raw_github_commits.py:111-129`, `pipeline/assets/raw/raw_github_contributors.py:199-229`, `pipeline/assets/raw/raw_scorecard.py:98-119`  
   **Evidence**: Per-repo exceptions are dropped (`return_exceptions=True` + `continue`) and the source tracker only marks failure when *all* rows are missing. Partial outages therefore report status `ok`, masking ingestion degradation and potentially inflating downstream confidence.  
   **Recommendation**: Track per-source failure counts and call `t.mark_degraded(...)` when any subset fails (or when success ratio drops below threshold); include coverage ratio in `note`.  
   **Test impact**: Add raw-asset tests with injected partial exceptions and assert `raw.source_health` status is `degraded`, not `ok`.  
   **Spectacles link**: N/A (Spectacles project not initialized).

## Suggested todos
- [ ] Replace hardcoded `methodology_version` in both snapshot SQL assets with config-driven value and verify exported `public-data/metadata.json` reflects `scoring.yml`.
- [ ] Reconcile flagged-threshold definitions across scoring SQL, docs (`docs/init.md`, `docs/methodology.md`), web methodology page, and weekly zero-flagged summary copy.
- [ ] Refactor scoring SQL (DuckDB + BigQuery) to source weights/thresholds from `pipeline/config/scoring.yml` rather than duplicated literals.
- [ ] Add partial-failure degradation handling to GitHub/Scorecard raw assets and enforce via tests on `raw.source_health` status behavior.

## Nits
- `pytest` initially failed before a fresh fixture run because `public-data/metadata.json` had stale extra fields from prior local output; consider documenting/running fixture export before schema tests in local dev docs.

## Verification checklist
- [x] `uv run ruff format --check .`
- [x] `uv run ruff check .`
- [x] `uv run mypy pipeline`
- [x] `uv run pytest`
- [x] `bruin validate pipeline/pipeline.yml`
- [x] `bruin run --workers=1 --full-refresh -e fixture pipeline/pipeline.yml`
- [x] `cd web && pnpm lint && pnpm typecheck && pnpm build`
- [ ] `spectacles validate-paths --paths <affected files> --strict` (blocked until Spectacles init)
- [ ] `spectacles impact --paths <affected files>` (recommended after Spectacles init)

## Codify candidates (optional)
1. **Lesson**: “Methodology/version metadata must be derived from scoring config, never manually duplicated in SQL.”  
   **Suggested node type**: principle  
   **Context files**: `pipeline/config/scoring.yml`, `pipeline/assets/intermediate/int_snapshot.sql`, `pipeline/assets_bq/intermediate/int_snapshot.bq.sql`
2. **Lesson**: “Public methodology copy must be generated or linted against scoring config thresholds to prevent contradictory claims.”  
   **Suggested node type**: contract  
   **Context files**: `docs/init.md`, `docs/methodology.md`, `web/src/pages/methodology.astro`, `pipeline/assets/marts/export_public_bundle.py`
